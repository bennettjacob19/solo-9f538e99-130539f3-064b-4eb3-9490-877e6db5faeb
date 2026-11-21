"""Optional on-disk persistence of drain rounds.

The cluster is the source of truth for the cordon and for which pods exist.
What the app must not lose across a restart is the *decision record*: which
node was cordoned, which round it belongs to, which pods were already offered
for eviction, and — when a node is drained a second time — which of several
rounds for the same node is the current one.

Set ``DRAIN_STATE_FILE`` to enable persistence.  Writes are atomic (temp file +
replace) and may be made strict for the handover of a node from one round to
the next: when strict, a write failure is reported so the caller can keep the
old round current.

State files written by older versions (no sequence numbers, no archive flags,
no active-round map) are still accepted.  On load they are normalised so that
exactly one round per node is current and a restart cannot mistake an older
same-second round for the current one.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .models import DrainPhase, PodBudget, PodState, PodStatus, Round


class StoreError(RuntimeError):
    """A strict state write failed; the caller must treat the action as failed."""


def round_to_dict(rnd: Round) -> dict:
    return {
        "node": rnd.node,
        "round_id": rnd.round_id,
        "seq": rnd.seq,
        "phase": rnd.phase.value,
        "offline": rnd.offline,
        "offline_reason": rnd.offline_reason,
        "offline_at": rnd.offline_at,
        "ever_offline": rnd.ever_offline,
        "offline_confirmed_at": rnd.offline_confirmed_at,
        "message": rnd.message,
        "resumed_from": rnd.resumed_from,
        "created_at": rnd.created_at,
        "updated_at": rnd.updated_at,
        "archived": rnd.archived,
        "superseded_by": rnd.superseded_by,
        "archived_at": rnd.archived_at,
        "attempted_names": sorted(rnd.attempted_names),
        "pods": [
            {
                "name": pod.name,
                "namespace": pod.namespace,
                "uid": pod.uid,
                "node": pod.node,
                "state": pod.state.value,
                "message": pod.message,
                "exempt_reason": pod.exempt_reason,
                "budgets": [budget.__dict__ for budget in pod.budgets],
            }
            for pod in rnd.pods.values()
        ],
    }


def round_from_dict(data: dict) -> Round:
    pods: dict[str, PodStatus] = {}
    for item in data.get("pods", []):
        pods[item["uid"]] = PodStatus(
            name=item["name"],
            namespace=item["namespace"],
            uid=item["uid"],
            node=item["node"],
            state=PodState(item["state"]),
            message=item.get("message", ""),
            exempt_reason=item.get("exempt_reason"),
            budgets=[PodBudget(**budget) for budget in item.get("budgets", [])],
        )
    offline = bool(data.get("offline", False))
    return Round(
        node=data["node"],
        round_id=data["round_id"],
        pods=pods,
        attempted_names=set(data.get("attempted_names", [])),
        phase=DrainPhase(data["phase"]),
        offline=offline,
        offline_reason=data.get("offline_reason", ""),
        offline_at=data.get("offline_at"),
        ever_offline=bool(data.get("ever_offline", offline)),
        offline_confirmed_at=data.get("offline_confirmed_at"),
        message=data.get("message", ""),
        resumed_from=data.get("resumed_from"),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
        seq=int(data.get("seq") or 0),
        archived=bool(data.get("archived", False)),
        superseded_by=data.get("superseded_by"),
        archived_at=data.get("archived_at"),
    )


def _assign_missing_sequences(rounds: list[Round]) -> int:
    """Give legacy rounds a deterministic creation order.

    ``created_at`` only has second precision.  Legacy files kept rounds in
    insertion order (oldest first), so that order breaks ties.  Rounds written
    by this version already carry an explicit ``seq`` which is never reordered.
    """
    highest = max((rnd.seq for rnd in rounds if rnd.seq > 0), default=0)
    if any(rnd.seq <= 0 for rnd in rounds):
        # Legacy rounds without a sequence: derive one from creation time and
        # file order, keeping already sequenced rounds where they are.
        ordered = sorted(
            ((index, rnd) for index, rnd in enumerate(rounds) if rnd.seq <= 0),
            key=lambda pair: (pair[1].created_at, pair[0]),
        )
        for _, rnd in ordered:
            highest += 1
            rnd.seq = highest
    return highest + 1


def _normalise_current(rounds: dict[str, Round], active: dict[str, str]) -> dict[str, str]:
    """Make sure exactly one round per node is current, including for v1 files.

    An old version refused a second round on a node unless the previous one was
    complete, so the newest non-complete round is the current one; if every
    round is complete the newest is the most recent drain.  Anything else is
    marked archived so opening its link can never revive it.
    """
    by_node: dict[str, list[Round]] = {}
    for rnd in rounds.values():
        by_node.setdefault(rnd.node, []).append(rnd)

    result: dict[str, str] = {}
    for node, items in by_node.items():
        items.sort(key=lambda rnd: (rnd.created_at, rnd.seq))
        candidate = active.get(node)
        if candidate not in {rnd.round_id for rnd in items}:
            unfinished = [rnd for rnd in items if rnd.phase is not DrainPhase.COMPLETE]
            candidate = (unfinished or items)[-1].round_id
        result[node] = candidate
        current = rounds[candidate]
        for rnd in items:
            if rnd.round_id == candidate:
                rnd.archived = False
                rnd.superseded_by = None
                rnd.archived_at = None
            elif not rnd.archived:
                # A v1 file has no archive marker; a round that is no longer
                # current was handed over to the round that followed it.
                rnd.archived = True
                rnd.superseded_by = candidate
                rnd.archived_at = rnd.updated_at or rnd.created_at or None
    # Drop active entries for nodes that no longer have rounds.
    return {node: rid for node, rid in result.items() if rid in rounds}


class RoundStore:
    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        # Populated by load() for the manager.  They are also what makes a
        # restart remember which same-second round is current.
        self.active_rounds: dict[str, str] = {}
        self.next_seq: int = 1

    @classmethod
    def from_env(cls) -> "RoundStore | None":
        path = os.environ.get("DRAIN_STATE_FILE")
        return cls(path) if path else None

    def load(self) -> dict[str, Round]:
        self.active_rounds = {}
        self.next_seq = 1
        try:
            raw = self.path.read_text()
        except FileNotFoundError:
            return {}
        except OSError:
            return {}
        try:
            data = json.loads(raw)
        except ValueError:
            return {}
        if not isinstance(data, dict):
            return {}
        rounds: dict[str, Round] = {}
        for item in data.get("rounds", []):
            try:
                rnd = round_from_dict(item)
            except (KeyError, TypeError, ValueError):
                continue
            # A round marked busy was interrupted mid-request; it can never
            # still be busy in this new process.
            rnd.busy = False
            rounds[rnd.round_id] = rnd

        self.next_seq = _assign_missing_sequences(list(rounds.values()))
        try:
            stored_next = int(data.get("next_seq") or 0)
        except (TypeError, ValueError):
            stored_next = 0
        # A burned sequence (a failed strict write) may have advanced the
        # counter past max(seq)+1; never reuse it.
        self.next_seq = max(self.next_seq, stored_next)
        stored_active = data.get("active_rounds")
        active = stored_active if isinstance(stored_active, dict) else {}
        self.active_rounds = _normalise_current(rounds, active)
        return rounds

    def save(
        self,
        rounds: dict[str, Round],
        active_rounds: dict[str, str] | None = None,
        strict: bool = False,
    ) -> None:
        """Write the state file atomically.

        ``strict`` is used when a new round must only become current after the
        record is safely on disk.  Without it, persistence stays best effort:
        losing the file is inconvenient, but the cluster remains authoritative
        and an in-flight drain must not fail because of a full disk.
        """
        payload = {
            "version": 2,
            "next_seq": self.next_seq,
            "active_rounds": dict(active_rounds or {}),
            "rounds": [round_to_dict(r) for r in rounds.values()],
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=".rounds-")
            try:
                with os.fdopen(fd, "w") as handle:
                    json.dump(payload, handle, ensure_ascii=False, indent=1)
                os.replace(tmp, self.path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except (OSError, TypeError, ValueError) as exc:
            if strict:
                raise StoreError(f"写入轮次状态失败：{exc}") from exc
            # Persistence is a convenience; the cluster remains authoritative.
            return

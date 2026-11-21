"""Drain orchestration.

Rules, in order:

1. Cordon the node *before* listing anything.  Only once the node reports
   unschedulable are its pods read and offered for eviction.
2. DaemonSet, static/mirror and finished pods are never evicted.
3. Every eviction uses the ``policy/v1`` Eviction subresource with the pod UID
   as a precondition.  A same-named replacement is a different object: if it
   appears mid-round it is not evicted on the old decision; it is marked as
   deferred and picked up by the next round.
4. A PodDisruptionBudget refusal from the API server is not fatal.  The pod is
   flagged as blocked and the round continues with the rest.  Budget
   accounting lives in the API server, so two nodes drained in parallel cannot
   jointly exceed a shared budget.
5. Interrupting keeps the cordon and every eviction already issued.  Resuming
   re-reads the cluster instead of trusting the snapshot.
6. A node is reported as safe to power off only after a fresh check shows it
   is still unschedulable and carries no ordinary pods.
7. A node that was already confirmed offline may be drained again.  Starting
   the new round hands the node over from the old one: the old round is
   archived (its offline conclusion and pod rows are frozen), its page becomes
   read-only, and only the new round may talk to the cluster.  The handover is
   committed only after the new round has a snapshot and — when state
   persistence is enabled — is safely on disk; a failure leaves the old round
   current.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone

from .kube import (
    EvictionBlocked,
    EvictionConflict,
    KubeClient,
    KubeError,
    pods_matching_budgets,
)
from .models import DrainPhase, PodState, PodStatus, Round
from .store import RoundStore, StoreError

ARCHIVED_MESSAGE = "该轮次已交接给更新的轮次，本页仅供归档查看"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DrainError(RuntimeError):
    pass


def _archive_snapshot(rnd: Round) -> dict:
    """Everything a rollback must restore after a failed strict write."""
    return {
        "offline": rnd.offline,
        "offline_reason": rnd.offline_reason,
        "offline_at": rnd.offline_at,
        "ever_offline": rnd.ever_offline,
        "offline_confirmed_at": rnd.offline_confirmed_at,
        "phase": rnd.phase,
        "message": rnd.message,
        "updated_at": rnd.updated_at,
        "pods": {
            uid: (
                status.state,
                status.message,
                status.exempt_reason,
                list(status.budgets),
            )
            for uid, status in rnd.pods.items()
        },
    }


def _restore_archive(rnd: Round, snapshot: dict) -> None:
    rnd.offline = snapshot["offline"]
    rnd.offline_reason = snapshot["offline_reason"]
    rnd.offline_at = snapshot["offline_at"]
    rnd.ever_offline = snapshot["ever_offline"]
    rnd.offline_confirmed_at = snapshot["offline_confirmed_at"]
    rnd.phase = snapshot["phase"]
    rnd.message = snapshot["message"]
    rnd.updated_at = snapshot["updated_at"]
    for status in rnd.pods.values():
        saved = snapshot["pods"].get(status.uid)
        if saved is None:
            continue
        status.state, status.message, status.exempt_reason, status.budgets = saved


def _mark_archived(rnd: Round, superseded_by: str, archived_at: str) -> None:
    """Freeze a handed-over round, keeping its last confirmed conclusion.

    If an earlier reflection had already turned the conclusion back into
    "尚有普通 Pod" (a replacement appeared before the handover), the archive
    still reports the confirmation the operator acted on.  Pod rows keep
    whatever they held at this moment; nothing after the handover may change
    them.
    """
    if rnd.ever_offline:
        rnd.offline = True
        rnd.offline_reason = "已确认：节点不可调度，且已无普通 Pod。"
        rnd.phase = DrainPhase.COMPLETE
    rnd.archived = True
    rnd.superseded_by = superseded_by
    rnd.archived_at = archived_at
    rnd.busy = False


class DrainManager:
    """Owns the round table and the calls to the cluster.

    A single re-entrant lock guards the round dicts.  Network I/O happens
    outside that lock; templates use ``busy`` to explain a slow request.

    A second, per-node lock serialises the operations that may change the
    cluster (start/resume/run).  Read-only reflection (recheck/observe) does
    not take it, so it can be in flight while a handover happens; such a late
    read is fenced by the ``archived`` flag and never rewrites the archive.
    """

    def __init__(self, kube: KubeClient, store: RoundStore | None = None):
        self.kube = kube
        self.store = store
        self._rounds: dict[str, Round] = store.load() if store else {}
        # Node -> round id that is current.  Persisted, because two rounds for
        # the same node can share ``created_at`` to the second, and only the
        # explicit mapping reliably survives a restart.
        self._active: dict[str, str] = dict(store.active_rounds) if store else {}
        self._next_seq: int = store.next_seq if store else 1
        self._lock = threading.RLock()
        self._node_locks: dict[str, threading.RLock] = {}

    def _persist(self, strict: bool = False) -> None:
        """Save the round table.  Must be called with the lock held."""
        if self.store is None:
            return
        self.store.next_seq = self._next_seq
        self.store.save(self._rounds, self._active, strict=strict)

    def _node_lock(self, node: str) -> threading.RLock:
        with self._lock:
            lock = self._node_locks.get(node)
            if lock is None:
                lock = threading.RLock()
                self._node_locks[node] = lock
            return lock

    def _current_round_locked(self, node: str) -> Round | None:
        """The round that currently owns ``node``.  Lock must be held."""
        round_id = self._active.get(node)
        rnd = self._rounds.get(round_id) if round_id else None
        if rnd is None:
            candidates = [r for r in self._rounds.values() if r.node == node]
            rnd = max(candidates, key=lambda r: (r.created_at, r.seq), default=None)
            if rnd is not None:
                self._active[node] = rnd.round_id
        return rnd

    @staticmethod
    def _can_be_handed_over(rnd: Round) -> bool:
        """A round may only be replaced once its node was confirmed offline.

        ``ever_offline`` is used instead of the live ``offline`` flag: opening
        the page of a completed round can make it look active again when a
        replacement pod shows up, and that must not block a new drain forever.
        """
        return rnd.ever_offline or rnd.phase is DrainPhase.COMPLETE

    # -- queries -----------------------------------------------------------
    def list_rounds(self) -> list[Round]:
        with self._lock:
            return sorted(
                self._rounds.values(),
                key=lambda r: (r.created_at, r.seq),
                reverse=True,
            )

    def get(self, round_id: str) -> Round | None:
        with self._lock:
            return self._rounds.get(round_id)

    def current_round_for_node(self, node: str) -> Round | None:
        with self._lock:
            return self._current_round_locked(node)

    def nodes(self):
        return self.kube.list_nodes()

    # -- round lifecycle ---------------------------------------------------
    def start(self, node: str) -> Round:
        """Cordon first, then snapshot; hand the node over from an old round.

        Nothing about the existing round changes until the new round has both
        a pod snapshot and — when persistence is enabled — a successful disk
        write.  Only then is the old round archived and the new one made
        current.  Any failure leaves the old round current; the cordon, once
        issued, is deliberately kept (the next attempt simply re-cordons).
        """
        with self._node_lock(node):
            with self._lock:
                current = self._current_round_locked(node)
                if current is not None and not self._can_be_handed_over(current):
                    raise DrainError(f"节点 {node} 已有进行中的排空轮次")

            # Cordon before listing: the pod list shown to the operator must
            # come from a node that can no longer receive new pods.
            self.kube.cordon(node)
            node_info = self.kube.get_node(node)
            if node_info is None:
                raise DrainError(f"节点 {node} 不存在")
            if not node_info.unschedulable:
                raise DrainError(f"节点 {node} 封锁后仍报告可调度，已停止。")

            with self._lock:
                # Re-read under the lock so a concurrent start for the same
                # node (it would wait on the node lock, so this is only a
                # sanity check) cannot race past a handover.
                current = self._current_round_locked(node)
                if current is not None and not self._can_be_handed_over(current):
                    raise DrainError(f"节点 {node} 已有进行中的排空轮次")
                rnd = Round(
                    node=node,
                    round_id=uuid.uuid4().hex[:12],
                    created_at=_now(),
                    updated_at=_now(),
                    message="节点已封锁，下面列出本轮要驱逐的 Pod。",
                    seq=self._next_seq,
                )

            # Snapshot the pods this round covers.  If this read fails, no new
            # round is recorded at all.
            self.refresh(rnd)

            with self._lock:
                current = self._current_round_locked(node)
                if current is not None and not self._can_be_handed_over(current):
                    raise DrainError(f"节点 {node} 已有进行中的排空轮次")
                self._next_seq += 1

                # Snapshot what changes so a failed strict write can be rolled
                # back exactly; the cordon itself is deliberately kept.
                prev_active = self._active.get(node)
                changed = [r for r in self._rounds.values() if r.node == node and not r.archived]
                previous = [
                    (old, old.archived, old.superseded_by, old.archived_at, _archive_snapshot(old))
                    for old in changed
                ]
                archived_at = _now()
                for old in changed:
                    _mark_archived(old, rnd.round_id, archived_at)
                self._rounds[rnd.round_id] = rnd
                self._active[node] = rnd.round_id
                try:
                    self._persist(strict=True)
                except StoreError as exc:
                    # Handover did not complete: the old round stays current
                    # and keeps every conclusion it had.
                    self._rounds.pop(rnd.round_id, None)
                    for old, archived, superseded_by, old_archived_at, snapshot in previous:
                        old.archived = archived
                        old.superseded_by = superseded_by
                        old.archived_at = old_archived_at
                        _restore_archive(old, snapshot)
                    if prev_active is None:
                        self._active.pop(node, None)
                    else:
                        self._active[node] = prev_active
                    raise DrainError(str(exc)) from exc
        return rnd

    def resume(self, round_id: str) -> Round:
        """Re-read the cluster and re-arm everything that is still retryable.

        Used both for "继续排空" after an interruption and for retrying after
        a budget recovers.  The cordon stays.  Pods that left stay gone; pods
        that appeared (including same-named replacements) are picked up as
        pending for this new pass.  An archived round is read-only.
        """
        rnd = self._require(round_id)
        with self._node_lock(rnd.node):
            self._refuse_archived(rnd)
            with self._lock:
                if rnd.busy:
                    raise DrainError("本轮正在处理中，请稍候")
                rnd.busy = True
            try:
                node = self.kube.get_node(rnd.node)
                if node is None:
                    raise DrainError(f"节点 {rnd.node} 不存在")
                if not node.unschedulable:
                    # Nobody may uncordon a node out from under a drain and
                    # have us keep evicting onto it.
                    self.kube.cordon(rnd.node)
                with self._lock:
                    if rnd.archived:
                        return rnd
                    rnd.phase = DrainPhase.RUNNING
                    rnd.resumed_from = rnd.updated_at
                    rnd.attempted_names.clear()
                    for status in rnd.pods.values():
                        if status.state in (PodState.BLOCKED, PodState.DEFERRED):
                            status.state = PodState.PENDING
                            status.message = "已重新读取集群，本轮重新尝试。"
                    rnd.message = "已重新读取集群，继续处理本轮剩余 Pod。"
                    rnd.updated_at = _now()
                    self._persist()
                self.refresh(rnd)
            finally:
                with self._lock:
                    rnd.busy = False
        return rnd

    def interrupt(self, round_id: str) -> Round:
        """Stop issuing evictions; keep the cordon and what already left."""
        rnd = self._require(round_id)
        with self._lock:
            self._refuse_archived(rnd)
            if rnd.phase is not DrainPhase.RUNNING:
                return rnd
            rnd.phase = DrainPhase.INTERRUPTED
            rnd.message = (
                "已中断：封锁保留，已发生的驱逐保留。"
                "点“继续排空”会重新读取集群再继续。"
            )
            rnd.updated_at = _now()
            self._persist()
        return rnd

    # -- the main loop -----------------------------------------------------
    def run(self, round_id: str) -> Round:
        """Offer every pending ordinary pod once, then verify the node.

        Blocked and deferred pods never stop the pass: the loop moves on and
        leaves them for a later resume.  An accepted eviction leaves the pod
        in ``TERMINATING`` ("待退出") until a re-read shows the object gone.
        """
        rnd = self._require(round_id)
        with self._node_lock(rnd.node):
            self._refuse_archived(rnd)
            with self._lock:
                if rnd.busy:
                    raise DrainError("本轮正在处理中，请稍候")
                if rnd.phase is not DrainPhase.RUNNING:
                    raise DrainError("本轮已中断，请先点击“继续排空”重新读取集群")
                rnd.busy = True
            try:
                self.refresh(rnd)
                for status in list(rnd.evictable):
                    with self._lock:
                        if rnd.archived or rnd.phase is not DrainPhase.RUNNING:
                            break
                        if status.state is not PodState.PENDING:
                            continue
                    self._evict_one(rnd, status)
                self.refresh(rnd)
                self._settle(rnd)
                self._verify_offline(rnd)
            finally:
                with self._lock:
                    rnd.busy = False
                    if not rnd.archived:
                        rnd.updated_at = _now()
                        self._persist()
        return rnd

    def _evict_one(self, rnd: Round, status: PodStatus) -> None:
        """Evict a single pod and record the outcome on its status row."""
        live = self.kube.get_pod(status.namespace, status.name)
        with self._lock:
            if rnd.archived:
                return
            if live is None:
                status.state = PodState.GONE
                status.message = "Pod 已离开节点。"
                return
            if live.uid != status.uid:
                # Same name, different object.  The decision to evict was made
                # for the old UID and never carries over to the replacement.
                # The old row is closed out; refresh() will list the new object
                # separately (as DEFERRED when it landed on this node).
                rnd.attempted_names.add(status.key)
                status.state = PodState.GONE
                status.message = (
                    f"原 Pod 已被同名新对象替换（新 UID {live.uid[:8]}，"
                    f"位于 {live.node}）。"
                )
                if not rnd.archived:
                    rnd.updated_at = _now()
                return
            rnd.attempted_names.add(status.key)
            status.state = PodState.TERMINATING
            status.message = "驱逐请求已发出，等待 Pod 退出。"
            if not rnd.archived:
                rnd.updated_at = _now()
        try:
            self.kube.evict(live)
        except EvictionBlocked as exc:
            with self._lock:
                status.state = PodState.BLOCKED
                status.message = f"受预算阻挡：{exc}"
        except EvictionConflict as exc:
            with self._lock:
                status.state = PodState.DEFERRED
                status.message = f"未驱逐：{exc}"
        except KubeError as exc:
            with self._lock:
                status.state = PodState.DEFERRED
                status.message = f"驱逐失败：{exc}"
        else:
            with self._lock:
                status.state = PodState.TERMINATING
                status.message = "已获准驱逐，等待 Pod 退出。"
        with self._lock:
            if not rnd.archived:
                rnd.updated_at = _now()

    def _settle(self, rnd: Round) -> None:
        """Write the round-level message after a pass over the pods."""
        with self._lock:
            if rnd.archived or rnd.phase is not DrainPhase.RUNNING:
                return
            counts = rnd.counts
            if counts["blocked"]:
                rnd.message = (
                    f"{counts['blocked']} 个 Pod 受 PodDisruptionBudget 阻挡，"
                    "本轮已继续处理其他 Pod；预算恢复后可点“继续排空”。"
                )
            elif counts["waiting"]:
                rnd.message = (
                    f"{counts['waiting']} 个 Pod 已获准驱逐但尚未退出（待退出）；"
                    "确认它们离开后点“重新检查”。"
                )
            elif counts["deferred"]:
                rnd.message = "有 Pod 留待续排，可点“继续排空”重新读取集群。"
            else:
                rnd.message = "本轮已无待处理的普通 Pod。"
            rnd.updated_at = _now()

    def recheck(self, round_id: str) -> Round:
        """Re-read the cluster and re-evaluate whether the node can go down.

        This never issues an eviction.  It is the action that moves a pod from
        "待退出" to "已离开" and the node to "可下线" once the cluster agrees.
        A recheck that was in flight while the round was handed over returns
        without touching the archive: the record keeps its archived content.
        """
        rnd = self._require(round_id)
        with self._lock:
            self._refuse_archived(rnd)
            if rnd.busy:
                raise DrainError("本轮正在处理中，请稍候")
            rnd.busy = True
        try:
            self.refresh(rnd)
            self._settle(rnd)
            self._verify_offline(rnd)
        finally:
            with self._lock:
                rnd.busy = False
                if not rnd.archived:
                    rnd.updated_at = _now()
                    self._persist()
        return rnd

    def observe(self, round_id: str) -> None:
        """Reflect cluster reality when a page is opened.

        Read-only with respect to the cluster.  It keeps the display honest —
        a pod that left, a node that was uncordoned, or a replacement that
        appeared are all visible without pressing a button.  Errors are
        swallowed so a transient API failure does not break the page; the
        stored snapshot is then shown as-is.  An archived round is never
        reflected into: its page shows the archive.
        """
        rnd = self.get(round_id)
        if rnd is None:
            return
        with self._lock:
            if rnd.busy or rnd.archived:
                return
            rnd.busy = True
        try:
            self.refresh(rnd)
            self._verify_offline(rnd)
        except KubeError:
            pass
        finally:
            with self._lock:
                rnd.busy = False

    # -- re-read -----------------------------------------------------------
    def refresh(self, rnd: Round) -> Round:
        """Reconcile the round snapshot with the live cluster.

        * Pod object no longer present -> ``GONE``.
        * Pod object still present keeps its state; a ``TERMINATING`` pod keeps
          showing "待退出".  Nothing is reverted to ``PENDING`` here, because
          re-issuing an eviction the server already accepted would be wrong.
        * New object (including a same-named replacement, new UID) enters as
          ``PENDING``, or as ``DEFERRED`` when this round already evicted that
          name — "同名替代者留待续排".

        Writes are fenced by ``archived``: a read that started before a
        handover may finish after it, and must then leave the archived record
        exactly as it was archived.
        """
        live = self.kube.list_pods_on_node(rnd.node)
        budgets = self.kube.list_budgets()
        by_uid = {pod.uid: pod for pod in live}
        with self._lock:
            if rnd.archived:
                return rnd
            for status in rnd.pods.values():
                pod = by_uid.get(status.uid)
                if pod is None:
                    if status.state is not PodState.GONE:
                        if status.state is PodState.TERMINATING:
                            status.message = "已退出节点。"
                        else:
                            status.message = status.message or "Pod 已离开节点。"
                        status.state = PodState.GONE
                    continue
                status.exempt_reason = pod.exempt_reason
                status.budgets = pods_matching_budgets(pod, budgets)
                if pod.terminating and status.state is PodState.PENDING:
                    # Deletion started elsewhere (for example a manual delete).
                    status.state = PodState.TERMINATING
                    status.message = "Pod 正在删除，等待退出。"
            for pod in live:
                if pod.uid in rnd.pods:
                    continue
                replacement = pod.key in rnd.attempted_names
                rnd.pods[pod.uid] = PodStatus(
                    name=pod.name,
                    namespace=pod.namespace,
                    uid=pod.uid,
                    node=pod.node,
                    state=PodState.DEFERRED if replacement else PodState.PENDING,
                    exempt_reason=pod.exempt_reason,
                    budgets=pods_matching_budgets(pod, budgets),
                    message=(
                        "同名替代者（新 UID），留待续排。"
                        if replacement
                        else ""
                    ),
                )
            rnd.updated_at = _now()
        return rnd

    def _verify_offline(self, rnd: Round) -> None:
        """Report the node safe to power off only after an independent check.

        Reads the node and its pods afresh.  The node must be unschedulable
        *now* and no ordinary pod may remain.  While any ordinary pod is still
        present — including one that was granted eviction but has not exited —
        the page must not claim the node is ready.
        """
        node = self.kube.get_node(rnd.node)
        live = self.kube.list_pods_on_node(rnd.node) if node is not None else []
        with self._lock:
            if rnd.archived:
                return
            if node is None:
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = "节点不存在，无法确认。"
                return
            if not node.unschedulable:
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = "节点当前可调度；未重新封锁前不能下线。"
                if rnd.phase is DrainPhase.COMPLETE:
                    rnd.phase = DrainPhase.RUNNING
                return
            remaining = [pod for pod in live if pod.is_normal]
            if remaining:
                names = "、".join(f"{pod.namespace}/{pod.name}" for pod in remaining[:5])
                more = "" if len(remaining) <= 5 else f" 等 {len(remaining)} 个"
                rnd.offline = False
                rnd.offline_at = None
                rnd.offline_reason = f"节点仍不可调度，但尚有普通 Pod：{names}{more}。"
                if rnd.phase is DrainPhase.COMPLETE:
                    rnd.phase = DrainPhase.RUNNING
                return
            rnd.offline = True
            rnd.ever_offline = True
            if not rnd.offline_confirmed_at:
                rnd.offline_confirmed_at = _now()
            if not rnd.offline_at:
                rnd.offline_at = _now()
            rnd.offline_reason = "已确认：节点不可调度，且已无普通 Pod。"
            rnd.phase = DrainPhase.COMPLETE

    # -- helpers -----------------------------------------------------------
    def _refuse_archived(self, rnd: Round) -> None:
        if rnd.archived:
            raise DrainError(ARCHIVED_MESSAGE)

    def _require(self, round_id: str) -> Round:
        rnd = self.get(round_id)
        if rnd is None:
            raise DrainError(f"排空轮次 {round_id} 不存在")
        return rnd

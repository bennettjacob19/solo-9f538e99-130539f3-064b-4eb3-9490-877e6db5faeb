"""Round handover: draining the same node a second time.

The scenario under test throughout:

1. node-a is drained and confirmed offline (round 1);
2. a pod is re-created under the same name on node-a, as a controller would;
3. a new round is started for node-a, archiving round 1;
4. old links must be read-only and must show the archive, never live state.
"""

from __future__ import annotations

import json
from urllib.parse import unquote

import pytest
from fastapi.testclient import TestClient

from app.drain import ARCHIVED_MESSAGE, DrainError, DrainManager
from app.kube import KubeError
from app.main import create_app
from app.models import DrainPhase
from app.store import RoundStore, StoreError


def finish_round(manager: DrainManager, sim, node: str = "node-a"):
    round_ = manager.start(node)
    manager.run(round_.round_id)
    sim.post("/api/sim/pods/exit-terminating")
    manager.recheck(round_.round_id)
    assert round_.offline is True
    return round_


def test_completed_round_can_be_handed_over_to_a_new_round(manager, sim, kube):
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    new = manager.start("node-a")

    assert new.round_id != old.round_id
    assert old.archived is True
    assert old.superseded_by == new.round_id
    assert old.archived_at
    # The archive keeps the conclusion the operator acted on.
    assert old.phase is DrainPhase.COMPLETE
    assert old.offline is True
    assert "已确认" in old.offline_reason
    # The new round covers the pod that was rebuilt onto the node.
    assert "shop/web-2" in {pod.display_name for pod in new.evictable}
    assert manager.current_round_for_node("node-a") is new
    assert kube.get_node("node-a").unschedulable is True


def test_opening_old_page_after_handover_keeps_archive(manager, sim):
    old = finish_round(manager, sim)
    pod_names = {pod.display_name for pod in old.evictable}
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    new = manager.start("node-a")
    new_uid = new.evictable[0].uid
    original_rows = {
        uid: (status.state, status.message) for uid, status in old.pods.items()
    }

    # Opening the old page does not revive it, even though the cluster now has
    # a fresh ordinary pod on the node.
    manager.observe(old.round_id)

    assert old.phase is DrainPhase.COMPLETE
    assert old.offline is True
    assert "已确认" in old.offline_reason
    assert old.archived is True
    assert {uid: (s.state, s.message) for uid, s in old.pods.items()} == original_rows
    assert {pod.display_name for pod in old.evictable} == pod_names
    assert new_uid not in old.pods


def test_archived_round_cannot_evict(manager, sim):
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    manager.start("node-a")

    with pytest.raises(DrainError) as excinfo:
        manager.run(old.round_id)
    assert ARCHIVED_MESSAGE in str(excinfo.value)

    with pytest.raises(DrainError):
        manager.resume(old.round_id)
    with pytest.raises(DrainError):
        manager.recheck(old.round_id)
    with pytest.raises(DrainError):
        manager.interrupt(old.round_id)


def test_late_recheck_does_not_rewrite_archive(manager, sim):
    """A recheck that started before the handover must not touch the archive."""
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    real_list = manager.kube.list_pods_on_node
    calls = {"count": 0}

    def slow_list(node):
        calls["count"] += 1
        result = real_list(node)
        if calls["count"] == 1:
            # The archive happens while this read is "in flight".
            manager.start("node-a")
        return result

    manager.kube.list_pods_on_node = slow_list
    manager.recheck(old.round_id)
    manager.kube.list_pods_on_node = real_list

    assert old.archived is True
    assert old.phase is DrainPhase.COMPLETE
    assert old.offline is True
    assert "已确认" in old.offline_reason
    # The replacement pod that appeared after the archive is not in the
    # archived rows.
    assert "shop/web-2" not in {p.display_name for p in old.pods.values()}


def test_current_round_is_still_rechecked_against_the_cluster(manager, sim):
    finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    new = manager.start("node-a")

    # A live recheck of the current round sees the new pod.
    manager.recheck(new.round_id)
    assert new.offline is False
    assert new.phase is DrainPhase.RUNNING
    assert "web-2" in new.offline_reason


def test_failed_start_keeps_old_round_current(manager, sim, kube):
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    def broken_cordon(node):
        raise RuntimeError("boom")

    manager.kube.cordon = broken_cordon
    with pytest.raises(RuntimeError):
        manager.start("node-a")

    assert manager.current_round_for_node("node-a") is old
    assert old.archived is False
    assert old.offline is True
    assert old.phase is DrainPhase.COMPLETE


def test_snapshot_failure_keeps_old_round_current(manager, sim):
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    def broken_list(node):
        raise KubeError("api down")

    manager.kube.list_pods_on_node = broken_list
    with pytest.raises(KubeError):
        manager.start("node-a")

    assert manager.current_round_for_node("node-a") is old
    assert old.archived is False
    assert old.offline is True
    assert len(manager.list_rounds()) == 1


def test_old_round_still_current_blocks_second_drain(manager):
    manager.start("node-a")
    with pytest.raises(DrainError):
        manager.start("node-a")


def test_old_round_that_was_never_offline_blocks_handover(manager, sim):
    round_ = manager.start("node-a")
    manager.run(round_.round_id)
    manager.interrupt(round_.round_id)
    with pytest.raises(DrainError):
        manager.start("node-a")


# -- persistence ------------------------------------------------------------


def test_strict_save_failure_keeps_old_round_current(kube, sim, tmp_path):
    store = RoundStore(tmp_path / "rounds.json")
    manager = DrainManager(kube, store)
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    def broken_save(rounds, active_rounds=None, strict=False):
        if strict:
            raise StoreError("disk full")

    store.save = broken_save
    with pytest.raises(DrainError) as excinfo:
        manager.start("node-a")
    assert "disk full" in str(excinfo.value)

    assert manager.current_round_for_node("node-a") is old
    assert old.archived is False
    assert old.offline is True
    assert old.phase is DrainPhase.COMPLETE
    assert all(r.node != "node-a" or r is old for r in manager.list_rounds())
    # Cordon already took effect and is kept, as required.
    assert kube.get_node("node-a").unschedulable is True
    # Nothing about the old round reached the disk as archived.
    data = json.loads((tmp_path / "rounds.json").read_text())
    stored = {item["round_id"]: item for item in data["rounds"]}
    assert stored[old.round_id]["archived"] is False
    assert data["active_rounds"]["node-a"] == old.round_id


def test_handover_is_persisted_before_it_is_reported(kube, sim, tmp_path):
    path = tmp_path / "rounds.json"
    manager = DrainManager(kube, RoundStore(path))
    old = finish_round(manager, sim)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    new = manager.start("node-a")

    data = json.loads(path.read_text())
    stored = {item["round_id"]: item for item in data["rounds"]}
    assert stored[old.round_id]["archived"] is True
    assert stored[old.round_id]["superseded_by"] == new.round_id
    assert data["active_rounds"]["node-a"] == new.round_id
    assert data["next_seq"] == new.seq + 1

    restarted = DrainManager(kube, RoundStore(path))
    assert restarted.current_round_for_node("node-a").round_id == new.round_id
    assert restarted.get(old.round_id).archived is True


def test_restart_recognises_current_of_same_second_rounds(kube, sim, tmp_path, monkeypatch):
    """Both rounds share created_at to the second; seq and the map decide."""
    import app.drain as drain_module

    monkeypatch.setattr(drain_module, "_now", lambda: "2026-01-01T00:00:00+00:00")
    path = tmp_path / "rounds.json"
    manager = DrainManager(kube, RoundStore(path))
    old = finish_round(manager, sim)
    new = manager.start("node-a")
    assert old.created_at == new.created_at  # the tie the sequence exists for

    restarted = DrainManager(kube, RoundStore(path))

    assert restarted.current_round_for_node("node-a").round_id == new.round_id
    assert restarted.get(old.round_id).archived is True
    assert restarted.get(new.round_id).archived is False
    # Listing is newest first even when timestamps tie.
    assert [r.round_id for r in restarted.list_rounds()] == [new.round_id, old.round_id]


def test_legacy_state_file_is_accepted(tmp_path):
    """A v1 file (no seq/archive/active map) still names the right current round."""
    legacy = {
        "version": 1,
        "rounds": [
            {
                "node": "node-a",
                "round_id": "older",
                "phase": "complete",
                "offline": True,
                "offline_reason": "已确认：节点不可调度，且已无普通 Pod。",
                "offline_at": "2026-01-01T00:00:10+00:00",
                "message": "",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:10+00:00",
                "attempted_names": [],
                "pods": [],
            },
            {
                "node": "node-a",
                "round_id": "newer",
                "phase": "running",
                "offline": False,
                "message": "",
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:11+00:00",
                "attempted_names": [],
                "pods": [],
            },
        ],
    }
    path = tmp_path / "rounds.json"
    path.write_text(json.dumps(legacy))

    store = RoundStore(path)
    rounds = store.load()

    assert store.active_rounds == {"node-a": "newer"}
    assert rounds["older"].archived is True
    assert rounds["newer"].archived is False
    assert rounds["newer"].seq > rounds["older"].seq
    assert store.next_seq > rounds["newer"].seq


def test_legacy_complete_only_file_keeps_newest_current(tmp_path):
    legacy = {
        "version": 1,
        "rounds": [
            {
                "node": "node-a",
                "round_id": "first",
                "phase": "complete",
                "offline": True,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:10+00:00",
            },
            {
                "node": "node-a",
                "round_id": "second",
                "phase": "complete",
                "offline": True,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:20+00:00",
            },
        ],
    }
    path = tmp_path / "rounds.json"
    path.write_text(json.dumps(legacy))

    store = RoundStore(path)
    rounds = store.load()

    assert store.active_rounds == {"node-a": "second"}
    assert rounds["second"].archived is False
    assert rounds["first"].archived is True


# -- HTTP -------------------------------------------------------------------


@pytest.fixture()
def client(simulator_url, monkeypatch):
    monkeypatch.setenv("DRAIN_KUBE_MODE", "host")
    monkeypatch.setenv("DRAIN_KUBE_HOST", simulator_url)
    return TestClient(create_app())


def start(client, node="node-a"):
    response = client.post("/drain", data={"node": node}, follow_redirects=False)
    assert response.status_code == 303
    return response.headers["location"].rsplit("/", 1)[-1]


def test_old_form_is_refused_without_cluster_calls(client, sim):
    first = start(client)
    client.post(f"/drain/{first}/run", follow_redirects=False)
    sim.post("/api/sim/pods/exit-terminating")
    client.post(f"/drain/{first}/recheck", follow_redirects=False)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    second = start(client)
    assert second != first
    events_before = len(sim.get("/api/sim/state").json()["events"])

    response = client.post(f"/drain/{first}/run", follow_redirects=False)
    assert response.status_code == 303
    page = client.get(f"/drain/{first}").text
    assert ARCHIVED_MESSAGE in page
    assert "已归档" in page

    state = sim.get("/api/sim/state").json()
    new_events = state["events"][: len(state["events"]) - events_before]
    assert [e for e in new_events if e["kind"] in ("eviction", "cordon", "uncordon")] == []


def test_archived_page_shows_frozen_rows_and_link(client, sim):
    first = start(client)
    client.post(f"/drain/{first}/run", follow_redirects=False)
    sim.post("/api/sim/pods/exit-terminating")
    client.post(f"/drain/{first}/recheck", follow_redirects=False)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})
    second = start(client)

    page = client.get(f"/drain/{first}").text
    assert "归档时结论：可下线" in page
    assert "已确认：节点不可调度，且已无普通 Pod" in page
    assert f"/drain/{second}" in page
    assert "shop/web-0" in page
    assert "shop/web-2" not in page

    current = client.get(f"/drain/{second}").text
    # The live round shows what the cluster says now and offers no eviction
    # before the operator asks for one.
    assert "shop/web-2" in current
    assert "待驱逐" in current
    assert "驱逐本轮 Pod" in current


def test_index_offers_new_drain_after_handover(client, sim):
    first = start(client)
    client.post(f"/drain/{first}/run", follow_redirects=False)
    sim.post("/api/sim/pods/exit-terminating")
    client.post(f"/drain/{first}/recheck", follow_redirects=False)

    page = client.get("/").text
    assert "再次封锁并排空（新的一轮）" in page

    second = start(client)
    page = client.get("/").text
    assert f"/drain/{second}" in page
    assert f"/drain/{first}" in page
    assert page.count("已归档") >= 1


def test_index_blocks_start_while_round_running(client, sim):
    first = start(client)
    client.post(f"/drain/{first}/run", follow_redirects=False)

    page = client.get("/").text
    assert f"/drain/{first}" in page
    assert "再次封锁并排空" not in page


def test_start_failure_redirects_with_error_and_keeps_current(client, sim, monkeypatch):
    first = start(client)
    client.post(f"/drain/{first}/run", follow_redirects=False)

    # The first round is not confirmed offline yet, so a second start is
    # refused rather than silently evicting on a fresh snapshot.
    response = client.post("/drain", data={"node": "node-a"}, follow_redirects=False)
    assert response.status_code == 303
    location = unquote(response.headers["location"])
    assert "已有进行中的排空轮次" in location

    payload = {item["round_id"]: item for item in client.get("/api/rounds").json()}
    assert payload[first]["archived"] is False
    assert payload[first]["phase"] == "running"
    # The page explains the refusal instead of showing a bare redirect.
    assert "已有进行中的排空轮次" in client.get(location).text


def test_handover_write_failure_is_reported_on_the_page(client, sim, tmp_path, monkeypatch):
    monkeypatch.setenv("DRAIN_STATE_FILE", str(tmp_path / "rounds.json"))
    app_client = TestClient(create_app())

    first = start(app_client)
    app_client.post(f"/drain/{first}/run", follow_redirects=False)
    sim.post("/api/sim/pods/exit-terminating")
    app_client.post(f"/drain/{first}/recheck", follow_redirects=False)
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-a"})

    from app.store import StoreError

    manager = app_client.app.state.manager

    def broken_save(rounds, active_rounds=None, strict=False):
        if strict:
            raise StoreError("磁盘已满")

    manager.store.save = broken_save
    response = app_client.post("/drain", data={"node": "node-a"}, follow_redirects=False)
    location = unquote(response.headers["location"])
    assert "磁盘已满" in location

    payload = {item["round_id"]: item for item in app_client.get("/api/rounds").json()}
    assert payload[first]["archived"] is False
    assert payload[first]["phase"] == "complete"
    # Cordon stays, per the rule that an effective cordon is never undone.
    assert sim.get("/api/sim/state").json()["nodes"][0]["unschedulable"] is True

from __future__ import annotations

from pathlib import Path

import pytest

from superharness.engine import runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator
from superharness.engine.shipper import ShipOutcome

NOW = "2026-01-01T00:00:00Z"


class FakeShipper:
    def __init__(self, outcome: ShipOutcome) -> None:
        self.outcome = outcome
        self.calls = 0

    def ship(self, **_kwargs):
        self.calls += 1
        return self.outcome


def _project(tmp_path: Path):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    return project, conn


def _task(conn, task_id: str = "t1", *, workflow: str = "reliable-orchestrator"):
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES (?, ?, 'claude-code', 'in_progress', 1, ?, ?)",
        (task_id, task_id, NOW, workflow),
    )
    conn.commit()


def _successful_implement(conn, task_id: str = "t1"):
    run = runs_dao.create_run(
        conn,
        id=f"impl-{task_id}",
        task_id=task_id,
        kind="implement",
        agent="claude-code",
        dedupe_key=f"implement:{task_id}:plan",
        worktree_path="/tmp/superharness-worktrees/reliable/project/t1",
        branch_name=f"shux/reliable/{task_id}",
        base_sha="base",
        head_sha="head",
        now=NOW,
    )
    runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    runs_dao.record_run_result(
        conn,
        run.id,
        {
            "schema_version": 1,
            "run_id": run.id,
            "task_id": task_id,
            "kind": "implement",
            "agent": "claude-code",
            "exit_code": 0,
            "completion_status": "completed",
            "worktree_path": run.worktree_path,
            "branch_name": run.branch_name,
            "base_sha": run.base_sha,
            "head_sha": run.head_sha,
            "dirty": True,
        },
        now=NOW,
    )
    runs_dao.transition_run(conn, run.id, to_status="succeeded", now=NOW)
    conn.commit()
    return run


def test_implementation_success_creates_one_system_ship_run(tmp_path):
    project, conn = _project(tmp_path)
    shipper = FakeShipper(ShipOutcome(ok=False, failure_category="push_failed"))
    try:
        _task(conn)
        _successful_implement(conn)
        orch = LifecycleOrchestrator(
            str(project), now=lambda: NOW, shipper_factory=lambda _project: shipper
        )
        orch.tick("t1")
        orch.tick("t1")
        ship_runs = runs_dao.list_runs_for_task(conn, "t1", kind="ship")
        task = tasks_dao.get(conn, "t1")
        assert len(ship_runs) == 1
        assert ship_runs[0].agent == "system"
        assert ship_runs[0].parent_run_id == "impl-t1"
        assert ship_runs[0].status == "failed"
        assert task is not None and task.status == "in_progress"

    finally:
        conn.close()


@pytest.mark.regression
def test_valid_repair_result_at_pr_head_creates_ship_without_redefining_base(tmp_path):
    """A repair at PR head A remains shippable when its result also reports A."""
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        repair = runs_dao.create_run(
            conn,
            id="repair-t1",
            task_id="t1",
            kind="repair",
            agent="claude-code",
            dedupe_key="repair:t1:review-t1",
            worktree_path="/tmp/superharness-worktrees/reliable/project/t1",
            branch_name="shux/reliable/t1",
            base_sha="sha-a",
            head_sha="sha-a",
            now=NOW,
        )
        runs_dao.transition_run(conn, repair.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, repair.id, to_status="running", now=NOW)
        runs_dao.record_run_result(
            conn,
            repair.id,
            {
                "schema_version": 1,
                "run_id": repair.id,
                "task_id": repair.task_id,
                "kind": repair.kind,
                "agent": repair.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": repair.worktree_path,
                "branch_name": repair.branch_name,
                "base_sha": "sha-a",
                "head_sha": "sha-a",
                "dirty": True,
                "changed_files": ["src/fixed.py"],
            },
            now=NOW,
        )
        runs_dao.transition_run(conn, repair.id, to_status="succeeded", now=NOW)
        conn.commit()

        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        persisted_repair = runs_dao.get_run(conn, repair.id)
        ship_runs = runs_dao.list_runs_for_task(conn, "t1", kind="ship")
        assert persisted_repair is not None
        assert persisted_repair.base_sha == "sha-a"
        assert persisted_repair.head_sha == "sha-a"
        assert len(ship_runs) == 1
        assert ship_runs[0].parent_run_id == repair.id
        assert ship_runs[0].base_sha == "sha-a"
    finally:
        conn.close()


def test_confirmed_ship_moves_task_to_pr_open_once(tmp_path):
    project, conn = _project(tmp_path)
    outcome = ShipOutcome(
        ok=True,
        branch_name="shux/reliable/t1",
        base_sha="head",
        head_sha="commit-sha",
        remote_head_sha="commit-sha",
        pr_number=42,
        pr_url="https://github.com/o/r/pull/42",
        worktree_path="/tmp/superharness-worktrees/reliable/project/t1",
    )
    shipper = FakeShipper(outcome)
    try:
        _task(conn)
        _successful_implement(conn)
        orch = LifecycleOrchestrator(
            str(project), now=lambda: NOW, shipper_factory=lambda _project: shipper
        )
        orch.tick("t1")
        orch.tick("t1")
        ship_runs = runs_dao.list_runs_for_task(conn, "t1", kind="ship")
        task = tasks_dao.get(conn, "t1")
        assert len(ship_runs) == 1
        assert shipper.calls == 1
        assert ship_runs[0].remote_head_sha == "commit-sha"
        assert task is not None and task.status == "review_requested"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 1

    finally:
        conn.close()


def test_remote_sha_mismatch_blocks_pr_open(tmp_path):
    project, conn = _project(tmp_path)
    outcome = ShipOutcome(
        ok=False,
        failure_category="remote_head_mismatch",
        failure_detail="remote mismatch",
        head_sha="local",
        remote_head_sha="remote",
    )
    try:
        _task(conn)
        _successful_implement(conn)
        orch = LifecycleOrchestrator(
            str(project),
            now=lambda: NOW,
            shipper_factory=lambda _project: FakeShipper(outcome),
        )
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        ship = runs_dao.list_runs_for_task(conn, "t1", kind="ship")[0]
        assert task is not None and task.status == "in_progress"
        assert ship.status == "failed"
        assert ship.failure_category == "remote_head_mismatch"

    finally:
        conn.close()


def test_legacy_task_success_is_ignored_by_shipping(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "legacy", workflow="implementation")
        _successful_implement(conn, "legacy")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick()
        assert runs_dao.list_runs_for_task(conn, "legacy", kind="ship") == []
        task = tasks_dao.get(conn, "legacy")
        assert task is not None and task.status == "in_progress"

    finally:
        conn.close()

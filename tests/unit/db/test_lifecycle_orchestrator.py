from __future__ import annotations

import json
from pathlib import Path

import pytest

from superharness.engine import inbox_dao, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator


T0 = "2026-01-01T00:00:00Z"
T1 = "2026-01-01T00:01:00Z"
T2 = "2026-01-01T00:02:00Z"
T3 = "2026-01-01T00:03:00Z"


def _project(tmp_path: Path):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    return project, conn


def _task(
    conn,
    task_id: str,
    *,
    status: str = "todo",
    workflow: str | None = "reliable-orchestrator",
):
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES (?, ?, 'claude-code', ?, 1, ?, ?)",
        (task_id, task_id, status, T0, workflow),
    )
    conn.commit()


def _complete(conn, run_id: str, *, result: dict | None = None):
    run = runs_dao.get_run(conn, run_id)
    assert run is not None
    runs_dao.transition_run(conn, run_id, to_status="claimed", now=T1)
    runs_dao.transition_run(conn, run_id, to_status="running", now=T2)
    payload = result or {
        "schema_version": 1,
        "run_id": run.id,
        "task_id": run.task_id,
        "kind": run.kind,
        "agent": run.agent,
        "exit_code": 0,
        "completion_status": "completed",
        "worktree_path": "/tmp/worktree",
        "branch_name": "main",
        "base_sha": "base",
        "head_sha": "head",
        "dirty": False,
    }
    runs_dao.record_run_result(conn, run_id, payload, now=T3)
    runs_dao.transition_run(conn, run_id, to_status="succeeded", now=T3)
    conn.commit()


def test_todo_creates_one_linked_plan_and_repeated_ticks_are_idempotent(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "t1")
        orch = LifecycleOrchestrator(str(project), now=lambda: T0)
        orch.tick("t1")
        orch.tick("t1")
        runs = runs_dao.list_runs_for_task(conn, "t1")
        inbox = inbox_dao.get_all(conn, status="pending")
        assert len(runs) == 1 and runs[0].kind == "plan"
        assert len(inbox) == 1 and inbox[0].run_id == runs[0].id
    finally:
        conn.close()


def test_legacy_task_is_ignored(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "legacy", workflow=None)
        LifecycleOrchestrator(str(project), now=lambda: T0).tick()
        assert runs_dao.list_runs_for_task(conn, "legacy") == []
    finally:
        conn.close()


def test_successful_plan_proposes_and_auto_approval_creates_one_implementation(
    tmp_path,
):
    project, conn = _project(tmp_path)
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_approve_plans: true\n", encoding="utf-8"
        )
        _task(conn, "t1")
        orch = LifecycleOrchestrator(str(project), now=lambda: T0)
        orch.tick("t1")
        plan = runs_dao.list_runs_for_task(conn, "t1", kind="plan")[0]
        _complete(conn, plan.id)
        orch.tick("t1")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "in_progress"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="implement")) == 1
    finally:
        conn.close()


def test_plan_success_without_auto_approval_stops_at_plan_proposed(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "t1")
        orch = LifecycleOrchestrator(str(project), now=lambda: T0)
        orch.tick("t1")
        plan = runs_dao.list_runs_for_task(conn, "t1", kind="plan")[0]
        _complete(conn, plan.id)
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "plan_proposed"
        assert runs_dao.list_runs_for_task(conn, "t1", kind="implement") == []
    finally:
        conn.close()


def test_implementation_success_is_consumed_but_does_not_ship_or_close(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "t1")
        orch = LifecycleOrchestrator(
            str(project), now=lambda: T0, auto_approve_plans=True
        )
        orch.tick("t1")
        plan = runs_dao.list_runs_for_task(conn, "t1", kind="plan")[0]
        _complete(conn, plan.id)
        orch.tick("t1")
        implementation = runs_dao.list_runs_for_task(conn, "t1", kind="implement")[0]
        _complete(conn, implementation.id)
        orch.tick("t1")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        run = runs_dao.get_run(conn, implementation.id)
        assert task is not None and task.status == "in_progress"
        assert run is not None and run.orchestrator_consumed_at is not None
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="ship")) == 0
    finally:
        conn.close()


def test_malformed_success_result_does_not_advance_task(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "t1")
        orch = LifecycleOrchestrator(str(project), now=lambda: T0)
        orch.tick("t1")
        plan = runs_dao.list_runs_for_task(conn, "t1", kind="plan")[0]
        runs_dao.transition_run(conn, plan.id, to_status="claimed", now=T1)
        runs_dao.transition_run(conn, plan.id, to_status="running", now=T2)
        conn.execute(
            "UPDATE runs SET result_json=? WHERE id=?",
            (json.dumps({"run_id": "wrong"}), plan.id),
        )
        runs_dao.transition_run(conn, plan.id, to_status="succeeded", now=T3)
        conn.commit()
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "todo"
        assert runs_dao.get_run(conn, plan.id).orchestrator_consumed_at is None
    finally:
        conn.close()


def test_run_and_inbox_roll_back_together(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, "t1")

        def fail_enqueue(*args, **kwargs):
            raise RuntimeError("inbox insert failed")

        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.inbox_dao.enqueue", fail_enqueue
        )
        with pytest.raises(RuntimeError, match="inbox insert failed"):
            LifecycleOrchestrator(str(project), now=lambda: T0).tick("t1")
        assert runs_dao.list_runs_for_task(conn, "t1") == []
    finally:
        conn.close()

from __future__ import annotations

from pathlib import Path

from superharness.commands.inbox_dispatch import (
    DispatchContext,
    _prepare_execution,
    _reliable_run_finished,
    _reliable_run_started,
    _sqlite_claim_next,
)
from superharness.engine import inbox_dao, runs_dao
from superharness.engine.db import get_connection, init_db


NOW = "2026-01-01T00:00:00Z"


def _project(tmp_path: Path):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES ('t1', 't1', 'claude-code', 'in_progress', 1, ?, 'reliable-orchestrator')",
        (NOW,),
    )
    run = runs_dao.create_run(
        conn,
        id="run-1",
        task_id="t1",
        kind="implement",
        agent="claude-code",
        dedupe_key="implement:t1:plan-1",
        now=NOW,
    )
    inbox = inbox_dao.enqueue(
        conn,
        id="inbox-1",
        task_id="t1",
        target_agent="claude-code",
        project_path=str(project),
        run_id=run.id,
        now=NOW,
    )
    runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox.id)
    conn.commit()
    return project, conn


def test_dispatch_claim_advances_linked_run_atomically(tmp_path):
    project, conn = _project(tmp_path)
    try:
        claimed = _sqlite_claim_next(str(project), "claude-code", NOW)
        assert claimed is not None and claimed["run_id"] == "run-1"
        assert runs_dao.get_run(conn, "run-1").status == "claimed"
        assert inbox_dao.get(conn, "inbox-1").status == "launched"
    finally:
        conn.close()


def test_dispatch_passes_run_id_to_agent_and_records_completion(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        assert ctx.spawn_env["SUPERHARNESS_RUN_ID"] == "run-1"
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None and run.status == "succeeded"
        assert run.result_json["run_id"] == "run-1"
        assert inbox_dao.get(conn, "inbox-1").status == "done"
    finally:
        conn.close()

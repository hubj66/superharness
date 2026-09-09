from __future__ import annotations

from pathlib import Path

from superharness.commands.inbox_watch import (
    _auto_close_report_ready,
    _auto_close_review_passed,
    _auto_fallback_owner_reassign,
    auto_enqueue_approved,
    auto_enqueue_todo,
)
from superharness.engine import inbox_dao, reliable_watcher, runs_dao
from superharness.engine.db import get_connection, init_db


def _project(tmp_path: Path, status: str):
    project = tmp_path / "project"
    harness = project / ".superharness"
    harness.mkdir(parents=True)
    (harness / "profile.yaml").write_text(
        "auto_dispatch: true\nautonomy: ai_driven\n", encoding="utf-8"
    )
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES ('t1', 't1', 'claude-code', ?, 1, '2026-01-01T00:00:00Z', 'reliable-orchestrator')",
        (status,),
    )
    conn.commit()
    return project, conn


def test_legacy_auto_enqueue_paths_ignore_reliable_tasks(tmp_path):
    project, conn = _project(tmp_path, "todo")
    try:
        assert auto_enqueue_todo(str(project)) == 0
        assert inbox_dao.get_all(conn) == []
    finally:
        conn.close()


def test_watcher_restart_and_repeated_ticks_do_not_duplicate_runs(tmp_path):
    project, conn = _project(tmp_path, "todo")
    try:
        report_errors = lambda *_args: None
        assert reliable_watcher.tick(
            str(project), "2026-01-01T00:00:00Z", report_errors
        )
        reliable_watcher.release(str(project))
        assert reliable_watcher.tick(
            str(project), "2026-01-01T00:01:00Z", report_errors
        )
        runs = runs_dao.list_runs_for_task(conn, "t1", kind="plan")
        assert len(runs) == 1
        assert len(inbox_dao.get_all(conn, status="pending")) == 1
    finally:
        reliable_watcher.release(str(project))
        conn.close()


def test_reliable_tasks_bypass_legacy_close_and_fallback_paths(tmp_path):
    project, conn = _project(tmp_path, "review_requested")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )
        _auto_close_review_passed(str(project))
        assert _auto_close_report_ready(str(project)) is None
        inbox = inbox_dao.enqueue(
            conn,
            id="failed-1",
            task_id="t1",
            target_agent="claude-code",
            now="2026-01-01T00:00:00Z",
        )
        inbox_dao.update_status(
            conn,
            inbox.id,
            from_status="pending",
            to_status="failed",
            now="2026-01-01T00:01:00Z",
            reason="test",
        )
        conn.commit()
        _auto_fallback_owner_reassign(str(project))
        assert inbox_dao.get(conn, inbox.id).status == "failed"
    finally:
        conn.close()

    project, conn = _project(tmp_path / "approved", "plan_approved")
    try:
        assert auto_enqueue_approved(str(project)) == 0
        assert inbox_dao.get_all(conn) == []
    finally:
        conn.close()

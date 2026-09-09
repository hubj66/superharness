from __future__ import annotations

import sqlite3

import pytest
from pydantic import ValidationError

from superharness.engine import inbox_dao, runs_dao
from superharness.engine.db import init_db
from superharness.engine.reliable_orchestrator_gate import is_reliable_orchestrated_task
from superharness.engine.state_errors import BoundaryError, StateError

T0 = "2026-01-01T00:00:00Z"
T1 = "2026-01-01T00:01:00Z"
T2 = "2026-01-01T00:02:00Z"
T3 = "2026-01-01T00:03:00Z"


def _make_task(conn: sqlite3.Connection, task_id: str = "t1") -> None:
    conn.execute(
        "INSERT INTO tasks (id, title, status, version, created_at) "
        "VALUES (?, ?, 'todo', 1, ?)",
        (task_id, task_id, T0),
    )


def _create_run(
    conn: sqlite3.Connection,
    *,
    id: str = "r1",
    task_id: str = "t1",
    kind: str = "implement",
    agent: str = "claude-code",
    model: str | None = None,
    dedupe_key: str | None = None,
    review_target_sha: str | None = None,
    trigger_run_id: str | None = None,
):
    return runs_dao.create_run(
        conn,
        id=id,
        task_id=task_id,
        kind=kind,
        agent=agent,
        model=model,
        dedupe_key=dedupe_key or id,
        review_target_sha=review_target_sha,
        trigger_run_id=trigger_run_id,
        now=T0,
    )


def test_run_creation_and_retrieval(db_conn):
    _make_task(db_conn)

    row = _create_run(db_conn, model="claude-sonnet-4-6")

    assert row.id == "r1"
    assert row.status == "queued"
    assert row.kind == "implement"
    assert runs_dao.get_run(db_conn, "r1") == row
    assert runs_dao.list_runs_for_task(db_conn, "t1") == [row]


@pytest.mark.parametrize(
    ("from_status", "to_status"),
    [
        ("queued", "claimed"),
        ("queued", "cancelled"),
        ("claimed", "running"),
        ("claimed", "cancelled"),
        ("claimed", "failed"),
        ("running", "succeeded"),
        ("running", "failed"),
        ("running", "crashed"),
        ("running", "timed_out"),
        ("running", "quota_blocked"),
        ("running", "cancelled"),
    ],
)
def test_every_legal_run_transition(db_conn, from_status, to_status):
    task_id = f"task-{from_status}-{to_status}"
    run_id = f"run-{from_status}-{to_status}"
    _make_task(db_conn, task_id)
    run = _create_run(db_conn, id=run_id, task_id=task_id)

    if from_status == "claimed":
        assert runs_dao.transition_run(db_conn, run.id, to_status="claimed", now=T1)
    elif from_status == "running":
        assert runs_dao.transition_run(db_conn, run.id, to_status="claimed", now=T1)
        assert runs_dao.transition_run(db_conn, run.id, to_status="running", now=T2)

    assert runs_dao.transition_run(db_conn, run.id, to_status=to_status, now=T3)
    updated = runs_dao.get_run(db_conn, run.id)
    assert updated is not None
    assert updated.status == to_status


def test_illegal_run_transition_rejected(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)

    with pytest.raises(StateError, match="Illegal run transition"):
        runs_dao.transition_run(db_conn, run.id, to_status="succeeded", now=T1)


def test_dedupe_key_uniqueness_returns_existing_run(db_conn):
    _make_task(db_conn)

    first = _create_run(db_conn, id="r1", dedupe_key="same-key")
    second = _create_run(db_conn, id="r2", dedupe_key="same-key")

    assert second == first
    assert len(runs_dao.list_runs_for_task(db_conn, "t1")) == 1


def test_one_active_mutating_run_per_task(db_conn):
    _make_task(db_conn)
    _create_run(db_conn, id="r1", kind="implement", dedupe_key="implement")

    with pytest.raises(StateError, match="Run creation rejected"):
        _create_run(db_conn, id="r2", kind="repair", dedupe_key="repair")


def test_multiple_terminal_historical_runs_allowed(db_conn):
    _make_task(db_conn)
    first = _create_run(db_conn, id="r1", kind="implement", dedupe_key="implement-1")
    assert runs_dao.transition_run(db_conn, first.id, to_status="cancelled", now=T1)

    second = _create_run(db_conn, id="r2", kind="repair", dedupe_key="repair-1")

    assert [r.id for r in runs_dao.list_runs_for_task(db_conn, "t1")] == ["r1", "r2"]
    assert second.status == "queued"


def test_one_active_review_per_task_and_head_sha(db_conn):
    _make_task(db_conn)
    _create_run(
        db_conn,
        id="review-1",
        kind="review",
        agent="codex-cli",
        dedupe_key="review-1",
        review_target_sha="sha-a",
    )

    with pytest.raises(StateError, match="Run creation rejected"):
        _create_run(
            db_conn,
            id="review-2",
            kind="review",
            agent="codex-cli",
            dedupe_key="review-2",
            review_target_sha="sha-a",
        )


def test_reviews_for_different_shas_allowed(db_conn):
    _make_task(db_conn)

    _create_run(
        db_conn,
        id="review-a",
        kind="review",
        agent="codex-cli",
        dedupe_key="review-a",
        review_target_sha="sha-a",
    )
    review_b = _create_run(
        db_conn,
        id="review-b",
        kind="review",
        agent="codex-cli",
        dedupe_key="review-b",
        review_target_sha="sha-b",
    )

    assert review_b.review_target_sha == "sha-b"


def test_exactly_one_repair_per_trigger_review(db_conn):
    _make_task(db_conn)
    review = _create_run(
        db_conn,
        id="review-1",
        kind="review",
        agent="codex-cli",
        dedupe_key="review-1",
        review_target_sha="sha-a",
    )
    assert runs_dao.transition_run(db_conn, review.id, to_status="claimed", now=T1)
    assert runs_dao.transition_run(db_conn, review.id, to_status="running", now=T2)
    assert runs_dao.transition_run(db_conn, review.id, to_status="succeeded", now=T3)
    repair = _create_run(
        db_conn,
        id="repair-1",
        kind="repair",
        dedupe_key="repair-1",
        trigger_run_id=review.id,
    )
    assert runs_dao.transition_run(db_conn, repair.id, to_status="cancelled", now=T3)

    with pytest.raises(StateError, match="Run creation rejected"):
        _create_run(
            db_conn,
            id="repair-2",
            kind="repair",
            dedupe_key="repair-2",
            trigger_run_id=review.id,
        )


def test_inbox_row_can_reference_run_id(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)

    inbox = inbox_dao.enqueue(
        db_conn,
        id="i1",
        task_id="t1",
        target_agent="claude-code",
        run_id=run.id,
        now=T0,
    )
    linked = runs_dao.link_inbox(db_conn, run_id=run.id, inbox_id=inbox.id)

    assert inbox_dao.get(db_conn, "i1").run_id == run.id
    assert linked.inbox_id == "i1"


def test_inbox_run_linkage_requires_matching_task(db_conn):
    _make_task(db_conn, "t1")
    _make_task(db_conn, "t2")
    run = _create_run(db_conn, task_id="t1")
    inbox = inbox_dao.enqueue(
        db_conn, id="i1", task_id="t2", target_agent="claude-code", now=T0
    )

    with pytest.raises(StateError, match="belongs to task"):
        runs_dao.link_inbox(db_conn, run_id=run.id, inbox_id=inbox.id)

    assert inbox_dao.get(db_conn, inbox.id).run_id is None
    assert runs_dao.get_run(db_conn, run.id).inbox_id is None


def test_inbox_enqueue_run_id_requires_existing_matching_run(db_conn):
    _make_task(db_conn, "t1")
    _make_task(db_conn, "t2")
    run = _create_run(db_conn, task_id="t1")

    with pytest.raises(StateError, match="Run 'missing' not found"):
        inbox_dao.enqueue(
            db_conn,
            id="missing-run",
            task_id="t1",
            target_agent="claude-code",
            run_id="missing",
            now=T0,
        )
    with pytest.raises(StateError, match="belongs to task"):
        inbox_dao.enqueue(
            db_conn,
            id="wrong-task",
            task_id="t2",
            target_agent="claude-code",
            run_id=run.id,
            now=T0,
        )


def test_legacy_inbox_row_with_null_run_id_remains_valid(db_conn):
    _make_task(db_conn)

    inbox = inbox_dao.enqueue(
        db_conn, id="legacy-i1", task_id="t1", target_agent="claude-code", now=T0
    )

    assert inbox.run_id is None
    assert inbox_dao.get(db_conn, "legacy-i1").run_id is None


def test_structured_result_valid(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)

    updated = runs_dao.record_run_result(
        db_conn,
        run.id,
        {
            "schema_version": 1,
            "run_id": run.id,
            "task_id": run.task_id,
            "kind": run.kind,
            "agent": run.agent,
            "exit_code": 0,
            "completion_status": "completed",
            "worktree_path": "/tmp/worktree",
            "branch_name": "task/t1",
            "base_sha": "base",
            "head_sha": "head",
            "dirty": True,
            "changed_files": ["src/app.py"],
        },
        now=T1,
    )

    assert updated.exit_code == 0
    assert updated.head_sha == "head"
    assert updated.result_json["run_id"] == run.id


def test_mismatched_run_id_rejected(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)

    with pytest.raises(BoundaryError, match="run_id"):
        runs_dao.record_run_result(
            db_conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": "wrong",
                "task_id": run.task_id,
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": "/tmp/worktree",
                "branch_name": "task/t1",
                "base_sha": "base",
                "head_sha": "head",
                "dirty": False,
            },
        )


def test_mismatched_task_id_rejected(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)

    with pytest.raises(BoundaryError, match="task_id"):
        runs_dao.record_run_result(
            db_conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": "wrong",
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": "/tmp/worktree",
                "branch_name": "task/t1",
                "base_sha": "base",
                "head_sha": "head",
                "dirty": False,
            },
        )


def test_invalid_review_verdict_usage_rejected(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn, kind="implement")

    with pytest.raises(ValidationError, match="review_verdict"):
        runs_dao.record_run_result(
            db_conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": run.task_id,
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "review_verdict": "REJECTED",
                "reviewed_sha": "sha-a",
            },
        )


def test_review_result_requires_matching_reviewed_sha(db_conn):
    _make_task(db_conn)
    run = _create_run(
        db_conn,
        id="review-1",
        kind="review",
        agent="codex-cli",
        dedupe_key="review-1",
        review_target_sha="sha-a",
    )

    with pytest.raises(BoundaryError, match="reviewed_sha"):
        runs_dao.record_run_result(
            db_conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": run.task_id,
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "review_verdict": "LGTM",
                "reviewed_sha": "sha-b",
            },
        )


def test_mark_run_consumed_idempotent(db_conn):
    _make_task(db_conn)
    run = _create_run(db_conn)
    assert runs_dao.transition_run(db_conn, run.id, to_status="cancelled", now=T1)

    assert [r.id for r in runs_dao.list_unconsumed_finished_runs(db_conn)] == [run.id]
    assert runs_dao.mark_run_consumed(db_conn, run.id, now=T2)
    assert runs_dao.mark_run_consumed(db_conn, run.id, now=T3)

    consumed = runs_dao.get_run(db_conn, run.id)
    assert consumed is not None
    assert consumed.orchestrator_consumed_at == T2
    assert runs_dao.list_unconsumed_finished_runs(db_conn) == []


def test_reliable_orchestrator_feature_gate_is_opt_in(db_conn):
    _make_task(db_conn, "legacy")
    _make_task(db_conn, "reliable")
    db_conn.execute(
        "UPDATE tasks SET workflow='reliable-orchestrator' WHERE id='reliable'"
    )

    legacy = db_conn.execute("SELECT * FROM tasks WHERE id='legacy'").fetchone()
    reliable = db_conn.execute("SELECT * FROM tasks WHERE id='reliable'").fetchone()

    assert not is_reliable_orchestrated_task(legacy)
    assert is_reliable_orchestrated_task(reliable)
    assert is_reliable_orchestrated_task({"extras_json": '{"reliable_orchestrator": true}'})


def test_migration_from_v39_database_succeeds():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL, "
        "version INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE inbox (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, target_agent TEXT NOT NULL, "
        "status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 2, retry_count INTEGER NOT NULL DEFAULT 0, "
        "max_retries INTEGER NOT NULL DEFAULT 3, pid INTEGER, project_path TEXT, "
        "plan_only INTEGER NOT NULL DEFAULT 0, failed_reason TEXT, created_at TEXT NOT NULL, "
        "launched_at TEXT, last_heartbeat TEXT, paused_at TEXT, failed_at TEXT, done_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE handoffs (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, "
        "phase TEXT NOT NULL, status TEXT NOT NULL, from_agent TEXT, to_agent TEXT, "
        "content TEXT, metadata TEXT, created_at TEXT NOT NULL)"
    )
    conn.execute("PRAGMA user_version=39")

    init_db(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 40
    assert conn.execute("SELECT name FROM sqlite_master WHERE name='runs'").fetchone()
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(inbox)").fetchall()}
    assert "run_id" in columns
    conn.close()

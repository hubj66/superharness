from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from superharness.engine import agent_availability, runs_dao, tasks_dao
from superharness.engine.agent_selector import AgentSelector
from superharness.engine.db import get_connection, init_db
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator
from superharness.engine.shipper import ShipOutcome

NOW = "2026-01-01T00:00:00Z"
SOON = "2026-01-01T00:30:00Z"
LATER = "2026-01-01T02:00:00Z"
OLD = "2025-12-31T22:00:00Z"


class FakeShipper:
    def __init__(self, outcome: ShipOutcome) -> None:
        self.outcome = outcome

    def ship(self, **_kwargs):
        return self.outcome


def _project(tmp_path: Path, *, profile: str = ""):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    if profile:
        (project / ".superharness" / "profile.yaml").write_text(
            profile, encoding="utf-8"
        )
    conn = get_connection(str(project))
    init_db(conn)
    return project, conn


def _task(
    conn: sqlite3.Connection,
    task_id: str = "t1",
    *,
    status: str = "todo",
    owner: str = "claude-code",
    extras: dict | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO tasks (
            id, title, owner, status, version, created_at, workflow,
            acceptance_criteria, context, extras_json
        ) VALUES (?, ?, ?, ?, 1, ?, 'reliable-orchestrator', ?, ?, ?)
        """,
        (
            task_id,
            "Reliable task",
            owner,
            status,
            NOW,
            json.dumps(["tests pass"]),
            "Durable original scope.",
            json.dumps(extras or {}),
        ),
    )
    conn.commit()


def _metadata(*, sha: str = "sha-a", source_agent: str = "claude-code") -> dict:
    return {
        "reliable_orchestrator": {
            "branch_name": "shux/reliable/t1",
            "base_sha": "base",
            "head_sha": sha,
            "remote_head_sha": sha,
            "pr_head_sha": sha,
            "pr_number": 42,
            "pr_url": "https://github.com/hubj66/superharness/pull/42",
            "ship_run_id": "ship-1",
            "source_run_id": f"{source_agent}-source-run",
            "source_run_kind": "implement",
            "source_agent": source_agent,
        }
    }


def _finish_run(
    conn: sqlite3.Connection,
    run: runs_dao.RunRow,
    *,
    status: str = "succeeded",
    failure_category: str | None = None,
    failure_detail: str | None = None,
    head_sha: str = "head",
) -> None:
    runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    if status == "succeeded":
        runs_dao.record_run_result(
            conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": run.task_id,
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": run.worktree_path or "/tmp/worktree",
                "branch_name": run.branch_name or "shux/reliable/t1",
                "base_sha": run.base_sha or "base",
                "head_sha": head_sha,
                "dirty": run.kind in {"implement", "repair", "fallback"},
            },
            now=NOW,
        )
    runs_dao.transition_run(
        conn,
        run.id,
        to_status=status,
        now=NOW,
        failure_category=failure_category,
        failure_detail=failure_detail,
    )
    conn.commit()


def _failed_run(
    conn: sqlite3.Connection,
    *,
    agent: str = "claude-code",
    kind: str = "implement",
    category: str = "quota",
    status: str = "failed",
    attempt: int = 1,
) -> runs_dao.RunRow:
    run = runs_dao.create_run(
        conn,
        id=f"{kind}-{agent}-{category}-{attempt}",
        task_id="t1",
        kind=kind,
        agent=agent,
        model="gpt-5.5" if agent == "codex-cli" else None,
        dedupe_key=f"{kind}:t1:{agent}:{category}:{attempt}",
        attempt=attempt,
        base_sha="base",
        head_sha="head",
        result_json={"prompt": "do work"},
        now=NOW,
    )
    _finish_run(
        conn,
        run,
        status=status,
        failure_category=category,
        failure_detail=category,
    )
    return runs_dao.get_run(conn, run.id) or run


def _finish_review(
    conn: sqlite3.Connection,
    review: runs_dao.RunRow,
    *,
    verdict: str = "REJECTED",
    sha: str = "sha-a",
) -> None:
    runs_dao.transition_run(conn, review.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, review.id, to_status="running", now=NOW)
    runs_dao.record_run_result(
        conn,
        review.id,
        {
            "schema_version": 1,
            "run_id": review.id,
            "task_id": review.task_id,
            "kind": "review",
            "agent": review.agent,
            "exit_code": 0,
            "completion_status": "completed",
            "review_verdict": verdict,
            "reviewed_sha": sha,
            "findings": ["needs a focused fix"],
        },
        now=NOW,
    )
    runs_dao.transition_run(conn, review.id, to_status="succeeded", now=NOW)
    conn.commit()


def test_agent_availability_marks_quota_auth_network_and_success(db_conn):
    agent_availability.mark_failure(
        db_conn,
        "claude-code",
        category="quota",
        detail="retry in 30 minutes",
        now=NOW,
        source_run_id=None,
    )
    record = agent_availability.get(db_conn, "claude-code")
    assert record is not None
    assert record.state == "temporarily_blocked"
    assert not agent_availability.is_selectable(db_conn, "claude-code", now=NOW)
    assert agent_availability.is_selectable(db_conn, "claude-code", now=LATER)

    agent_availability.mark_failure(
        db_conn,
        "claude-code",
        category="auth",
        detail="login required",
        now=NOW,
        source_run_id=None,
    )
    record = agent_availability.get(db_conn, "claude-code")
    assert record is not None
    assert record.state == "auth_blocked"
    assert record.blocked_until == "2026-01-01T01:00:00Z"
    assert not agent_availability.is_selectable(db_conn, "claude-code", now=SOON)
    assert agent_availability.is_selectable(db_conn, "claude-code", now=LATER)

    agent_availability.mark_failure(
        db_conn,
        "codex-cli",
        category="network",
        detail="connection reset",
        now=NOW,
        source_run_id=None,
    )
    assert agent_availability.is_selectable(db_conn, "codex-cli", now=NOW)

    agent_availability.mark_success(
        db_conn, "claude-code", now=LATER, source_run_id=None
    )
    assert agent_availability.get(db_conn, "claude-code").state == "available"


def test_auth_block_cooldown_refresh_and_success_recovery(db_conn):
    agent_availability.mark_failure(
        db_conn,
        "codex-cli",
        category="auth",
        detail="agent authentication failed",
        now=OLD,
        source_run_id=None,
    )
    stale = agent_availability.get(db_conn, "codex-cli")
    assert stale is not None
    assert stale.state == "auth_blocked"
    assert agent_availability.is_selectable(db_conn, "codex-cli", now=NOW)

    agent_availability.mark_failure(
        db_conn,
        "codex-cli",
        category="auth",
        detail="agent authentication failed",
        now=NOW,
        source_run_id=None,
    )
    refreshed = agent_availability.get(db_conn, "codex-cli")
    assert refreshed is not None
    assert refreshed.state == "auth_blocked"
    assert refreshed.blocked_until == "2026-01-01T01:00:00Z"
    assert refreshed.last_failure_at == NOW
    assert refreshed.source_run_id is None
    assert not agent_availability.is_selectable(db_conn, "codex-cli", now=SOON)

    agent_availability.mark_success(
        db_conn,
        "codex-cli",
        now=LATER,
        source_run_id=None,
    )
    recovered = agent_availability.get(db_conn, "codex-cli")
    assert recovered is not None
    assert recovered.state == "available"
    assert recovered.blocked_until is None
    assert recovered.retry_after_at is None


def test_selector_prefers_claude_then_codex_and_honors_blocks(db_conn):
    selector = AgentSelector({})
    assert selector.select_mutator(db_conn, now=NOW).agent == "claude-code"
    agent_availability.mark_failure(
        db_conn,
        "claude-code",
        category="session_limit",
        detail="session limit",
        now=NOW,
        source_run_id=None,
    )
    assignment = selector.select_mutator(db_conn, now=NOW)
    assert assignment is not None
    assert assignment.agent == "codex-cli"
    assert assignment.model == "gpt-5.5"


def test_planning_uses_codex_when_claude_temporarily_blocked(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        agent_availability.mark_failure(
            conn,
            "claude-code",
            category="quota",
            detail="quota",
            now=NOW,
            source_run_id=None,
        )
        conn.commit()
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        orch.tick("t1")
        plans = runs_dao.list_runs_for_task(conn, "t1", kind="plan")
        assert len(plans) == 1
        assert plans[0].agent == "codex-cli"
        assert plans[0].model == "gpt-5.5"
    finally:
        conn.close()


def test_both_mutators_blocked_waits_without_creating_plan(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        for agent in ("claude-code", "codex-cli"):
            agent_availability.mark_failure(
                conn,
                agent,
                category="quota",
                detail="quota",
                now=NOW,
                source_run_id=None,
            )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert runs_dao.list_runs_for_task(conn, "t1") == []
        assert tasks_dao.get(conn, "t1").status == "todo"
    finally:
        conn.close()


def test_implementation_assignment_is_fixed_after_run_creation(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="plan_approved")
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        impl = runs_dao.list_runs_for_task(conn, "t1", kind="implement")[0]
        agent_availability.mark_failure(
            conn,
            "claude-code",
            category="quota",
            detail="quota",
            now=NOW,
            source_run_id=None,
        )
        conn.commit()
        orch.tick("t1")
        assert runs_dao.get_run(conn, impl.id).agent == "claude-code"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="implement")) == 1
    finally:
        conn.close()


def test_claude_quota_records_availability_and_creates_codex_fallback(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="in_progress")
        _failed_run(conn, agent="claude-code", category="quota", status="quota_blocked")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        availability = agent_availability.get(conn, "claude-code")
        fallback = runs_dao.list_runs_for_task(conn, "t1", kind="fallback")
        assert availability is not None
        assert availability.state == "temporarily_blocked"
        assert len(fallback) == 1
        assert fallback[0].agent == "codex-cli"
    finally:
        conn.close()


def test_codex_failure_uses_claude_alternate_without_ping_pong(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="in_progress", owner="codex-cli")
        _failed_run(conn, agent="codex-cli", kind="implement", category="agent_crash")
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        fallback = runs_dao.list_runs_for_task(conn, "t1", kind="fallback")[0]
        assert fallback.agent == "claude-code"
        _finish_run(
            conn,
            fallback,
            status="failed",
            failure_category="network",
            failure_detail="network",
        )
        orch.tick("t1")
        retry = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="fallback"),
            key=lambda run: run.attempt,
        )
        assert retry.agent == "claude-code"
        assert retry.attempt == 2
    finally:
        conn.close()


def test_reviewer_excludes_source_agent_and_waits_if_only_self_available(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", extras=_metadata(source_agent="codex-cli"))
        agent_availability.mark_failure(
            conn,
            "claude-code",
            category="quota",
            detail="quota",
            now=NOW,
            source_run_id=None,
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert runs_dao.list_runs_for_task(conn, "t1", kind="review") == []
        assert tasks_dao.get(conn, "t1").status == "pr_open"

        agent_availability.mark_success(
            conn, "claude-code", now=LATER, source_run_id=None
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: LATER).tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        assert review.agent == "claude-code"
        assert review.review_target_sha == "sha-a"
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


def test_pr_open_with_fresh_codex_auth_block_waits_without_self_review(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", extras=_metadata(source_agent="claude-code"))
        agent_availability.mark_failure(
            conn,
            "codex-cli",
            category="auth",
            detail="agent authentication failed",
            now=NOW,
            source_run_id=None,
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert runs_dao.list_runs_for_task(conn, "t1", kind="review") == []
        assert tasks_dao.get(conn, "t1").status == "pr_open"
    finally:
        conn.close()


def test_pr_open_with_expired_codex_auth_block_creates_one_review(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", extras=_metadata(source_agent="claude-code"))
        agent_availability.mark_failure(
            conn,
            "codex-cli",
            category="auth",
            detail="agent authentication failed",
            now=OLD,
            source_run_id=None,
        )
        conn.commit()
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        orch.tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        task = tasks_dao.get(conn, "t1")
        assert len(reviews) == 1
        assert reviews[0].agent == "codex-cli"
        assert reviews[0].review_target_sha == "sha-a"
        assert task is not None and task.status == "review_requested"
    finally:
        conn.close()


def test_stale_auth_recovery_works_after_reopening_database(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", extras=_metadata(source_agent="claude-code"))
        agent_availability.mark_failure(
            conn,
            "codex-cli",
            category="auth",
            detail="agent authentication failed",
            now=OLD,
            source_run_id=None,
        )
        conn.commit()
    finally:
        conn.close()

    reopened = get_connection(str(project))
    try:
        availability = agent_availability.get(reopened, "codex-cli")
        assert availability is not None
        assert availability.state == "auth_blocked"
        assert agent_availability.is_selectable(reopened, "codex-cli", now=NOW)
    finally:
        reopened.close()

    orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
    orch.tick("t1")
    conn = get_connection(str(project))
    try:
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 1
        assert reviews[0].agent == "codex-cli"
        assert reviews[0].review_target_sha == "sha-a"
    finally:
        conn.close()


def test_expired_auth_block_allows_codex_mutator_when_claude_blocked(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        agent_availability.mark_failure(
            conn,
            "claude-code",
            category="quota",
            detail="quota",
            now=NOW,
            source_run_id=None,
        )
        agent_availability.mark_failure(
            conn,
            "codex-cli",
            category="auth",
            detail="agent authentication failed",
            now=OLD,
            source_run_id=None,
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        plans = runs_dao.list_runs_for_task(conn, "t1", kind="plan")
        assert len(plans) == 1
        assert plans[0].agent == "codex-cli"
    finally:
        conn.close()


def test_repair_prefers_source_agent_but_uses_alternate_when_blocked(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", extras=_metadata(source_agent="claude-code"))
        agent_availability.mark_failure(
            conn,
            "claude-code",
            category="quota",
            detail="quota",
            now=NOW,
            source_run_id=None,
        )
        conn.commit()
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _finish_review(conn, review, verdict="REJECTED", sha="sha-a")
        orch.tick("t1")
        repair = runs_dao.list_runs_for_task(conn, "t1", kind="repair")[0]
        assert repair.agent == "codex-cli"
        assert repair.trigger_run_id == review.id
    finally:
        conn.close()


def test_codex_repair_head_gets_claude_review_after_ship(tmp_path):
    project, conn = _project(tmp_path)
    shipper = FakeShipper(
        ShipOutcome(
            ok=True,
            branch_name="shux/reliable/t1",
            base_sha="repair-head",
            head_sha="sha-b",
            remote_head_sha="sha-b",
            pr_number=42,
            pr_url="https://github.com/hubj66/superharness/pull/42",
            worktree_path="/tmp/superharness-worktrees/reliable/project/t1",
        )
    )
    try:
        _task(conn, status="in_progress")
        repair = runs_dao.create_run(
            conn,
            id="repair-codex",
            task_id="t1",
            kind="repair",
            agent="codex-cli",
            model="gpt-5.5",
            dedupe_key="repair:t1:codex",
            worktree_path="/tmp/worktree",
            branch_name="shux/reliable/t1",
            base_sha="sha-a",
            head_sha="repair-head",
            now=NOW,
        )
        _finish_run(conn, repair, head_sha="repair-head")
        orch = LifecycleOrchestrator(
            str(project), now=lambda: NOW, shipper_factory=lambda _project: shipper
        )
        orch.tick("t1")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_requested"
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        assert review.agent == "claude-code"
        metadata = json.loads(task.extras_json)["reliable_orchestrator"]
        assert metadata["source_agent"] == "codex-cli"
        assert metadata["source_run_id"] == "repair-codex"
    finally:
        conn.close()


def test_v41_to_v42_availability_migration_is_additive_and_idempotent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    conn.execute("PRAGMA user_version=41")
    init_db(conn)
    init_db(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 42
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name='agent_availability'"
    ).fetchone()
    conn.close()

from __future__ import annotations

import os
from pathlib import Path

import pytest

from superharness.commands.inbox_dispatch import DispatchContext, _reliable_run_finished
from superharness.engine import inbox_dao, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.failure_classifier import classify_reliable
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator
from superharness.engine.process import probe_process, process_starttime

NOW = "2026-01-01T00:00:00Z"


def _project(tmp_path: Path, *, status: str = "in_progress"):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id,title,owner,status,version,created_at,workflow) "
        "VALUES ('t1','Task','claude-code',?,?,?,'reliable-orchestrator')",
        (status, 1, NOW),
    )
    conn.commit()
    return project, conn


def _failed_run(
    conn,
    *,
    category: str,
    attempt: int = 1,
    kind: str = "implement",
    terminal_status: str = "failed",
    pid: int | None = None,
    pid_starttime: str | None = None,
    review_target_sha: str | None = None,
):
    agent = "codex-cli" if kind in {"fallback", "review"} else "claude-code"
    run = runs_dao.create_run(
        conn,
        id=f"run-{kind}-{attempt}-{category}",
        task_id="t1",
        kind=kind,
        agent=agent,
        model="gpt-5.5" if agent == "codex-cli" else None,
        dedupe_key=f"{kind}:t1:{attempt}:{category}",
        attempt=attempt,
        base_sha="base-sha",
        result_json={"prompt": "original prompt"},
        pid=pid,
        pid_starttime=pid_starttime,
        review_target_sha=review_target_sha,
        now=NOW,
    )
    runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    runs_dao.transition_run(
        conn,
        run.id,
        to_status=terminal_status,
        now=NOW,
        failure_category=category,
        failure_detail=category,
    )
    conn.commit()
    return run.id


def test_classifier_uses_narrow_reliable_categories():
    assert (
        classify_reliable(log_tail="You've hit your session limit").category
        == "session_limit"
    )
    assert classify_reliable(log_tail="quota exceeded").category == "quota"
    assert (
        classify_reliable(
            log_tail="You've hit your usage limit, try again later"
        ).category
        == "quota"
    )
    assert classify_reliable(launcher_rc=139).category == "agent_crash"
    assert classify_reliable(log_tail="connection reset by peer").category == "network"
    assert (
        classify_reliable(
            launcher_rc=1,
            log_tail="model is not supported when using Codex with a ChatGPT account",
        ).category
        == "auth"
    )
    assert (
        classify_reliable(launcher_rc=1, log_tail="ordinary failure").category
        == "unknown"
    )


def test_claude_quota_immediately_creates_one_codex_fallback(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _failed_run(conn, category="quota")
        orchestrator = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orchestrator.tick("t1")
        orchestrator.tick("t1")
        fallback = runs_dao.list_runs_for_task(conn, "t1", kind="fallback")
        assert len(fallback) == 1
        assert fallback[0].agent == "codex-cli"
        assert fallback[0].model == "gpt-5.5"
        assert fallback[0].trigger_run_id is not None
    finally:
        conn.close()


def test_quota_blocked_status_routes_without_claude_retry(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _failed_run(conn, category="quota", terminal_status="quota_blocked")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="implement")) == 1
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="fallback")) == 1
    finally:
        conn.close()


def test_claude_session_limit_and_sigsegv_do_not_retry_claude(tmp_path):
    for category in ("session_limit", "agent_crash", "hang"):
        project, conn = _project(tmp_path / category)
        try:
            _failed_run(conn, category=category)
            LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
            assert (
                max(
                    run.attempt
                    for run in runs_dao.list_runs_for_task(conn, "t1", kind="implement")
                )
                == 1
            )
            assert len(runs_dao.list_runs_for_task(conn, "t1", kind="fallback")) == 1
        finally:
            conn.close()


def test_claude_network_retries_once_then_blocks(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _failed_run(conn, category="network")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        retries = runs_dao.list_runs_for_task(conn, "t1", kind="implement")
        assert len(retries) == 2
        retry = max(retries, key=lambda run: run.attempt)
        assert retry.attempt == 2
        runs_dao.transition_run(conn, retry.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, retry.id, to_status="running", now=NOW)
        runs_dao.transition_run(
            conn,
            retry.id,
            to_status="failed",
            now=NOW,
            failure_category="network",
            failure_detail="network",
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert tasks_dao.get(conn, "t1").status == "blocked"
    finally:
        conn.close()


def test_process_identity_rejects_pid_reuse_and_detects_dead_pid():
    start = process_starttime(os.getpid())
    assert start is not None
    assert probe_process(os.getpid(), start) == "live"
    assert probe_process(os.getpid(), "different-start-time") == "reused"
    assert probe_process(99999999, "1") == "dead"


def test_live_run_is_observed_without_age_failure(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        run = runs_dao.create_run(
            conn,
            id="run-live",
            task_id="t1",
            kind="implement",
            agent="claude-code",
            dedupe_key="implement:t1:live",
            now=NOW,
            base_sha="base",
            pid=os.getpid(),
            pid_starttime="start",
        )
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
        conn.commit()
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.probe_process",
            lambda pid, start: "live",
        )
        LifecycleOrchestrator(str(project), now=lambda: "2026-01-01T01:00:00Z").tick(
            "t1"
        )
        assert runs_dao.get_run(conn, run.id).status == "running"
    finally:
        conn.close()


@pytest.mark.parametrize(
    "category",
    ["quota", "session_limit", "agent_crash", "hang", "network", "unknown"],
)
def test_failed_claude_run_with_verified_live_owner_does_not_route(
    tmp_path, monkeypatch, category
):
    project, conn = _project(tmp_path)
    try:
        _failed_run(conn, category=category, pid=os.getpid(), pid_starttime="start")
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.probe_process",
            lambda _pid, _start: "live",
        )
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="fallback")) == 0
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="implement")) == 1
    finally:
        conn.close()


def test_claimed_launched_run_without_pid_is_not_requeued_after_spawn_race(tmp_path):
    project, conn = _project(tmp_path)
    try:
        run = runs_dao.create_run(
            conn,
            id="run-claimed-race",
            task_id="t1",
            kind="implement",
            agent="claude-code",
            dedupe_key="implement:t1:claimed-race",
            now=NOW,
        )
        inbox = inbox_dao.enqueue(
            conn,
            id="inbox-claimed-race",
            task_id="t1",
            target_agent="claude-code",
            project_path=str(project),
            run_id=run.id,
            now=NOW,
        )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox.id)
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
        inbox_dao.update_status(
            conn, inbox.id, from_status="pending", to_status="launched", now=NOW
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        refreshed_run = runs_dao.get_run(conn, run.id)
        refreshed_inbox = inbox_dao.get(conn, inbox.id)
        assert refreshed_run is not None and refreshed_run.status == "claimed"
        assert refreshed_run.failure_category == "lost_process"
        assert refreshed_inbox is not None and refreshed_inbox.status == "launched"
    finally:
        conn.close()


def test_dead_child_during_dispatcher_finalization_does_not_false_crash(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    try:
        run = runs_dao.create_run(
            conn,
            id="run-finalizing",
            task_id="t1",
            kind="implement",
            agent="claude-code",
            model="claude-sonnet-4-6",
            dedupe_key="implement:t1:finalizing",
            now=NOW,
            pid=99999999,
            pid_starttime="1",
        )
        inbox = inbox_dao.enqueue(
            conn,
            id="inbox-finalizing",
            task_id="t1",
            target_agent="claude-code",
            project_path=str(project),
            run_id=run.id,
            now=NOW,
        )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox.id)
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
        inbox_dao.update_status(
            conn, inbox.id, from_status="pending", to_status="launched", now=NOW
        )
        conn.commit()
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.probe_process",
            lambda _pid, _start: "dead",
        )

        LifecycleOrchestrator(str(project), now=lambda: "2026-01-01T00:00:30Z").tick(
            "t1"
        )

        active = runs_dao.get_run(conn, run.id)
        assert active is not None and active.status == "running"
        assert active.failure_detail != "process probe: dead"

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
            item_id=inbox.id,
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            task_log=str(project / ".superharness" / "launcher-logs" / "run.log"),
            run_id=run.id,
            item={"plan_only": False},
        )
        ctx.launcher_rc = 1
        _reliable_run_finished(ctx)

        finished = runs_dao.get_run(conn, run.id)
        assert finished is not None
        assert finished.status == "failed"
        assert finished.exit_code == 1
        assert finished.failure_detail is not None
        assert "exit code 1" in finished.failure_detail
        assert finished.failure_detail != "process probe: dead"
        assert inbox_dao.get(conn, inbox.id).status == "failed"
        assert runs_dao.list_runs_for_task(conn, "t1", kind="fallback") == []
    finally:
        conn.close()


def test_dead_child_after_finalization_grace_is_reconciled_as_crash(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    try:
        run = runs_dao.create_run(
            conn,
            id="run-abandoned",
            task_id="t1",
            kind="implement",
            agent="claude-code",
            model="claude-sonnet-4-6",
            dedupe_key="implement:t1:abandoned",
            now=NOW,
            pid=99999999,
            pid_starttime="1",
        )
        inbox = inbox_dao.enqueue(
            conn,
            id="inbox-abandoned",
            task_id="t1",
            target_agent="claude-code",
            project_path=str(project),
            run_id=run.id,
            now=NOW,
        )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox.id)
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
        inbox_dao.update_status(
            conn, inbox.id, from_status="pending", to_status="launched", now=NOW
        )
        conn.commit()
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.probe_process",
            lambda _pid, _start: "dead",
        )

        LifecycleOrchestrator(str(project), now=lambda: "2026-01-01T00:01:01Z").tick(
            "t1"
        )

        refreshed = runs_dao.get_run(conn, run.id)
        assert refreshed is not None
        assert refreshed.status == "crashed"
        assert refreshed.failure_category == "agent_crash"
        assert refreshed.failure_detail == "process probe: dead"
    finally:
        conn.close()


@pytest.mark.parametrize("category", ["agent_crash", "network", "invalid_result"])
def test_codex_review_execution_failure_retries_once_same_sha(tmp_path, category):
    project, conn = _project(tmp_path, status="review_requested")
    try:
        run_id = _failed_run(
            conn,
            category=category,
            kind="review",
            terminal_status="failed",
            review_target_sha="sha-a",
        )
        conn.execute(
            "UPDATE runs SET base_sha='sha-a', head_sha='sha-a' WHERE id=?", (run_id,)
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 2
        assert {run.review_target_sha for run in reviews} == {"sha-a"}
        retry = max(reviews, key=lambda run: run.attempt)
        assert retry.attempt == 2
        assert retry.agent == "codex-cli"
        assert tasks_dao.get(conn, "t1").status == "review_requested"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="repair")) == 0
    finally:
        conn.close()


def test_codex_review_retry_is_bounded(tmp_path):
    project, conn = _project(tmp_path, status="review_requested")
    try:
        _failed_run(
            conn,
            category="network",
            kind="review",
            terminal_status="failed",
            review_target_sha="sha-a",
        )
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        retry = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )
        runs_dao.transition_run(conn, retry.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, retry.id, to_status="running", now=NOW)
        runs_dao.transition_run(
            conn,
            retry.id,
            to_status="failed",
            now=NOW,
            failure_category="network",
            failure_detail="network",
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 2
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="repair")) == 0
    finally:
        conn.close()


def test_codex_fallback_retry_is_bounded_and_never_returns_to_claude(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _failed_run(conn, category="network", kind="fallback", attempt=1)
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        retry = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="fallback"),
            key=lambda run: run.attempt,
        )
        assert retry.attempt == 2
        runs_dao.transition_run(conn, retry.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, retry.id, to_status="running", now=NOW)
        runs_dao.transition_run(
            conn,
            retry.id,
            to_status="failed",
            now=NOW,
            failure_category="network",
            failure_detail="network",
        )
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="fallback")) == 2
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="implement")) == 0
        assert tasks_dao.get(conn, "t1").status == "failed"
    finally:
        conn.close()


def test_codex_fallback_quota_blocks_without_retry(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _failed_run(
            conn,
            category="quota",
            kind="fallback",
            terminal_status="quota_blocked",
        )
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="fallback")) == 1
        assert tasks_dao.get(conn, "t1").status == "blocked"
    finally:
        conn.close()


def test_old_failed_inbox_row_remains_failed_after_later_fallback_success(tmp_path):
    project, conn = _project(tmp_path)
    try:
        old_run = runs_dao.create_run(
            conn,
            id="run-old",
            task_id="t1",
            kind="implement",
            agent="claude-code",
            dedupe_key="implement:t1:old",
            now=NOW,
        )
        old_inbox = inbox_dao.enqueue(
            conn,
            id="inbox-old",
            task_id="t1",
            target_agent="claude-code",
            run_id=old_run.id,
            now=NOW,
        )
        runs_dao.link_inbox(conn, run_id=old_run.id, inbox_id=old_inbox.id)
        inbox_dao.update_status(
            conn,
            old_inbox.id,
            from_status="pending",
            to_status="failed",
            now=NOW,
            reason="old failure",
        )
        runs_dao.transition_run(conn, old_run.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, old_run.id, to_status="running", now=NOW)
        runs_dao.transition_run(
            conn,
            old_run.id,
            to_status="failed",
            now=NOW,
            failure_category="agent_crash",
            failure_detail="old failure",
        )
        runs_dao.mark_run_consumed(conn, old_run.id, now=NOW)
        fallback = runs_dao.create_run(
            conn,
            id="run-fallback-success",
            task_id="t1",
            kind="fallback",
            agent="codex-cli",
            model="gpt-5.5",
            dedupe_key="fallback:t1:old:codex-cli",
            now=NOW,
        )
        new_inbox = inbox_dao.enqueue(
            conn,
            id="inbox-fallback-success",
            task_id="t1",
            target_agent="codex-cli",
            run_id=fallback.id,
            now=NOW,
        )
        runs_dao.link_inbox(conn, run_id=fallback.id, inbox_id=new_inbox.id)
        runs_dao.transition_run(conn, fallback.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, fallback.id, to_status="running", now=NOW)
        runs_dao.record_run_result(
            conn,
            fallback.id,
            {
                "schema_version": 1,
                "run_id": fallback.id,
                "task_id": "t1",
                "kind": "fallback",
                "agent": "codex-cli",
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": "/tmp/superharness-worktrees/t1",
                "branch_name": "shux/reliable/t1",
                "base_sha": "base",
                "head_sha": "head",
                "dirty": True,
            },
            now=NOW,
        )
        runs_dao.transition_run(conn, fallback.id, to_status="succeeded", now=NOW)
        conn.commit()
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        assert inbox_dao.get(conn, old_inbox.id).status == "failed"
        assert inbox_dao.get(conn, new_inbox.id).status == "done"
    finally:
        conn.close()


def test_reliable_task_over_180_minutes_is_not_auto_archived_but_legacy_still_is(
    tmp_path,
):
    from superharness.commands.inbox_watch import _auto_archive_stale_tasks

    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    try:
        init_db(conn)
        old = "2000-01-01T00:00:00Z"
        conn.execute(
            "INSERT INTO tasks (id,title,owner,status,version,created_at,in_progress_at,workflow) "
            "VALUES ('t1','Reliable','claude-code','in_progress',1,?,?,"
            "'reliable-orchestrator')",
            (old, old),
        )
        conn.execute(
            "INSERT INTO tasks (id,title,owner,status,version,created_at,in_progress_at,workflow) "
            "VALUES ('legacy','Legacy','claude-code','in_progress',1,?,?,NULL)",
            (old, old),
        )
        conn.commit()
        assert _auto_archive_stale_tasks(str(project)) == 1
        assert tasks_dao.get(conn, "t1").status == "in_progress"
        assert tasks_dao.get(conn, "legacy").status == "archived"
    finally:
        conn.close()

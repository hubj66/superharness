from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from superharness.engine import agent_availability, inbox_dao, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator
from superharness.engine.reliable_worktree import ManagedWorktree
from superharness.engine.shipper import ShipOutcome
from superharness.engine.state_errors import StateError

NOW = "2026-01-01T00:00:00Z"
LATER = "2026-01-01T02:00:00Z"
R6_SHA = "55ca1b066c9956a743d0bb3aa951a540ef41f819"


class FakeShipper:
    def __init__(self, outcomes: list[ShipOutcome]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    def ship(self, **_kwargs):
        self.calls += 1
        return self.outcomes.pop(0)


def _project(tmp_path: Path):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    return project, conn


def _metadata(sha: str = "sha-a", *, source_agent: str = "claude-code") -> str:
    return json.dumps(
        {
            "reliable_orchestrator": {
                "branch_name": "shux/reliable/t1",
                "base_sha": "base",
                "head_sha": sha,
                "remote_head_sha": sha,
                "pr_head_sha": sha,
                "pr_number": 42,
                "pr_url": "https://github.com/o/r/pull/42",
                "ship_run_id": "ship-1",
                "source_agent": source_agent,
            }
        }
    )


def _task(
    conn,
    *,
    status: str = "pr_open",
    sha: str = "sha-a",
    source_agent: str = "claude-code",
) -> None:
    conn.execute(
        """
        INSERT INTO tasks (
            id, title, owner, status, version, created_at, workflow,
            acceptance_criteria, context, extras_json
        ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
        """,
        (
            "t1",
            "Fix the checkout flow",
            "claude-code",
            status,
            NOW,
            "reliable-orchestrator",
            json.dumps(["checkout succeeds", "tests pass"]),
            "Original durable task context.",
            _metadata(sha, source_agent=source_agent),
        ),
    )
    conn.commit()


def _complete_review(
    conn,
    run: runs_dao.RunRow,
    *,
    verdict: str = "LGTM",
    sha: str | None = "sha-a",
    findings: list[str] | None = None,
) -> None:
    runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    payload = {
        "schema_version": 1,
        "run_id": run.id,
        "task_id": run.task_id,
        "kind": "review",
        "agent": run.agent,
        "exit_code": 0,
        "completion_status": "completed",
        "review_verdict": verdict,
        "findings": findings or [],
    }
    if sha is not None:
        payload["reviewed_sha"] = sha
    # Echo authoritative identity so strict validate_result_for_run passes.
    for field in ("worktree_path", "branch_name", "base_sha", "head_sha"):
        value = getattr(run, field, None)
        if value is not None:
            payload[field] = value
    runs_dao.record_run_result(conn, run.id, payload, now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="succeeded", now=NOW)
    conn.commit()


def _fail_review(
    conn,
    run: runs_dao.RunRow,
    *,
    category: str = "network",
    status: str = "failed",
) -> None:
    if run.status == "queued":
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    run = runs_dao.get_run(conn, run.id) or run
    if run.status == "claimed":
        runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    runs_dao.transition_run(
        conn,
        run.id,
        to_status=status,
        now=NOW,
        failure_category=category,
        failure_detail=category,
    )
    conn.commit()


def _complete_repair(conn, run: runs_dao.RunRow) -> None:
    runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
    runs_dao.record_run_result(
        conn,
        run.id,
        {
            "schema_version": 1,
            "run_id": run.id,
            "task_id": run.task_id,
            "kind": "repair",
            "agent": run.agent,
            "exit_code": 0,
            "completion_status": "completed",
            "worktree_path": run.worktree_path or "/tmp/superharness-worktrees/t1",
            "branch_name": run.branch_name or "shux/reliable/t1",
            "base_sha": run.base_sha or "sha-a",
            "head_sha": run.head_sha or run.base_sha or "sha-a",
            "dirty": True,
        },
        now=NOW,
    )
    runs_dao.transition_run(conn, run.id, to_status="succeeded", now=NOW)
    conn.commit()


def _set_pr_head(conn, sha: str) -> None:
    task = tasks_dao.get(conn, "t1")
    assert task is not None
    tasks_dao.update(conn, task.id, task.version, {"extras_json": _metadata(sha)})
    conn.commit()


def _failed_unsafe_repair_ship(conn) -> runs_dao.RunRow:
    review = runs_dao.create_run(
        conn,
        id="review-trigger",
        task_id="t1",
        kind="review",
        agent="codex-cli",
        dedupe_key="review:t1:sha-a:historical",
        review_target_sha="sha-a",
        now=NOW,
    )
    runs_dao.transition_run(conn, review.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, review.id, to_status="running", now=NOW)
    runs_dao.transition_run(conn, review.id, to_status="succeeded", now=NOW)
    runs_dao.mark_run_consumed(conn, review.id, now=NOW)
    repair = runs_dao.create_run(
        conn,
        id="repair-corrupted",
        task_id="t1",
        kind="repair",
        agent="claude-code",
        dedupe_key="repair:t1:review-trigger",
        trigger_run_id=review.id,
        worktree_path="/historical/untrusted/path",
        branch_name="historical/untrusted-branch",
        base_sha="sha-b",
        head_sha="sha-a",
        now=NOW,
    )
    runs_dao.transition_run(conn, repair.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, repair.id, to_status="running", now=NOW)
    runs_dao.transition_run(conn, repair.id, to_status="succeeded", now=NOW)
    runs_dao.mark_run_consumed(conn, repair.id, now=NOW)
    ship = runs_dao.create_run(
        conn,
        id="ship-failed",
        task_id="t1",
        kind="ship",
        agent="system",
        dedupe_key="ship:t1:repair-corrupted:sha-a",
        parent_run_id=repair.id,
        now=NOW,
    )
    runs_dao.transition_run(conn, ship.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, ship.id, to_status="running", now=NOW)
    runs_dao.transition_run(
        conn,
        ship.id,
        to_status="failed",
        now=NOW,
        failure_category="unsafe_agent_commit",
        failure_detail="remote branch is sha-a, not recorded base sha-b",
    )
    runs_dao.mark_run_consumed(conn, ship.id, now=NOW)
    conn.commit()
    return runs_dao.get_run(conn, ship.id) or ship


def _consumed_exhausted_reviews(
    conn, *, category: str = "invalid_result", sha: str = "sha-a"
) -> tuple[runs_dao.RunRow, runs_dao.RunRow]:
    runs = []
    parent_id = None
    for attempt in (1, 2):
        run = runs_dao.create_run(
            conn,
            id=f"review-exhausted-{attempt}",
            task_id="t1",
            kind="review",
            agent="codex-cli",
            model="gpt-5.5",
            attempt=attempt,
            dedupe_key=f"review:t1:{sha}:attempt-{attempt}",
            parent_run_id=parent_id,
            trigger_run_id=parent_id,
            review_target_sha=sha,
            now=NOW,
        )
        runs_dao.transition_run(conn, run.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, run.id, to_status="running", now=NOW)
        runs_dao.transition_run(
            conn,
            run.id,
            to_status="failed",
            now=NOW,
            failure_category=category,
            failure_detail=category,
        )
        runs_dao.mark_run_consumed(conn, run.id, now=NOW)
        runs.append(runs_dao.get_run(conn, run.id) or run)
        parent_id = run.id
    conn.commit()
    return runs[0], runs[1]


def _allow_review_recovery(monkeypatch, path: str = "/tmp/review-sha-a") -> None:
    monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: True)
    monkeypatch.setattr(
        "superharness.engine.lifecycle_orchestrator.create_review_worktree",
        lambda project_dir, task_id, branch_name, review_target_sha: ManagedWorktree(
            path, None, review_target_sha
        ),
    )


def test_pr_open_creates_one_codex_review_for_exact_sha(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        orch.tick("t1")
        runs = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        inbox = inbox_dao.get_all(conn, status="pending")
        task = tasks_dao.get(conn, "t1")
        assert len(runs) == 1
        assert runs[0].agent == "codex-cli"
        assert runs[0].model == "gpt-5.5"
        assert runs[0].review_target_sha == "sha-a"
        assert "Review only" in runs[0].result_json["prompt"]
        assert "PR description" in runs[0].result_json["prompt"]
        assert (
            "dispatcher-provided structured result contract"
            in runs[0].result_json["prompt"]
        )
        assert (
            "review_verdict must be LGTM or REJECTED" in runs[0].result_json["prompt"]
        )
        assert "reviewed_sha must exactly equal" in runs[0].result_json["prompt"]
        assert len(inbox) == 1 and inbox[0].run_id == runs[0].id
        assert task is not None and task.status == "review_requested"
    finally:
        conn.close()


def test_active_review_for_current_sha_does_not_duplicate(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        orch.tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 1
        assert reviews[0].status == "queued"
        assert reviews[0].review_target_sha == "sha-a"
    finally:
        conn.close()


def test_lgtm_for_current_sha_moves_to_review_passed_only(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _complete_review(conn, review, verdict="LGTM", sha="sha-a")
        orch.tick("t1")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_passed"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="repair")) == 0
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="ship")) == 0
        metadata = json.loads(task.extras_json)["reliable_orchestrator"]
        assert metadata["last_reviewed_head_sha"] == "sha-a"
    finally:
        conn.close()


def test_successful_consumed_review_for_current_sha_does_not_duplicate(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _complete_review(conn, review, verdict="LGTM", sha="sha-a")
        orch.tick("t1")
        orch.tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 1
        assert tasks_dao.get(conn, "t1").status == "review_passed"
    finally:
        conn.close()


def test_failed_current_sha_review_retries_once_when_reviewer_selectable(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _fail_review(conn, review, category="network")
        orch.tick("t1")
        orch.tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        retry = max(reviews, key=lambda run: run.attempt)
        assert len(reviews) == 2
        assert retry.attempt == 2
        assert retry.trigger_run_id == review.id
        assert retry.review_target_sha == "sha-a"
        assert retry.agent == "codex-cli"
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


def test_auth_blocked_review_waits_then_recovers_once(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _fail_review(conn, review, category="auth")
        orch.tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 1
        availability = agent_availability.get(conn, "codex-cli")
        assert availability is not None and availability.state == "auth_blocked"

        recovered = LifecycleOrchestrator(str(project), now=lambda: LATER)
        recovered.tick("t1")
        recovered.tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        retry = max(reviews, key=lambda run: run.attempt)
        assert len(reviews) == 2
        assert retry.attempt == 2
        assert retry.review_target_sha == "sha-a"
        assert retry.status == "queued"
    finally:
        conn.close()


def test_consumed_auth_review_with_missing_recorded_worktree_recreates_attempt_2(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    stale_registered = {"value": True}
    managed_root = tmp_path / "managed-worktrees"
    worktree = managed_root / f"review-t1-{R6_SHA[:12]}"

    def fake_git(project_dir, *args, check=True):
        if project_dir == str(worktree) and args == ("rev-parse", "HEAD"):
            if stale_registered["value"]:
                raise StateError("missing review worktree")
            return subprocess.CompletedProcess(["git", *args], 0, R6_SHA + "\n", "")
        if project_dir == str(worktree) and args == (
            "status", "--porcelain=v1", "--untracked-files=normal"
        ):
            return subprocess.CompletedProcess(["git", *args], 0, "", "")
        if project_dir == str(worktree) and args == (
            "symbolic-ref", "--quiet", "--short", "HEAD"
        ):
            return subprocess.CompletedProcess(["git", *args], 1, "", "")
        assert project_dir == str(project)
        if args == ("fetch", "origin", "shux/reliable/t1"):
            return subprocess.CompletedProcess(["git", *args], 0, "", "")
        if args == (
            "rev-parse",
            "refs/remotes/origin/shux/reliable/t1^{commit}",
        ):
            return subprocess.CompletedProcess(["git", *args], 0, R6_SHA + "\n", "")
        if args == ("worktree", "list", "--porcelain"):
            stdout = f"worktree {project}\n\nworktree {worktree}\n"
            return subprocess.CompletedProcess(["git", *args], 0, stdout, "")
        if args == ("worktree", "add", "--detach", str(worktree), R6_SHA):
            if stale_registered["value"]:
                return subprocess.CompletedProcess(
                    ["git", *args],
                    128,
                    "",
                    "missing but already registered worktree",
                )
            worktree.mkdir(parents=True)
            return subprocess.CompletedProcess(["git", *args], 0, "", "")
        if args == ("worktree", "add", "-f", "--detach", str(worktree), R6_SHA):
            stale_registered["value"] = False
            worktree.mkdir(parents=True)
            return subprocess.CompletedProcess(["git", *args], 0, "", "")
        raise AssertionError(args)

    try:
        (project / ".git").mkdir()
        monkeypatch.setenv("SUPERHARNESS_WORKTREE_ROOT", str(managed_root))
        monkeypatch.setattr("superharness.engine.reliable_worktree._run_git", fake_git)
        _task(conn, status="review_requested", sha=R6_SHA)
        review = runs_dao.create_run(
            conn,
            id="run-a75e27df71191f81020a0e40",
            task_id="t1",
            kind="review",
            agent="codex-cli",
            model="gpt-5.5",
            dedupe_key=f"review:t1:{R6_SHA}",
            review_target_sha=R6_SHA,
            result_json={"prompt": "original review"},
            worktree_path=str(worktree),
            branch_name="shux/reliable/t1",
            base_sha=R6_SHA,
            head_sha=R6_SHA,
            pr_number=2,
            pr_url="https://github.com/o/r/pull/2",
            now=NOW,
        )
        assert not worktree.exists()
        _fail_review(conn, review, category="auth")

        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        consumed = runs_dao.get_run(conn, review.id)
        assert consumed is not None and consumed.orchestrator_consumed_at is not None
        availability = agent_availability.get(conn, "codex-cli")
        assert availability is not None and availability.state == "auth_blocked"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 1

        recovered = LifecycleOrchestrator(str(project), now=lambda: LATER)
        recovered.tick("t1")
        recovered.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        retry = max(reviews, key=lambda run: run.attempt)
        active = [run for run in reviews if run.status in runs_dao.ACTIVE_RUN_STATUSES]
        assert len(reviews) == 2
        assert len(active) == 1
        assert retry.agent == "codex-cli"
        assert retry.attempt == 2
        assert retry.trigger_run_id == review.id
        assert retry.review_target_sha == R6_SHA
        assert retry.worktree_path == str(worktree)
        assert retry.base_sha == R6_SHA
        assert retry.head_sha == R6_SHA
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


def test_review_retry_bound_prevents_third_attempt(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _fail_review(conn, review, category="network")
        orch.tick("t1")
        retry = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda run: run.attempt,
        )
        _fail_review(conn, retry, category="network")
        orch.tick("t1")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 2
    finally:
        conn.close()


def test_stale_failed_review_does_not_block_current_sha_review(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, sha="sha-b")
        stale = runs_dao.create_run(
            conn,
            id="review-sha-a",
            task_id="t1",
            kind="review",
            agent="codex-cli",
            model="gpt-5.5",
            dedupe_key="review:t1:sha-a",
            review_target_sha="sha-a",
            result_json={"prompt": "old review"},
            now=NOW,
        )
        _fail_review(conn, stale, category="network")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 2
        assert {run.review_target_sha for run in reviews} == {"sha-a", "sha-b"}
        current = next(run for run in reviews if run.review_target_sha == "sha-b")
        assert current.status == "queued"
    finally:
        conn.close()


def test_review_retry_does_not_self_review_sha_producer(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, source_agent="codex-cli")
        bad_review = runs_dao.create_run(
            conn,
            id="self-review",
            task_id="t1",
            kind="review",
            agent="codex-cli",
            model="gpt-5.5",
            dedupe_key="review:t1:sha-a",
            review_target_sha="sha-a",
            result_json={"prompt": "bad historical self review"},
            now=NOW,
        )
        _fail_review(conn, bad_review, category="network")
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 1
        assert reviews[0].agent == "codex-cli"
        assert tasks_dao.get(conn, "t1").status == "pr_open"
    finally:
        conn.close()


def test_rejected_current_sha_creates_exactly_one_claude_repair(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _complete_review(
            conn,
            review,
            verdict="REJECTED",
            sha="sha-a",
            findings=["checkout ignores coupons"],
        )
        orch.tick("t1")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        repairs = runs_dao.list_runs_for_task(conn, "t1", kind="repair")
        assert task is not None and task.status == "in_progress"
        assert len(repairs) == 1
        assert repairs[0].agent == "claude-code"
        assert repairs[0].trigger_run_id == review.id
        assert repairs[0].base_sha == "sha-a"
        assert "checkout ignores coupons" in repairs[0].result_json["prompt"]
        assert "Reviewed SHA: sha-a" in repairs[0].result_json["prompt"]
    finally:
        conn.close()


def test_repair_success_ships_and_new_pr_head_gets_fresh_review(tmp_path):
    project, conn = _project(tmp_path)
    shipper = FakeShipper(
        [
            ShipOutcome(
                ok=True,
                branch_name="shux/reliable/t1",
                base_sha="repair-head",
                head_sha="sha-b",
                remote_head_sha="sha-b",
                pr_number=42,
                pr_url="https://github.com/o/r/pull/42",
                worktree_path="/tmp/superharness-worktrees/reliable/project/t1",
            )
        ]
    )
    try:
        _task(conn)
        orch = LifecycleOrchestrator(
            str(project), now=lambda: NOW, shipper_factory=lambda _project: shipper
        )
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _complete_review(
            conn,
            review,
            verdict="REJECTED",
            sha="sha-a",
            findings=["needs repair"],
        )
        orch.tick("t1")
        repair = runs_dao.list_runs_for_task(conn, "t1", kind="repair")[0]
        _complete_repair(conn, repair)
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_requested"
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert [run.review_target_sha for run in reviews] == ["sha-a", "sha-b"]
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="ship")) == 1
    finally:
        conn.close()


def test_stale_lgtm_or_rejection_cannot_advance_or_create_repair(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _set_pr_head(conn, "sha-b")
        _complete_review(conn, review, verdict="LGTM", sha="sha-a")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "review_requested"
        assert len(reviews) == 2
        assert reviews[0].failure_category == "stale_review"
        assert reviews[1].review_target_sha == "sha-b"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="repair")) == 0
    finally:
        conn.close()


def test_blocked_or_invalid_review_result_does_not_advance(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        runs_dao.transition_run(conn, review.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, review.id, to_status="running", now=NOW)
        conn.execute(
            "UPDATE runs SET result_json=?, exit_code=0 WHERE id=?",
            (
                json.dumps({
                    "schema_version": 1,
                    "run_id": review.id,
                    "task_id": "t1",
                    "kind": "review",
                    "agent": review.agent,
                    "exit_code": 0,
                    "completion_status": "completed",
                    "review_verdict": "BLOCKED",
                    "reviewed_sha": "sha-a",
                }),
                review.id,
            ),
        )
        runs_dao.transition_run(conn, review.id, to_status="succeeded", now=NOW)
        conn.commit()
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_requested"
        assert runs_dao.get_run(conn, review.id).failure_category == "invalid_review_result"
        conn.execute("UPDATE inbox SET status=\"failed\" WHERE task_id=\"t1\" AND status IN (\"pending\", \"claimed\", \"launched\", \"running\")")
        conn.commit()

        _set_pr_head(conn, "sha-b")
        orch.tick("t1")
        new_review = next(
            run
            for run in runs_dao.list_runs_for_task(conn, "t1", kind="review")
            if run.review_target_sha == "sha-b"
        )
        runs_dao.transition_run(conn, new_review.id, to_status="claimed", now=NOW)
        runs_dao.transition_run(conn, new_review.id, to_status="running", now=NOW)
        conn.execute(
            "UPDATE runs SET result_json=? WHERE id=?",
            (
                json.dumps(
                    {
                        "schema_version": 1,
                        "run_id": new_review.id,
                        "task_id": "t1",
                        "kind": "review",
                        "agent": "codex-cli",
                        "exit_code": 0,
                        "completion_status": "completed",
                        "review_verdict": "LGTM",
                    }
                ),
                new_review.id,
            ),
        )
        runs_dao.transition_run(conn, new_review.id, to_status="succeeded", now=NOW)
        conn.commit()
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_requested"
        assert (
            runs_dao.get_run(conn, new_review.id).failure_category
            == "invalid_review_result"
        )
    finally:
        conn.close()


def test_unsafe_ship_recovers_once_from_remote_head_not_corrupted_run(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    recovered_path = "/tmp/superharness-worktrees/reliable/project/shux-reliable-t1"
    calls = []
    try:
        _task(conn, status="in_progress", sha="sha-a")
        failed_ship = _failed_unsafe_repair_ship(conn)

        def recover(project_dir, task_id, *, branch_name, expected_head_sha):
            calls.append((project_dir, task_id, branch_name, expected_head_sha))
            return ManagedWorktree(recovered_path, branch_name, expected_head_sha)

        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: True)
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.recover_repair_worktree",
            recover,
        )
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)

        orch.tick("t1")
        orch.tick("t1")

        repairs = runs_dao.list_runs_for_task(conn, "t1", kind="repair")
        assert len(repairs) == 2
        recovery = next(run for run in repairs if run.id != "repair-corrupted")
        assert recovery.trigger_run_id == failed_ship.id
        assert recovery.parent_run_id == failed_ship.id
        assert recovery.worktree_path == recovered_path
        assert recovery.branch_name == "shux/reliable/t1"
        assert recovery.base_sha == "sha-a"
        assert recovery.head_sha == "sha-a"
        assert recovery.base_sha != "sha-b"
        assert calls == [(str(project), "t1", "shux/reliable/t1", "sha-a")]
        assert runs_dao.get_run(conn, failed_ship.id).orchestrator_consumed_at == NOW
        assert tasks_dao.get(conn, "t1").status == "in_progress"
    finally:
        conn.close()


def test_recovered_repair_can_ship_and_create_review(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    recovered_path = "/tmp/superharness-worktrees/reliable/project/shux-reliable-t1"
    shipper = FakeShipper(
        [
            ShipOutcome(
                ok=True,
                branch_name="shux/reliable/t1",
                base_sha="sha-a",
                head_sha="sha-c",
                remote_head_sha="sha-c",
                pr_number=42,
                pr_url="https://github.com/o/r/pull/42",
                worktree_path=recovered_path,
            )
        ]
    )
    try:
        _task(conn, status="in_progress", sha="sha-a")
        _failed_unsafe_repair_ship(conn)
        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: True)
        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.recover_repair_worktree",
            lambda project_dir, task_id, branch_name, expected_head_sha: (
                ManagedWorktree(recovered_path, branch_name, expected_head_sha)
            ),
        )
        orch = LifecycleOrchestrator(
            str(project), now=lambda: NOW, shipper_factory=lambda _project: shipper
        )

        orch.tick("t1")
        recovery = next(
            run
            for run in runs_dao.list_runs_for_task(conn, "t1", kind="repair")
            if run.trigger_run_id == "ship-failed"
        )
        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: False)
        _complete_repair(conn, recovery)
        orch.tick("t1")
        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "review_requested"
        assert shipper.calls == 1
        assert any(review.review_target_sha == "sha-c" for review in reviews)
    finally:
        conn.close()


def test_unsafe_ship_without_safe_recovery_worktree_blocks_task(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="in_progress", sha="sha-a")
        failed_ship = _failed_unsafe_repair_ship(conn)
        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: True)

        def reject_recovery(*args, **kwargs):
            raise StateError("managed repair worktree is not based on remote PR head")

        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.recover_repair_worktree",
            reject_recovery,
        )

        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        task = tasks_dao.get(conn, "t1")
        ship = runs_dao.get_run(conn, failed_ship.id)
        assert task is not None and task.status == "blocked"
        assert "managed repair worktree" in (task.pause_reason or "")
        assert ship is not None and "managed repair worktree" in (
            ship.failure_detail or ""
        )
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="repair")) == 1
    finally:
        conn.close()


def test_unsafe_ship_without_pr_metadata_blocks_task(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="in_progress", sha="sha-a")
        failed_ship = _failed_unsafe_repair_ship(conn)
        task = tasks_dao.get(conn, "t1")
        assert task is not None
        tasks_dao.update(conn, task.id, task.version, {"extras_json": "{}"})
        conn.commit()

        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        task = tasks_dao.get(conn, "t1")
        ship = runs_dao.get_run(conn, failed_ship.id)
        assert task is not None and task.status == "blocked"
        assert "missing authoritative task PR metadata" in (task.pause_reason or "")
        assert ship is not None and "missing authoritative task PR metadata" in (
            ship.failure_detail or ""
        )
    finally:
        conn.close()


def test_ship_failure_without_safe_recovery_class_blocks_task(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="in_progress", sha="sha-a")
        failed_ship = _failed_unsafe_repair_ship(conn)
        conn.execute(
            "UPDATE runs SET failure_category='push_failed', "
            "failure_detail='origin unavailable' WHERE id=?",
            (failed_ship.id,),
        )
        conn.commit()

        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "blocked"
        assert "push_failed" in (task.pause_reason or "")
        assert runs_dao.list_active_runs(conn, task_id="t1") == []
    finally:
        conn.close()


@pytest.mark.regression
def test_exhausted_invalid_reviews_create_one_exact_sha_recovery(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _first, second = _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)

        first_tick = orch.tick("t1")
        second_tick = orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        recovery = next(run for run in reviews if run.attempt == 3)
        assert len(reviews) == 3
        assert first_tick.runs_created == 1
        assert second_tick.runs_created == 0
        assert recovery.dedupe_key == (
            f"review-recovery:t1:sha-a:{second.id}"
        )
        assert recovery.parent_run_id == second.id
        assert recovery.trigger_run_id == second.id
        assert recovery.review_target_sha == "sha-a"
        assert recovery.base_sha == "sha-a"
        assert recovery.head_sha == "sha-a"
        assert recovery.agent == "codex-cli"
        assert recovery.model == "gpt-5.5"
        assert recovery.worktree_path == "/tmp/review-sha-a"
    finally:
        conn.close()


def test_recovery_review_lgtm_advances_normally(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = runs_dao.list_runs_for_task(conn, "t1", kind="review")[-1]

        _complete_review(conn, recovery, verdict="LGTM", sha="sha-a")
        orch.tick("t1")

        assert tasks_dao.get(conn, "t1").status == "review_passed"
    finally:
        conn.close()


def test_recovery_review_rejected_enters_repair_flow(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = runs_dao.list_runs_for_task(conn, "t1", kind="review")[-1]
        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: False)

        _complete_review(
            conn,
            recovery,
            verdict="REJECTED",
            sha="sha-a",
            findings=["Ruff violations remain"],
        )
        orch.tick("t1")

        repairs = runs_dao.list_runs_for_task(conn, "t1", kind="repair")
        assert tasks_dao.get(conn, "t1").status == "in_progress"
        assert len(repairs) == 1
        assert repairs[0].trigger_run_id == recovery.id
    finally:
        conn.close()


def test_recovery_invalid_result_blocks_instead_of_retrying(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = runs_dao.list_runs_for_task(conn, "t1", kind="review")[-1]

        _fail_review(conn, recovery, category="invalid_result")
        orch.tick("t1")
        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "blocked"
        assert "recovery review" in (task.pause_reason or "")
        assert len(reviews) == 3
    finally:
        conn.close()


def test_review_recovery_remote_head_mismatch_blocks(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        monkeypatch.setattr(LifecycleOrchestrator, "_is_git_repo", lambda self: True)

        def mismatch(*args, **kwargs):
            raise StateError("Remote origin/task is sha-b, not sha-a")

        monkeypatch.setattr(
            "superharness.engine.lifecycle_orchestrator.create_review_worktree",
            mismatch,
        )
        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "blocked"
        assert "sha-b, not sha-a" in (task.pause_reason or "")
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 2
    finally:
        conn.close()


def test_review_recovery_missing_pr_metadata_blocks(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        task = tasks_dao.get(conn, "t1")
        assert task is not None
        tasks_dao.update(conn, task.id, task.version, {"extras_json": "{}"})
        conn.commit()

        LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "blocked"
        assert "missing authoritative task PR metadata" in (task.pause_reason or "")
    finally:
        conn.close()


def test_exhausted_non_invalid_reviews_do_not_use_special_recovery(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn, category="network")
        _allow_review_recovery(monkeypatch)

        result = LifecycleOrchestrator(str(project), now=lambda: NOW).tick("t1")

        assert result.runs_created == 0
        assert tasks_dao.get(conn, "t1").status == "review_requested"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 2
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Recovery review quota/session_limit/auth edge cases
# ---------------------------------------------------------------------------


def _create_consumed_recovery(
    conn,
    *,
    category: str,
    status: str = "failed",
    sha: str = "sha-a",
) -> runs_dao.RunRow:
    """Create a pre-consumed recovery run (attempt 3) for use in stranded tests."""
    _first, second = _consumed_exhausted_reviews(conn, sha=sha)
    dedupe_key = f"review-recovery:t1:{sha}:{second.id}"
    recovery = runs_dao.create_run(
        conn,
        id="review-recovery-1",
        task_id="t1",
        kind="review",
        agent="codex-cli",
        model="gpt-5.5",
        attempt=3,
        dedupe_key=dedupe_key,
        parent_run_id=second.id,
        trigger_run_id=second.id,
        review_target_sha=sha,
        now=NOW,
    )
    runs_dao.transition_run(conn, recovery.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, recovery.id, to_status="running", now=NOW)
    runs_dao.transition_run(
        conn,
        recovery.id,
        to_status=status,
        now=NOW,
        failure_category=category,
        failure_detail=category,
    )
    runs_dao.mark_run_consumed(conn, recovery.id, now=NOW)
    conn.commit()
    return runs_dao.get_run(conn, recovery.id) or recovery


def test_recovery_quota_blocks_task(tmp_path, monkeypatch):
    """Recovery review attempt 3 with quota → task blocked, no attempt 4."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )
        assert recovery.attempt == 3

        _fail_review(conn, recovery, category="quota", status="quota_blocked")
        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer unavailable due to Codex quota" in (
            task.pause_reason or ""
        )
        assert len(reviews) == 3
        availability = agent_availability.get(conn, "codex-cli")
        assert availability is not None and availability.state == "temporarily_blocked"
    finally:
        conn.close()


def test_recovery_session_limit_blocks_task(tmp_path, monkeypatch):
    """Recovery review attempt 3 with session_limit → task blocked, no attempt 4."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )

        _fail_review(conn, recovery, category="session_limit")
        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer unavailable due to Codex session_limit" in (
            task.pause_reason or ""
        )
        assert len(reviews) == 3
        availability = agent_availability.get(conn, "codex-cli")
        assert availability is not None and availability.state == "temporarily_blocked"
    finally:
        conn.close()


def test_recovery_auth_blocks_task(tmp_path, monkeypatch):
    """Recovery review attempt 3 with auth → task blocked, no attempt 4."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )

        _fail_review(conn, recovery, category="auth")
        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer authentication failure" in (task.pause_reason or "")
        assert len(reviews) == 3
    finally:
        conn.close()


def test_historical_consumed_recovery_quota_blocks_on_next_tick(tmp_path):
    """Stranded task with consumed recovery (quota) → blocked on next tick."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _create_consumed_recovery(conn, category="quota", status="quota_blocked")
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)

        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer unavailable due to Codex quota" in (
            task.pause_reason or ""
        )
        assert len(reviews) == 3
    finally:
        conn.close()


def test_historical_consumed_recovery_session_limit_blocks_on_next_tick(tmp_path):
    """Stranded task with consumed recovery (session_limit) → blocked on next tick."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _create_consumed_recovery(conn, category="session_limit")
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)

        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer unavailable due to Codex session_limit" in (
            task.pause_reason or ""
        )
    finally:
        conn.close()


def test_historical_consumed_recovery_auth_blocks_on_next_tick(tmp_path):
    """Stranded task with consumed recovery (auth) → blocked on next tick."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _create_consumed_recovery(conn, category="auth")
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)

        orch.tick("t1")

        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "blocked"
        assert "independent reviewer authentication failure" in (task.pause_reason or "")
    finally:
        conn.close()


def test_repeated_ticks_remain_idempotently_blocked(tmp_path, monkeypatch):
    """Once blocked, repeated ticks do not create new runs or change status."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )
        _fail_review(conn, recovery, category="quota", status="quota_blocked")
        orch.tick("t1")

        assert tasks_dao.get(conn, "t1").status == "blocked"

        for _ in range(3):
            result = orch.tick("t1")
            assert result.runs_created == 0
            assert result.transitions == 0

        assert tasks_dao.get(conn, "t1").status == "blocked"
        assert len(runs_dao.list_runs_for_task(conn, "t1", kind="review")) == 3
    finally:
        conn.close()


def test_no_attempt_4_is_created_after_recovery_failure(tmp_path, monkeypatch):
    """After recovery review fails with quota, no fourth review run is ever created."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        recovery = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )
        assert recovery.attempt == 3

        _fail_review(conn, recovery, category="quota", status="quota_blocked")
        for _ in range(5):
            orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 3
        assert all(r.attempt <= 3 for r in reviews)
        assert tasks_dao.get(conn, "t1").status == "blocked"
    finally:
        conn.close()


def test_claude_never_selected_as_reviewer(tmp_path, monkeypatch):
    """Claude (claude-code) is never selected as the recovery reviewer."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a", source_agent="claude-code")
        _consumed_exhausted_reviews(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        recovery_runs = [r for r in reviews if r.attempt == 3]
        for run in recovery_runs:
            assert run.agent != "claude-code", (
                f"Claude was selected as recovery reviewer: {run.agent}"
            )
        if recovery_runs:
            assert recovery_runs[0].agent == "codex-cli"
    finally:
        conn.close()


def test_normal_attempt_1_retry_behavior_unchanged(tmp_path):
    """Normal review attempt 1 with network failure → retries as attempt 2."""
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        assert review.attempt == 1

        _fail_review(conn, review, category="network")
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 2
        retry = max(reviews, key=lambda r: r.attempt)
        assert retry.attempt == 2
        assert retry.trigger_run_id == review.id
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


def test_normal_attempt_2_does_not_create_attempt_3(tmp_path):
    """Normal review attempt 2 failure does not create attempt 3 (only recovery does)."""
    project, conn = _project(tmp_path)
    try:
        _task(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")
        review1 = runs_dao.list_runs_for_task(conn, "t1", kind="review")[0]
        _fail_review(conn, review1, category="network")
        orch.tick("t1")
        review2 = max(
            runs_dao.list_runs_for_task(conn, "t1", kind="review"),
            key=lambda r: r.attempt,
        )
        assert review2.attempt == 2

        _fail_review(conn, review2, category="network")
        orch.tick("t1")
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(reviews) == 2
        assert all(r.attempt <= 2 for r in reviews)
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Operator re-review path
# ---------------------------------------------------------------------------


def _operator_review_setup(conn, *, sha: str = "sha-a") -> None:
    """Pre-populate two exhausted consumed failed reviews so the operator path fires."""
    _consumed_exhausted_reviews(conn, sha=sha)


def test_operator_review_created_when_pr_open_after_exhausted_attempts(tmp_path):
    """task.status==pr_open with exhausted reviews -> fresh operator-review run created."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        operator_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(operator_reviews) == 1, f"Expected 1 operator review, got {operator_reviews}"
        assert operator_reviews[0].review_target_sha == "sha-a"
        assert tasks_dao.get(conn, "t1").status == "review_requested"
    finally:
        conn.close()


def test_operator_review_uses_distinct_dedupe_key(tmp_path):
    """Operator review uses operator-review: prefix, not review: prefix."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        operator_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(operator_reviews) == 1
        assert operator_reviews[0].dedupe_key.startswith("operator-review:t1:sha-a:")
    finally:
        conn.close()


def test_operator_review_not_created_from_review_requested(tmp_path):
    """task.status==review_requested with exhausted reviews -> no operator review (safety gate)."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="review_requested", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        for _ in range(3):
            orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        operator_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(operator_reviews) == 0
        assert tasks_dao.get(conn, "t1").status in {"review_requested", "blocked"}
    finally:
        conn.close()


def test_operator_review_is_idempotent(tmp_path):
    """Multiple ticks do not create duplicate operator reviews."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        for _ in range(5):
            orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        operator_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(operator_reviews) == 1
    finally:
        conn.close()


def test_operator_review_lgtm_moves_to_review_passed(tmp_path):
    """Successful LGTM from operator review moves task to review_passed."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        op_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(op_reviews) == 1
        op_review = op_reviews[0]

        _complete_review(conn, op_review, verdict="LGTM", sha="sha-a")
        orch.tick("t1")

        assert tasks_dao.get(conn, "t1").status == "review_passed"
    finally:
        conn.close()


def test_operator_review_rejected_moves_to_review_failed(tmp_path):
    """REJECTED from operator review moves task to review_failed."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        op_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(op_reviews) == 1
        op_review = op_reviews[0]

        _complete_review(conn, op_review, verdict="REJECTED", sha="sha-a",
                         findings=["needs a focused fix"])
        orch.tick("t1")

        # REJECTED triggers repair run creation, which transitions to in_progress
        assert tasks_dao.get(conn, "t1").status in {"review_failed", "in_progress"}
    finally:
        conn.close()


def test_operator_review_terminal_failure_blocks_task(tmp_path):
    """Non-retriable operator review failure blocks the task immediately."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        op_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(op_reviews) == 1
        op_review = op_reviews[0]

        _fail_review(conn, op_review, category="quota", status="quota_blocked")
        orch.tick("t1")

        assert tasks_dao.get(conn, "t1").status == "blocked"
        all_reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        recovery_reviews = [r for r in all_reviews if r.dedupe_key.startswith("review-recovery:")]
        assert len(recovery_reviews) == 0
    finally:
        conn.close()


def test_operator_review_any_failure_blocks_task(tmp_path):
    """Operator review failing with any category blocks task immediately (no retry)."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        op_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(op_reviews) == 1
        op_review = op_reviews[0]

        _fail_review(conn, op_review, category="network")
        orch.tick("t1")

        # No retry — operator review blocks on any failure
        assert tasks_dao.get(conn, "t1").status == "blocked"
        all_reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        assert len(all_reviews) == 3  # 2 exhausted + 1 operator review
    finally:
        conn.close()





def test_operator_review_does_not_trigger_auto_recovery(tmp_path, monkeypatch):
    """Exhausted operator review does not create an auto-recovery review."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        _operator_review_setup(conn)
        _allow_review_recovery(monkeypatch)
        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        orch.tick("t1")

        reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        op_reviews = [r for r in reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(op_reviews) == 1
        op_review = op_reviews[0]
        _fail_review(conn, op_review, category="invalid_result")
        for _ in range(4):
            orch.tick("t1")

        all_reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        recovery_reviews = [r for r in all_reviews if r.dedupe_key.startswith("review-recovery:")]
        assert len(recovery_reviews) == 0
        assert tasks_dao.get(conn, "t1").status == "blocked"
    finally:
        conn.close()


def test_operator_review_not_created_when_consumed_valid_verdict_exists(tmp_path):
    """If a consumed LGTM already exists for the SHA, no operator review is created."""
    project, conn = _project(tmp_path)
    try:
        _task(conn, status="pr_open", sha="sha-a")
        # Create a review run and complete it with LGTM (consumed verdict)
        lgtm_run = runs_dao.create_run(
            conn,
            id="review-lgtm-consumed",
            task_id="t1",
            kind="review",
            agent="codex-cli",
            model="gpt-5.5",
            attempt=2,
            dedupe_key="review:t1:sha-a:lgtm",
            review_target_sha="sha-a",
            now=NOW,
        )
        _complete_review(conn, lgtm_run, verdict="LGTM", sha="sha-a")
        runs_dao.mark_run_consumed(conn, lgtm_run.id, now=NOW)
        conn.commit()

        orch = LifecycleOrchestrator(str(project), now=lambda: NOW)
        for _ in range(3):
            orch.tick("t1")

        all_reviews = runs_dao.list_runs_for_task(conn, "t1", kind="review")
        operator_reviews = [r for r in all_reviews if r.dedupe_key.startswith("operator-review:")]
        assert len(operator_reviews) == 0
    finally:
        conn.close()

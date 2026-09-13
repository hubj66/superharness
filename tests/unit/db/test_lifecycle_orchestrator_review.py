from __future__ import annotations

import json
from pathlib import Path

from superharness.engine import agent_availability, inbox_dao, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator
from superharness.engine.shipper import ShipOutcome

NOW = "2026-01-01T00:00:00Z"
LATER = "2026-01-01T02:00:00Z"


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


def _complete_repair(conn, run: runs_dao.RunRow, *, head: str = "repair-head") -> None:
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
            "head_sha": head,
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
        _complete_review(conn, review, verdict="BLOCKED", sha="sha-a")
        orch.tick("t1")
        task = tasks_dao.get(conn, "t1")
        assert task is not None and task.status == "review_requested"
        assert runs_dao.get_run(conn, review.id).failure_category == "review_blocked"

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

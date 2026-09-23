"""Fail-closed reliable-orchestrator review auto-close eligibility."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from collections.abc import Callable
from typing import Any

from superharness.engine import runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.reliable_orchestrator_gate import (
    is_reliable_orchestrated_task,
)
from superharness.engine.run_results import validate_result_for_run

_GITHUB_PR_URL_RE = re.compile(r"^https://github\.com/([^/]+)/([^/]+)/pull/(\d+)/?$")


def authoritative_reliable_lgtm_review(conn, task):
    """Return the consumed reliable LGTM review Run that authorizes close."""
    if not is_reliable_orchestrated_task(task):
        return None
    if str(getattr(task, "status", "")) != "review_passed":
        return None
    metadata = _reliable_metadata(task)
    if metadata is None:
        return None

    review_run_id = metadata.get("last_review_run_id")
    reviewed_sha = metadata.get("last_reviewed_head_sha")
    pr_head_sha = metadata.get("pr_head_sha")
    if (
        not isinstance(review_run_id, str)
        or not review_run_id
        or metadata.get("last_review_verdict") != "LGTM"
        or not isinstance(reviewed_sha, str)
        or not reviewed_sha
        or not isinstance(pr_head_sha, str)
        or reviewed_sha != pr_head_sha
    ):
        return None

    run = runs_dao.get_run(conn, review_run_id)
    if run is None:
        return None
    if (
        run.task_id != getattr(task, "id", None)
        or run.kind != "review"
        or run.status != "succeeded"
        or run.finished_at is None
        or run.orchestrator_consumed_at is None
        or run.review_verdict != "LGTM"
        or run.review_target_sha != reviewed_sha
    ):
        return None

    try:
        result = validate_result_for_run(run.result_json, run)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if (
        result.completion_status != "completed"
        or result.review_verdict != "LGTM"
        or result.reviewed_sha != reviewed_sha
    ):
        return None
    return run


def parse_github_pr_url(pr_url: str) -> tuple[str, str, int] | None:
    match = _GITHUB_PR_URL_RE.match(pr_url.strip())
    if not match:
        return None
    owner, repo, number = match.groups()
    return owner, repo, int(number)


def fetch_github_pr(project_dir: str, pr_url: str) -> dict | None:
    parsed = parse_github_pr_url(pr_url)
    if parsed is None or shutil.which("gh") is None:
        return None
    owner, repo, number = parsed
    try:
        result = subprocess.run(
            ["gh", "api", f"repos/{owner}/{repo}/pulls/{number}"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def recorded_pr_was_merged_at_sha(
    project_dir: str,
    metadata: dict,
    expected_sha: str,
    *,
    fetch_pr: Callable[[str, str], dict | None] | None = None,
) -> bool:
    if fetch_pr is None:
        fetch_pr = fetch_github_pr
    pr_url = metadata.get("pr_url")
    pr_number = metadata.get("pr_number")
    if not isinstance(pr_url, str) or not isinstance(pr_number, int):
        return False
    parsed = parse_github_pr_url(pr_url)
    if parsed is None:
        return False
    _owner, _repo, url_number = parsed
    if url_number != pr_number:
        return False

    payload = fetch_pr(project_dir, pr_url)
    if payload is None:
        return False
    if payload.get("number") != pr_number:
        return False
    html_url = payload.get("html_url")
    if not isinstance(html_url, str) or html_url.rstrip("/") != pr_url.rstrip("/"):
        return False
    if payload.get("merged") is not True:
        return False
    head = payload.get("head")
    if not isinstance(head, dict):
        return False
    return head.get("sha") == expected_sha


def auto_close_reliable_review_passed(
    project_dir: str,
    close_task=None,
    *,
    fetch_pr: Callable[[str, str], dict | None] | None = None,
) -> int:
    """Close reliable review_passed tasks only after validated LGTM and PR merge."""
    if close_task is None:
        from superharness.commands.close import close_task as close_task_fn

        close_task = close_task_fn
    if fetch_pr is None:
        fetch_pr = fetch_github_pr

    closed = 0
    conn = get_connection(project_dir)
    try:
        init_db(conn)
        for task in tasks_dao.get_all(conn, status="review_passed"):
            run = authoritative_reliable_lgtm_review(conn, task)
            if run is None:
                continue
            metadata = _reliable_metadata(task)
            if metadata is None or not recorded_pr_was_merged_at_sha(
                project_dir, metadata, run.review_target_sha, fetch_pr=fetch_pr
            ):
                continue
            actor = task.owner or "owner"
            rc = close_task(
                project_dir=project_dir,
                task_id=task.id,
                actor=actor,
                summary=(
                    "Reliable orchestrator review passed: consumed LGTM "
                    f"from {run.agent} for {run.review_target_sha}."
                ),
                skip_verify=True,
            )
            if rc == 0:
                closed += 1
    finally:
        conn.close()
    return closed


def _reliable_metadata(task: Any) -> dict | None:
    try:
        extras = json.loads(getattr(task, "extras_json", None) or "{}")
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(extras, dict):
        return None
    metadata = extras.get("reliable_orchestrator")
    return metadata if isinstance(metadata, dict) else None

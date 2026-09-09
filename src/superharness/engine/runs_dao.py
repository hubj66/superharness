from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from superharness.engine.run_results import (
    RUN_KINDS,
    ExecutionResult,
    validate_result_for_run,
)
from superharness.engine.state_errors import StateError

VALID_RUN_STATUSES = frozenset(
    {
        "queued",
        "claimed",
        "running",
        "succeeded",
        "failed",
        "crashed",
        "timed_out",
        "quota_blocked",
        "cancelled",
    }
)
ACTIVE_RUN_STATUSES = frozenset({"queued", "claimed", "running"})
TERMINAL_RUN_STATUSES = VALID_RUN_STATUSES - ACTIVE_RUN_STATUSES

MUTATING_RUN_KINDS = frozenset({"plan", "implement", "repair", "fallback", "ship"})

RUN_TRANSITIONS: dict[str, frozenset[str]] = {
    "queued": frozenset({"claimed", "cancelled"}),
    "claimed": frozenset({"running", "cancelled", "failed"}),
    "running": frozenset(
        {"succeeded", "failed", "crashed", "timed_out", "quota_blocked", "cancelled"}
    ),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "crashed": frozenset(),
    "timed_out": frozenset(),
    "quota_blocked": frozenset(),
    "cancelled": frozenset(),
}


@dataclass(frozen=True)
class RunRow:
    id: str
    task_id: str
    kind: str
    agent: str
    model: str | None
    status: str
    attempt: int
    parent_run_id: str | None
    trigger_run_id: str | None
    inbox_id: str | None
    dedupe_key: str
    worktree_path: str | None
    branch_name: str | None
    base_sha: str | None
    head_sha: str | None
    remote_head_sha: str | None
    pr_number: int | None
    pr_url: str | None
    review_verdict: str | None
    review_target_sha: str | None
    failure_category: str | None
    failure_detail: str | None
    exit_code: int | None
    pid: int | None
    pid_starttime: str | None
    log_path: str | None
    result_handoff_id: int | None
    result_json: dict[str, Any]
    orchestrator_consumed_at: str | None
    created_at: str
    claimed_at: str | None
    started_at: str | None
    heartbeat_at: str | None
    finished_at: str | None


def create_run(
    conn: sqlite3.Connection,
    *,
    id: str,
    task_id: str,
    kind: str,
    agent: str,
    dedupe_key: str,
    now: str,
    model: str | None = None,
    status: str = "queued",
    attempt: int = 1,
    parent_run_id: str | None = None,
    trigger_run_id: str | None = None,
    inbox_id: str | None = None,
    worktree_path: str | None = None,
    branch_name: str | None = None,
    base_sha: str | None = None,
    head_sha: str | None = None,
    remote_head_sha: str | None = None,
    pr_number: int | None = None,
    pr_url: str | None = None,
    review_target_sha: str | None = None,
    pid: int | None = None,
    pid_starttime: str | None = None,
    log_path: str | None = None,
    result_json: dict[str, Any] | None = None,
) -> RunRow:
    """Create a durable run, returning the existing row for duplicate dedupe keys."""

    _validate_run_core(kind=kind, status=status, agent=agent, dedupe_key=dedupe_key)
    if attempt < 1:
        raise StateError("Run attempt must be >= 1")
    if kind == "review" and not review_target_sha:
        raise StateError("Review runs require review_target_sha")

    payload_json = json.dumps(result_json or {})
    try:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO runs (
                id, task_id, kind, agent, model, status, attempt,
                parent_run_id, trigger_run_id, inbox_id, dedupe_key,
                worktree_path, branch_name, base_sha, head_sha, remote_head_sha,
                pr_number, pr_url, review_target_sha, pid, pid_starttime,
                log_path, result_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING *
            """,
            (
                id,
                task_id,
                kind,
                agent,
                model,
                status,
                attempt,
                parent_run_id,
                trigger_run_id,
                inbox_id,
                dedupe_key,
                worktree_path,
                branch_name,
                base_sha,
                head_sha,
                remote_head_sha,
                pr_number,
                pr_url,
                review_target_sha,
                pid,
                pid_starttime,
                log_path,
                payload_json,
                now,
            ),
        )
        row = cursor.fetchone()
        if row:
            return _row_to_run(row)
        existing = get_run_by_dedupe_key(conn, dedupe_key)
        if existing is not None:
            return existing
        raise StateError(
            f"Run creation rejected by database constraints for task '{task_id}' "
            f"kind '{kind}'"
        )
    except sqlite3.Error as e:
        raise StateError(f"Failed to create run '{id}': {e}") from e


def get_run(conn: sqlite3.Connection, id: str) -> RunRow | None:
    cursor = conn.execute("SELECT * FROM runs WHERE id = ?", (id,))
    row = cursor.fetchone()
    return _row_to_run(row) if row else None


def get_run_by_dedupe_key(conn: sqlite3.Connection, dedupe_key: str) -> RunRow | None:
    cursor = conn.execute("SELECT * FROM runs WHERE dedupe_key = ?", (dedupe_key,))
    row = cursor.fetchone()
    return _row_to_run(row) if row else None


def list_runs_for_task(
    conn: sqlite3.Connection,
    task_id: str,
    *,
    status: str | None = None,
    kind: str | None = None,
) -> list[RunRow]:
    query = "SELECT * FROM runs WHERE task_id = ?"
    params: list[Any] = [task_id]
    if status is not None:
        _validate_status(status)
        query += " AND status = ?"
        params.append(status)
    if kind is not None:
        _validate_kind(kind)
        query += " AND kind = ?"
        params.append(kind)
    query += " ORDER BY created_at ASC, id ASC"
    return [_row_to_run(row) for row in conn.execute(query, params).fetchall()]


def list_unconsumed_finished_runs(
    conn: sqlite3.Connection, *, task_id: str | None = None
) -> list[RunRow]:
    query = """
        SELECT * FROM runs
        WHERE finished_at IS NOT NULL
          AND orchestrator_consumed_at IS NULL
    """
    params: list[Any] = []
    if task_id is not None:
        query += " AND task_id = ?"
        params.append(task_id)
    query += " ORDER BY finished_at ASC, id ASC"
    return [_row_to_run(row) for row in conn.execute(query, params).fetchall()]


def transition_run(
    conn: sqlite3.Connection,
    id: str,
    *,
    to_status: str,
    now: str,
    from_status: str | None = None,
    failure_category: str | None = None,
    failure_detail: str | None = None,
) -> bool:
    run = get_run(conn, id)
    if run is None:
        raise StateError(f"Run '{id}' not found")
    _validate_status(to_status)
    if from_status is not None:
        _validate_status(from_status)
        if run.status != from_status:
            return False
    allowed = RUN_TRANSITIONS[run.status]
    if to_status not in allowed:
        raise StateError(f"Illegal run transition: {run.status} -> {to_status}")

    sets = ["status = ?"]
    params: list[Any] = [to_status]
    if to_status == "claimed":
        sets.append("claimed_at = ?")
        params.append(now)
    elif to_status == "running":
        sets.extend(["started_at = ?", "heartbeat_at = ?"])
        params.extend([now, now])
    elif to_status in TERMINAL_RUN_STATUSES:
        sets.append("finished_at = ?")
        params.append(now)
    if failure_category is not None:
        sets.append("failure_category = ?")
        params.append(failure_category)
    if failure_detail is not None:
        sets.append("failure_detail = ?")
        params.append(failure_detail)

    params.extend([id, run.status])
    cursor = conn.execute(
        f"UPDATE runs SET {', '.join(sets)} WHERE id = ? AND status = ?",
        params,
    )
    return cursor.rowcount > 0


def record_run_result(
    conn: sqlite3.Connection,
    run_id: str,
    result: ExecutionResult | dict[str, Any],
    *,
    now: str | None = None,
    result_handoff_id: int | None = None,
) -> RunRow:
    run = get_run(conn, run_id)
    if run is None:
        raise StateError(f"Run '{run_id}' not found")
    parsed = validate_result_for_run(result, run)
    result_payload = parsed.model_dump(mode="json")

    updates: dict[str, Any] = {
        "exit_code": parsed.exit_code,
        "result_json": json.dumps(result_payload),
        "worktree_path": parsed.worktree_path,
        "branch_name": parsed.branch_name,
        "base_sha": parsed.base_sha,
        "head_sha": parsed.head_sha,
        "review_verdict": parsed.review_verdict,
    }
    if parsed.kind == "review" and parsed.reviewed_sha:
        updates["review_target_sha"] = parsed.reviewed_sha
    if result_handoff_id is not None:
        updates["result_handoff_id"] = result_handoff_id
    if now is not None:
        updates["heartbeat_at"] = now

    safe_updates = {key: value for key, value in updates.items() if value is not None}
    assignments = ", ".join(f"{key}=?" for key in safe_updates)
    params = list(safe_updates.values()) + [run_id]
    conn.execute(f"UPDATE runs SET {assignments} WHERE id=?", params)
    updated = get_run(conn, run_id)
    if updated is None:
        raise StateError(f"Run '{run_id}' disappeared while recording result")
    return updated


def mark_run_consumed(conn: sqlite3.Connection, run_id: str, *, now: str) -> bool:
    cursor = conn.execute(
        """
        UPDATE runs
        SET orchestrator_consumed_at = COALESCE(orchestrator_consumed_at, ?)
        WHERE id = ?
        """,
        (now, run_id),
    )
    return cursor.rowcount > 0


def link_inbox(conn: sqlite3.Connection, *, run_id: str, inbox_id: str) -> RunRow:
    run = get_run(conn, run_id)
    if run is None:
        raise StateError(f"Run '{run_id}' not found")
    inbox = conn.execute(
        "SELECT task_id, run_id FROM inbox WHERE id = ?", (inbox_id,)
    ).fetchone()
    if inbox is None:
        raise StateError(f"Inbox item '{inbox_id}' not found")
    if inbox["task_id"] != run.task_id:
        raise StateError(
            f"Inbox item '{inbox_id}' belongs to task '{inbox['task_id']}', "
            f"not '{run.task_id}'"
        )
    if inbox["run_id"] not in (None, run_id):
        raise StateError(f"Inbox item '{inbox_id}' is already linked to another run")
    if run.inbox_id not in (None, inbox_id):
        raise StateError(f"Run '{run_id}' is already linked to another inbox item")
    try:
        conn.execute("UPDATE inbox SET run_id = ? WHERE id = ?", (run_id, inbox_id))
        cursor = conn.execute(
            "UPDATE runs SET inbox_id = ? WHERE id = ? RETURNING *", (inbox_id, run_id)
        )
        row = cursor.fetchone()
        if row is None:
            raise StateError(f"Run '{run_id}' not found")
        return _row_to_run(row)
    except sqlite3.Error as e:
        raise StateError(f"Failed to link run '{run_id}' to inbox '{inbox_id}': {e}") from e


def _validate_run_core(
    *, kind: str, status: str, agent: str, dedupe_key: str
) -> None:
    _validate_kind(kind)
    _validate_status(status)
    if not agent:
        raise StateError("Run agent is required")
    if not dedupe_key:
        raise StateError("Run dedupe_key is required")


def _validate_kind(kind: str) -> None:
    if kind not in RUN_KINDS:
        raise StateError(f"Invalid run kind '{kind}'")


def _validate_status(status: str) -> None:
    if status not in VALID_RUN_STATUSES:
        raise StateError(f"Invalid run status '{status}'")


def _row_to_run(row: sqlite3.Row) -> RunRow:
    try:
        result_json = json.loads(row["result_json"] or "{}")
    except json.JSONDecodeError:
        result_json = {}
    return RunRow(
        id=row["id"],
        task_id=row["task_id"],
        kind=row["kind"],
        agent=row["agent"],
        model=row["model"],
        status=row["status"],
        attempt=row["attempt"],
        parent_run_id=row["parent_run_id"],
        trigger_run_id=row["trigger_run_id"],
        inbox_id=row["inbox_id"],
        dedupe_key=row["dedupe_key"],
        worktree_path=row["worktree_path"],
        branch_name=row["branch_name"],
        base_sha=row["base_sha"],
        head_sha=row["head_sha"],
        remote_head_sha=row["remote_head_sha"],
        pr_number=row["pr_number"],
        pr_url=row["pr_url"],
        review_verdict=row["review_verdict"],
        review_target_sha=row["review_target_sha"],
        failure_category=row["failure_category"],
        failure_detail=row["failure_detail"],
        exit_code=row["exit_code"],
        pid=row["pid"],
        pid_starttime=row["pid_starttime"],
        log_path=row["log_path"],
        result_handoff_id=row["result_handoff_id"],
        result_json=result_json,
        orchestrator_consumed_at=row["orchestrator_consumed_at"],
        created_at=row["created_at"],
        claimed_at=row["claimed_at"],
        started_at=row["started_at"],
        heartbeat_at=row["heartbeat_at"],
        finished_at=row["finished_at"],
    )

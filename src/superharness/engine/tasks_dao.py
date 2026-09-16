from __future__ import annotations

import sqlite3
import json
from dataclasses import dataclass
from typing import Any
from collections import defaultdict

from superharness.engine.state_errors import StateError, ConcurrencyError
from superharness.engine.next_action import ALL_STATUSES

# arch A2: canonical status enum, derived from next_action.ALL_STATUSES (the
# existing single source of truth for the lifecycle graph) rather than
# duplicated here. update() uses this as a hard guard so a typo'd or
# otherwise garbage status value can never reach the tasks.status column —
# state_writer.validate_status_transition() already blocks illegal *edges*
# between known statuses, but force=True callers (the lifecycle reconciler)
# bypass that graph entirely and previously had no floor at all.
VALID_STATUSES: frozenset[str] = frozenset(ALL_STATUSES)


@dataclass(frozen=True)
class TaskRow:
    id: str
    title: str
    owner: str | None
    status: str
    effort: str | None
    project_path: str | None
    development_method: str | None
    acceptance_criteria: list[str]
    test_types: list[str]
    out_of_scope: list[str]
    definition_of_done: list[str]
    context: str | None
    tdd: dict[str, Any] | None
    version: int
    created_at: str
    updated_at: str | None = None
    plan_proposed_at: str | None = None
    plan_approved_at: str | None = None
    in_progress_at: str | None = None
    report_ready_at: str | None = None
    review_requested_at: str | None = None
    done_at: str | None = None
    cancelled_at: str | None = None
    blocked_by: list[str] = None  # type: ignore
    parent_id: str | None = None
    verified: bool = False
    verified_at: str | None = None
    verified_by: str | None = None
    deadline_minutes: int | None = None
    # v4 lifecycle columns
    failed_at: str | None = None
    stopped_at: str | None = None
    failed_reason: str | None = None
    archived_at: str | None = None
    archived_reason: str | None = None
    model_tier: str | None = None
    pause_reason: str | None = None
    # v7
    worktree_path: str | None = None
    # v9: soft (informational) blocked_by storage — JSON-encoded list,
    # may include refs to non-existent task IDs that the strict
    # task_dependencies FK would reject.
    blocked_by_raw: str | None = None
    # v10: per-task stamping
    workflow: str | None = None
    autonomy: str | None = None
    require_tdd: bool | None = None
    # v11: nested metadata (subtasks, classifier, decomposer, retry)
    # stored as a JSON string on the row; consumers merge it into the
    # task dict at read time.
    extras_json: str | None = None
    # v12: contract lock — snapshot of acceptance_criteria + tdd frozen at plan_approved
    locked_contract: str | None = None
    contract_locked_at: str | None = None
    # v16: explicit timeout override (stored as string of minutes, e.g. "45")
    estimated_minutes: str | None = None
    # v30: linked GitHub/GitLab issue URL — one-way snapshot pointer, never
    # written back to by shux (see shux task link / --from-issue).
    issue_url: str | None = None


def upsert(conn: sqlite3.Connection, task: TaskRow) -> TaskRow:
    """Insert or update a task. Bumps version on update."""
    ac = json.dumps(task.acceptance_criteria)
    tt = json.dumps(task.test_types)
    oos = json.dumps(task.out_of_scope)
    dod = json.dumps(task.definition_of_done)
    tdd = json.dumps(task.tdd) if task.tdd else None

    require_tdd_val = int(task.require_tdd) if task.require_tdd is not None else None
    try:
        cursor = conn.execute(
            """
            INSERT INTO tasks (
                id, title, owner, status, effort, project_path,
                development_method, acceptance_criteria, test_types,
                out_of_scope, definition_of_done, context, tdd, created_at,
                updated_at, plan_proposed_at, plan_approved_at, in_progress_at,
                report_ready_at, review_requested_at, done_at, cancelled_at,
                version, parent_id, verified, verified_at, verified_by, deadline_minutes,
                failed_at, stopped_at, failed_reason, archived_at, archived_reason,
                model_tier, pause_reason, worktree_path, blocked_by_raw,
                locked_contract, contract_locked_at,
                workflow, autonomy, require_tdd, estimated_minutes, extras_json,
                issue_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title, owner=excluded.owner, status=excluded.status,
                effort=excluded.effort, project_path=excluded.project_path,
                development_method=excluded.development_method,
                acceptance_criteria=excluded.acceptance_criteria,
                test_types=excluded.test_types, out_of_scope=excluded.out_of_scope,
                definition_of_done=excluded.definition_of_done,
                context=excluded.context, tdd=excluded.tdd,
                updated_at=excluded.updated_at,
                plan_proposed_at=excluded.plan_proposed_at,
                plan_approved_at=excluded.plan_approved_at,
                in_progress_at=excluded.in_progress_at,
                report_ready_at=excluded.report_ready_at,
                review_requested_at=excluded.review_requested_at,
                done_at=excluded.done_at,
                cancelled_at=excluded.cancelled_at,
                parent_id=excluded.parent_id,
                verified=excluded.verified,
                verified_at=excluded.verified_at,
                verified_by=excluded.verified_by,
                deadline_minutes=excluded.deadline_minutes,
                failed_at=excluded.failed_at,
                stopped_at=excluded.stopped_at,
                failed_reason=excluded.failed_reason,
                archived_at=excluded.archived_at,
                archived_reason=excluded.archived_reason,
                model_tier=excluded.model_tier,
                pause_reason=excluded.pause_reason,
                worktree_path=excluded.worktree_path,
                blocked_by_raw=excluded.blocked_by_raw,
                locked_contract=excluded.locked_contract,
                contract_locked_at=excluded.contract_locked_at,
                workflow=excluded.workflow,
                autonomy=excluded.autonomy,
                require_tdd=excluded.require_tdd,
                estimated_minutes=excluded.estimated_minutes,
                extras_json=excluded.extras_json,
                issue_url=excluded.issue_url,
                version=tasks.version + 1
            RETURNING *
        """,
            (
                task.id,
                task.title,
                task.owner,
                task.status,
                task.effort,
                task.project_path,
                task.development_method,
                ac,
                tt,
                oos,
                dod,
                task.context,
                tdd,
                task.created_at,
                task.updated_at,
                task.plan_proposed_at,
                task.plan_approved_at,
                task.in_progress_at,
                task.report_ready_at,
                task.review_requested_at,
                task.done_at,
                task.cancelled_at,
                task.version,
                task.parent_id,
                int(task.verified),
                task.verified_at,
                task.verified_by,
                task.deadline_minutes,
                task.failed_at,
                task.stopped_at,
                task.failed_reason,
                task.archived_at,
                task.archived_reason,
                task.model_tier,
                task.pause_reason,
                task.worktree_path,
                task.blocked_by_raw,
                task.locked_contract,
                task.contract_locked_at,
                task.workflow,
                task.autonomy,
                require_tdd_val,
                task.estimated_minutes,
                task.extras_json,
                task.issue_url,
            ),
        )
        row = cursor.fetchone()
        if not row:
            raise StateError("Upsert failed: no row returned")
        return _row_to_task(conn, row)
    except sqlite3.Error as e:
        raise StateError(f"Failed to upsert task '{task.id}': {e}") from e


def get(conn: sqlite3.Connection, id: str) -> TaskRow | None:
    """Get a task by ID, including its dependencies."""
    cursor = conn.execute("SELECT * FROM tasks WHERE id = ?", (id,))
    row = cursor.fetchone()
    if not row:
        return None
    return _row_to_task(conn, row)


def get_all(
    conn: sqlite3.Connection,
    *,
    status: str | None = None,
    owner: str | None = None,
    top_level_only: bool = False,
) -> list[TaskRow]:
    """Get all tasks, optionally filtered by status, owner, or top-level only (parent_id IS NULL)."""
    query = "SELECT * FROM tasks WHERE 1=1"
    params: list[Any] = []
    if status:
        query += " AND status = ?"
        params.append(status)
    if owner:
        query += " AND owner = ?"
        params.append(owner)
    if top_level_only:
        query += " AND parent_id IS NULL"

    query += " ORDER BY created_at ASC"
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    # Optimized: Fetch all dependencies for these tasks in one query
    if not rows:
        return []

    task_ids = [r["id"] for r in rows]
    placeholders = ",".join(["?" for _ in task_ids])
    dep_cursor = conn.execute(
        f"SELECT dependent_task_id, prerequisite_task_id FROM task_dependencies WHERE dependent_task_id IN ({placeholders})",
        task_ids,
    )
    deps_map = defaultdict(list)
    for dep_row in dep_cursor.fetchall():
        deps_map[dep_row[0]].append(dep_row[1])

    return [_row_to_task(conn, row, deps_map[row["id"]]) for row in rows]


CONTRACT_LOCKED_FIELDS = frozenset({"acceptance_criteria", "tdd"})


def update(
    conn: sqlite3.Connection,
    id: str,
    version: int,
    changes: dict[str, Any],
) -> TaskRow:
    """Update specific fields of a task with optimistic concurrency check."""
    from superharness.engine.state_errors import ContractLockError

    if "status" in changes and changes["status"] not in VALID_STATUSES:
        raise ValueError(
            f"Invalid status {changes['status']!r} for task '{id}'. "
            f"Valid statuses: {', '.join(sorted(VALID_STATUSES))}"
        )
    locked_fields = CONTRACT_LOCKED_FIELDS & changes.keys()
    if locked_fields:
        row = conn.execute(
            "SELECT contract_locked_at FROM tasks WHERE id = ?", (id,)
        ).fetchone()
        if row and row["contract_locked_at"]:
            raise ContractLockError(
                f"Cannot modify {sorted(locked_fields)} on task '{id}': "
                f"contract locked at {row['contract_locked_at']}"
            )
    if not changes:
        task = get(conn, id)
        if not task:
            raise StateError(f"Task {id} not found")
        return task

    set_clauses = []
    params: list[Any] = []
    for key, value in changes.items():
        if key in (
            "acceptance_criteria",
            "test_types",
            "out_of_scope",
            "definition_of_done",
        ):
            set_clauses.append(f"{key} = ?")
            params.append(json.dumps(value))
        elif key == "tdd":
            set_clauses.append(f"{key} = ?")
            params.append(json.dumps(value) if value else None)
        else:
            set_clauses.append(f"{key} = ?")
            params.append(value)

    set_clauses.append("version = version + 1")
    sql = f"UPDATE tasks SET {', '.join(set_clauses)} WHERE id = ? AND version = ? RETURNING *"
    params.extend([id, version])

    try:
        cursor = conn.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            # Check if it was a version mismatch or missing ID
            cursor = conn.execute("SELECT version FROM tasks WHERE id = ?", (id,))
            existing = cursor.fetchone()
            if existing:
                raise ConcurrencyError(
                    f"Task '{id}' version mismatch: expected {version}, got {existing['version']}"
                )
            else:
                raise StateError(f"Task '{id}' not found")
        return _row_to_task(conn, row)
    except sqlite3.Error as e:
        raise StateError(f"Failed to update task '{id}': {e}") from e


def set_status(
    conn: sqlite3.Connection,
    task_id: str,
    new_status: str,
    expected_version: int,
    **fields: Any,
) -> TaskRow:
    """Set a task's status (plus any additional columns) through the
    version-checked `update()`.

    Thin wrapper for the common status-transition call shape used by the
    watcher's auto-recovery/reconciliation paths (see
    commands/inbox_watch.py's `_with_task_lock`) — those seven sites used to
    write `status` via raw `UPDATE tasks ...` statements with no version
    check at all, so a concurrent writer could silently clobber another
    agent's change. Raises `ConcurrencyError` on a stale `expected_version`,
    same as `update()`.
    """
    changes = {"status": new_status, **fields}
    return update(conn, task_id, expected_version, changes)


def set_dependencies(
    conn: sqlite3.Connection,
    task_id: str,
    prerequisites: list[str],
) -> None:
    """Replace dependencies for a task."""
    try:
        conn.execute(
            "DELETE FROM task_dependencies WHERE dependent_task_id = ?", (task_id,)
        )
        for prereq in prerequisites:
            conn.execute(
                "INSERT INTO task_dependencies (dependent_task_id, prerequisite_task_id) VALUES (?, ?)",
                (task_id, prereq),
            )
    except sqlite3.Error as e:
        raise StateError(f"Failed to set dependencies for task '{task_id}': {e}") from e


def get_unblocked(
    conn: sqlite3.Connection,
    *,
    status_filter: list[str] | None = None,
) -> list[TaskRow]:
    """Get tasks whose prerequisites are all 'done'."""
    query = """
        SELECT * FROM tasks t
        WHERE NOT EXISTS (
            SELECT 1 FROM task_dependencies d
            JOIN tasks p ON d.prerequisite_task_id = p.id
            WHERE d.dependent_task_id = t.id AND p.status != 'done'
        )
    """
    params: list[Any] = []
    if status_filter:
        query += f" AND t.status IN ({','.join(['?' for _ in status_filter])})"
        params.extend(status_filter)

    cursor = conn.execute(query, params)
    rows = cursor.fetchall()
    if not rows:
        return []

    task_ids = [r["id"] for r in rows]
    placeholders = ",".join(["?" for _ in task_ids])
    dep_cursor = conn.execute(
        f"SELECT dependent_task_id, prerequisite_task_id FROM task_dependencies WHERE dependent_task_id IN ({placeholders})",
        task_ids,
    )
    deps_map = defaultdict(list)
    for dep_row in dep_cursor.fetchall():
        deps_map[dep_row[0]].append(dep_row[1])

    return [_row_to_task(conn, row, deps_map[row["id"]]) for row in rows]


def _safe_json_load(
    raw: str | None,
    default: Any = None,
    *,
    task_id: str | None = None,
    column: str | None = None,
) -> Any:
    """Parse JSON without crashing on malformed data. Returns default on failure.

    One malformed row must not break bulk reads like `shux contract`
    (arch A4). task_id/column are included in the warning so an operator
    can find and repair the offending row instead of just seeing "some
    JSON somewhere failed to parse".
    """
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        logger = __import__("logging").getLogger(__name__)
        logger.warning(
            "Failed to parse JSON field %r on task %r: %s", column, task_id, e
        )
        return default


def _safe_get(
    row: sqlite3.Row, key: str, default: Any = None, *, coerce: type | None = None
) -> Any:
    """Get a column value that may not exist on the row (pre-migration DBs)."""
    keys = row.keys() if hasattr(row, "keys") else []
    if key not in keys:
        return default
    val = row[key]
    if coerce is bool:
        return bool(val)
    return val


def _row_to_task(
    conn: sqlite3.Connection, row: sqlite3.Row, blocked_by: list[str] | None = None
) -> TaskRow:
    if blocked_by is None:
        task_id = row["id"]
        cursor = conn.execute(
            "SELECT prerequisite_task_id FROM task_dependencies WHERE dependent_task_id = ?",
            (task_id,),
        )
        blocked_by = [r[0] for r in cursor.fetchall()]

    row.keys() if hasattr(row, "keys") else []
    return TaskRow(
        id=row["id"],
        title=row["title"],
        owner=row["owner"],
        status=row["status"],
        effort=row["effort"],
        project_path=row["project_path"],
        development_method=row["development_method"],
        acceptance_criteria=_safe_json_load(
            row["acceptance_criteria"],
            [],
            task_id=row["id"],
            column="acceptance_criteria",
        ),
        test_types=_safe_json_load(
            row["test_types"], [], task_id=row["id"], column="test_types"
        ),
        out_of_scope=_safe_json_load(
            row["out_of_scope"], [], task_id=row["id"], column="out_of_scope"
        ),
        definition_of_done=_safe_json_load(
            row["definition_of_done"],
            [],
            task_id=row["id"],
            column="definition_of_done",
        ),
        context=row["context"],
        tdd=_safe_json_load(row["tdd"], task_id=row["id"], column="tdd"),
        version=row["version"],
        created_at=row["created_at"],
        updated_at=_safe_get(row, "updated_at"),
        plan_proposed_at=row["plan_proposed_at"],
        plan_approved_at=row["plan_approved_at"],
        in_progress_at=row["in_progress_at"],
        report_ready_at=row["report_ready_at"],
        review_requested_at=_safe_get(row, "review_requested_at"),
        done_at=row["done_at"],
        cancelled_at=row["cancelled_at"],
        blocked_by=blocked_by,
        parent_id=_safe_get(row, "parent_id"),
        verified=_safe_get(row, "verified", False, coerce=bool),
        verified_at=_safe_get(row, "verified_at"),
        verified_by=_safe_get(row, "verified_by"),
        deadline_minutes=_safe_get(row, "deadline_minutes"),
        failed_at=_safe_get(row, "failed_at"),
        stopped_at=_safe_get(row, "stopped_at"),
        failed_reason=_safe_get(row, "failed_reason"),
        archived_at=_safe_get(row, "archived_at"),
        archived_reason=_safe_get(row, "archived_reason"),
        model_tier=_safe_get(row, "model_tier"),
        pause_reason=_safe_get(row, "pause_reason"),
        worktree_path=_safe_get(row, "worktree_path"),
        blocked_by_raw=_safe_get(row, "blocked_by_raw"),
        workflow=_safe_get(row, "workflow"),
        autonomy=_safe_get(row, "autonomy"),
        require_tdd=_safe_get(row, "require_tdd", coerce=bool)
        if _safe_get(row, "require_tdd") is not None
        else None,
        extras_json=_safe_get(row, "extras_json"),
        locked_contract=_safe_get(row, "locked_contract"),
        contract_locked_at=_safe_get(row, "contract_locked_at"),
        estimated_minutes=_safe_get(row, "estimated_minutes"),
        issue_url=_safe_get(row, "issue_url"),
    )

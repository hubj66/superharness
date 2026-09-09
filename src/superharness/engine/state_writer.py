"""state_writer — unified write API for tasks, inbox, handoffs.

Foundation for SQLite-as-SoT migration. Writes YAML (source of truth
during transition) and mirrors to SQLite so both stores stay in sync.

API:
  set_task_status(project_dir, task_id, status, *, from_status=None) -> bool
  set_inbox_status(project_dir, item_id, status, **fields) -> bool
  upsert_handoff(project_dir, handoff_id, content) -> bool
  mirror_task_dict(project_dir, task) -> None        # best-effort SQLite sync
  mirror_inbox_item_dict(project_dir, item) -> None  # best-effort SQLite sync
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import yaml

import logging

from superharness.engine.state_errors import BoundaryError

logger = logging.getLogger(__name__)


def _is_running_tests() -> bool:
    """Return True if running inside a pytest session."""
    return "pytest" in sys.modules or os.environ.get("PYTEST_CURRENT_TEST") is not None


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_ACTIVE_WORK_STATES = frozenset(
    {"in_progress", "launched", "running", "waiting_input", "pending_user_approval"}
)


def _ensure_active_inbox(project_dir: str, task_id: str, owner: str, now: str) -> None:
    """Ensure an active inbox item exists for a task entering an active work state.

    Called by set_task_status when transitioning to in_progress, waiting_input, etc.
    Silently skipped if an active inbox item already exists for this task.
    """
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            existing = conn.execute(
                "SELECT COUNT(*) FROM inbox WHERE task_id=? AND status IN ('pending','launched','running','paused')",
                (task_id,),
            ).fetchone()
            if not existing or existing[0] == 0:
                import uuid

                iid = f"auto-{uuid.uuid4().hex[:6]}"
                inbox_dao.enqueue(
                    conn,
                    id=iid,
                    task_id=task_id,
                    target_agent=owner,
                    priority=2,
                    max_retries=3,
                    project_path=project_dir,
                    plan_only=False,
                    now=now,
                )
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)


def set_task_status(
    project_dir: str,
    task_id: str,
    status: str,
    *,
    from_status: str | None = None,
    force: bool = False,
    **fields,
) -> bool:
    """Update a contract task's status via SQLite (post-YAML removal).

    SQLite is the only backend (is_sqlite_only is permanently true);
    no YAML write path exists. force=True bypasses the user-facing
    transition graph. The lifecycle reconciler uses it for system-driven
    moves (timeout → archived/failed) that are intentionally outside the
    normal interactive flow.
    """
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    now = _now_utc()
    conn = get_connection(project_dir)
    try:
        init_db(conn)
        task_row = tasks_dao.get(conn, task_id)
        if not task_row:
            # Auto-ingest YAML fixtures the same way state_reader does, so
            # tests that seed contract.yaml then call set_task_status work.
            try:
                from superharness.engine.state_reader import _ensure_ingested

                _ensure_ingested(project_dir)
                task_row = tasks_dao.get(conn, task_id)
            except Exception as e:
                logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        if not task_row:
            return False

        if from_status is not None and task_row.status != from_status:
            return False

        # Idempotent: setting the same status twice is a no-op success,
        # not a transition. Skip validation and the change set.
        if task_row.status == status:
            return True

        # Validate transition against legal status graph (skipped under force=True)
        if not force:
            try:
                from superharness.engine.next_action import validate_status_transition

                validate_status_transition(task_row.status, status)
            except ValueError as e:
                logger.warning("status transition rejected: %s: %s", task_id, e)
                return False

        changes = {"status": status, "updated_at": now}

        # Lifecycle timestamps
        ts_map = {
            "plan_proposed": "plan_proposed_at",
            "plan_approved": "plan_approved_at",
            "in_progress": "in_progress_at",
            "report_ready": "report_ready_at",
            "done": "done_at",
            "failed": "failed_at",
            "stopped": "stopped_at",
            "archived": "archived_at",
            "waiting_input": "updated_at",
        }
        if status in ts_map:
            changes[ts_map[status]] = now

        # Contract lock: freeze acceptance_criteria + tdd at plan_approved time
        if status == "plan_approved" and not task_row.contract_locked_at:
            import json as _json

            snapshot = {
                "acceptance_criteria": task_row.acceptance_criteria,
                "tdd": task_row.tdd,
            }
            changes["locked_contract"] = _json.dumps(snapshot)
            changes["contract_locked_at"] = now

        # Contract lock release: review_failed sends the task back to rework —
        # clear the lock so the plan can be revised before the next approval.
        if status == "plan_proposed" and task_row.status == "review_failed":
            changes["locked_contract"] = None
            changes["contract_locked_at"] = None

        changes.update(fields)
        tasks_dao.update(conn, task_id, version=task_row.version, changes=changes)
        conn.commit()

        # Write event stream
        try:
            from superharness.engine.event_stream import write_event

            write_event(
                project_dir,
                "status_change",
                task_id=task_id,
                from_status=task_row.status,
                to_status=status,
            )
        except Exception as e:
            logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        # Typed telemetry event (additive, SQLite-backed; see engine/events.py).
        # No-op unless events.configure(project_dir) was already called for
        # this process — never spawns a background thread on its own.
        try:
            from superharness.engine import events

            events.emit(
                events.TaskTransition(
                    task_id=task_id,
                    from_status=task_row.status,
                    to_status=status,
                )
            )
        except Exception as e:
            logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        # Auto-capture observation snapshot on report_ready transition.
        # Defensive: capture_observation never raises, but the import
        # could fail in unusual environments. project_dir is threaded
        # through so the rate limiter can use the cross-process
        # SQLite-backed bucket.
        if status == "report_ready":
            try:
                from superharness.engine.observation_capture import capture_observation

                capture_observation(
                    conn, task_id, "report_ready", project_dir=project_dir
                )
            except Exception as e:
                logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        # Guard: active states must have matching inbox item
        if status in _ACTIVE_WORK_STATES:
            from superharness.engine.reliable_orchestrator_gate import (
                is_reliable_orchestrated_task,
            )

            if not is_reliable_orchestrated_task(task_row):
                _ensure_active_inbox(
                    project_dir, task_id, task_row.owner or "claude-code", now
                )

        # I5.4: Auto-record review on terminal statuses
        if status in ("done", "review_passed", "failed", "stopped"):
            try:
                from superharness.engine.behavioral import record_review

                record_review(project_dir, task_id, status)
            except Exception as e:
                logger.warning("Failed to auto-record review for %s: %s", task_id, e)

        return True
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        return False
    finally:
        conn.close()


def set_inbox_status(
    project_dir: str,
    item_id: str,
    status: str,
    **fields,
) -> bool:
    """Update an inbox item's status via SQLite (post-YAML removal)."""
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import inbox_dao

    now = _now_utc()
    conn = get_connection(project_dir)
    try:
        init_db(conn)
        row = inbox_dao.get(conn, item_id)
        if not row:
            return False

        # Extract reason for update_status (handles timestamps natively)
        reason = fields.get("failed_reason")
        inbox_dao.update_status(
            conn,
            item_id,
            from_status=row.status,
            to_status=status,
            now=now,
            reason=reason,
        )

        # Mirror additional fields (map YAML names → SQLite names, filter valid columns)
        _COLUMN_MAP = {
            "task": "task_id",
            "to": "target_agent",
            "project": "project_path",
        }
        _VALID_COLUMNS = frozenset(
            {
                "task_id",
                "target_agent",
                "project_path",
                "priority",
                "retry_count",
                "max_retries",
                "pid",
                "plan_only",
                "failed_reason",
                "created_at",
                "launched_at",
                "last_heartbeat",
                "paused_at",
                "failed_at",
                "done_at",
            }
        )
        db_fields: dict[str, object] = {}
        for k, v in fields.items():
            col = _COLUMN_MAP.get(k, k)
            if col in _VALID_COLUMNS and col not in (
                "status",
                "failed_reason",
                "failed_at",
                "done_at",
                "paused_at",
                "launched_at",
            ):
                db_fields[col] = v
        if db_fields:
            inbox_dao.set_fields(conn, item_id, **db_fields)

        conn.commit()
        from superharness.engine.sqlite_only import is_sqlite_only

        if not is_sqlite_only():
            _export_inbox_yaml(project_dir)
        return True
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        return False
    finally:
        conn.close()


def _export_contract_yaml(project_dir: str) -> None:
    """Regenerate contract.yaml from the current SQLite state (export only).

    In sqlite_only mode, skips the write entirely — YAML is generated
    only on explicit `shux export-yaml` command.
    """
    try:
        from superharness.engine.sqlite_only import is_sqlite_only

        if is_sqlite_only():
            return  # SQLite is SoT — YAML is not needed for runtime
        from superharness.engine import state_reader, contract_io

        doc = state_reader.get_contract_doc(project_dir)
        contract_path = os.path.join(project_dir, ".superharness", "contract.yaml")
        contract_io.write_contract(contract_path, doc)
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)


def _export_inbox_yaml(project_dir: str) -> None:
    """Regenerate inbox.yaml from the current SQLite state (export only).

    In sqlite_only mode, skips the write entirely — YAML is generated
    only on explicit `shux export-yaml` command.
    """
    try:
        from superharness.engine.sqlite_only import is_sqlite_only

        if is_sqlite_only():
            return  # SQLite is SoT — YAML is not needed for runtime
        from superharness.engine import state_reader

        items = state_reader.get_inbox_items(project_dir)
        inbox_path = os.path.join(project_dir, ".superharness", "inbox.yaml")
        with open(inbox_path, "w", encoding="utf-8") as f:
            f.write(
                "# Delegation inbox\n# status: pending|launched|running|done|failed|stale\n"
            )
            yaml.dump(items, f, default_flow_style=False, sort_keys=True)
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)


def write_handoff_to_db(
    project_dir: str,
    content: dict,
    *,
    task_id: str | None = None,
    phase: str | None = None,
    created_at: str | None = None,
) -> bool:
    """Persist a handoff to the SQLite handoffs table (source of truth).

    Maps a handoff content dict to the handoffs table columns. The full
    document is stored both as readable text (content) and structured json
    (metadata) so recall can search it and readers can reconstruct it.

    Best-effort for infrastructure failures (DB unreachable, disk full,
    etc.): those never raise, so a storage outage must not break the YAML
    write path that un-migrated readers still depend on during the
    transition. A `BoundaryError` from the typed handoff boundary
    (`handoffs_dao.append`, e.g. an invalid `phase`/`status`) is a caller
    bug, not an infrastructure failure, and is re-raised so it is not
    silently swallowed.
    """
    try:
        from superharness.engine.db import managed_connection, now_iso
        from superharness.engine import handoffs_dao

        tid = task_id or content.get("task") or content.get("task_id") or ""
        ph = phase or content.get("phase") or "report"
        status = content.get("status") or "report_ready"
        frm = content.get("from") or content.get("from_agent")
        to = content.get("to") or content.get("to_agent")
        created = created_at or (
            content.get("date")
            or content.get("closed_at")
            or content.get("created_at")
            or now_iso()
        )
        try:
            body = yaml.dump(
                content, default_flow_style=False, allow_unicode=True, sort_keys=False
            )
        except Exception:
            body = str(content)

        with managed_connection(project_dir) as conn:
            # Stub the task row if it doesn't exist (FK guard).
            if (
                str(tid)
                and not conn.execute(
                    "SELECT 1 FROM tasks WHERE id = ?", (str(tid),)
                ).fetchone()
            ):
                conn.execute(
                    "INSERT OR IGNORE INTO tasks (id, title, status, project_path, created_at, version)"
                    " VALUES (?, ?, 'todo', ?, ?, 1)",
                    (str(tid), str(tid), str(project_dir), str(created)),
                )
            handoffs_dao.append(
                conn,
                task_id=str(tid),
                phase=str(ph),
                status=str(status),
                from_agent=frm,
                to_agent=to,
                content=body,
                metadata=content,
                now=str(created),
            )

            # Self-reported usage (optional): agents with no programmatic usage
            # data (Codex CLI, Gemini CLI, OpenCode) report tokens/cost via the
            # handoff payload. Only record when at least one field is present.
            has_usage = any(
                content.get(k) is not None
                for k in ("input_tokens", "output_tokens", "cost_usd")
            )
            if has_usage:
                from superharness.engine import usage_dao

                usage_dao.record(
                    conn,
                    task_id=str(tid),
                    agent=str(frm) if frm else "unknown",
                    source="handoff",
                    model=content.get("model"),
                    input_tokens=content.get("input_tokens"),
                    output_tokens=content.get("output_tokens"),
                    cost_usd=content.get("cost_usd"),
                    now=str(created),
                )
        return True
    except BoundaryError:
        raise
    except Exception as e:
        logger.warning("write_handoff_to_db failed (non-fatal): %s", e, exc_info=True)
        return False


def backfill_handoffs_from_yaml(project_dir: str) -> dict[str, int]:
    """One-time import of existing handoff YAML files into the SQLite table.

    Idempotent: skips handoffs already present (matched on task/phase/created_at)
    and skips orphans whose task no longer exists (the FK would reject them).
    Returns counts: {added, skipped_dup, skipped_orphan, errors}.
    """
    import glob as _glob
    from superharness.engine.db import managed_connection, now_iso
    from superharness.engine import handoffs_dao

    counts = {"added": 0, "skipped_dup": 0, "skipped_orphan": 0, "errors": 0}
    handoffs_dir = os.path.join(project_dir, ".superharness", "handoffs")
    files = sorted(
        _glob.glob(os.path.join(handoffs_dir, "*.yaml"))
        + _glob.glob(os.path.join(handoffs_dir, "*.yml"))
    )
    if not files:
        return counts

    with managed_connection(project_dir) as conn:
        known_tasks = {r["id"] for r in conn.execute("SELECT id FROM tasks")}
        for fpath in files:
            try:
                with open(fpath, encoding="utf-8") as f:
                    content = yaml.safe_load(f) or {}
                if not isinstance(content, dict):
                    continue
                tid = str(content.get("task") or content.get("task_id") or "")
                phase = str(content.get("phase") or "report")
                created = str(
                    content.get("date")
                    or content.get("closed_at")
                    or content.get("created_at")
                    or now_iso()
                )
                if tid not in known_tasks:
                    counts["skipped_orphan"] += 1
                    continue
                dup = conn.execute(
                    "SELECT 1 FROM handoffs WHERE task_id=? AND phase=? AND created_at=? LIMIT 1",
                    (tid, phase, created),
                ).fetchone()
                if dup:
                    counts["skipped_dup"] += 1
                    continue
                try:
                    body = yaml.dump(
                        content,
                        default_flow_style=False,
                        allow_unicode=True,
                        sort_keys=False,
                    )
                except Exception:
                    body = str(content)
                handoffs_dao.append(
                    conn,
                    task_id=tid,
                    phase=phase,
                    status=str(content.get("status") or "report_ready"),
                    from_agent=content.get("from") or content.get("from_agent"),
                    to_agent=content.get("to") or content.get("to_agent"),
                    content=body,
                    metadata=content,
                    now=created,
                )
                counts["added"] += 1
            except Exception as e:
                logger.warning("backfill_handoffs: %s failed: %s", fpath, e)
                counts["errors"] += 1
    return counts


def backfill_ledger_from_yaml(project_dir: str) -> dict[str, int]:
    """One-time import of ledger.md lines into the SQLite ledger table.

    Idempotent: skips entries already present (matched on created_at + action).
    Returns counts: {added, skipped_dup, errors}.
    """
    import re as _re
    from superharness.engine.db import managed_connection
    from superharness.engine import ledger_dao

    counts = {"added": 0, "skipped_dup": 0, "errors": 0}
    ledger_path = os.path.join(project_dir, ".superharness", "ledger.md")
    if not os.path.isfile(ledger_path):
        return counts

    lines = []
    try:
        with open(ledger_path, encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        logger.warning("backfill_ledger_from_yaml: cannot read ledger.md: %s", e)
        return counts

    _TS_RE = _re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?)")
    _SEP_RE = _re.compile(r"\s+[—\-–]\s+")

    with managed_connection(project_dir) as conn:
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = _TS_RE.search(line)
            if not m:
                continue
            ts = m.group(1)
            clean = line.lstrip("- ").replace(ts, "").strip(" —–-")
            parts = _SEP_RE.split(clean, maxsplit=2)
            if len(parts) >= 2:
                agent = parts[0].strip()
                action = parts[1].strip()
            else:
                agent = "system"
                action = clean or "operational event"

            try:
                dup = conn.execute(
                    "SELECT 1 FROM ledger WHERE created_at=? AND action=? LIMIT 1",
                    (ts, action),
                ).fetchone()
                if dup:
                    counts["skipped_dup"] += 1
                    continue
                ledger_dao.record(
                    conn, agent=agent, action=action, details=None, now=ts
                )
                counts["added"] += 1
            except Exception as e:
                logger.warning("backfill_ledger_from_yaml: line failed: %s", e)
                counts["errors"] += 1

    return counts


def upsert_handoff(project_dir: str, handoff_id: str, content: dict) -> bool:
    """Write a handoff to SQLite (source of truth) and YAML (export).

    SQLite is authoritative. The YAML file is a compat export — all readers
    now query SQLite via handoffs_dao.
    """
    handoffs = os.path.join(project_dir, ".superharness", "handoffs")
    os.makedirs(handoffs, exist_ok=True)
    safe_id = handoff_id.replace("/", "-")
    path = os.path.join(handoffs, f"{safe_id}.yaml")

    write_handoff_to_db(project_dir, content)

    try:
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(content, f, default_flow_style=False, allow_unicode=True)
        return True
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)
        return False


_KNOWN_TASK_COLS = frozenset(
    {
        "title",
        "owner",
        "effort",
        "project_path",
        "development_method",
        "acceptance_criteria",
        "test_types",
        "out_of_scope",
        "definition_of_done",
        "context",
        "tdd",
        "created_at",
        "updated_at",
        "plan_proposed_at",
        "plan_approved_at",
        "in_progress_at",
        "report_ready_at",
        "review_requested_at",
        "done_at",
        "cancelled_at",
        "blocked_by_raw",
        "parent_id",
        "verified",
        "verified_at",
        "verified_by",
        "deadline_minutes",
        "failed_at",
        "stopped_at",
        "failed_reason",
        "archived_at",
        "archived_reason",
        "model_tier",
        "pause_reason",
        "worktree_path",
        "workflow",
        "require_tdd",
        "estimated_minutes",
        "locked_contract",
        "contract_locked_at",
    }
)
# Fields to skip entirely — either handled elsewhere or not real task columns.
_SKIP_TASK_FIELDS = frozenset(
    {"id", "status", "version", "blocked_by", "depends_on", "extras_json"}
)


def _mirror_task_to_sqlite(
    project_dir: str, task_id: str, status: str, **fields
) -> None:
    """Best-effort SQLite sync from state_writer.

    Unknown fields (not real task columns) are merged into extras_json so
    they survive round-trips through SQLite without silently failing.
    """
    try:
        import json as _j
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import tasks_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            row = tasks_dao.get(conn, task_id)
            if row:
                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                changes: dict = {"status": status, "updated_at": now}
                ts_map = {
                    "plan_proposed": "plan_proposed_at",
                    "plan_approved": "plan_approved_at",
                    "in_progress": "in_progress_at",
                    "report_ready": "report_ready_at",
                    "done": "done_at",
                    "failed": "failed_at",
                    "stopped": "stopped_at",
                }
                if status in ts_map:
                    changes[ts_map[status]] = now
                # Separate known columns from extras
                extras: dict = {}
                for k, v in fields.items():
                    if k in _SKIP_TASK_FIELDS:
                        continue
                    elif k in _KNOWN_TASK_COLS:
                        changes[k] = v
                    else:
                        extras[k] = v
                # Merge extras into existing extras_json blob
                if extras:
                    existing_extras: dict = {}
                    if row.extras_json:
                        try:
                            existing_extras = _j.loads(row.extras_json) or {}
                        except Exception as e:
                            logger.warning(
                                "state_writer unexpected error: %s", e, exc_info=True
                            )
                    existing_extras.update(extras)
                    changes["extras_json"] = _j.dumps(existing_extras)
                tasks_dao.update(conn, task_id, version=row.version, changes=changes)
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)


def _mirror_inbox_to_sqlite(
    project_dir: str, item_id: str, status: str, **fields
) -> None:
    """Best-effort SQLite sync for inbox items from state_writer.

    Maps YAML field names (task, to, project) to SQLite column names
    (task_id, target_agent, project_path) before executing raw UPDATE.
    """
    # YAML → SQLite column name mapping
    _COLUMN_MAP = {
        "task": "task_id",
        "to": "target_agent",
        "project": "project_path",
    }
    # Valid SQLite inbox columns (prevents SQL errors from extraneous keys)
    _VALID_COLUMNS = frozenset(
        {
            "task_id",
            "target_agent",
            "project_path",
            "status",
            "priority",
            "retry_count",
            "max_retries",
            "pid",
            "plan_only",
            "failed_reason",
            "created_at",
            "launched_at",
            "last_heartbeat",
            "paused_at",
            "failed_at",
            "done_at",
        }
    )

    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            row = inbox_dao.get(conn, item_id)
            if row:
                now = _now_utc()
                # Extract reason for update_status (it handles timestamps natively)
                reason = fields.get("failed_reason")
                inbox_dao.update_status(
                    conn,
                    item_id,
                    from_status=row.status,
                    to_status=status,
                    now=now,
                    reason=reason,
                )
                # Mirror remaining fields (map YAML names → SQLite names, filter invalid)
                db_fields: dict[str, object] = {}
                for k, v in fields.items():
                    col = _COLUMN_MAP.get(k, k)
                    if col in _VALID_COLUMNS and col not in (
                        "status",
                        "failed_reason",
                        "failed_at",
                        "done_at",
                        "paused_at",
                        "launched_at",
                    ):
                        db_fields[col] = v
                if db_fields:
                    inbox_dao.set_fields(conn, item_id, **db_fields)
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("state_writer unexpected error: %s", e, exc_info=True)


def mirror_task_dict(project_dir: str, task: dict) -> None:
    """Public API to mirror a task dictionary to SQLite."""
    if not isinstance(task, dict):
        return
    tid = str(task.get("id", ""))
    status = str(task.get("status", "todo"))
    fields = {k: v for k, v in task.items() if k not in ("id", "status")}
    _mirror_task_to_sqlite(project_dir, tid, status, **fields)


def mirror_inbox_item_dict(project_dir: str, item: dict) -> None:
    """Public API to mirror an inbox item dictionary to SQLite."""
    if not isinstance(item, dict):
        return
    iid = str(item.get("id", ""))
    status = str(item.get("status", "pending"))
    fields = {k: v for k, v in item.items() if k not in ("id", "status")}
    _mirror_inbox_to_sqlite(project_dir, iid, status, **fields)

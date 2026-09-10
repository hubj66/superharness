"""Python port of task.sh — create/delete/status operations on contract tasks.

Output format is byte-for-byte identical to the Ruby version.
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

from superharness.engine.taxonomy import VALID_EFFORTS
from superharness.engine.next_action import ALL_STATUSES
from superharness.harnesses import KNOWN_HARNESSES
from superharness.utils.paths import is_project_initialized

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

VALID_OWNERS = frozenset(KNOWN_HARNESSES) | {"owner"}
VALID_CREATE_STATUSES = {"todo", "in_progress", "pending_user_approval", "done"}
VALID_WORKFLOWS = {
    "implementation",
    "quick",
    "discussion",
    "review",
    "approval",
    "note",
    "reliable-orchestrator",
}
TOKEN_RE = re.compile(r"^[A-Za-z0-9._/-]+$")


def _profile_autonomy_is_ai_driven(profile: dict) -> bool:
    from superharness.engine.profile import normalize_autonomy

    return normalize_autonomy(profile.get("autonomy")) == "ai_driven"


def _load_require_tdd_from_profile(project_path: str) -> bool:
    """Load require_tdd default from project profile.yaml. Never raises."""
    profile_path = os.path.join(project_path, ".superharness", "profile.yaml")
    if not os.path.exists(profile_path):
        return True
    try:
        import yaml as _yaml

        with open(profile_path) as _f:
            profile = _yaml.safe_load(_f) or {}
    except Exception as e:
        logger.warning("task.py unexpected error: %s", e, exc_info=True)
        return True
    wf = profile.get("workflow")
    if isinstance(wf, dict) and "require_tdd" in wf:
        return bool(wf["require_tdd"])
    return True


def _validate_token(name: str, value: str) -> None:
    if not value:
        _abort(f"{name} must not be empty", 2)
    if "\n" in value or "\r" in value or "\t" in value:
        _abort(f"{name} contains control characters", 2)
    if not TOKEN_RE.match(value):
        _abort(f"{name} must match ^[A-Za-z0-9._/-]+$", 2)
    # TOKEN_RE has to permit "." and "/" because real discussion round ids look
    # like "disc-fresh/round-1" — but that also admits ".." components, and a
    # task id reaches os.path.join() when a worktree path is built for dispatch
    # (inbox_dispatch._git_worktree_add), then shutil.rmtree() when the task is
    # closed. Reject traversal explicitly rather than relying on the charset.
    if value.startswith("/"):
        _abort(f"{name} must not be an absolute path", 2)
    if ".." in value.split("/"):
        _abort(f"{name} must not contain '..' path components", 2)


def _validate_issue_url(url: str) -> str:
    """Validate a GitHub/GitLab issue URL. Returns the URL unchanged on
    success; raises ValueError on an unusable scheme or missing host."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"issue url must be an http(s) URL: {url!r}")
    return url


_JSON_MODE = False
_JSON_CTX: dict = {}


def _abort(msg: str, code: int = 1) -> None:
    if _JSON_MODE:
        from superharness.utils.json_output import emit_error

        emit_error(msg, exit_code=code, **_JSON_CTX)
    print(msg, file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _parse_blocked_by(value: str | list | None) -> str | list:
    """Normalise blocked_by input to 'none', a single ID, or a list."""
    if value is None or value == "" or value == "none":
        return "none"
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    s = str(value).strip()
    if "," in s:
        return [v.strip() for v in s.split(",") if v.strip()]
    return s


def create(
    project_dir: str,
    task_id: str,
    title: str,
    owner: str,
    status: str,
    project_path: str,
    dependency: Optional[str] = None,
    criteria: Optional[list] = None,
    blocked_by: str | list | None = None,
    tdd_red: str = "",
    tdd_green: str = "",
    tdd_refactor: str = "",
    workflow: str = "quick",
    development_method: str = "",
    effort: str = "medium",
    test_types: Optional[list[str]] = None,
    out_of_scope: Optional[list[str]] = None,
    definition_of_done: Optional[list[str]] = None,
    context: Optional[str] = None,
    timeout_minutes: Optional[int] = None,
    plan: Optional[dict] = None,
    ship_on_complete: bool = False,
    require_tdd: Optional[bool] = None,
    issue_url: Optional[str] = None,
) -> int:
    _validate_token("task id", task_id)
    if dependency:
        _validate_token("dependency id", dependency)
    if issue_url:
        try:
            issue_url = _validate_issue_url(issue_url)
        except ValueError as e:
            _abort(str(e), 2)

    if owner not in VALID_OWNERS:
        _abort(f"owner must be one of: {', '.join(sorted(VALID_OWNERS))}", 2)
    if status not in VALID_CREATE_STATUSES:
        _abort("status must be todo, in_progress, pending_user_approval, or done", 2)
    if workflow and workflow not in VALID_WORKFLOWS:
        _abort(
            f"workflow must be one of: {', '.join(sorted(VALID_WORKFLOWS))}",
            2,
        )
    # development_method accepts any string (no hardcoded enum)
    if effort and effort not in VALID_EFFORTS:
        _abort(f"effort must be one of: {', '.join(sorted(VALID_EFFORTS))}", 2)

    if require_tdd is None:
        require_tdd = _load_require_tdd_from_profile(project_path)

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao
    from superharness.engine.tasks_dao import TaskRow

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        existing_rows = tasks_dao.get_all(conn)
        existing_ids = {r.id for r in existing_rows}

        if task_id in existing_ids:
            _abort(f"task '{task_id}' already exists")

        if dependency:
            if dependency == task_id:
                _abort(f"task '{task_id}' cannot depend on itself")
            if dependency not in existing_ids:
                _abort(f"dependency task '{dependency}' not found")

        blocked = _parse_blocked_by(blocked_by)
        if blocked != "none":
            ids_to_check = blocked if isinstance(blocked, list) else [blocked]
            for bid in ids_to_check:
                if bid == task_id:
                    _abort(f"task '{task_id}' cannot be blocked by itself")
                if bid not in existing_ids:
                    _abort(f"blocked_by task '{bid}' not found")

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        tdd_dict: dict | None = None
        if plan:
            tdd_dict = dict(plan)
        elif tdd_red or tdd_green or tdd_refactor:
            tdd_dict = {}
            if tdd_red:
                tdd_dict["red"] = tdd_red
            if tdd_green:
                tdd_dict["green"] = tdd_green
            if tdd_refactor:
                tdd_dict["refactor"] = tdd_refactor

        import json as _json

        extras: dict = {}
        if ship_on_complete:
            extras["ship_on_complete"] = True

        row = TaskRow(
            id=task_id,
            title=title,
            owner=owner,
            status=status,
            effort=effort or None,
            project_path=project_path,
            development_method=development_method or "tdd",
            acceptance_criteria=list(criteria) if criteria else [],
            test_types=list(test_types) if test_types else [],
            out_of_scope=list(out_of_scope) if out_of_scope else [],
            definition_of_done=list(definition_of_done) if definition_of_done else [],
            context=context,
            tdd=tdd_dict,
            version=1,
            created_at=now,
            deadline_minutes=timeout_minutes,
            workflow=workflow or None,
            require_tdd=bool(require_tdd),
            extras_json=_json.dumps(extras) if extras else None,
            issue_url=issue_url or None,
        )
        tasks_dao.upsert(conn, row)

        if blocked != "none":
            dep_ids = blocked if isinstance(blocked, list) else [blocked]
            tasks_dao.set_dependencies(conn, task_id, dep_ids)

        conn.commit()
    finally:
        conn.close()

    print(
        f"Created task '{task_id}' (owner={owner}, status={status}, blocked_by={blocked})"
    )
    return 0


def archive_done(project_dir: str, ids: list[str] | None = None) -> int:
    """Flip every done task (or specific ids) to archived in one pass.

    Bypasses the per-task actor/owner guard used by status_update, because
    this is a bulk admin operation run by the operator (e.g. end-of-session
    cleanup).
    """
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    targets = set(ids) if ids else None
    flipped: list[str] = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        rows = tasks_dao.get_all(conn)
        for row in rows:
            if targets is not None and row.id not in targets:
                continue
            if row.status != "done":
                continue
            tasks_dao.update(
                conn,
                row.id,
                row.version,
                {
                    "status": "archived",
                    "archived_at": now,
                    "updated_at": now,
                },
            )
            flipped.append(row.id)
        if flipped:
            conn.commit()
    finally:
        conn.close()

    if not flipped:
        print("No done tasks to archive.")
        return 0

    print(f"Archived {len(flipped)} task(s):")
    for tid in flipped:
        print(f"  - {tid}")
    return 0


def delete(project_dir: str, task_id: str) -> int:
    _validate_token("task id", task_id)

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        if tasks_dao.get(conn, task_id) is None:
            _abort(f"task '{task_id}' not found")
        conn.execute(
            "DELETE FROM task_dependencies WHERE dependent_task_id=? OR prerequisite_task_id=?",
            (task_id, task_id),
        )
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
    finally:
        conn.close()

    print(f"Deleted task '{task_id}'")
    return 0


def _cascade_unblocked_tasks(project_dir: str, finished_task_id: str) -> None:
    """Scan tasks blocked by finished_task_id and enqueue them if now fully unblocked."""
    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")

    if not os.path.exists(profile_file):
        return
    try:
        import yaml as _yaml

        with open(profile_file) as _f:
            profile = _yaml.safe_load(_f) or {}
        if not profile.get("auto_dispatch"):
            return
    except Exception as e:
        logger.warning("task.py unexpected error: %s", e, exc_info=True)
        return

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao, inbox_dao
    from superharness.engine.inbox import _deps_satisfied
    from superharness.engine.state_errors import StateError as _StateError

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # _deps_satisfied accepts project_dir; derive a canonical contract_file path for it
    _contract_file = os.path.join(project_dir, ".superharness", "contract.yaml")

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        rows = tasks_dao.get_all(conn)

        dependents = [r for r in rows if finished_task_id in (r.blocked_by or [])]
        if not dependents:
            return

        for row in dependents:
            if row.status not in ("todo", "plan_approved"):
                continue
            if not _deps_satisfied(_contract_file, row.id):
                continue

            if row.status == "todo" and not _profile_autonomy_is_ai_driven(profile):
                continue

            item_id = f"cascade-{uuid.uuid4().hex[:6]}"
            plan_only = row.status == "todo"
            owner = row.owner or "claude-code"
            try:
                inbox_dao.enqueue(
                    conn,
                    id=item_id,
                    task_id=row.id,
                    target_agent=owner,
                    priority=2,
                    project_path=project_dir,
                    plan_only=plan_only,
                    now=now,
                )
                conn.commit()
                print(
                    f"cascading-dispatch: task {row.id} unblocked and enqueued (item {item_id}, plan_only={plan_only})"
                )
            except _StateError:
                pass
    finally:
        conn.close()


def _load_latest_plan_handoff(project_dir: str, task_id: str) -> dict | None:
    """Return the latest phase=plan handoff for a task from SQLite."""
    try:
        from superharness.engine import state_reader as _sr_t
        import yaml as _yaml_t

        rows = _sr_t.get_handoffs(project_dir, task_id=task_id)
        plan_rows = [r for r in rows if str(r.get("phase", "")) == "plan"]
        if not plan_rows:
            return None
        row = plan_rows[-1]
        content_text = row.get("content") or ""
        if content_text:
            try:
                parsed = _yaml_t.safe_load(content_text)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return dict(row)
    except Exception as e:
        logger.warning("task.py _load_latest_plan_handoff failed: %s", e, exc_info=True)
        return None


def _enqueue_for_implementation(project_dir: str, task_id: str) -> None:
    """Instantly enqueue a plan_approved task for implementation."""
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao, inbox_dao
    from superharness.engine.state_errors import StateError as _StateError

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        row = tasks_dao.get(conn, task_id)
        if not row:
            return
        owner = row.owner or "claude-code"
        item_id = f"auto-impl-{uuid.uuid4().hex[:6]}"
        try:
            inbox_dao.enqueue(
                conn,
                id=item_id,
                task_id=task_id,
                target_agent=owner,
                priority=2,
                project_path=project_dir,
                plan_only=False,
                now=now,
            )
            conn.commit()
            print(
                f"auto-dispatch: task {task_id} enqueued for implementation (item {item_id})"
            )
        except _StateError:
            pass
    finally:
        conn.close()


def set_owner(project_dir: str, task_id: str, new_owner: str) -> int:
    """Update task owner and clean up any active inbox items for the old owner."""
    import signal as _signal

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao as _tdao

    conn_t = get_connection(project_dir)
    try:
        init_db(conn_t)
        task_row = _tdao.get(conn_t, task_id)
        if task_row is None:
            _abort(f"task '{task_id}' not found")
        old_owner = task_row.owner or "unknown"
        if old_owner == new_owner:
            print(f"Task '{task_id}' is already owned by '{new_owner}'")
            return 0
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _tdao.update(
            conn_t, task_id, task_row.version, {"owner": new_owner, "updated_at": now}
        )
        conn_t.commit()
    finally:
        conn_t.close()

    print(f"Reassigned task '{task_id}': {old_owner} → {new_owner}")

    # Cancel active inbox rows that were dispatched to the old owner.
    from superharness.utils.paths import resolve_active_state_db_path

    db_path = resolve_active_state_db_path(project_dir)

    if not os.path.exists(db_path):
        return 0

    ACTIVE = ("pending", "launched", "running")
    removed_ids: list[str] = []

    try:
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            placeholders = ",".join("?" * len(ACTIVE))
            rows = conn.execute(
                f"SELECT id, pid FROM inbox WHERE task_id = ? "
                f"AND target_agent = ? AND status IN ({placeholders})",
                (task_id, old_owner, *ACTIVE),
            ).fetchall()
            for row in rows:
                item_id, pid = row[0], row[1]
                if pid:
                    try:
                        os.kill(int(pid), _signal.SIGTERM)
                        print(f"  stopped pid={pid} (item {item_id})")
                    except (ProcessLookupError, PermissionError, ValueError):
                        pass
                removed_ids.append(str(item_id))
            if removed_ids:
                # Cancel (don't delete) — preserves audit trail in the inbox table.
                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                conn.executemany(
                    "UPDATE inbox SET status='cancelled', failed_at=? WHERE id=?",
                    [(now, rid) for rid in removed_ids],
                )
                conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        # Don't swallow silently. The original bug masked an ImportError —
        # surface failures clearly so the operator knows cleanup didn't run.
        print(f"  warning: inbox cleanup failed: {exc}", file=sys.stderr)

    if removed_ids:
        print(
            f"  canceled {len(removed_ids)} inbox item(s) for old owner '{old_owner}': {', '.join(removed_ids)}"
        )

    # Re-enqueue to the new owner when the task is dispatch-ready.
    if removed_ids:
        from superharness.engine.next_action import (
            allowed_statuses_for_workflow,
            plan_only_allowed_statuses,
        )
        from superharness.commands.inbox_enqueue import enqueue_cmd

        task_status = str(task_row.status or "")
        task_workflow = str(task_row.workflow or "implementation")
        dispatch_ready = task_status in allowed_statuses_for_workflow(task_workflow)
        plan_only_ready = task_status in plan_only_allowed_statuses(task_workflow)

        if dispatch_ready or plan_only_ready:
            plan_only = not dispatch_ready and plan_only_ready
            try:
                enqueue_cmd(
                    project_dir=project_dir,
                    task_id=task_id,
                    target=new_owner,
                    item_id=None,
                    priority=2,
                    plan_only=plan_only,
                    force_reassign=True,
                )
                suffix = " (plan-only)" if plan_only else ""
                print(f"  re-enqueued '{task_id}' to '{new_owner}'{suffix}")
            except SystemExit as exc:
                if exc.code != 0:
                    print(
                        "  warning: re-enqueue failed — task is reassigned but not in inbox",
                        file=sys.stderr,
                    )
        else:
            print(
                f"  task status '{task_status}' is not dispatch-ready — not re-enqueued"
            )

    return 0


def status_update(
    project_dir: str,
    task_id: str,
    status: str,
    actor: str,
    reason: str = "",
    summary: str = "",
    _recursion_guard: bool = False,
) -> int:
    _validate_token("task id", task_id)

    if status not in ALL_STATUSES:
        _abort(f"status must be one of: {', '.join(sorted(ALL_STATUSES))}", 2)

    if status in ("failed", "stopped") and not reason:
        _abort(f"error: --reason is required when status={status}", 2)

    if (
        status in ("todo", "in_progress", "pending_user_approval", "done")
        and not summary
    ):
        _abort(f"error: --summary is required when status={status}", 2)

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        task_row = tasks_dao.get(conn, task_id)
        if task_row is None:
            _abort(f"task '{task_id}' not found")

        owner = str(task_row.owner or "")
        if not owner:
            _abort(f"task '{task_id}' has no owner set")

        if actor != owner and not _recursion_guard:
            _abort(
                f"forbidden: actor '{actor}' cannot update task '{task_id}' owned by '{owner}'"
            )  # shipguard:ignore PY-007

        # Validate against the legal status transition graph
        try:
            from superharness.engine.next_action import validate_status_transition

            validate_status_transition(str(task_row.status or ""), status)
        except ValueError as _e:
            _abort(f"status transition rejected: {_e}", 2)

        # Scope guard on plan_approved
        if status == "plan_approved":
            ac_count = len(task_row.acceptance_criteria or [])
            if ac_count > 3:
                print(
                    f"⚠  Scope warning: task '{task_id}' has {ac_count} acceptance criteria (threshold: 3).\n"
                    f"   Consider decomposing into subtasks before dispatch.\n"
                    f"   Use: shux delegate {task_id} --orchestrate  (auto-decompose via Opus)\n"
                    f"   Or: manually split into smaller tasks with blocked_by ordering.",
                    file=sys.stderr,
                )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        changes: dict = {"status": status, "updated_at": now}

        # Stamp lifecycle timestamps
        _ts_col = {
            "plan_proposed": "plan_proposed_at",
            "plan_approved": "plan_approved_at",
            "in_progress": "in_progress_at",
            "report_ready": "report_ready_at",
            "review_requested": "review_requested_at",
            "done": "done_at",
            "cancelled": "cancelled_at",
            "archived": "archived_at",
        }
        if status in _ts_col:
            changes[_ts_col[status]] = now

        if status in ("failed", "stopped") and reason:
            changes["failed_reason"] = reason
            changes["failed_at"] = now
            if status == "stopped":
                changes["stopped_at"] = now
        else:
            changes["failed_reason"] = None
            changes["failed_at"] = None
            changes["stopped_at"] = None

        tasks_dao.update(conn, task_id, task_row.version, changes)
        conn.commit()
    finally:
        conn.close()

    # Auto-approve hook: plan_proposed → plan_approved when auto_approve_plans=true.
    # Only fires for the implementation workflow — quick/note/review/discussion tasks
    # have no plan cycle, and auto-approving them causes a permanent dispatch block
    # because plan_approved is not in their allowed dispatch status set.
    from superharness.engine.reliable_orchestrator_gate import (
        is_reliable_orchestrated_task,
    )

    if (
        status == "plan_proposed"
        and not _recursion_guard
        and task_row.workflow == "implementation"
        and not is_reliable_orchestrated_task(task_row)
    ):
        profile_path = os.path.join(project_dir, ".superharness", "profile.yaml")
        auto_approve = False
        if os.path.exists(profile_path):
            try:
                import yaml as _yaml

                with open(profile_path) as _f:
                    profile = _yaml.safe_load(_f) or {}
                    auto_approve = bool(profile.get("auto_approve_plans", False))
            except Exception as e:
                logger.warning("task.py unexpected error: %s", e, exc_info=True)
                pass
        if auto_approve:
            try:
                from superharness.engine.plan_validator import validate_plan

                plan_handoff = _load_latest_plan_handoff(project_dir, task_id)
                if plan_handoff:
                    task_dict = {
                        "id": task_row.id,
                        "acceptance_criteria": task_row.acceptance_criteria,
                        "tdd": task_row.tdd,
                        "owner": task_row.owner,
                    }
                    result = validate_plan(plan_handoff, task_dict)  # type: ignore[arg-type]
                    if not result.passed:
                        print(
                            f"Auto-approve blocked for '{task_id}': "
                            + "; ".join(result.failures)
                        )
                        return 0
            except Exception as e:
                print(f"Warning: plan validation skipped: {e}", file=sys.stderr)

            print(f"Auto-approving task '{task_id}' (auto-approved per project policy)")
            status_update(
                project_dir,
                task_id,
                "plan_approved",
                actor="ai-autonomy",
                summary="auto-approved per project policy",
                _recursion_guard=True,
            )

            try:
                _enqueue_for_implementation(project_dir, task_id)
            except Exception as e:
                print(f"Warning: auto-dispatch re-enqueue failed: {e}", file=sys.stderr)

    # Subtask resolution gate for done transition
    if status == "done":
        try:
            from superharness.engine.subtask_gate import (
                evaluate_subtask_gate_from_disk,
                format_gate_error,
            )

            task_dict = {"id": task_row.id, "extras_json": task_row.extras_json}
            gate = evaluate_subtask_gate_from_disk(task_dict, project_dir)
            if gate.enabled and gate.blocking:
                _abort(format_gate_error(task_id, gate))
        except ImportError:
            pass

    # Warn about unverified acceptance criteria when marking done
    if status == "done":
        ac = task_row.acceptance_criteria
        if ac:
            print(
                f"Warning: task '{task_id}' has acceptance criteria — verify before closing:",
                file=sys.stderr,
            )
            for c in ac:
                print(f"  - {c}", file=sys.stderr)

        try:
            _cascade_unblocked_tasks(project_dir, task_id)
        except Exception as e:
            print(f"Warning: cascading dispatch failed: {e}", file=sys.stderr)

        try:
            from superharness.engine.skill_extractor import record_skill

            task_dict = {
                "id": task_row.id,
                "title": task_row.title,
                "owner": task_row.owner,
                "status": task_row.status,
                "tdd": task_row.tdd,
            }
            skill = record_skill(project_dir, task_dict)
            if skill:
                print(f"Skill recorded: [{skill.category}] {skill.title}")
        except Exception as e:
            logger.warning("task.py unexpected error: %s", e, exc_info=True)
            pass
    print(f"Updated task '{task_id}' status={status} by actor={actor}")
    return 0


# ---------------------------------------------------------------------------
# Capability requirements (requires: block) — set/show on a task's extras_json
# ---------------------------------------------------------------------------


def set_requires(
    project_dir: str,
    task_id: str,
    cli_add: list[str] | None = None,
    cli_remove: list[str] | None = None,
    env_add: list[str] | None = None,
    env_remove: list[str] | None = None,
    skill_add: list[str] | None = None,
    skill_remove: list[str] | None = None,
    mcp_add: list[str] | None = None,
    mcp_remove: list[str] | None = None,
    fail_mode: str | None = None,
    clear: bool = False,
    show: bool = False,
) -> int:
    """Read/write the `requires:` block on a task's extras_json (SQLite source of truth)."""
    import json as _j

    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        row = tasks_dao.get(conn, task_id)
        if row is None:
            _abort(f"task '{task_id}' not found")
            return 1

        extras: dict = _j.loads(row.extras_json) if row.extras_json else {}
        req: dict = dict(extras.get("requires") or {})

        if show:
            if req:
                import yaml as _yaml

                print(f"requires: for '{task_id}':")
                print(_yaml.safe_dump(req, default_flow_style=False).rstrip())
            else:
                print(f"No requires: block set for '{task_id}'")
            return 0

        if clear:
            extras.pop("requires", None)
            conn.execute(
                "UPDATE tasks SET extras_json = ?, version = version + 1 WHERE id = ?",
                (_j.dumps(extras), task_id),
            )
            conn.commit()
            print(f"requires: cleared for '{task_id}'")
            return 0

        # Apply fail_mode
        if fail_mode:
            if fail_mode not in ("block", "warn"):
                _abort("--fail-mode must be 'block' or 'warn'", 2)
            req["fail_mode"] = fail_mode

        # Mutation helpers — each category stores list[str | dict]
        def _add_items(key: str, ids: list[str] | None) -> None:
            if not ids:
                return
            existing = list(req.get(key) or [])
            existing_ids = {
                (i.get("id") or i.get("name") or i.get("server") or "")
                if isinstance(i, dict)
                else str(i)
                for i in existing
            }
            for item_id in ids:
                if item_id not in existing_ids:
                    existing.append({"id": item_id})
                    existing_ids.add(item_id)
            req[key] = existing

        def _remove_items(key: str, ids: list[str] | None) -> None:
            if not ids:
                return
            remove_set = set(ids)
            req[key] = [
                i
                for i in (req.get(key) or [])
                if (i.get("id") or i.get("name") or i.get("server") or str(i))
                not in remove_set
            ]
            if not req[key]:
                del req[key]

        _add_items("cli", cli_add)
        _remove_items("cli", cli_remove)
        _add_items("env", env_add)
        _remove_items("env", env_remove)
        _add_items("skills", skill_add)
        _remove_items("skills", skill_remove)
        _add_items("mcp", mcp_add)
        _remove_items("mcp", mcp_remove)

        extras["requires"] = req
        conn.execute(
            "UPDATE tasks SET extras_json = ?, version = version + 1 WHERE id = ?",
            (_j.dumps(extras), task_id),
        )
        conn.commit()
    finally:
        conn.close()

    import yaml as _yaml

    print(f"requires: for '{task_id}':")
    print(_yaml.safe_dump(req, default_flow_style=False).rstrip())
    return 0


def link(
    project_dir: str,
    task_id: str,
    url: str | None = None,
    clear: bool = False,
) -> int:
    """Set or clear the issue_url on an existing task (one-way pointer;
    never written back to by shux)."""
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        row = tasks_dao.get(conn, task_id)
        if row is None:
            _abort(f"task '{task_id}' not found")
            return 1

        if clear:
            new_url = None
        elif url:
            try:
                new_url = _validate_issue_url(url)
            except ValueError as e:
                _abort(str(e), 2)
                return 2
        else:
            _abort("--url or --clear is required", 2)
            return 2

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        updated = tasks_dao.update(
            conn,
            task_id,
            row.version,
            {
                "issue_url": new_url,
                "updated_at": now,
            },
        )
        conn.commit()
    finally:
        conn.close()

    if clear:
        print(f"issue_url cleared for '{task_id}'")
    else:
        print(f"issue_url set for '{task_id}': {updated.issue_url}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    import argparse

    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog="task",
        description="Manage contract tasks",
        add_help=True,
    )
    sub = parser.add_subparsers(dest="subcmd")

    # create
    p_create = sub.add_parser("create", add_help=True)
    p_create.add_argument("--project", "-p", default=None)
    p_create.add_argument(
        "--id",
        dest="task_id",
        default=None,
        help="Task ID (auto-generated as t-XXXXXX if omitted)",
    )
    p_create.add_argument(
        "--title", default=None, help="Required unless --from-issue supplies a title"
    )
    p_create.add_argument("--owner", default=None)
    p_create.add_argument("--status", default="todo")
    p_create.add_argument("--dependency", default="")
    p_create.add_argument(
        "--blocked-by",
        dest="blocked_by",
        default=None,
        help="Task ID(s) this task is blocked by (comma-separated, or 'none')",
    )
    p_create.add_argument(
        "--tdd-red",
        dest="tdd_red",
        default="",
        help="TDD red phase: failing tests that define done",
    )
    p_create.add_argument(
        "--tdd-green",
        dest="tdd_green",
        default="",
        help="TDD green phase: minimal code to make tests pass",
    )
    p_create.add_argument(
        "--tdd-refactor",
        dest="tdd_refactor",
        default="",
        help="TDD refactor phase: cleanup after green, no new behaviour",
    )
    p_create.add_argument(
        "--workflow",
        default="implementation",
        help=(
            "Optional workflow template: implementation, quick, discussion, "
            "review, approval, note, reliable-orchestrator "
            "(default: implementation)"
        ),
    )
    p_create.add_argument(
        "--development-method",
        dest="development_method",
        default="",
        help="Optional development method: tdd, bdd, sdd, none",
    )
    p_create.add_argument(
        "--criteria",
        action="append",
        default=[],
        metavar="CRITERION",
        help="Acceptance criterion (repeat for multiple)",
    )
    p_create.add_argument(
        "--effort",
        default="medium",
        help="Effort level: low, medium, high, max (default: medium)",
    )
    p_create.add_argument(
        "--test-types",
        dest="test_types",
        default=None,
        help="Comma-separated test types (e.g. unit,integration,e2e)",
    )
    p_create.add_argument(
        "--out-of-scope",
        dest="out_of_scope",
        action="append",
        default=[],
        help="Out of scope item (repeat for multiple)",
    )
    p_create.add_argument(
        "--definition-of-done",
        dest="definition_of_done",
        action="append",
        default=[],
        help="Definition of done item (repeat for multiple)",
    )
    p_create.add_argument(
        "--context",
        default=None,
        help="Operator-authored context string injected into dispatch prompt",
    )
    p_create.add_argument(
        "--timeout-minutes",
        dest="timeout_minutes",
        type=int,
        default=None,
        help="Timeout in minutes for task execution",
    )
    p_create.add_argument(
        "--bdd-given", dest="bdd_given", default="", help="BDD given phase"
    )
    p_create.add_argument(
        "--bdd-when", dest="bdd_when", default="", help="BDD when phase"
    )
    p_create.add_argument(
        "--bdd-then", dest="bdd_then", default="", help="BDD then phase"
    )
    p_create.add_argument(
        "--ship-on-complete",
        dest="ship_on_complete",
        action="store_true",
        default=False,
        help="Agent must run /ship commit before report_ready; watcher validates PR URL",
    )
    p_create.add_argument(
        "--require-tdd",
        dest="require_tdd",
        action="store_true",
        default=None,
        help="Force require_tdd=true on this task (default: read from profile)",
    )
    p_create.add_argument(
        "--no-require-tdd",
        dest="require_tdd",
        action="store_false",
        default=None,
        help="Force require_tdd=false on this task",
    )
    p_create.add_argument(
        "--issue",
        dest="issue_url",
        default=None,
        help="Linked GitHub/GitLab issue URL (one-way snapshot pointer)",
    )
    p_create.add_argument(
        "--from-issue",
        dest="from_issue",
        default=None,
        help="Import title/context/acceptance_criteria from a GitHub/GitLab "
        "issue URL via gh/glab (one-way snapshot; explicit flags override)",
    )

    # delete
    p_delete = sub.add_parser("delete", add_help=True)
    p_delete.add_argument("--project", "-p", default=None)
    p_delete.add_argument("--id", dest="task_id", required=True)

    # archive-done: bulk-flip every done task to archived
    p_archive = sub.add_parser(
        "archive-done",
        add_help=True,
        help="Move every done task (or specific --id) to archived",
    )
    p_archive.add_argument("--project", "-p", default=None)
    p_archive.add_argument(
        "--id",
        action="append",
        dest="ids",
        default=None,
        help="Specific task id(s) to archive (repeat). Default: all done tasks.",
    )

    # status
    p_status = sub.add_parser("status", add_help=True)
    p_status.add_argument("--project", "-p", default=None)
    p_status.add_argument("--id", dest="task_id", required=True)
    _valid_status_hint = "{" + "|".join(sorted(ALL_STATUSES)) + "}"
    p_status.add_argument(
        "--status",
        required=True,
        metavar=_valid_status_hint,
        help=f"Lifecycle status. One of: {', '.join(sorted(ALL_STATUSES))}",
    )
    p_status.add_argument("--actor", required=True)
    p_status.add_argument("--reason", default="")
    p_status.add_argument("--summary", default="")
    p_status.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit machine-readable JSON on stdout instead of human text.",
    )

    # set-owner
    p_owner = sub.add_parser("set-owner", help="Change the owner (agent) of a task")
    p_owner.add_argument("--project", "-p", default=None)
    p_owner.add_argument("--id", dest="task_id", required=True)
    p_owner.add_argument("--owner", required=True, choices=list(VALID_OWNERS))

    # requires
    p_req = sub.add_parser(
        "requires",
        help="Read/write the requires: block on a task (skills / CLIs / env / MCP).",
    )
    p_req.add_argument("--project", "-p", default=None)
    p_req.add_argument("--id", dest="task_id", required=True)
    p_req.add_argument(
        "--show",
        action="store_true",
        default=False,
        help="Print current requires: block and exit",
    )
    p_req.add_argument(
        "--clear",
        action="store_true",
        default=False,
        help="Remove the entire requires: block",
    )
    p_req.add_argument(
        "--fail-mode",
        dest="fail_mode",
        default=None,
        choices=["block", "warn"],
        help="Dispatch behaviour on unmet deps: block (default) or warn",
    )
    p_req.add_argument(
        "--cli",
        dest="cli_add",
        action="append",
        default=None,
        metavar="ID",
        help="Require CLI binary on PATH (repeatable)",
    )
    p_req.add_argument(
        "--rm-cli",
        dest="cli_remove",
        action="append",
        default=None,
        metavar="ID",
        help="Remove CLI requirement (repeatable)",
    )
    p_req.add_argument(
        "--env",
        dest="env_add",
        action="append",
        default=None,
        metavar="NAME",
        help="Require env var to be set (repeatable)",
    )
    p_req.add_argument(
        "--rm-env",
        dest="env_remove",
        action="append",
        default=None,
        metavar="NAME",
        help="Remove env var requirement (repeatable)",
    )
    p_req.add_argument(
        "--skill",
        dest="skill_add",
        action="append",
        default=None,
        metavar="ID",
        help="Require skill/command to be installed (repeatable)",
    )
    p_req.add_argument(
        "--rm-skill",
        dest="skill_remove",
        action="append",
        default=None,
        metavar="ID",
        help="Remove skill requirement (repeatable)",
    )
    p_req.add_argument(
        "--mcp",
        dest="mcp_add",
        action="append",
        default=None,
        metavar="SERVER",
        help="Require MCP server to be registered (repeatable)",
    )
    p_req.add_argument(
        "--rm-mcp",
        dest="mcp_remove",
        action="append",
        default=None,
        metavar="SERVER",
        help="Remove MCP server requirement (repeatable)",
    )

    # link
    p_link = sub.add_parser(
        "link",
        help="Set or clear the linked GitHub/GitLab issue URL on an existing task.",
    )
    p_link.add_argument("--project", "-p", default=None)
    p_link.add_argument("--id", dest="task_id", required=True)
    p_link.add_argument("--url", default=None, help="Issue URL to attach")
    p_link.add_argument(
        "--clear",
        action="store_true",
        default=False,
        help="Remove the linked issue URL",
    )

    opts = parser.parse_args(argv)
    if not opts.subcmd:
        parser.print_help(sys.stderr)
        sys.exit(2)

    project_dir = os.path.realpath(opts.project or os.getcwd())
    if not is_project_initialized(project_dir):
        _abort(f"Missing project state at {project_dir}. Run 'shux init' first.")

    if opts.subcmd == "create":
        owner = opts.owner or ""
        if not owner:
            # Try profile.yaml primary_agent
            profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
            if os.path.exists(profile_file):
                try:
                    import yaml as _yaml

                    with open(profile_file) as _f:
                        _profile = _yaml.safe_load(_f) or {}
                    owner = str(_profile.get("primary_agent") or "")
                except Exception as e:
                    logger.warning("task.py unexpected error: %s", e, exc_info=True)
                    pass
        if not owner:
            # Prompt via stdin
            try:
                if sys.stdin.isatty():
                    sys.stderr.write(f"Task owner ({'|'.join(sorted(VALID_OWNERS))}): ")
                    sys.stderr.flush()
                owner = sys.stdin.readline().strip()
            except (EOFError, OSError):
                pass
        if not owner:
            _abort("--owner is required (or set in profile.yaml)", 2)
        task_id = opts.task_id or f"t-{uuid.uuid4().hex[:6]}"

        # --from-issue pre-fill: fetch once, seed defaults, explicit flags override.
        imported_title = None
        imported_context = None
        imported_criteria: list[str] = []
        imported_issue_url = None
        if opts.from_issue:
            from superharness.commands.issue_import import (
                _fetch_issue,
                _issue_to_task_fields,
            )

            try:
                issue = _fetch_issue(opts.from_issue)
            except RuntimeError as e:
                _abort(str(e), 1)
            fields = _issue_to_task_fields(issue, opts.from_issue)
            imported_title = fields["title"]
            imported_context = fields["context"]
            imported_criteria = fields["acceptance_criteria"]
            imported_issue_url = fields["issue_url"]

        title = opts.title or imported_title
        if not title:
            _abort("--title is required (or use --from-issue to import one)", 2)
        criteria = opts.criteria or imported_criteria
        context = opts.context if opts.context is not None else imported_context
        issue_url = opts.issue_url or imported_issue_url

        # Build plan dict from method-specific flags
        plan = None
        if opts.bdd_given or opts.bdd_when or opts.bdd_then:
            plan = {}
            if opts.bdd_given:
                plan["given"] = opts.bdd_given
            if opts.bdd_when:
                plan["when"] = opts.bdd_when
            if opts.bdd_then:
                plan["then"] = opts.bdd_then
        # Parse test_types from comma-separated string
        test_types = None
        if opts.test_types:
            test_types = [t.strip() for t in opts.test_types.split(",") if t.strip()]
        rc = create(
            project_dir,
            task_id=task_id,
            title=title,
            owner=owner,
            status=opts.status,
            project_path=project_dir,
            dependency=opts.dependency or None,
            criteria=criteria or None,
            blocked_by=opts.blocked_by,
            tdd_red=opts.tdd_red,
            tdd_green=opts.tdd_green,
            tdd_refactor=opts.tdd_refactor,
            workflow=opts.workflow,
            development_method=opts.development_method,
            effort=opts.effort,
            test_types=test_types,
            out_of_scope=opts.out_of_scope or None,
            definition_of_done=opts.definition_of_done or None,
            context=context,
            timeout_minutes=opts.timeout_minutes,
            plan=plan,
            ship_on_complete=opts.ship_on_complete,
            require_tdd=opts.require_tdd,
            issue_url=issue_url,
        )
        sys.exit(rc)

    elif opts.subcmd == "delete":
        rc = delete(project_dir, task_id=opts.task_id)
        sys.exit(rc)

    elif opts.subcmd == "archive-done":
        rc = archive_done(project_dir, ids=opts.ids)
        sys.exit(rc)

    elif opts.subcmd == "status":
        global _JSON_MODE, _JSON_CTX
        if getattr(opts, "json", False):
            _JSON_MODE = True
            _JSON_CTX = {
                "task_id": opts.task_id,
                "new_status": opts.status,
                "actor": opts.actor,
            }

        # Capture old status for the JSON payload
        old_status = None
        if _JSON_MODE:
            try:
                from superharness.engine.db import get_connection, init_db
                from superharness.engine import tasks_dao as _tdao

                _conn = get_connection(project_dir)
                try:
                    init_db(_conn)
                    _row = _tdao.get(_conn, opts.task_id)
                    if _row is not None:
                        old_status = str(_row.status or "")
                finally:
                    _conn.close()
            except SystemExit:
                raise
            except Exception as e:
                logger.warning("task.py unexpected error: %s", e, exc_info=True)
                pass
        # Pre-validate before calling status_update so shell exit codes match
        if opts.status in ("failed", "stopped") and not opts.reason:
            _abort(f"error: --reason is required when status={opts.status}", 2)
        if (
            opts.status in ("todo", "in_progress", "pending_user_approval", "done")
            and not opts.summary
        ):
            _abort(f"error: --summary is required when status={opts.status}", 2)

        # In JSON mode, temporarily suppress stdout prints from status_update
        if _JSON_MODE:
            import io

            _orig_stdout = sys.stdout
            sys.stdout = io.StringIO()
            try:
                rc = status_update(
                    project_dir,
                    task_id=opts.task_id,
                    status=opts.status,
                    actor=opts.actor,
                    reason=opts.reason or "",
                    summary=opts.summary or "",
                )
            finally:
                sys.stdout = _orig_stdout
            _sync_inbox_after_status(project_dir, opts.task_id, opts.status)
            from superharness.utils.json_output import emit_json

            emit_json(
                {
                    "task_id": opts.task_id,
                    "old_status": old_status,
                    "new_status": opts.status,
                    "actor": opts.actor,
                },
                ok=(rc == 0),
                exit_code=rc,
            )

        rc = status_update(
            project_dir,
            task_id=opts.task_id,
            status=opts.status,
            actor=opts.actor,
            reason=opts.reason or "",
            summary=opts.summary or "",
        )
        # Sync inbox after status update
        _sync_inbox_after_status(project_dir, opts.task_id, opts.status)
        sys.exit(rc)

    elif opts.subcmd == "set-owner":
        rc = set_owner(project_dir, opts.task_id, opts.owner)
        sys.exit(rc)

    elif opts.subcmd == "requires":
        rc = set_requires(
            project_dir,
            task_id=opts.task_id,
            cli_add=opts.cli_add,
            cli_remove=opts.cli_remove,
            env_add=opts.env_add,
            env_remove=opts.env_remove,
            skill_add=opts.skill_add,
            skill_remove=opts.skill_remove,
            mcp_add=opts.mcp_add,
            mcp_remove=opts.mcp_remove,
            fail_mode=opts.fail_mode,
            clear=opts.clear,
            show=opts.show,
        )
        sys.exit(rc)

    elif opts.subcmd == "link":
        rc = link(project_dir, task_id=opts.task_id, url=opts.url, clear=opts.clear)
        sys.exit(rc)


def _sync_inbox_after_status(project_dir: str, task_id: str, status: str) -> None:
    """Sync inbox rows to terminal state when task reaches done/failed/stopped."""
    if status not in ("done", "failed", "stopped"):
        return
    try:
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            inbox_status = "done" if status == "done" else "failed"
            conn.execute(
                "UPDATE inbox SET status=?, failed_at=? WHERE task_id=? AND status IN ('pending','launched','running')",
                (inbox_status, now, task_id),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(
            f"Warning: failed to sync inbox task status for '{task_id}': {e}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()

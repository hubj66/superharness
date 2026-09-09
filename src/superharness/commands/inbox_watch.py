"""Python port of inbox-watch.sh.

Watches the inbox and dispatches pending items. Supports single-cycle
(launchd) and foreground (polling) modes.
"""

from __future__ import annotations

import importlib.resources as _importlib_resources
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from superharness.engine.reliable_orchestrator_gate import is_reliable_orchestrated_task as _is_reliable_task
from superharness.engine.reliable_watcher import is_reliable_task_id as _is_reliable_task_id, reliable_task_ids, release as _reliable_release, tick as _reliable_tick
if TYPE_CHECKING:
    from superharness.engine import live_state


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _profile_autonomy(profile: dict) -> str:
    from superharness.engine.profile import normalize_autonomy

    return normalize_autonomy(profile.get("autonomy", "ai_driven"))


def _load_tasks(project_dir: str) -> list[dict]:
    """Return all contract tasks via state_reader (SQLite)."""
    try:
        from superharness.engine.state_reader import get_tasks

        return [task for task in get_tasks(project_dir) if not _is_reliable_task(task)]
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return []


def _deps_satisfied_from_tasks(tasks: list[dict], task_id: str) -> bool:
    """Dependency check using an already-loaded task list (no file I/O)."""
    task = next(
        (t for t in tasks if isinstance(t, dict) and str(t.get("id", "")) == task_id),
        None,
    )
    if task is None:
        return True
    blocked_by = task.get("blocked_by")
    if not blocked_by or str(blocked_by).strip().lower() in ("none", "", "null"):
        return True
    if isinstance(blocked_by, str):
        dep_ids = [d.strip() for d in blocked_by.split(",") if d.strip()]
    elif isinstance(blocked_by, list):
        dep_ids = [str(d).strip() for d in blocked_by if str(d).strip()]
    else:
        return True
    status_map = {
        str(t.get("id", "")): str(t.get("status", ""))
        for t in tasks
        if isinstance(t, dict)
    }
    return all(status_map.get(dep_id, "") == "done" for dep_id in dep_ids)


def _ensure_task_in_sqlite(conn, task_id: str, project_dir: str, now: str) -> None:
    """No-op if the task is already in SQLite; silently returns if not found."""
    try:
        from superharness.engine import tasks_dao

        tasks_dao.get(conn, task_id)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _sqlite_mirror_inbox_enqueue(project_dir: str, items: list[dict], now: str) -> None:
    """Mirror new inbox items to SQLite. Never raises."""
    try:
        from superharness.engine.db import get_connection, init_db, transaction
        from superharness.engine import inbox_dao, ledger_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                for item in items:
                    task_id = str(item.get("task", item.get("task_id", "")))
                    _ensure_task_in_sqlite(conn, task_id, project_dir, now)
                    inbox_dao.enqueue(
                        conn,
                        id=str(item["id"]),
                        task_id=task_id,
                        target_agent=str(item.get("to", item.get("target_agent", ""))),
                        priority=int(item.get("priority", 2)),
                        max_retries=int(item.get("max_retries", 3)),
                        project_path=item.get("project", item.get("project_path")),
                        plan_only=bool(item.get("plan_only", False)),
                        now=item.get("created_at", now),
                    )
                ledger_dao.record(
                    conn,
                    agent="watcher",
                    action="auto_enqueue",
                    details={"count": len(items)},
                    now=now,
                )
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


_INBOX_RETRY_WRITERS: dict[str, "live_state.LiveStateWriter"] = {}
_TASK_STATUS_WRITERS: dict[str, "live_state.LiveStateWriter"] = {}


def _get_inbox_retry_writer(project_dir: str):
    """Return (creating if needed) the ordered live-state writer for inbox
    retry mirrors in this project. Cached per project_dir for the life of the
    process so ordering/dedupe apply across calls, not just within one."""
    from superharness.engine import live_state

    writer = _INBOX_RETRY_WRITERS.get(project_dir)
    if writer is None:

        def _write_fn(key: str, value: str) -> None:
            import json
            from superharness.engine.db import get_connection, init_db, transaction
            from superharness.engine import inbox_dao, ledger_dao

            payload = json.loads(value)
            conn = get_connection(project_dir)
            try:
                init_db(conn)
                with transaction(conn):
                    for item in payload["items"]:
                        inbox_dao.set_retry(
                            conn,
                            str(item["id"]),
                            int(item["retry_count"]),
                            None,
                            payload["now"],
                        )
                    ledger_dao.record(
                        conn,
                        agent="watcher",
                        action="auto_retry",
                        details={"ids": [i["id"] for i in payload["items"]]},
                        now=payload["now"],
                    )
            finally:
                conn.close()

        writer = live_state.LiveStateWriter(_write_fn)
        _INBOX_RETRY_WRITERS[project_dir] = writer
    return writer


def _sqlite_mirror_inbox_retry(
    project_dir: str, retried_items: list[dict], now: str
) -> None:
    """Mirror inbox retry resets to SQLite via the ordered live-state
    chokepoint (engine/live_state.py). Never raises — write failures are
    logged inside the chokepoint, not propagated here.

    Each entry in retried_items must have 'id' and 'retry_count'.
    """
    import json

    if not retried_items:
        return
    writer = _get_inbox_retry_writer(project_dir)
    key = "inbox_retry:" + ",".join(sorted(str(i["id"]) for i in retried_items))
    value = json.dumps(
        {
            "items": [
                {"id": str(i["id"]), "retry_count": int(i.get("retry_count", 0))}
                for i in retried_items
            ],
            "now": now,
        },
        sort_keys=True,
    )
    writer.publish(key, value)
    writer.flush(timeout=5.0)


def _get_task_status_writer(project_dir: str):
    """Return (creating if needed) the ordered live-state writer for task
    status mirrors in this project."""
    from superharness.engine import live_state

    writer = _TASK_STATUS_WRITERS.get(project_dir)
    if writer is None:

        def _write_fn(key: str, value: str) -> None:
            import json
            from superharness.engine.db import get_connection, init_db, transaction
            from superharness.engine import tasks_dao, ledger_dao

            payload = json.loads(value)
            conn = get_connection(project_dir)
            try:
                init_db(conn)
                with transaction(conn):
                    task = tasks_dao.get(conn, key)
                    if task is None:
                        return
                    changes: dict = {
                        "status": payload["status"],
                        **(payload.get("extra") or {}),
                    }
                    tasks_dao.update(conn, key, task.version, changes=changes)
                    ledger_dao.record(
                        conn,
                        agent="watcher",
                        action="task_status_change",
                        task_id=key,
                        details={"status": payload["status"]},
                        now=payload["now"],
                    )
            finally:
                conn.close()

        writer = live_state.LiveStateWriter(_write_fn)
        _TASK_STATUS_WRITERS[project_dir] = writer
    return writer


def _sqlite_mirror_task_status(
    project_dir: str, task_id: str, status: str, now: str, extra: dict | None = None
) -> None:
    """Mirror a task status change to SQLite via the ordered live-state
    chokepoint (engine/live_state.py). Never raises — write failures are
    logged inside the chokepoint, not propagated here."""
    import json

    writer = _get_task_status_writer(project_dir)
    value = json.dumps(
        {"status": status, "now": now, "extra": extra or {}}, sort_keys=True
    )
    writer.publish(task_id, value)
    writer.flush(timeout=5.0)


def _poll_operator_commands(project_dir: str) -> None:
    """Drain pending operator_commands rows and apply approve/reject transitions.

    Rows left with status='pending' are gateway-issued commands that could not
    be applied synchronously at insert time (task missing, DB contention, etc.).
    The watcher retries them on each cycle until they succeed or exhaust retries.
    """
    from superharness.engine.db import get_connection, init_db, transaction
    from superharness.engine import operator_commands_dao, tasks_dao, ledger_dao

    _COMMAND_MAP = {
        "approve": "plan_approved",
        "reject": "stopped",
    }
    _VALID_FROM = {
        "approve": {"plan_proposed"},
        "reject": {"plan_proposed", "plan_approved"},
    }

    now = _now_utc()
    conn = get_connection(project_dir)
    try:
        init_db(conn)
        pending = operator_commands_dao.poll_pending(conn)
        if not pending:
            return
        for cmd in pending:
            if cmd.command not in _COMMAND_MAP:
                with transaction(conn):
                    operator_commands_dao.update_status(
                        conn,
                        cmd.id,
                        status="failed",
                        result={"message": f"unknown command: {cmd.command!r}"},
                        now=now,
                    )
                continue

            target_status = _COMMAND_MAP[cmd.command]
            valid_from = _VALID_FROM[cmd.command]

            task = tasks_dao.get(conn, cmd.task_id) if cmd.task_id else None
            if task is None:
                # Task not found yet — leave pending for next cycle
                print(
                    f"operator_commands: task {cmd.task_id!r} not found — will retry",
                    file=sys.stderr,
                )
                continue

            if task.status not in valid_from:
                with transaction(conn):
                    operator_commands_dao.update_status(
                        conn,
                        cmd.id,
                        status="skipped",
                        result={
                            "message": f"task status {task.status!r} not in {valid_from!r}"
                        },
                        now=now,
                    )
                continue

            with transaction(conn):
                tasks_dao.update(
                    conn, cmd.task_id, task.version, {"status": target_status}
                )
                ledger_dao.record(
                    conn,
                    agent="watcher",
                    action="operator_command",
                    task_id=cmd.task_id,
                    details={"command": cmd.command, "new_status": target_status},
                    now=now,
                )
                operator_commands_dao.update_status(
                    conn,
                    cmd.id,
                    status="executed",
                    result={
                        "message": f"watcher applied {cmd.command} → {target_status}"
                    },
                    now=now,
                )
            print(
                f"operator_commands: {cmd.command} → {cmd.task_id!r} transitioned to {target_status}"
            )
    finally:
        conn.close()


def _log_watcher_error(project_dir, component, error):
    """Write watcher errors to a log file."""
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_path = os.path.join(project_dir, ".superharness", "watcher-errors.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a") as f:
        f.write(f"[{ts}] [{component}] {error}\n")


def _abort(msg: str, code: int = 1) -> None:
    from superharness.logging_utils import get_logger

    get_logger("inbox_watch").error("abort: %s", msg)
    print(msg, file=sys.stderr)
    sys.exit(code)


# ---------------------------------------------------------------------------
# Lock (directory-based)
# ---------------------------------------------------------------------------


def _lock_dir_path(project_dir: str) -> str:
    from superharness.engine.platform_runtime import watcher_lock_path

    return watcher_lock_path(project_dir)


def _lock_pid_file(lock_dir: str) -> str:
    return os.path.join(lock_dir, "owner.pid")


def _write_lock_pid(lock_dir: str) -> None:
    try:
        with open(_lock_pid_file(lock_dir), "w", encoding="utf-8") as f:
            f.write(f"{os.getpid()}\n")
    except OSError:
        pass


def _read_lock_pid(lock_dir: str) -> int | None:
    try:
        with open(_lock_pid_file(lock_dir), encoding="utf-8") as f:
            raw = f.readline().strip()
        pid = int(raw)
        return pid if pid > 0 else None
    except (OSError, ValueError):
        return None


def _pid_is_running(pid: int | None) -> bool:
    from superharness.engine.process import pid_alive

    if pid is None:
        return False
    return pid_alive(pid)


def _reconcile_paused_dead_pids(inbox: list) -> bool:
    """Transition paused items whose launcher pid is dead to failed.

    Returns True if any item was changed.
    Called on every watcher tick so dead-pid lanes are unblocked within one interval.
    Only acts on items with status=paused AND a recorded pid.
    Items without a pid are left alone, handled by lifecycle_rules.reconcile_lifecycle instead.
    """
    changed = False
    for item in inbox:
        if item.get("status") != "paused":
            continue
        raw_pid = item.get("pid")
        if not raw_pid:
            continue
        try:
            pid = int(raw_pid)
        except (ValueError, TypeError):
            continue
        if not _pid_is_running(pid):
            item["status"] = "failed"
            item["failed_reason"] = f"launcher pid {pid} disappeared"
            item["failed_at"] = _now_utc()
            changed = True
    return changed


def _heartbeat_age_seconds(project_dir: str) -> int | None:
    hb_file = os.path.join(project_dir, ".superharness", "watcher.heartbeat")
    if not os.path.isfile(hb_file):
        return None
    try:
        with open(hb_file, encoding="utf-8") as f:
            hb_ts = f.readline().strip()
        if not hb_ts:
            return None
        hb_dt = datetime.fromisoformat(hb_ts.replace("Z", "+00:00"))
        return int((datetime.now(timezone.utc) - hb_dt).total_seconds())
    except (OSError, ValueError):
        return None


def _remove_lock_dir(lock_dir: str) -> None:
    try:
        os.unlink(_lock_pid_file(lock_dir))
    except OSError:
        pass
    try:
        os.rmdir(lock_dir)
    except OSError:
        pass


def _auto_break_stale_lock(
    lock_dir: str,
    stale_minutes: int,
    *,
    project_dir: str | None = None,
    heartbeat_stale_seconds: int | None = None,
) -> bool:
    """Remove an orphaned or stale lock dir. Returns True if broken."""
    if not os.path.isdir(lock_dir):
        return False

    lock_pid = _read_lock_pid(lock_dir)
    if lock_pid is not None and not _pid_is_running(lock_pid):
        print(
            f"Auto-breaking orphaned watcher lock (pid {lock_pid} not running): {lock_dir}"
        )
        _remove_lock_dir(lock_dir)
        return True

    try:
        stat = os.stat(lock_dir)
        lock_age = time.time() - stat.st_mtime
        if project_dir and heartbeat_stale_seconds is not None and lock_pid is None:
            hb_age = _heartbeat_age_seconds(project_dir)
            if (
                hb_age is not None
                and hb_age >= heartbeat_stale_seconds
                and lock_age >= heartbeat_stale_seconds
            ):
                print(
                    f"Auto-breaking orphaned watcher lock "
                    f"(stale heartbeat: {hb_age}s, no lock pid): {lock_dir}"
                )
                _remove_lock_dir(lock_dir)
                return True

        if stale_minutes <= 0:
            return False

        stale_secs = stale_minutes * 60
        if lock_pid is None and lock_age >= stale_secs:
            print(
                f"Auto-breaking stale watcher lock (age: {int(lock_age)}s, "
                f"threshold: {stale_secs}s): {lock_dir}"
            )
            _remove_lock_dir(lock_dir)
            return True
    except OSError:
        pass
    return False


def _acquire_watcher_lock(lock_dir: str) -> bool:
    try:
        os.mkdir(lock_dir)
        _write_lock_pid(lock_dir)
        return True
    except FileExistsError:
        return False


def _release_watcher_lock(lock_dir: str) -> None:
    _remove_lock_dir(lock_dir)


# ---------------------------------------------------------------------------
# Single cycle
# ---------------------------------------------------------------------------


def _run_dispatch_cmd(
    project_dir: str,
    target: str,
    print_only: bool,
    non_interactive: bool,
    codex_bypass: bool,
    launcher_timeout: int,
) -> None:
    env = os.environ.copy()

    # Allow test/override via DISPATCH env var (mirrors old inbox-watch.sh behaviour)
    dispatch_override = env.get("DISPATCH", "")
    if dispatch_override and os.path.isfile(dispatch_override):
        args = ["bash", dispatch_override, "--project", project_dir, "--to", target]
        if print_only:
            args.append("--print-only")
        if non_interactive:
            args.append("--non-interactive")
        if codex_bypass:
            args.append("--codex-bypass")
        if launcher_timeout > 0:
            args += ["--launcher-timeout", str(launcher_timeout)]
        # Run in background (detached), like old Bash script used `... &`
        subprocess.Popen(
            args,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return

    args = [
        sys.executable,
        "-m",
        "superharness.commands.inbox_dispatch",
        "--project",
        project_dir,
        "--to",
        target,
    ]

    # Ensure spawned process uses the same source as the watcher
    src_dir = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{env.get('PYTHONPATH', '')}".strip(
        os.pathsep
    )

    if print_only:
        args.append("--print-only")
    if non_interactive:
        args.append("--non-interactive")
    if codex_bypass:
        args.append("--codex-bypass")
    if launcher_timeout > 0:
        args += ["--launcher-timeout", str(launcher_timeout)]

    if print_only:
        subprocess.run(args, check=False, env=env)
        return

    subprocess.Popen(
        args,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def _watcher_targets(target: str) -> list[str]:
    """Expand a watcher target to every registered, executable harness."""
    if target == "both":
        from superharness.engine.adapter_registry import list_adapters
        from superharness.harnesses import KNOWN_HARNESSES

        # The manifest registry discovers new adapters automatically, while the
        # harness registry excludes manifest-only/inert entries such as
        # ``prime-agent`` that the watcher cannot execute.
        executable = set(KNOWN_HARNESSES)
        return [adapter for adapter in list_adapters() if adapter in executable]
    return [target]


def _run_scripts_heartbeat(project_dir: str) -> None:
    """Write both legacy timestamp and structured heartbeat contract for the watcher."""
    # Legacy: plain timestamp — consumed by existing health checks
    heartbeat_file = os.path.join(project_dir, ".superharness", "watcher.heartbeat")
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(heartbeat_file, "w") as _hf:
            _hf.write(ts + "\n")
    except OSError:
        pass

    # Heartbeat contract v1: YAML heartbeat — runtime-agnostic, consumed by dashboard
    try:
        from superharness.engine.heartbeat_contract import (
            AgentHeartbeat,
            write_heartbeat,
        )

        write_heartbeat(
            project_dir,
            AgentHeartbeat(
                agent_id="watcher",
                runtime="native",
                status="idle",
                pid=os.getpid(),
            ),
        )
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


_STALE_NO_HANDOFF_HOURS = 4  # archive tasks with no handoff after this many hours


def _auto_archive_stale_tasks(project_dir: str) -> int:
    """Archive tasks stuck in non-terminal states with no handoff file.

    Covers: report_ready, plan_proposed, in_progress with no handoff after
    STALE_NO_HANDOFF_HOURS. These are tasks where agents were dispatched
    but never produced output — dead processes, lost sessions, etc.
    """
    from datetime import datetime, timezone

    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import tasks_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            all_tasks = tasks_dao.get_all(conn)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0

    now = datetime.now(timezone.utc)
    os.path.join(project_dir, ".superharness", "handoffs")
    archived = 0

    for task in all_tasks:
        if task.status in ("done", "archived", "stopped", "failed"):
            continue
        # Check if task has been in this state long enough
        ts_str = (
            task.report_ready_at
            or task.plan_proposed_at
            or task.in_progress_at
            or task.created_at
            or ""
        )
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        hours = (now - ts).total_seconds() / 3600
        if hours < _STALE_NO_HANDOFF_HOURS:
            continue

        # Check if a report/completion handoff exists (plan-phase handoffs don't count)
        try:
            from superharness.engine import state_reader as _sr_iw

            task_handoffs = _sr_iw.get_handoffs(project_dir, task_id=task.id)
            report_handoffs = [
                h
                for h in task_handoffs
                if str(h.get("phase", "")) in ("report", "done")
            ]
        except Exception:
            logger.warning(
                "_auto_archive_stale_tasks: unexpected error: %s",
                sys.exc_info()[1],
                exc_info=True,
            )
            report_handoffs = []
        if report_handoffs:
            continue  # Has a completion handoff — agent produced output

        # No handoff after timeout — archive it
        try:
            conn2 = get_connection(project_dir)
            try:
                init_db(conn2)
                tasks_dao.upsert(
                    conn2,
                    tasks_dao.TaskRow(
                        id=task.id,
                        title=task.title,
                        owner=task.owner,
                        status="archived",
                        effort=task.effort,
                        project_path=task.project_path,
                        development_method=task.development_method,
                        acceptance_criteria=task.acceptance_criteria,
                        test_types=task.test_types,
                        out_of_scope=task.out_of_scope,
                        definition_of_done=task.definition_of_done,
                        context=(task.context or "")
                        + f"\n[auto-clean] archived: no handoff after {hours:.0f}h in {task.status}",
                        tdd=task.tdd,
                        version=task.version,
                        created_at=task.created_at,
                        blocked_by=task.blocked_by,
                        parent_id=task.parent_id,
                    ),
                )
                conn2.commit()
                archived += 1
                print(
                    f"auto-clean: archived '{task.id}' (no handoff, {hours:.0f}h in {task.status})"
                )
            finally:
                conn2.close()
        except Exception as e:
            logger.warning(
                "_auto_archive_stale_tasks: unexpected error: %s", e, exc_info=True
            )
            print(f"auto-clean: failed to archive '{task.id}': {e}", file=sys.stderr)

    if archived:
        print(f"auto-clean: archived {archived} stale task(s)")
    return archived


_PEER_AGENTS: dict[str, str] = {
    "claude-code": "gemini-cli",  # Claude proposes → Gemini reviews
    "gemini-cli": "codex-cli",  # Gemini proposes → Codex reviews
    "codex-cli": "claude-code",  # Codex proposes → Claude reviews
}

# Cooldown window after a peer-review row fails. Prevents the auto-spawn loop
# (each fast lifecycle-gate failure ~14s would otherwise unblock the next
# enqueue, producing 19 rows in 4m40s on feat-rules-dashboard 2026-05-08).
_PEER_REVIEW_COOLDOWN_MIN = 15

_PEER_REVIEW_PROMPT = """Review this plan and approve or reject it.

The task owner ({owner}) proposed a plan for: {task_title}

Acceptance criteria:
{criteria}

Your job as peer reviewer (max-tier model):
1. Read the plan handoff file at .superharness/handoffs/{task_id}*.yaml
2. Check that the plan:
   - Has a complete TDD block (red/green/refactor)
   - Addresses each acceptance criterion
   - Has a risks section
   - Has no TODO placeholders
   - Is feasible in the estimated effort ({effort})

3. Write a 1-sentence verdict: APPROVED or REJECTED with reason.
4. If APPROVED, the task advances to implementation automatically.
5. If REJECTED, the task returns to todo for re-planning.

Verdict:"""


def _auto_peer_approve_plans(project_dir: str) -> int:
    """Find plan_proposed tasks and dispatch to a different max-tier agent for review.

    Never auto-approves without peer review. The peer must be a different agent
    at max tier to ensure impartial judgment. If the peer is unavailable, the
    task stays plan_proposed for operator review.

    Returns count of review tasks enqueued.
    """
    import uuid
    from datetime import datetime, timedelta, timezone

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if not os.path.exists(profile_file):
        return 0
    try:
        import yaml as _yaml

        profile = _yaml.safe_load(open(profile_file, encoding="utf-8").read()) or {}
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0

    # Only run when auto-approve is enabled (now means "use peer review")
    if not profile.get("auto_approve_plans"):
        return 0
    if _profile_autonomy(profile) != "ai_driven":
        return 0

    tasks = _load_tasks(project_dir)
    if not tasks:
        return 0

    # Get active inbox items to avoid double-dispatch
    active_tasks: set[str] = set()
    try:
        from superharness.engine.state_reader import get_inbox_items

        for item in get_inbox_items(project_dir):
            if isinstance(item, dict) and item.get("status") in (
                "pending",
                "launched",
                "running",
                "paused",
            ):
                task_id = item.get("task", item.get("task_id", ""))
                if task_id:
                    active_tasks.add(str(task_id))
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    # Circuit breaker: skip tasks where a peer-review row failed in the last
    # _PEER_REVIEW_COOLDOWN_MIN minutes. Without this, each fast-failing row
    # (~14s lifecycle-gate rejection) vanishes from active_tasks and the next
    # watcher cycle queues another one — the runaway pattern that hit
    # feat-rules-dashboard on 2026-05-08 (19 rows in 4m40s).
    recent_peer_failure_tasks: set[str] = set()
    try:
        from superharness.engine.db import get_connection, init_db as _init_db

        _conn = get_connection(project_dir)
        try:
            _init_db(_conn)
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(minutes=_PEER_REVIEW_COOLDOWN_MIN)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            for (tid,) in _conn.execute(
                "SELECT DISTINCT task_id FROM inbox "
                "WHERE id LIKE 'peer-review-%' AND status='failed' "
                "AND failed_at IS NOT NULL AND failed_at > ?",
                (cutoff,),
            ).fetchall():
                if tid:
                    recent_peer_failure_tasks.add(str(tid))
        finally:
            _conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    enqueued = 0
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    from superharness.engine.next_action import infer_workflow as _infer_workflow

    for task in tasks:
        if not isinstance(task, dict):
            continue
        if task.get("status") != "plan_proposed":
            continue
        task_id = str(task.get("id", ""))
        if not task_id:
            continue
        if task_id in active_tasks:
            continue  # Already has an active inbox item
        if task_id in recent_peer_failure_tasks:
            continue  # Circuit breaker: recent peer-review failure
        # Discussion sub-tasks are already a multi-agent flow — never spawn
        # peer-review rows for them (they have no AC, so every dispatch fails
        # gate 4, producing the inbox flood we saw on 2026-05-09).
        if _infer_workflow(task_id, task) == "discussion":
            continue

        owner = str(task.get("owner") or "claude-code")
        peer = _PEER_AGENTS.get(owner, "gemini-cli")

        # Only dispatch if peer is different from owner
        if peer == owner:
            continue

        # Build review prompt
        criteria = task.get("acceptance_criteria") or []
        criteria_str = (
            "\n".join(f"  - {c}" for c in criteria) if criteria else "  (none)"
        )
        _PEER_REVIEW_PROMPT.format(
            owner=owner,
            task_title=task.get("title", task_id),
            criteria=criteria_str,
            effort=task.get("effort", "medium"),
            task_id=task_id,
        )

        # Enqueue to peer agent as plan-only review
        try:
            from superharness.engine.db import get_connection, init_db
            from superharness.engine import inbox_dao
            from superharness.engine.burst_guard import task_burst_suppressed

            conn = get_connection(project_dir)
            try:
                init_db(conn)
                if task_burst_suppressed(conn, task_id):
                    continue
                item_id = (
                    f"peer-review-{task_id.replace('.', '-')}-{uuid.uuid4().hex[:8]}"
                )
                inbox_dao.enqueue(
                    conn,
                    id=item_id,
                    task_id=task_id,
                    target_agent=peer,
                    priority=1,  # high priority
                    max_retries=2,
                    project_path=project_dir,
                    plan_only=True,  # review only, don't implement
                    now=now,
                )
                conn.commit()
                enqueued += 1
                print(f"peer-approve: dispatched '{task_id}' plan review → {peer}")
            finally:
                conn.close()
        except Exception as e:
            logger.warning(
                "_auto_peer_approve_plans: unexpected error: %s", e, exc_info=True
            )
            print(f"peer-approve: failed to enqueue '{task_id}': {e}", file=sys.stderr)

    if enqueued:
        print(f"peer-approve: {enqueued} plan(s) dispatched for peer review")
    return enqueued


_PR_URL_RE = __import__("re").compile(r"https://github\.com/[^/]+/[^/]+/pull/\d+")


def _find_pr_url_in_handoff(handoff_dir: str, task_id: str) -> str | None:
    """Return the first GitHub PR URL found in any handoff for task_id — reads from SQLite."""
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(handoff_dir)))
    try:
        from superharness.engine.db import managed_connection
        from superharness.engine import handoffs_dao

        with managed_connection(project_dir) as conn:
            rows = handoffs_dao.search(conn, task_id)
        for row in rows:
            if str(row.task_id) != task_id:
                continue
            m = _PR_URL_RE.search(str(row.content or ""))
            if m:
                return m.group(0)
            for item in (row.metadata or {}).get("outcomes") or []:
                m = _PR_URL_RE.search(str(item))
                if m:
                    return m.group(0)
        return None
    except Exception as e:
        logger.warning("_find_pr_url_in_handoff SQLite error: %s", e, exc_info=True)
        return None


def _select_reviewers(task: dict, candidates: list[str], profile: dict) -> list[str]:
    """Filter candidate reviewers based on cross-pollination and model-tier gates."""
    from superharness.engine.model_budget import (
        reviewer_meets_tier,
        AGENT_DEFAULT_TIERS,
    )

    owner = str(task.get("owner", ""))
    # author tier: 1. task field, 2. owner default, 3. standard
    author_tier = str(task.get("model_tier") or "")
    if not author_tier:
        author_tier = AGENT_DEFAULT_TIERS.get(owner, "standard")

    # 1. Cross-pollination guard: owner cannot review own task
    peers = [a for a in candidates if a != owner]

    # 2. Model-tier gate: reviewer must be >= author tier
    qualified = [a for a in peers if reviewer_meets_tier(a, author_tier, profile)]

    return qualified


def _trigger_auto_review(project_dir: str, task_id: str, reviewers: list[str]) -> bool:
    """Transition task to review_requested and enqueue multiple reviewers."""
    import subprocess

    # 1. Enqueue each reviewer first (while task is still in report_ready)
    success_count = 0
    enqueued = []
    import time

    for target in reviewers:
        try:
            cmd = [
                sys.executable,
                "-m",
                "superharness.commands.inbox_enqueue",
                "--project",
                project_dir,
                "--to",
                target,
                "--task",
                task_id,
                "--priority",
                "1",
                "--force-reassign",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                success_count += 1
                enqueued.append(target)
            else:
                print(
                    f"auto-review: failed to enqueue {task_id} for {target}: {result.stdout.strip()} {result.stderr.strip()}",
                    file=sys.stderr,
                )
            # Small sleep to ensure file-based inbox lock settles
            time.sleep(0.1)
        except Exception as e:
            logger.warning(
                "_trigger_auto_review: unexpected error: %s", e, exc_info=True
            )
            print(
                f"auto-review: error enqueuing {task_id} for {target}: {e}",
                file=sys.stderr,
            )

    if success_count == 0:
        return False

    # 2. Update task status to review_requested via SQLite
    try:
        from superharness.engine.state_writer import set_task_status

        set_task_status(project_dir, task_id, "review_requested")
    except Exception as e:
        logger.warning("_trigger_auto_review: unexpected error: %s", e, exc_info=True)
        print(
            f"auto-review: failed to update status for {task_id}: {e}", file=sys.stderr
        )
        return False

    print(f"auto-review: triggered reviews for {task_id} via {', '.join(enqueued)}")
    return True


def _auto_close_review_passed(project_dir: str) -> None:
    """Auto-close review_requested tasks when a reviewer submits a verdict report.

    Scans inbox for items with status==done and outcome containing "LGTM" or "REJECTED".
    """
    import yaml as _yaml
    import re as _re
    from superharness.commands.close import close_task

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    profile: dict = {}
    if os.path.isfile(profile_file):
        try:
            with open(profile_file, encoding="utf-8") as _f:
                profile = _yaml.safe_load(_f.read()) or {}
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            return

    # Same opt-in rule as _auto_close_report_ready
    auto_close = profile.get("auto_close", _profile_autonomy(profile) == "ai_driven")
    if not auto_close:
        return

    tasks = _load_tasks(project_dir)
    if not tasks:
        return

    from superharness.engine import state_reader as _sr

    try:
        inbox_items = _sr.get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return

    for task in tasks:
        if str(task.get("status")) != "review_requested":
            continue
        task_id = str(task.get("id"))

        # Detect LGTM/REJECTED in any done inbox item for this task
        verdict = ""
        reviewer = "reviewer"
        for item in inbox_items:
            if not isinstance(item, dict):
                continue
            if item.get("task") == task_id and item.get("status") == "done":
                outcome_raw = item.get("outcome")

                # F14: If outcome missing from item, check handoffs as fallback
                if not outcome_raw:
                    try:
                        history = _sr.get_handoffs(project_dir, task_id=task_id)
                        if history:
                            # Check the latest report/handoff from this agent
                            agent = str(item.get("to") or "")
                            latest = next(
                                (h for h in history if h.get("from_agent") == agent),
                                None,
                            )
                            if latest:
                                outcome_raw = str(latest.get("content") or "")
                    except Exception as e:
                        logger.warning(
                            "inbox_watch unexpected error: %s", e, exc_info=True
                        )
                        pass
                if not outcome_raw:
                    continue

                outcome_str = str(outcome_raw).upper()

                # Check 1: Structured YAML verdict (highest precision)
                if isinstance(outcome_raw, str) and (
                    "review_verdict:" in outcome_raw or "verdict:" in outcome_raw
                ):
                    try:
                        # Try full YAML load first
                        parsed = _yaml.safe_load(outcome_raw)
                        if isinstance(parsed, dict):
                            v_val = str(
                                parsed.get("review_verdict")
                                or parsed.get("verdict")
                                or ""
                            ).lower()
                            if v_val in ("lgtm", "rejected", "fail"):
                                verdict = "lgtm" if v_val == "lgtm" else "rejected"
                                reviewer = str(item.get("to") or "reviewer")
                                break
                    except Exception as e:
                        logger.warning(
                            "inbox_watch unexpected error: %s", e, exc_info=True
                        )
                        # Fallback to regex if YAML load fails (e.g. mixed content)
                        pass

                # Check 2: Regex extraction (robust against mixed text/YAML)
                if not verdict and isinstance(outcome_raw, str):
                    m = _re.search(
                        r"(?:review_verdict|verdict):\s*(lgtm|rejected|fail)",
                        outcome_raw,
                        _re.IGNORECASE,
                    )
                    if m:
                        v_val = m.group(1).lower()
                        verdict = "lgtm" if v_val == "lgtm" else "rejected"
                        reviewer = str(item.get("to") or "reviewer")
                        break

                # Check 3: Simple string match (legacy fallback)
                if not verdict:
                    if "LGTM" in outcome_str:
                        verdict = "lgtm"
                        reviewer = str(item.get("to") or "reviewer")
                        break
                    elif "REJECTED" in outcome_str:
                        verdict = "rejected"
                        reviewer = str(item.get("to") or "reviewer")
                        break

        if verdict == "lgtm":
            print(
                f"auto-close: review passed for '{task_id}' (detected LGTM from {reviewer})"
            )

            # 1. Transition to review_passed first (required by close_task gate)
            from superharness.engine.state_writer import set_task_status

            set_task_status(project_dir, task_id, "review_passed")

            # 2. Call close_task
            try:
                close_task(
                    project_dir=project_dir,
                    task_id=task_id,
                    actor=reviewer,
                    summary=f"Review passed: detected LGTM in {reviewer} report.",
                    skip_verify=True,
                )
            except Exception as e:
                logger.warning(
                    "_auto_close_review_passed: unexpected error: %s", e, exc_info=True
                )
                print(
                    f"auto-close: failed to close task '{task_id}': {e}",
                    file=sys.stderr,
                )

        elif verdict == "rejected":
            print(
                f"auto-close: review REJECTED for '{task_id}' (detected REJECTION from {reviewer})"
            )
            from superharness.engine.state_writer import set_task_status

            if set_task_status(project_dir, task_id, "review_failed"):
                # Append to ledger
                ledger_file = os.path.join(project_dir, ".superharness", "ledger.md")
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                line = f"- {ts} — watcher — REJECTED: {task_id} review failed by {reviewer}\n"
                try:
                    with open(ledger_file, "a", encoding="utf-8") as f:
                        f.write(line)
                except OSError as e:
                    # File I/O only raises OSError; a narrower catch here
                    # (vs Exception) lets an unrelated bug in this branch
                    # propagate instead of being reported as "ledger write
                    # failed".
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)


def _auto_close_report_ready(project_dir: str) -> None:
    """Auto-close report_ready tasks whose latest report handoff has tests_passed: true.

    Only runs when auto_close: true in profile.yaml (defaults to autonomy=autonomous).
    Calls close_task(skip_verify=True) with actor='watcher'.
    """
    import yaml as _yaml

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    profile: dict = {}
    if os.path.isfile(profile_file):
        try:
            with open(profile_file, encoding="utf-8") as _f:
                profile = _yaml.safe_load(_f.read()) or {}
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            return

    auto_close = profile.get("auto_close", _profile_autonomy(profile) == "ai_driven")
    if not auto_close:
        return

    # Read tasks from SQLite via state_reader (post-migration).
    tasks: list[dict] = _load_tasks(project_dir)
    if not tasks:
        return

    close_count = 0

    for task in tasks:
        if not isinstance(task, dict):
            continue
        if str(task.get("status", "")) != "report_ready":
            continue
        task_id = str(task.get("id", ""))
        if not task_id:
            continue

        # Find the latest report handoff for this task via SQLite.
        handoff: dict = {}
        try:
            from superharness.engine.db import managed_connection
            from superharness.engine import handoffs_dao
            from dataclasses import asdict

            with managed_connection(project_dir) as _conn:
                row = handoffs_dao.get_latest(_conn, task_id, "report")
                if row:
                    handoff = asdict(row)
                    # Flatten metadata into handoff dict for downstream consumers.
                    handoff.update(handoff.pop("metadata", {}) or {})
        except Exception as e:
            logger.warning(
                "inbox_watch handoffs_dao.get_latest failed: %s", e, exc_info=True
            )

        if not handoff:
            continue
        # Require tests_passed only when auto_close was inferred from autonomy,
        # not when explicitly set — explicit opt-in trusts the operator.
        auto_close_explicit = "auto_close" in profile
        if not handoff.get("tests_passed") and not auto_close_explicit:
            continue

        # iter 6: report verification gate. Stamps verification_failures on
        # the task and routes per suggested_action.
        try:
            from superharness.engine.report_verifier import (
                verify_report as _verify_report,
            )

            verification = _verify_report(handoff, task, project_dir)
            if not verification.passed:
                # Stamp verification_failures so dashboard surfaces them
                task["verification_failures"] = verification.failures
                # Persist verification failures to SQLite
                try:
                    from superharness.engine.state_writer import mirror_task_dict

                    mirror_task_dict(project_dir, task)
                except Exception as e:
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                    pass
                if verification.suggested_action == "fail":
                    print(
                        f"auto-close: task '{task_id}' failed verification: "
                        + "; ".join(verification.failures)
                    )
                    # Leave for operator (do not auto-fail to avoid surprise)
                else:
                    print(
                        f"auto-close: task '{task_id}' needs operator review: "
                        + "; ".join(verification.failures)
                    )
                continue
        except Exception as e:
            logger.warning(
                "_auto_close_report_ready: unexpected error: %s", e, exc_info=True
            )
            print(f"Warning: report verification skipped: {e}", file=sys.stderr)

        # NEW: Auto-review logic (Human-in-the-loop bypass)
        budget_cfg = profile.get("budget", {})
        auto_review_all = bool(budget_cfg.get("auto_review_all", False))
        threshold = float(budget_cfg.get("auto_review_usd_threshold", 0.0))
        cost_usd = float(handoff.get("cost_usd", 0.0))
        peer_reviewers = profile.get("peer_reviewers", [])
        autonomy = profile.get("autonomy", "ai_driven")

        # Autonomous Peer Review selection (cross-pollination)
        if not peer_reviewers and autonomy == "ai_driven":
            known_agents = ["claude-code", "codex-cli", "gemini-cli", "opencode"]
            owner = str(task.get("owner", ""))
            peers = [a for a in known_agents if a != owner]
            if peers:
                peer_reviewers = [peers[0]]

        if peer_reviewers and (
            auto_review_all
            or (threshold > 0 and cost_usd <= threshold)
            or autonomy == "ai_driven"
        ):
            # Reasoning: Reviewer must use a higher or equal model tier than the author
            author_tier = str(task.get("model_tier") or "standard")
            if author_tier == "mini":
                task["model_tier"] = (
                    "standard"  # Upgrade to at least standard for review
                )

            if _trigger_auto_review(project_dir, task_id, peer_reviewers):
                # Persist tier upgrade to SQLite
                try:
                    from superharness.engine.state_writer import mirror_task_dict

                    mirror_task_dict(project_dir, task)
                except Exception as e:
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                    pass
                continue  # Task moved to review_requested, skip auto-close

        # All gates passed — auto-close via SQLite
        try:
            from superharness.engine.db import get_connection as _gc3, init_db as _idb3
            from superharness.engine import tasks_dao as _td3

            conn3 = _gc3(project_dir)
            try:
                _idb3(conn3)
                existing = _td3.get(conn3, task_id)
                if existing:
                    # Build review context
                    outcome = str(handoff.get("outcome") or "auto-closed by watcher")
                    review_note = (
                        f"\n[auto-mode review] Task completed by {existing.owner or 'agent'}. "
                        f"Outcome: {outcome[:200]}. "
                        f"Review: verify changes, check tests pass, approve or request changes."
                    )
                    new_context = (existing.context or "") + review_note
                    _td3.upsert(
                        conn3,
                        _td3.TaskRow(
                            id=task_id,
                            title=existing.title,
                            owner=existing.owner or "watcher",
                            status="done",
                            effort=existing.effort,
                            project_path=project_dir,
                            development_method=existing.development_method,
                            acceptance_criteria=existing.acceptance_criteria,
                            test_types=existing.test_types,
                            out_of_scope=existing.out_of_scope,
                            definition_of_done=existing.definition_of_done,
                            context=new_context,
                            tdd=existing.tdd,
                            version=existing.version + 1,
                            created_at=existing.created_at,
                            blocked_by=existing.blocked_by,
                            parent_id=existing.parent_id,
                            report_ready_at=existing.report_ready_at,
                        ),
                    )
                    conn3.commit()
                    close_count += 1
                    summary = str(handoff.get("outcome") or "auto-closed by watcher")
                    print(
                        f"auto-close: '{task_id}' → done: {summary.split(chr(10))[0][:120]}"
                    )
            finally:
                conn3.close()
        except Exception as exc:
            logger.warning(
                "_auto_close_report_ready: unexpected error: %s", exc, exc_info=True
            )
            print(f"auto-close: failed to close '{task_id}': {exc}", file=sys.stderr)

    if close_count:
        _fire_hook("task:completed", {"count": close_count}, project_dir)
    print(f"auto-close: closed {close_count} task(s)")


def _auto_retry_failed(project_dir: str) -> None:
    """Auto-retry failed inbox items that have retries remaining.

    Runs when auto_retry: true in profile.yaml (or autonomy=autonomous).
    Items with retry_count >= max_retries are left as failed — permanent
    failures surface in the dashboard for operator review.
    """
    import yaml as _yaml

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    profile: dict = {}
    if os.path.isfile(profile_file):
        try:
            with open(profile_file, encoding="utf-8") as _f:
                profile = _yaml.safe_load(_f.read()) or {}
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            return

    auto_retry = profile.get("auto_retry", False)  # opt-in, not default
    if not auto_retry:
        return

    # Reset failed items directly in SQLite
    _auto_retry_failed_sqlite(project_dir)


def _auto_retry_failed_sqlite(project_dir: str) -> None:
    """Reset failed SQLite inbox items that still have retries remaining. Never raises."""
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        now = _now_utc()
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            failed = inbox_dao.get_all(conn, status="failed")
            for row in failed:
                if _is_reliable_task_id(conn, row.task_id): continue
                if row.retry_count < row.max_retries:
                    new_count = row.retry_count + 1
                    # Preserve the original failure reason so operator can see it
                    preserved_reason = (
                        row.failed_reason or "auto-retry (reason unavailable)"
                    )
                    inbox_dao.set_retry(conn, row.id, new_count, preserved_reason, now)
                    try:
                        from superharness.engine.ledger_dao import (
                            record as _ledger_record,
                        )

                        _now = now
                        _ledger_record(
                            conn,
                            task_id=row.task_id,
                            agent="watcher",
                            action="auto_retry",
                            details={
                                "reason": preserved_reason,
                                "attempt": f"{new_count}/{row.max_retries}",
                                "item_id": row.id,
                            },
                            now=_now,
                        )
                    except Exception as e:
                        logger.warning(
                            "inbox_watch unexpected error: %s", e, exc_info=True
                        )
                        pass
                    # Discussion round tasks must not be plan_only
                    if "/round-" in str(row.task_id) or "round-" in str(row.task_id):
                        inbox_dao.set_plan_only(conn, row.id, 0)
                    print(
                        f"auto-retry (sqlite): re-queued '{row.task_id}' "
                        f"(attempt {new_count}/{row.max_retries})"
                    )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "_auto_retry_failed_sqlite: unexpected error: %s", exc, exc_info=True
        )
        print(f"auto-retry (sqlite): error: {exc}", file=sys.stderr)


_AGENT_FALLBACK: dict[str, list[str]] = {
    "claude-code": ["codex-cli", "gemini-cli", "opencode"],
    "gemini-cli": ["claude-code", "codex-cli", "opencode"],
    "codex-cli": ["claude-code", "gemini-cli", "opencode"],
    "opencode": ["claude-code", "codex-cli", "gemini-cli"],
}

# Ordered preference for fallback when tried_agents is derived from inbox history
_FALLBACK_ORDER = ["claude-code", "codex-cli", "gemini-cli", "opencode"]


def _rank_fallback_agents(conn, candidates: list[str]) -> list[str]:
    """Reorder fallback candidates by recorded outcome quality.

    Uses review_dao.rank_owners (fail_rate ASC, then avg_duration_s ASC) to
    put agents with a good track record first. Candidates with too little
    review_store history to rank (< min_task_count rows) keep their
    original relative order, placed after every ranked candidate. Falls
    back to the input order unchanged on any error or when nothing is
    ranked yet (cold-start projects) — never blocks recovery.
    """
    try:
        from superharness.engine import review_dao

        ranked = review_dao.rank_owners(conn)
        rank_index = {stats.owner: i for i, stats in enumerate(ranked)}
    except Exception:
        logger.warning(
            "_rank_fallback_agents: unexpected error: %s",
            sys.exc_info()[1],
            exc_info=True,
        )
        return candidates
    if not rank_index:
        return candidates
    return sorted(
        candidates,
        key=lambda agent: (
            rank_index.get(agent, len(rank_index)),
            candidates.index(agent),
        ),
    )


# Owner id -> CLI binary name, for live reachability checks (shutil.which).
# Agents not listed here are checked by their own id (e.g. "opencode").
_AGENT_CLI_BINARY = {
    "claude-code": "claude",
    "codex-cli": "codex",
    "gemini-cli": "gemini",
}


def _agent_cli_reachable(agent: str) -> bool:
    """True if the agent's CLI binary is present on PATH right now.

    Availability-only signal — does not check auth/quota (see
    is_agent_quota_limited for that). Used to keep the exhausted-retry
    fallback path from re-routing a task to an agent that can't even run.
    """
    import shutil

    binary = _AGENT_CLI_BINARY.get(agent, agent)
    return shutil.which(binary) is not None


_RECOVERY_MAX = 2  # max recovery attempts before escalating to operator
_ABSOLUTE_MAX_RETRIES = 12  # hard cap on inbox.max_retries to prevent runaway loops
_IDENTICAL_FAILURE_THRESHOLD = 4  # N identical error_snippets in a row → escalate


def _has_identical_failure_loop(conn, task_id: str) -> bool:
    """Return True when the last N failures for this task share an
    identical error_snippet — indicates the failure is environmental
    (missing dir, missing CLI, bad config) and no agent reroute will fix it."""
    try:
        rows = conn.execute(
            "SELECT error_snippet FROM failures WHERE task_id = ? "
            "ORDER BY created_at DESC LIMIT ?",
            (task_id, _IDENTICAL_FAILURE_THRESHOLD),
        ).fetchall()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return False
    if len(rows) < _IDENTICAL_FAILURE_THRESHOLD:
        return False
    snippets = {(r["error_snippet"] or "").strip() for r in rows}
    # All identical (and non-empty) → loop
    return len(snippets) == 1 and next(iter(snippets)) != ""


def _with_task_lock(
    conn, task_id: str, changes: dict, *, guard=None, context: str = ""
):
    """Apply `changes` (status and/or other columns) to `task_id` through
    `tasks_dao.set_status`/`update` — the version-checked writer — instead
    of a raw, unversioned SQL update statement against the tasks table.

    Reads the task's current version immediately before writing, retries
    once on a `ConcurrencyError` (re-reading the fresh version), and on a
    second conflict logs and returns None rather than raising or silently
    overwriting a concurrent writer's change. The 2026-07-20 audit found 17
    of 18 task writers bypassed the version check entirely and
    `ConcurrencyError` was caught nowhere in the codebase — this is the one
    seam all of the watcher's status-writing call sites now share.

    `guard`, if given, receives the freshly-read TaskRow and must return
    True for the write to proceed — used by call sites whose original raw
    SQL carried a `WHERE ... AND status='...'` condition rather than a bare
    `WHERE id=?`. Returns None (no write attempted) when the guard rejects
    the current state, same as a task that no longer exists.
    """
    from superharness.engine import tasks_dao
    from superharness.engine.state_errors import ConcurrencyError

    for attempt in range(2):
        task = tasks_dao.get(conn, task_id)
        if task is None:
            return None
        if guard is not None and not guard(task):
            return None
        try:
            if "status" in changes:
                status = changes["status"]
                extra = {k: v for k, v in changes.items() if k != "status"}
                return tasks_dao.set_status(
                    conn, task_id, status, task.version, **extra
                )
            return tasks_dao.update(conn, task_id, task.version, changes)
        except ConcurrencyError:
            if attempt == 0:
                continue  # re-read the fresh version and retry once
            logger.warning(
                "inbox_watch: skipping task '%s' after repeated version conflict (%s)",
                task_id,
                context or sorted(changes.keys()),
            )
            return None
    return None


def _escalate_runaway_inbox(conn, row, reason_label: str, now: str) -> None:
    """Mark task as waiting_input and close the inbox row so a human can
    act. Called when auto-recover hits an absolute ceiling or detects
    that no reroute will help."""
    try:
        from superharness.engine import tasks_dao
        from superharness.engine.next_action import validate_status_transition

        task = tasks_dao.get(conn, row.task_id)
        if task and task.status in ("in_progress", "todo"):
            try:
                validate_status_transition(task.status, "waiting_input")
                _with_task_lock(
                    conn,
                    row.task_id,
                    {
                        "status": "waiting_input",
                        "in_progress_at": None,
                        "failed_reason": f"auto-recover: {reason_label} ({row.failed_reason or 'unknown'})",
                    },
                    context="escalate_runaway_inbox",
                )
            except Exception as e:
                logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                pass
        from superharness.engine import inbox_dao as _inbox_dao_esc

        _inbox_dao_esc.mark_failed(
            conn, row.id, reason=f"escalated: {reason_label}", now=now
        )
        print(
            f"auto-recover: ESCALATED '{row.task_id}' → waiting_input "
            f"(reason: {reason_label})"
        )
    except Exception as exc:
        logger.warning(
            "_escalate_runaway_inbox: unexpected error: %s", exc, exc_info=True
        )
        print(f"auto-recover: escalation failed for {row.id}: {exc}", file=sys.stderr)


_VALID_FALLBACK_OWNERS = frozenset({"claude-code", "codex-cli", "gemini-cli"})


def _auto_fallback_owner_reassign(project_dir: str) -> None:
    """Reassign exhausted-retry tasks to the configured auto_fallback_owner.

    When profile.yaml sets `auto_fallback_owner: <agent>` and a failed inbox
    item has exhausted its retry budget (retry_count >= max_retries):
      - If the task's current owner is NOT the fallback owner:
        reassign the task owner, re-route the inbox item to the fallback owner,
        and reset the retry budget to give the fallback owner a fresh start.
      - If the task is already owned by the fallback owner (fallback exhausted too):
        skip — the existing auto_recover escalation handles waiting_input.

    Reads `auto_fallback_max_retries` from profile.yaml (default: 3) to set
    the retry budget for the fallback owner's attempt.

    This runs before _auto_recover_exhausted_failures_sqlite so the profile-
    configured fallback takes priority over generic agent re-routing.
    Never raises.
    """
    import yaml as _yaml

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    profile: dict = {}
    if os.path.isfile(profile_file):
        try:
            with open(profile_file, encoding="utf-8") as _f:
                profile = _yaml.safe_load(_f.read()) or {}
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            return

    fallback_owner = str(profile.get("auto_fallback_owner") or "").strip()
    if not fallback_owner:
        return
    if fallback_owner not in _VALID_FALLBACK_OWNERS:
        print(
            f"auto-fallback-owner: invalid auto_fallback_owner '{fallback_owner}' "
            f"(valid: {', '.join(sorted(_VALID_FALLBACK_OWNERS))})",
            file=sys.stderr,
        )
        return

    try:
        max_retries_for_fallback = int(profile.get("auto_fallback_max_retries", 3))
    except (ValueError, TypeError):
        max_retries_for_fallback = 3

    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao, tasks_dao

        now = _now_utc()
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            failed = inbox_dao.get_all(conn, status="failed")
            reassigned = 0

            for row in failed:
                if _is_reliable_task_id(conn, row.task_id): continue
                if row.retry_count < row.max_retries:
                    continue  # still has retries — handled by _auto_retry_failed

                task = tasks_dao.get(conn, row.task_id)
                if task is None:
                    continue
                if task.status in ("done", "stopped", "archived", "waiting_input"):
                    continue

                current_owner = task.owner or row.target_agent
                if current_owner == fallback_owner:
                    # Fallback owner has also exhausted retries — let auto_recover escalate
                    continue

                _with_task_lock(
                    conn,
                    row.task_id,
                    {"owner": fallback_owner},
                    context="auto_fallback_owner",
                )
                inbox_dao.reassign(
                    conn,
                    row.id,
                    target_agent=fallback_owner,
                    max_retries=max_retries_for_fallback,
                    reason=f"auto-fallback: reassigned from '{current_owner}' to '{fallback_owner}'",
                )
                print(
                    f"auto-fallback-owner: reassigned '{row.task_id}' "
                    f"{current_owner} -> {fallback_owner} "
                    f"(fresh budget: {max_retries_for_fallback})"
                )
                try:
                    from superharness.engine.ledger_dao import record as _ledger_record

                    _ledger_record(
                        conn,
                        task_id=row.task_id,
                        agent="watcher",
                        action="auto_fallback_owner",
                        details={
                            "from_owner": current_owner,
                            "to_owner": fallback_owner,
                            "inbox_id": row.id,
                            "prev_failed_reason": row.failed_reason,
                        },
                        now=now,
                    )
                except Exception as e:
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                    pass
                reassigned += 1

            conn.commit()
            if reassigned:
                print(
                    f"auto-fallback-owner: {reassigned} task(s) reassigned to '{fallback_owner}'"
                )
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "_auto_fallback_owner_reassign: unexpected error: %s", exc, exc_info=True
        )
        print(f"auto-fallback-owner: error: {exc}", file=sys.stderr)


def _auto_recover_exhausted_failures_sqlite(project_dir: str) -> None:
    """Recover failed inbox items that have exhausted retries.

    Uses the failure_classifier to decide if recovery is possible:
      - permanent_block / no_op → skip (different agent won't help)
      - quota / transient / agent_crash / unknown → re-enqueue to a fallback agent

    Items already recovered RECOVERY_MAX times are escalated to operator:
      marks the parent task as blocked with failure_context.
    Never raises.
    """
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao, tasks_dao

        now = _now_utc()
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            failed = inbox_dao.get_all(conn, status="failed")
            recovered = 0
            escalated = 0

            for row in failed:
                if _is_reliable_task_id(conn, row.task_id): continue
                if row.retry_count < row.max_retries:
                    continue  # _auto_retry_failed handles these

                # Read recovery_count from its own column (durable across
                # failed_reason overwrites). Fall back to legacy parse for
                # rows that pre-date the migration backfill.
                import re

                recovery_count = getattr(row, "recovery_count", 0) or 0
                reason = (row.failed_reason or "").lower()
                if recovery_count == 0:
                    rc_match = re.search(r"recovery_(\d+)", reason)
                    if rc_match:
                        recovery_count = int(rc_match.group(1))

                # Skip permanent failures — agent routing won't help
                # But revert stuck in_progress tasks so they can be re-dispatched.
                # EXCEPTION: discussion rounds (/round-N) are multi-agent —
                # one agent's permanent block must not freeze the entire round.
                if (
                    "permanent_block" in reason
                    or "no_op" in reason
                    or "permanent block" in reason
                ):
                    is_discussion_round = "/round-" in row.task_id
                    task_pb = tasks_dao.get(conn, row.task_id)
                    if (
                        task_pb
                        and task_pb.status in ("in_progress", "todo")
                        and not is_discussion_round
                    ):
                        try:
                            from superharness.engine.next_action import (
                                validate_status_transition,
                            )

                            validate_status_transition(task_pb.status, "waiting_input")
                            gate_reason = row.failed_reason or "lifecycle gate rejected"
                            _with_task_lock(
                                conn,
                                row.task_id,
                                {
                                    "status": "waiting_input",
                                    "in_progress_at": None,
                                    "failed_reason": gate_reason,
                                },
                                context="permanent_block_escalation",
                            )
                            # Mark inbox as done so it doesn't block re-dispatch
                            inbox_dao.mark_done(conn, row.id, now=now)
                            print(
                                f"auto-recover: permanent block escalated '{row.task_id}' "
                                f"in_progress → waiting_input (lifecycle gate)"
                            )
                            try:
                                from superharness.engine.ledger_dao import (
                                    record as _ledger_record2,
                                )

                                _ledger_record2(
                                    conn,
                                    task_id=row.task_id,
                                    agent="watcher",
                                    action="escalate",
                                    details={
                                        "reason": gate_reason,
                                        "from_status": "in_progress",
                                        "to_status": "waiting_input",
                                        "item_id": row.id,
                                    },
                                    now=now,
                                )
                            except Exception as e:
                                logger.warning(
                                    "inbox_watch unexpected error: %s", e, exc_info=True
                                )
                                pass
                            recovered += 1
                        except Exception as e:
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                            pass
                    continue

                # Skip if parent task is no longer dispatch-ready
                task = tasks_dao.get(conn, row.task_id)
                if task is None or task.status in ("done", "stopped", "archived"):
                    continue
                # Determine fallback agent — skip owners already tried on this task
                current_agent = row.target_agent
                tried_agents: set[str] = set()
                try:
                    tried_rows = conn.execute(
                        "SELECT DISTINCT target_agent FROM inbox WHERE task_id=? AND status IN ('failed','done','archived')",
                        (row.task_id,),
                    ).fetchall()
                    tried_agents = {r[0] for r in tried_rows if r[0]}
                except Exception as e:
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                    pass
                tried_agents.add(current_agent)
                try:
                    from superharness.engine.model_router import (
                        is_agent_quota_limited as _is_quota,
                    )

                    fallback_agents = [
                        a
                        for a in _FALLBACK_ORDER
                        if a not in tried_agents
                        and not _is_quota(str(project_dir), a)
                        and _agent_cli_reachable(a)
                    ]
                except Exception:
                    logger.warning(
                        "_auto_recover_exhausted_failures_sqlite: unexpected error: %s",
                        sys.exc_info()[1],
                        exc_info=True,
                    )
                    fallback_agents = [
                        a for a in _FALLBACK_ORDER if a not in tried_agents
                    ]
                fallback_agents = _rank_fallback_agents(conn, fallback_agents)
                if not fallback_agents:
                    # All known owners exhausted — escalate with rich context
                    per_owner_summary = ", ".join(sorted(tried_agents))
                    _escalate_runaway_inbox(
                        conn,
                        row,
                        f"all_owners_exhausted (tried: {per_owner_summary})",
                        now,
                    )
                    # Tag task with escalation_reason for dashboard/status distinction.
                    # Guarded on status=='waiting_input' — same condition the
                    # original raw SQL's WHERE clause carried — because
                    # _escalate_runaway_inbox() above only actually moves the
                    # task to waiting_input when it was in_progress/todo.
                    try:
                        _with_task_lock(
                            conn,
                            row.task_id,
                            {
                                "failed_reason": f"all_owners_exhausted: {per_owner_summary}"
                            },
                            guard=lambda t: t.status == "waiting_input",
                            context="tag_all_owners_exhausted",
                        )
                    except Exception as e:
                        logger.warning(
                            "inbox_watch unexpected error: %s", e, exc_info=True
                        )
                        pass
                    escalated += 1
                    continue
                next_agent = fallback_agents[0]

                # If already recovered too many times, escalate with root cause analysis
                if recovery_count >= _RECOVERY_MAX:
                    reason_lower = (row.failed_reason or "").lower()

                    agents_tried: list[str] = []
                    rc_matches = re.findall(r"_to_(\S+)", reason_lower)
                    agents_tried = [row.target_agent] + rc_matches
                    agents_tried = list(dict.fromkeys(agents_tried))

                    is_architecture_bug = (
                        "permanent_block" in reason_lower
                        or "command not found" in reason_lower
                        or "syntax error" in reason_lower
                        or "unbound variable" in reason_lower
                    )

                    if is_architecture_bug:
                        (
                            f"\n[auto-recovery] INFRA ESCALATION: task failed on "
                            f"{', '.join(agents_tried)} with: "
                            f"{row.failed_reason or 'unknown error'}. "
                            f"Check launcher logs, CLI availability, and superharness hooks."
                        )
                        # Record to failures ledger for diagnostics
                        try:
                            from superharness.engine import failures_dao

                            failures_dao.record(
                                conn,
                                agent=row.target_agent,
                                error=f"[auto-recovery] infra escalation: "
                                f"task {row.task_id} failed on {agents_tried}",
                                details={
                                    "task_id": row.task_id,
                                    "agents": agents_tried,
                                    "reason": row.failed_reason,
                                },
                            )
                        except Exception as e:
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                            pass
                # Hard cap absolute retry budget so a runaway loop can't
                # bump max_retries indefinitely (was the cause of max_retries=65).
                if row.max_retries >= _ABSOLUTE_MAX_RETRIES:
                    _escalate_runaway_inbox(
                        conn, row, "absolute retry ceiling reached", now
                    )
                    escalated += 1
                    continue

                # Identical-error escalation: if the same error_snippet has
                # repeated >= _IDENTICAL_FAILURE_THRESHOLD times for this
                # task, no agent reroute will help — escalate to operator.
                if _has_identical_failure_loop(conn, row.task_id):
                    _escalate_runaway_inbox(
                        conn, row, "identical-error loop detected", now
                    )
                    escalated += 1
                    continue

                # Recover: re-enqueue to fallback agent. Counter lives in
                # its own column now so subsequent failures can't wipe it
                # by overwriting failed_reason.
                new_recovery = recovery_count + 1
                new_reason = f"recovery_{new_recovery}:{current_agent}_to_{next_agent}"
                inbox_dao.mark_recovered(
                    conn,
                    row.id,
                    target_agent=next_agent,
                    recovery_count=new_recovery,
                    reason=new_reason,
                )
                print(
                    f"auto-recover: re-routed '{row.task_id}' "
                    f"{current_agent} → {next_agent} "
                    f"(recovery_{new_recovery}/{_RECOVERY_MAX})"
                )
                recovered += 1

            conn.commit()
            if recovered or escalated:
                print(
                    f"auto-recover: {recovered} re-routed, "
                    f"{escalated} escalated to operator"
                )
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "_auto_recover_exhausted_failures_sqlite: unexpected error: %s",
            exc,
            exc_info=True,
        )
        print(f"auto-recover: error: {exc}", file=sys.stderr)


def _reconcile_permanent_blocks(project_dir: str) -> int:
    """Revert in_progress tasks stuck behind permanent-block inbox failures.

    Returns count of tasks reverted.
    Public entry point for the watcher loop and tests.
    Delegates to _auto_recover_exhausted_failures_sqlite.
    """
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import tasks_dao, inbox_dao

    count = 0
    try:
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            failed = inbox_dao.get_all(conn, status="failed")
            for row in failed:
                if _is_reliable_task_id(conn, row.task_id): continue
                if row.retry_count < row.max_retries:
                    continue
                reason = (row.failed_reason or "").lower()
                if (
                    "permanent_block" not in reason
                    and "no_op" not in reason
                    and "permanent block" not in reason
                ):
                    continue
                task = tasks_dao.get(conn, row.task_id)
                if not task or task.status not in ("in_progress", "todo"):
                    continue
                from superharness.engine.next_action import (
                    validate_status_transition as _vst2,
                )

                _vst2(task.status, "waiting_input")
                _with_task_lock(
                    conn,
                    row.task_id,
                    {
                        "status": "waiting_input",
                        "in_progress_at": None,
                        "failed_reason": row.failed_reason or "lifecycle gate rejected",
                    },
                    context="reconcile_permanent_blocks",
                )
                inbox_dao.mark_done(conn, row.id, now=_now_utc())
                count += 1
                print(
                    f"reconcile-permanent-block: escalated '{row.task_id}' "
                    f"in_progress → waiting_input"
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "_reconcile_permanent_blocks: unexpected error: %s", exc, exc_info=True
        )
        print(f"reconcile-permanent-block: error: {exc}", file=sys.stderr)
    return count


def _auto_bootstrap_empty_tasks(project_dir: str) -> int:
    """For tasks escalated to waiting_input with empty AC, dispatch a plan-only
    agent to propose acceptance criteria, definition of done, and context.

    This closes the loop: Gate 4 blocks empty tasks → auto-recovery escalates
    → auto-bootstrap dispatches AC-proposal agent → task gets content → re-dispatch.
    Returns count of tasks bootstrapped.
    """
    count = 0
    try:
        from superharness.engine.db import get_connection, init_db, now_iso as _now_iso
        from superharness.engine import inbox_dao, tasks_dao
        import uuid

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            tasks = tasks_dao.get_all(conn, status="waiting_input")
            now = _now_iso()
            from superharness.engine.next_action import (
                infer_workflow as _infer_workflow,
                plan_only_allowed_statuses as _plan_only_allowed,
            )

            for task in tasks:
                # Bootstrap demotes status to plan_proposed and dispatches a
                # plan-only inbox row. That's safe only when plan_proposed is
                # in the workflow's plan-only-dispatch set — otherwise the
                # lifecycle gate rejects every subsequent dispatch, trapping
                # the task. Only the implementation workflow includes
                # plan_proposed in its plan_only set; discussion/quick/note/
                # review/approval all exclude it.
                workflow = _infer_workflow(task.id, {"workflow": task.workflow})
                if "plan_proposed" not in _plan_only_allowed(workflow):
                    continue
                # Only bootstrap tasks with genuinely empty content
                ac = task.acceptance_criteria or []
                dod = task.definition_of_done or []
                ctx = task.context or ""
                if ac or dod or ctx:
                    continue  # already has content
                # Create plan-only inbox item for AC proposal
                item_id = f"bootstrap-{task.id}-{uuid.uuid4().hex[:8]}"
                inbox_dao.enqueue(
                    conn,
                    id=item_id,
                    task_id=task.id,
                    target_agent=task.owner,
                    priority=1,
                    max_retries=1,
                    project_path="",
                    plan_only=True,
                    now=now,
                )
                # Revert task to plan_proposed so watcher can auto-dispatch it
                from superharness.engine.next_action import (
                    validate_status_transition as _vst3,
                )

                _vst3("waiting_input", "plan_proposed")
                _with_task_lock(
                    conn,
                    task.id,
                    {
                        "status": "plan_proposed",
                        "failed_reason": None,
                        "in_progress_at": None,
                    },
                    context="auto_bootstrap_empty_tasks",
                )
                count += 1
                print(
                    f"auto-bootstrap: dispatching AC-proposal for '{task.id}' "
                    f"→ {task.owner} (plan_only)"
                )
                try:
                    from superharness.engine.ledger_dao import record as _lr

                    _lr(
                        conn,
                        task_id=task.id,
                        agent="watcher",
                        action="auto_bootstrap",
                        details={
                            "reason": task.failed_reason or "empty content",
                            "item_id": item_id,
                        },
                        now=now,
                    )
                except Exception as e:
                    logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                    pass
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(
            "_auto_bootstrap_empty_tasks: unexpected error: %s", exc, exc_info=True
        )
        print(f"auto-bootstrap: error: {exc}", file=sys.stderr)
    return count


def _check_ship_on_complete_tasks(project_dir: str) -> None:
    """For ship_on_complete tasks at report_ready with no PR URL, mark failed."""
    from superharness.engine import state_reader, state_writer

    try:
        tasks = state_reader.get_tasks(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return

    for task in tasks:
        if not isinstance(task, dict):
            continue
        if not task.get("ship_on_complete"):
            continue
        if task.get("status") != "report_ready":
            continue
        task_id = str(task.get("id", ""))
        if not task_id:
            continue
        handoff_dir = os.path.join(project_dir, ".superharness", "handoffs")
        pr_url = _find_pr_url_in_handoff(handoff_dir, task_id)
        if not pr_url:
            print(
                f"ship_on_complete: task '{task_id}' reached report_ready without a PR URL "
                f"in handoff outcomes — marking failed.",
                file=sys.stderr,
            )
            state_writer.set_task_status(
                project_dir, task_id, "failed", from_status="report_ready"
            )


def run_once(
    project_dir: str,
    *,
    to: str = "both",
    non_interactive: bool = True,  # default non-interactive for auto-mode
    recover_timeout_minutes: int = 3,
    recover_action: str = "retry",
    launcher_timeout: int = 0,
) -> None:
    """Run a single watcher tick without acquiring the watcher lock. For tests."""
    _run_scripts(
        project_dir,
        target=to,
        print_only=False,
        non_interactive=non_interactive,
        codex_bypass=False,
        launcher_timeout=launcher_timeout,
        recover_timeout_minutes=recover_timeout_minutes,
        recover_action=recover_action,
    )


# Mutable cycle counter for GC interval tracking (reset per watcher session)
_watcher_cycle_count = [0]

DEFAULT_GC_INTERVAL_CYCLES = 5


def _run_gc_if_due(project_dir: str, cycle_count: int) -> bool:
    """Run inbox GC if the current cycle is a multiple of gc_interval_cycles. Returns True if GC ran."""
    gc_interval = DEFAULT_GC_INTERVAL_CYCLES
    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if os.path.isfile(profile_file):
        try:
            import yaml as _yaml

            with open(profile_file) as f:
                profile = _yaml.safe_load(f) or {}
            gc_interval = int(
                profile.get("gc_interval_cycles", DEFAULT_GC_INTERVAL_CYCLES)
            )
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            pass
    if gc_interval < 1:
        gc_interval = DEFAULT_GC_INTERVAL_CYCLES
    if cycle_count % gc_interval != 0:
        return False
    from superharness.commands.inbox_gc import run_gc

    result = run_gc(project_dir)
    return result.get("reconciled", 0) >= 0


def _fire_hook(event: str, data: dict, project_dir: str | None = None) -> None:
    """Fire an event hook. Never raises."""
    try:
        from superharness.engine.hooks import get_registry

        get_registry().fire(event, data)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _sqlite_singleton_acquire(project_dir: str) -> None:
    """Acquire the SQLite watcher singleton lease. Never raises."""
    try:
        import socket
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import watcher_singleton

        now = _now_utc()
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            watcher_singleton.acquire(
                conn, pid=os.getpid(), hostname=socket.gethostname(), now=now
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _sqlite_singleton_release(project_dir: str) -> None:
    """Release the SQLite watcher singleton lease. Never raises."""
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import watcher_singleton

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            watcher_singleton.release(conn, os.getpid())
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _sqlite_tick(project_dir: str, now: str) -> None:
    """Run SQLite-side per-tick operations: record watcher heartbeat and
    flag stale agent_heartbeats as zombie.

    Never raises. Silently skipped if SQLite backend is not initialised yet.
    """
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import watcher_singleton
        from superharness.engine import heartbeat_dao
        from superharness.engine import liveness

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            watcher_singleton.heartbeat(conn, os.getpid(), now)
            heartbeat_dao.mark_stale(conn, now=now)
            liveness.touch_watcher(conn, project_dir)
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _self_diagnosis(project_dir: str) -> list[str]:
    """Check environment health before running auto-mode. Returns list of warnings."""
    warnings = []

    # Check Python has yaml (prevents ModuleNotFoundError)
    try:
        import yaml
    except ImportError:
        warnings.append("MISSING: pyyaml not installed — dispatch will fail")

    # Check SQLite DB exists and is writable
    from superharness.utils.paths import resolve_active_state_db_path

    db_path = resolve_active_state_db_path(project_dir)
    if not os.path.isfile(db_path):
        warnings.append(f"MISSING: {db_path} — run shux init or start watcher first")
    elif not os.access(db_path, os.W_OK):
        warnings.append(f"PERMISSION: {db_path} is not writable")

    # Check agent binaries exist
    for agent, binary in _AGENT_CLI_BINARY.items():
        if not _agent_cli_reachable(agent):
            warnings.append(f"MISSING: {agent} binary '{binary}' not on PATH")

    # Check profile.yaml exists and has required fields
    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if os.path.isfile(profile_file):
        try:
            profile = yaml.safe_load(open(profile_file).read()) or {}
            if not profile.get("auto_dispatch"):
                warnings.append("CONFIG: auto_dispatch not enabled in profile.yaml")
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            warnings.append("CORRUPT: profile.yaml cannot be parsed")
    else:
        warnings.append("MISSING: profile.yaml — auto-mode needs project config")

    # Log warnings
    if warnings:
        for w in warnings:
            print(f"self-diagnosis: {w}")

    return warnings


def auto_enqueue_todo(project_dir: str) -> int:
    """Scan contract for todo tasks and enqueue them for planning.

    Only runs if auto_dispatch=True and autonomy=(autonomous OR ai_driven) in profile.yaml.
    """
    import uuid
    from superharness.engine import state_reader
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import inbox_dao

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if not os.path.exists(profile_file):
        return 0
    try:
        import yaml as _yaml

        profile = _yaml.safe_load(open(profile_file, encoding="utf-8").read()) or {}
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0
    if not profile.get("auto_dispatch"):
        return 0

    if _profile_autonomy(profile) != "ai_driven":
        return 0

    tasks = _load_tasks(project_dir)
    if not tasks:
        return 0

    inbox_items = []
    try:
        inbox_items = state_reader.get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    # Track tasks already in inbox (active)
    active_tasks: set[str] = set()
    for item in inbox_items:
        if not isinstance(item, dict):
            continue
        if item.get("status") in ("pending", "launched", "running", "paused"):
            active_tasks.add(str(item.get("task", "")))

    added = 0
    now = _now_utc()
    new_items = []

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        for task in tasks:
            if not isinstance(task, dict) or task.get("status") != "todo":
                continue
            task_id = str(task.get("id", ""))
            if not task_id or task_id in active_tasks:
                continue

            # Check dependencies
            if not _deps_satisfied_from_tasks(tasks, task_id):
                continue

            # Skip tasks whose deadline has already passed (lifecycle will fail them anyway)
            deadline = task.get("deadline_minutes")
            if deadline:
                try:
                    deadline = int(deadline)
                except (ValueError, TypeError):
                    deadline = None
            if deadline and deadline > 0:
                created = task.get("created_at", "")
                if created:
                    try:
                        from datetime import datetime as _dt, timezone as _tz

                        t = _dt.fromisoformat(str(created).replace("Z", "+00:00"))
                        now_dt = _dt.now(_tz.utc)
                        age = (now_dt - t).total_seconds() / 60
                        if age >= deadline:
                            continue  # deadline already expired — don't enqueue
                    except (ValueError, TypeError):
                        pass

            owner = str(task.get("owner", "claude-code"))
            item_id = f"auto-{uuid.uuid4().hex[:6]}"

            # 1. Mirror task to SQLite if missing
            _ensure_task_in_sqlite(conn, task_id, project_dir, now)

            # 2. Burst guard: skip if this task has had too many recent failures
            from superharness.engine.burst_guard import task_burst_suppressed

            if task_burst_suppressed(conn, task_id):
                continue

            # 3. Enqueue in SQLite
            inbox_dao.enqueue(
                conn,
                id=item_id,
                task_id=task_id,
                target_agent=owner,
                priority=2,
                max_retries=3,
                project_path=project_dir,
                plan_only=True,
                now=now,
            )

            new_items.append(
                {
                    "id": item_id,
                    "task": task_id,
                    "to": owner,
                    "status": "pending",
                    "priority": 2,
                    "retry_count": 0,
                    "max_retries": 3,
                    "created_at": now,
                    "project": project_dir,
                    "plan_only": True,
                }
            )
            active_tasks.add(task_id)
            added += 1
            print(f"auto-dispatch: enqueued todo {task_id} for planning → {owner}")

        conn.commit()
    finally:
        conn.close()

    return added


def auto_enqueue_approved(project_dir: str) -> int:
    """Scan contract for plan_approved tasks and enqueue them.

    Only runs if auto_dispatch=True and autonomy=(autonomous OR oversight OR ai_driven) in profile.yaml.
    """
    import uuid
    from superharness.engine import state_reader
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import inbox_dao

    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if not os.path.exists(profile_file):
        return 0
    try:
        import yaml as _yaml

        profile = _yaml.safe_load(open(profile_file, encoding="utf-8").read()) or {}
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0
    if not profile.get("auto_dispatch"):
        return 0

    if _profile_autonomy(profile) != "ai_driven":
        return 0

    tasks = _load_tasks(project_dir)
    if not tasks:
        return 0

    inbox_items = []
    try:
        inbox_items = state_reader.get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    # Track tasks already in inbox (active)
    active_tasks: set[str] = set()
    for item in inbox_items:
        if not isinstance(item, dict):
            continue
        if item.get("status") in ("pending", "launched", "running", "paused"):
            active_tasks.add(str(item.get("task", "")))

    # Track per-task failure counts to enforce a retry cap.
    # Without this guard, every failed item exits active_tasks and the next
    # watcher tick creates a fresh item with retry_count=0 — causing an
    # infinite flood of new items for permanently-failing tasks.
    # Prefer SQLite (authoritative in production); fall back to YAML items.
    failed_counts: dict[str, int] = {}
    _default_max_retries = 3
    from superharness.utils.paths import resolve_active_state_db_path as _rap

    _db_path = _rap(project_dir)
    if os.path.isfile(_db_path):
        try:
            _fc = get_connection(project_dir)
            try:
                init_db(_fc)
                for row in _fc.execute(
                    "SELECT task_id, COUNT(*) FROM inbox WHERE status='failed' GROUP BY task_id"
                ).fetchall():
                    failed_counts[str(row[0])] = int(row[1])
            finally:
                _fc.close()
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            pass
    else:
        for item in inbox_items:
            if not isinstance(item, dict):
                continue
            if item.get("status") == "failed":
                tid = str(item.get("task", ""))
                failed_counts[tid] = failed_counts.get(tid, 0) + 1

    added = 0
    now = _now_utc()
    new_items = []

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        for task in tasks:
            if not isinstance(task, dict) or task.get("status") != "plan_approved":
                continue
            task_id = str(task.get("id", ""))
            if not task_id or task_id in active_tasks:
                continue
            # Stop re-enqueueing tasks that have already exhausted their retry budget.
            max_retries = int(task.get("max_retries", _default_max_retries))
            if failed_counts.get(task_id, 0) >= max_retries:
                continue

            # Check dependencies
            if not _deps_satisfied_from_tasks(tasks, task_id):
                continue

            # Skip tasks whose deadline has already passed (lifecycle will fail them anyway)
            deadline = task.get("deadline_minutes")
            if deadline:
                try:
                    deadline = int(deadline)
                except (ValueError, TypeError):
                    deadline = None
            if deadline and deadline > 0:
                created = task.get("created_at", "")
                if created:
                    try:
                        from datetime import datetime as _dt, timezone as _tz

                        t = _dt.fromisoformat(str(created).replace("Z", "+00:00"))
                        now_dt = _dt.now(_tz.utc)
                        age = (now_dt - t).total_seconds() / 60
                        if age >= deadline:
                            continue  # deadline already expired — don't enqueue
                    except (ValueError, TypeError):
                        pass

            owner = str(task.get("owner", "claude-code"))
            item_id = f"auto-{uuid.uuid4().hex[:6]}"

            # 1. Mirror task to SQLite if missing
            _ensure_task_in_sqlite(conn, task_id, project_dir, now)

            # 2. Enqueue in SQLite; catch duplicate if another process raced us.
            try:
                inbox_dao.enqueue(
                    conn,
                    id=item_id,
                    task_id=task_id,
                    target_agent=owner,
                    priority=2,
                    max_retries=3,
                    project_path=project_dir,
                    plan_only=False,
                    now=now,
                )
            except Exception as _enq_err:
                logger.warning(
                    "auto_enqueue_approved: unexpected error: %s",
                    _enq_err,
                    exc_info=True,
                )
                # StateError (duplicate) or any other DB error — skip silently.
                active_tasks.add(task_id)
                continue

            new_items.append(
                {
                    "id": item_id,
                    "task": task_id,
                    "to": owner,
                    "status": "pending",
                    "priority": 2,
                    "retry_count": 0,
                    "max_retries": 3,
                    "created_at": now,
                    "project": project_dir,
                    "plan_only": False,
                }
            )
            active_tasks.add(task_id)
            added += 1
            print(f"auto-dispatch: enqueued approved {task_id} → {owner}")

        conn.commit()
    finally:
        conn.close()

    return added


_LAUNCHER_LOG_MAX_FILES = 200


def _rotate_launcher_logs_if_needed(project_dir: str) -> None:
    """Remove old launcher logs if there are too many. Never raises."""
    import glob

    try:
        log_dir = os.path.join(project_dir, ".superharness", "launcher-logs")
        if not os.path.isdir(log_dir):
            return
        logs = sorted(glob.glob(os.path.join(log_dir, "*.log")), key=os.path.getmtime)
        if len(logs) > _LAUNCHER_LOG_MAX_FILES:
            to_remove = len(logs) - _LAUNCHER_LOG_MAX_FILES
            for lf in logs[:to_remove]:
                os.remove(lf)
            print(
                f"disk-guard: removed {to_remove} old launcher log(s) ({len(logs)} -> {_LAUNCHER_LOG_MAX_FILES})"
            )
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _run_scripts(
    project_dir: str,
    *,
    target: str,
    print_only: bool,
    non_interactive: bool,
    codex_bypass: bool,
    launcher_timeout: int,
    recover_timeout_minutes: int,
    recover_action: str,
) -> None:
    script_dir = _find_scripts_dir()

    # Self-diagnosis: check environment before running auto-mode
    _self_diagnosis(project_dir)

    # Disk guard: rotate launcher logs if too many (>200 total)
    _rotate_launcher_logs_if_needed(project_dir)

    # SQLite tick: drain dual-write queue + record heartbeat
    _sqlite_tick(project_dir, _now_utc())

    # Reliable-orchestrator tasks have one lifecycle owner.  The lease is
    # independent of the legacy watcher singleton during the migration.
    _reliable_tick(project_dir, _now_utc(), _log_watcher_error)

    # Operator commands: process pending approve/reject requests (gateway or retry)
    try:
        _poll_operator_commands(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "operator_commands", str(e))

    # Deadline check
    deadline_check = os.path.join(script_dir, "inbox-deadline-check.sh")
    if os.path.isfile(deadline_check) and os.access(deadline_check, os.X_OK):
        subprocess.run(
            ["bash", deadline_check, "--project", project_dir],
            check=False,
            capture_output=False,
        )

    # Heartbeat: write legacy timestamp + structured contract heartbeat
    _run_scripts_heartbeat(project_dir)

    # Fire on_watcher_tick hooks (e.g., auto-schedule module)
    try:
        from pathlib import Path
        from superharness.modules.runner import run_hooks

        run_hooks("on_watcher_tick", {"project_dir": project_dir}, Path(project_dir))
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: on_watcher_tick hook failed: {e}", file=sys.stderr)

    # Operator memory: check known failure patterns before retry/recovery
    try:
        if _should_run(project_dir, "operator_memory", cooldown=15):
            _check_operator_memory(project_dir)
            _learn_from_recovery(project_dir)
            if _watcher_cycle_count[0] % 20 == 0:
                _prune_operator_memory(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass  # never block the watcher cycle on memory errors

    # Behavioral profile: refresh + evaluate trials every 10 cycles (I5.2 + I6)
    if _watcher_cycle_count[0] % 10 == 0:
        try:
            from superharness.engine.behavioral import (
                refresh_behavioral_profile,
                evaluate_all_open_trials,
            )

            if refresh_behavioral_profile(project_dir):
                completed = evaluate_all_open_trials(project_dir)
                if completed:
                    print(f"profile: {completed} trial(s) completed")
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            pass

    # Agent memory: promote project→global every 20 cycles (Hermes adaptation)
    if _watcher_cycle_count[0] % 20 == 0:
        try:
            from superharness.engine.agent_memory import promote_all_project_memory

            promoted = promote_all_project_memory(project_dir)
            if promoted:
                print(f"memory: promoted {promoted} file(s) to global")
        except Exception as e:
            logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
            pass

    # Auto-retry failed inbox items that still have retries remaining
    try:
        if _should_run(project_dir, "auto_retry", cooldown=10):
            _auto_retry_failed(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Auto-fallback-owner: reassign exhausted tasks to profile-configured owner
    try:
        if _should_run(project_dir, "auto_fallback_owner", cooldown=15):
            _auto_fallback_owner_reassign(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_fallback_owner_reassign failed: {e}", file=sys.stderr)

    # Auto-recover exhausted failures: re-route to a different agent
    try:
        if _should_run(project_dir, "auto_recover", cooldown=15):
            _auto_recover_exhausted_failures_sqlite(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_recover_exhausted_failures failed: {e}", file=sys.stderr)

    # Auto-bootstrap: dispatch AC-proposal for tasks escalated with empty content
    try:
        if _should_run(
            project_dir, "auto_bootstrap", cooldown=30
        ) and not _circuit_breaker_tripped(project_dir):
            _auto_bootstrap_empty_tasks(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_bootstrap_empty_tasks failed: {e}", file=sys.stderr)

    # Auto-close report_ready tasks with tests_passed: true in their handoff
    try:
        if _should_run(project_dir, "auto_close_report_ready", cooldown=15):
            _auto_close_report_ready(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Auto-close review_requested tasks when a reviewer submits a verdict report
    try:
        if _should_run(project_dir, "auto_close_review_passed", cooldown=15):
            _auto_close_review_passed(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Advance orphaned discussion rounds (inbox done, verdicts never submitted)
    # to pending-review consensus so the operator can validate and close.
    try:
        _auto_advance_orphaned_rounds(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: discussion orphan-advance failed: {e}", file=sys.stderr)

    # Auto-close consensus discussions after grace period
    try:
        _auto_close_consensus_discussions(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: discussion auto-close failed: {e}", file=sys.stderr)

    # Sync cancelled/closed discussions back to contract task status + clean inbox
    try:
        if _should_run(project_dir, "reconcile_discussion_contract", cooldown=15):
            _reconcile_discussion_contract(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(
            f"Warning: discussion contract reconciliation failed: {e}", file=sys.stderr
        )

    # review_requested timeout is handled by reconcile_lifecycle (above, after dispatch reconciliation)

    # ship_on_complete guard: mark failed when report_ready has no PR URL
    try:
        if _should_run(project_dir, "check_ship_on_complete", cooldown=15):
            _check_ship_on_complete_tasks(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: _check_ship_on_complete_tasks failed: {e}", file=sys.stderr)

    # Auto-enqueue todo tasks for planning when auto_dispatch=True and autonomy=autonomous
    try:
        if _should_run(
            project_dir, "auto_enqueue_todo", cooldown=15
        ) and not _circuit_breaker_tripped(project_dir):
            auto_enqueue_todo(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_enqueue_todo failed: {e}", file=sys.stderr)

    # Auto peer-approve plan_proposed tasks: dispatch to a different max-tier agent for review
    try:
        if _should_run(
            project_dir, "auto_peer_approve", cooldown=30
        ) and not _circuit_breaker_tripped(project_dir):
            _auto_peer_approve_plans(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: peer_approve_plans failed: {e}", file=sys.stderr)

    # Auto-enqueue plan_approved tasks when auto_dispatch=True in profile.yaml
    try:
        if _should_run(
            project_dir, "auto_enqueue_approved", cooldown=15
        ) and not _circuit_breaker_tripped(project_dir):
            auto_enqueue_approved(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_enqueue_approved failed: {e}", file=sys.stderr)

    # Clean stale tasks with no handoff after timeout
    try:
        _auto_archive_stale_tasks(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: auto_archive_stale_tasks failed: {e}", file=sys.stderr)

    # Reconcile zombie inbox items (launched but process gone)
    try:
        _reconcile_zombies(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Analyze task logs for stuck agents
    try:
        _analyze_task_logs(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Transcript tailing for live dispatch progress (feature-flagged off by
    # default; docs/PLAN-adopt-omnigent.md iteration 7). No-op, no DB
    # connection opened, unless profile.yaml sets transcript_tail: true.
    try:
        _run_transcript_tail_if_enabled(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Reconcile paused dead-pid items — read from SQLite, write to SQLite
    try:
        from dataclasses import asdict
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn_paused = get_connection(project_dir)
        try:
            init_db(conn_paused)
            paused_items = [
                asdict(r)
                for r in inbox_dao.get_all(conn_paused, status="paused")
                if not r.run_id
            ]
            if _reconcile_paused_dead_pids(paused_items):
                for item in paused_items:
                    if isinstance(item, dict) and item.get("status") != "paused":
                        inbox_dao.update_status(
                            conn_paused,
                            item.get("id", ""),
                            from_status="paused",
                            to_status=item.get("status", "failed"),
                            now=_now_utc(),
                        )
                conn_paused.commit()
        finally:
            conn_paused.close()
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: paused dead-pid reconciliation failed: {e}", file=sys.stderr)

    # iter 7: review escalation — runs before lifecycle reconciler so chain
    # advancement takes priority over the simple revert behavior.
    try:
        from superharness.engine.review_escalation import escalate_stale_reviews

        escalate_stale_reviews(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: review escalation failed: {e}", file=sys.stderr)

    # Unified lifecycle reconciler (paused timeout, in_progress timeout, and
    # any review_requested without a review_chain that the escalation pass left)
    try:
        from superharness.engine.lifecycle_rules import reconcile_lifecycle

        reconcile_lifecycle(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Proactive session flush: save partial work before lifecycle timeout
    try:
        from superharness.engine.session_flush import check_expiring, flush_task

        expiring = check_expiring(project_dir)
        for task_id in expiring:
            flush_task(project_dir, task_id)
        if expiring:
            print(f"session-flush: flushed {len(expiring)} expiring task(s)")
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    # Inbox GC: reconcile stale items against contract
    try:
        _watcher_cycle_count[0] += 1
        _run_gc_if_due(project_dir, _watcher_cycle_count[0])
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: inbox gc failed: {e}", file=sys.stderr)

    # Auto-delete terminal stale inbox items (status='stale') — they're dead data
    try:
        _auto_delete_stale_inbox(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: stale inbox cleanup failed: {e}", file=sys.stderr)

    # Comprehensive GC — time-gated, runs at most once per minute
    try:
        gc_results = _comprehensive_gc(project_dir)
        if gc_results:
            _total = sum(gc_results.values())
            if _total > 0:
                _parts = [f"{k}={v}" for k, v in gc_results.items() if v > 0]
                print(f"GC: {' '.join(_parts)}", file=sys.stderr)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: comprehensive gc failed: {e}", file=sys.stderr)

    # Reinforcement loop — learn from history and auto-adjust behavior.
    # Runs every 5 minutes (cooldown-gated). Reads trace, failures, and
    # discussions to detect patterns and take corrective action.
    try:
        if _should_run(project_dir, "reinforce", cooldown=300):
            _reinforce_loop(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: reinforce loop failed: {e}", file=sys.stderr)

    # Cancel pending items for agents without dispatch scripts (will never dispatch)
    try:
        _cancel_undispatchable_agents(project_dir)
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        print(f"Warning: undispatchable agent cleanup failed: {e}", file=sys.stderr)

    # Dispatch — check budget before launching agents
    targets = _watcher_targets(target)

    # Budget gate: skip dispatch if daily budget is exceeded (strict mode)
    try:
        from superharness.engine.model_budget import check_budget, BudgetStatus

        budget = check_budget(project_dir)
        if budget.status == BudgetStatus.BLOCK:
            print(
                f"budget-gate: BLOCKED — daily budget exceeded (${budget.used_today:.2f} / ${budget.daily_limit:.2f}). Skipping dispatch."
            )
            return
        elif budget.status == BudgetStatus.WARN:
            print(f"budget-gate: WARN — {budget.message}")
    except Exception as e:
        logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
        _log_watcher_error(project_dir, "watcher", str(e))

    for t in targets:
        # Per-agent budget check: skip agents over their limit
        try:
            from superharness.engine.model_budget import (
                check_agent_budget,
                BudgetStatus,
            )

            agent_budget = check_agent_budget(project_dir, t)
            if agent_budget.status == BudgetStatus.BLOCK:
                print(
                    f"budget-gate: skipping {t} — per-agent budget exceeded (${agent_budget.used_today:.2f} / ${agent_budget.daily_limit:.2f})"
                )
                continue
            elif agent_budget.status == BudgetStatus.WARN:
                print(
                    f"budget-gate: {t} WARN — ${agent_budget.used_today:.2f} / ${agent_budget.daily_limit:.2f}"
                )
        except Exception as e:
            logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
            _log_watcher_error(project_dir, "watcher", str(e))

        # Loop detection: stateful warn→block using LoopGuard
        try:
            from superharness.engine.loop_detector import detect_loop, LoopGuard

            sh_dir = os.path.join(project_dir, ".superharness")
            guard = LoopGuard(state_dir=sh_dir)
            log_dir = os.path.join(sh_dir, "launcher-logs")
            _loop_action = "allow"
            if os.path.isdir(log_dir):
                for lf in sorted(os.listdir(log_dir), reverse=True):
                    if lf.endswith(".log") and t in lf:
                        loop = detect_loop(os.path.join(log_dir, lf))
                        decision = guard.check(t, loop)
                        _loop_action = decision["action"]
                        if _loop_action == "warn":
                            print(
                                f"loop-guard: WARN {t} — {decision['reason']} (pattern: {loop['pattern']})"
                            )
                        elif _loop_action == "block":
                            from superharness.engine.policy_gate import (
                                check_agent_policy,
                            )

                            check_agent_policy(t, loop_detected=True)
                            print(
                                f"loop-guard: BLOCKED {t} — {decision['reason']} (pattern: {loop['pattern']})"
                            )
                        break
            if _loop_action == "block":
                continue
        except Exception as e:
            logger.warning("_run_scripts: unexpected error: %s", e, exc_info=True)
            _log_watcher_error(project_dir, "watcher", str(e))

        _run_dispatch_cmd(
            project_dir=project_dir,
            target=t,
            print_only=print_only,
            non_interactive=non_interactive,
            codex_bypass=codex_bypass,
            launcher_timeout=launcher_timeout,
        )

    # Discussion dispatch — call Python module directly (shell script no longer used)
    try:
        from superharness.commands import discussion_dispatch as _dd

        _dd.dispatch(project_dir)
    except Exception as _exc:
        import logging as _logging

        _logging.getLogger(__name__).warning(
            "inbox_watch: discussion_dispatch raised an exception: %s",
            _exc,
            exc_info=True,
        )


_TASK_LOG_STALE_MINUTES = 15  # mark as failed if no activity for this long


# ---------------------------------------------------------------------------
# Operator memory check
# ---------------------------------------------------------------------------


def _check_operator_memory(project_dir: str) -> None:
    """Scan inbox items with failed_reason against operator_memory.

    If a known pattern matches with high confidence, log the fix hint.
    This runs before auto-retry so the watcher can surface known solutions.
    """
    from superharness.utils.paths import resolve_active_state_db_path

    db_path = resolve_active_state_db_path(project_dir)
    if not os.path.isfile(db_path):
        return

    from superharness.engine.operator_memory import OperatorMemory
    from superharness.engine.failure_patterns import match_patterns

    om = OperatorMemory(db_path)
    om.ensure_table()

    # Read inbox items (SQLite)
    try:
        from superharness.engine.state_reader import get_inbox_items

        items = get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return

    for item in items:
        if not isinstance(item, dict):
            continue
        failed_reason = str(item.get("failed_reason", "") or "")
        if not failed_reason:
            continue

        # Match against failure pattern library, plus the unknown:<hash>
        # signature derived from the failed_reason text. The unknown
        # signature lets memory learn about repeated environmental
        # failures (missing dirs/CLIs) the regex library doesn't catch.
        from superharness.engine.failure_patterns import unknown_signature

        matched = match_patterns(failed_reason)
        signatures = [p.id for p in matched]
        if not signatures:
            signatures = [unknown_signature(failed_reason)]

        for sig in signatures:
            mem = om.find_pattern(sig)
            if mem is None:
                continue
            if mem["confidence"] >= 0.7:
                print(
                    f"operator-memory: known fix for '{sig}' "
                    f"(confidence={mem['confidence']:.2f}) -> {mem['resolution'][:120]}"
                )

            # Record a miss for every auto-retry -- confidence will drop
            # if the fix keeps failing. The watcher records hits after
            # successful recovery.
            om.record_match(sig, success=False)


def _learn_from_recovery(project_dir: str) -> None:
    """Record hits for failure patterns whose tasks have since recovered.

    Scans the failures table for tasks that were previously failed but
    are now done. Records a hit for each pattern, raising confidence.
    """
    from superharness.utils.paths import resolve_active_state_db_path

    db_path = resolve_active_state_db_path(project_dir)
    if not os.path.isfile(db_path):
        return

    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import failures_dao
        from superharness.engine import tasks_dao
        from superharness.engine.operator_memory import OperatorMemory

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            rows = failures_dao.get_recent(conn, limit=9999)
            om = OperatorMemory(db_path)
            om.ensure_table()
            recorded = set()

            for row in rows:
                task_id = row.task_id
                if not task_id or task_id in recorded:
                    continue
                task = tasks_dao.get(conn, task_id)
                if task is None:
                    continue
                if getattr(task, "status", None) != "done":
                    continue

                patterns = (row.pattern or "").split(",")
                # Build candidate signatures: matched pattern ids + the
                # unknown:<hash> signature derived from the error_snippet.
                candidates: list[str] = []
                for pid in patterns:
                    pid = pid.strip()
                    if not pid or pid == "unknown":
                        continue
                    candidates.append(pid)
                if (not candidates) and getattr(row, "error_snippet", None):
                    from superharness.engine.failure_patterns import unknown_signature

                    candidates.append(unknown_signature(row.error_snippet))

                for sig in candidates:
                    mem = om.find_pattern(sig)
                    if mem is not None:
                        om.record_match(sig, success=True)
                        recorded.add(task_id)

            if recorded:
                print(
                    f"operator-memory: learned from {len(recorded)} recovered task(s)"
                )
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _prune_operator_memory(project_dir: str) -> None:
    """Remove low-confidence patterns from operator memory."""
    from superharness.utils.paths import resolve_active_state_db_path

    db_path = resolve_active_state_db_path(project_dir)
    if not os.path.isfile(db_path):
        return

    try:
        from superharness.engine.operator_memory import OperatorMemory

        om = OperatorMemory(db_path)
        om.ensure_table()
        removed = om.prune_stale(threshold=0.3)
        if removed:
            print(f"operator-memory: pruned {removed} low-confidence pattern(s)")
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _analyze_task_logs(project_dir: str) -> None:
    """Check launched task logs for activity. Stale tasks get marked failed."""
    import glob
    from dataclasses import asdict
    from datetime import datetime, timezone
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import inbox_dao

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        launched = inbox_dao.get_all(conn, status="launched")
        if not launched:
            return

        now = datetime.now(timezone.utc)
        launcher_logs = os.path.join(project_dir, ".superharness", "launcher-logs")
        escalated = 0

        for item in launched:
            if item.run_id and item.run_id.strip():
                continue
            d = asdict(item)
            launched_at = d.get("launched_at", "")
            if not launched_at:
                continue
            try:
                lt = datetime.fromisoformat(launched_at.replace("Z", "+00:00"))
                age_minutes = (now - lt).total_seconds() / 60
            except (ValueError, TypeError):
                continue

            if age_minutes < _TASK_LOG_STALE_MINUTES:
                continue  # not stale yet

            # Find log file for this task
            task_id = item.task_id.replace("/", "_")
            agent = item.target_agent
            log_pattern = os.path.join(launcher_logs, f"*{task_id}*{agent}*.log")

            logs = sorted(glob.glob(log_pattern), key=os.path.getmtime, reverse=True)
            if not logs:
                # No log file at all → likely never started
                inbox_dao.update_status(
                    conn,
                    item.id,
                    from_status="launched",
                    to_status="failed",
                    now=_now_utc(),
                )
                print(
                    f"log-analyzer: '{item.task_id}' → failed (no log file after {int(age_minutes)}m)"
                )
                escalated += 1
                continue

            # Check latest log for activity
            latest_log = logs[0]

            # ── Tool-loop guardrail (Hermes adaptation) ──────────────────
            # Before escalating to failed, check if the agent is stuck in a
            # tool-call loop. If detected, block the task instead of just
            # failing the inbox item — prevents infinite retry loops.
            try:
                from superharness.engine.loop_detector import detect_loop, LoopGuard

                loop_result = detect_loop(latest_log)
                if loop_result.get("loop_detected"):
                    guard = LoopGuard(os.path.join(project_dir, ".superharness"))
                    action = guard.check(item.task_id, loop_result)
                    if action["action"] == "block":
                        # Block the task (not just the inbox item)
                        from superharness.engine.state_writer import set_task_status

                        block_reason = action.get(
                            "reason", loop_result.get("reason", "tool-loop detected")
                        )
                        set_task_status(
                            project_dir,
                            item.task_id,
                            "blocked",
                            force=True,
                            failed_reason=block_reason,
                        )
                        inbox_dao.update_status(
                            conn,
                            item.id,
                            from_status="launched",
                            to_status="failed",
                            now=_now_utc(),
                            reason=f"blocked: {block_reason}",
                        )
                        # Record to agent memory so future dispatches learn
                        try:
                            from superharness.engine.agent_memory import append

                            pattern = loop_result.get("pattern", "unknown")
                            append(
                                project_dir,
                                "pitfalls.md",
                                f"Tool loop detected: {pattern} — {block_reason}. "
                                "Avoid repeating the same tool call without progress.",
                            )
                        except Exception as e:
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                            pass
                        from superharness.engine.ledger_dao import (
                            record as _ledger_record2,
                        )

                        _ledger_record2(
                            conn,
                            task_id=item.task_id,
                            agent="watcher",
                            action="block_loop",
                            details={
                                "pattern": loop_result.get("pattern"),
                                "reason": block_reason,
                                "count": loop_result.get("count"),
                            },
                            now=_now_utc(),
                        )
                        print(
                            f"log-analyzer: '{item.task_id}' → BLOCKED (tool-loop: "
                            f"{loop_result.get('pattern')} — {block_reason})"
                        )
                        escalated += 1
                        continue
                    elif action["action"] == "warn":
                        print(
                            f"log-analyzer: '{item.task_id}' — tool-loop WARNING "
                            f"({loop_result.get('pattern')}, cycle {action.get('reason', '')})"
                        )
            except Exception as e:
                logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                pass
            # ── End tool-loop guardrail ─────────────────────────────────

            try:
                log_mtime = datetime.fromtimestamp(
                    os.path.getmtime(latest_log), tz=timezone.utc
                )
                inactive_minutes = (now - log_mtime).total_seconds() / 60
            except Exception as e:
                logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                inactive_minutes = 0

            # Check for git changes as activity signal
            has_activity = False
            try:
                import subprocess

                r = subprocess.run(
                    ["git", "diff", "--stat", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=5,
                    cwd=project_dir,
                )
                if r.stdout.strip():
                    has_activity = True
            except Exception as e:
                logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
                pass
            if has_activity:
                print(
                    f"log-analyzer: '{item.task_id}' active (files changing, {int(age_minutes)}m elapsed)"
                )
                continue

            if inactive_minutes >= _TASK_LOG_STALE_MINUTES:
                inbox_dao.update_status(
                    conn,
                    item.id,
                    from_status="launched",
                    to_status="failed",
                    now=_now_utc(),
                )
                print(
                    f"log-analyzer: '{item.task_id}' → failed (no log activity for {int(inactive_minutes)}m, {int(age_minutes)}m total)"
                )
                escalated += 1

        if escalated:
            print(f"log-analyzer: {escalated} stale task(s) marked failed")
            conn.commit()
    finally:
        conn.close()


def _claude_transcript_dir(project_dir: str) -> str:
    """Resolve the Claude Code session-transcript directory for project_dir.

    Assumption (unverified against a live directory — SIDE-EFFECT FENCE
    forbids reading ~/.claude/projects/ from this codebase): Claude Code
    mirrors `~/.claude/projects/<project-path-with-/-replaced-by-->/`
    (leading path separator becomes a leading dash). If this convention is
    wrong, select_transcript() simply finds no matching directory and
    _run_transcript_tail_if_enabled() no-ops — it never raises either way.
    """
    munged = os.path.realpath(project_dir).replace(os.sep, "-")
    return os.path.join(os.path.expanduser("~/.claude/projects"), munged)


def _run_transcript_tail_if_enabled(project_dir: str) -> None:
    """Tail each launched dispatch's Claude Code transcript by persisted
    byte offset, emitting telemetry events (engine/transcript_tail.py).

    Feature-flagged OFF by default via profile.yaml `transcript_tail: true`
    (docs/PLAN-adopt-omnigent.md iteration 7 default; iteration 8 flips the
    project default on). When disabled/unset, this is a true no-op — no DB
    connection is even opened — so existing watcher behavior and tests are
    byte-identical. Never raises.
    """
    try:
        profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
        enabled = False
        if os.path.isfile(profile_file):
            import yaml as _yaml

            with open(profile_file) as f:
                profile = _yaml.safe_load(f) or {}
            enabled = bool(profile.get("transcript_tail", False))
        if not enabled:
            return

        from dataclasses import asdict

        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao
        from superharness.engine.transcript_tail import select_transcript, tail_step

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            launched = inbox_dao.get_all(conn, status="launched")
            transcript_dir = _claude_transcript_dir(project_dir)
            for item in launched:
                d = asdict(item)
                dispatch_id = d.get("task_id") or d.get("id")
                launched_at = d.get("launched_at") or ""
                if not dispatch_id or not launched_at:
                    continue
                transcript = select_transcript(transcript_dir, launched_at)
                if transcript is None:
                    continue
                tail_step(conn, dispatch_id, transcript)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass


def _reconcile_zombies(project_dir: str, max_age_seconds: int = 300) -> int:
    """Reconcile launched inbox items that have no running process.

    Checks in order:
    1. Contract says done → mark inbox done
    2. PID set but process dead → mark inbox failed
    2b. PID alive + plan-only + age > 15 min → kill and fail
    2c. PID alive + non-plan-only + age > 2 hours → kill and fail
    3. No PID + launched > max_age_seconds ago → mark inbox failed

    Returns count of reconciled items.
    """
    os.path.join(project_dir, ".superharness")

    # Read from SQLite via state_reader
    items = []
    try:
        from superharness.engine.state_reader import get_inbox_items

        items = get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    if not isinstance(items, list) or not items:
        return 0

    contract_statuses: dict[str, str] = {}
    try:
        from superharness.engine.state_reader import get_tasks

        for t in get_tasks(project_dir):
            if isinstance(t, dict) and t.get("id"):
                contract_statuses[str(t["id"])] = str(t.get("status", ""))
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    now = datetime.now(timezone.utc)
    reconciled = 0
    changed = False

    for item in items:
        if not isinstance(item, dict) or item.get("status") != "launched":
            continue

        task_id = str(item.get("task", ""))
        item_id = str(item.get("id", ""))
        if task_id in reliable_task_ids(project_dir):
            continue
        pid = item.get("pid", "")
        launched_at = str(item.get("launched_at", ""))

        # Check 1: contract says done → mark inbox done + kill lingering process
        if contract_statuses.get(task_id) == "done":
            if pid:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    print(f"zombie-reconcile: killed lingering pid {pid} for {task_id}")
                except (ProcessLookupError, ValueError, PermissionError):
                    pass
            item["status"] = "done"
            item["completed_at"] = _now_utc()
            item["pid"] = ""
            reconciled += 1
            changed = True
            print(f"zombie-reconcile: {item_id} ({task_id}) → done (contract done)")
            continue

        # Check 2: PID set but process dead → mark failed
        if pid:
            try:
                pid_int = int(pid)
            except ValueError:
                pid_int = None
            if not _pid_is_running(pid_int):
                item["status"] = "failed"
                item["failed_at"] = _now_utc()
                item["pid"] = ""
                reconciled += 1
                changed = True
                print(
                    f"zombie-reconcile: {item_id} ({task_id}) → failed (pid {pid} dead)"
                )
            # Check 2b: plan-only task alive but timed out → kill and fail
            elif item.get("plan_only") and launched_at:
                _PLAN_ONLY_TIMEOUT = 900  # 15 minutes
                try:
                    lt = datetime.fromisoformat(launched_at.replace("Z", "+00:00"))
                    age = (now - lt).total_seconds()
                    if age > _PLAN_ONLY_TIMEOUT:
                        try:
                            os.kill(pid_int, signal.SIGTERM)
                        except OSError as e:
                            # os.kill only ever raises OSError (typically
                            # ProcessLookupError — the pid is already gone —
                            # or PermissionError). Anything else propagates
                            # instead of being silently treated as "already
                            # dead", per CONTRIBUTING.md's exception policy.
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                        item["status"] = "failed"
                        item["failed_at"] = _now_utc()
                        item["pid"] = ""
                        item["failed_reason"] = (
                            f"plan-only timeout ({int(age)}s > {_PLAN_ONLY_TIMEOUT}s)"
                        )
                        reconciled += 1
                        changed = True
                        print(
                            f"zombie-reconcile: {item_id} ({task_id}) → failed (plan-only timeout, {int(age)}s)"
                        )
                except (ValueError, TypeError):
                    pass
            # Check 2c: non-plan-only task alive but running beyond max wall-clock cap → kill and fail
            elif launched_at:
                _MAX_LAUNCH_AGE_SECONDS = 7200  # 2 hours
                try:
                    lt = datetime.fromisoformat(launched_at.replace("Z", "+00:00"))
                    age = (now - lt).total_seconds()
                    if age > _MAX_LAUNCH_AGE_SECONDS:
                        try:
                            os.kill(pid_int, signal.SIGTERM)
                        except OSError as e:
                            # See the plan-only-timeout branch above: os.kill
                            # only raises OSError; anything else propagates.
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                        item["status"] = "failed"
                        item["failed_at"] = _now_utc()
                        item["pid"] = ""
                        item["failed_reason"] = (
                            f"max launch age exceeded ({int(age)}s > {_MAX_LAUNCH_AGE_SECONDS}s)"
                        )
                        reconciled += 1
                        changed = True
                        print(
                            f"zombie-reconcile: {item_id} ({task_id}) → failed (max age, {int(age)}s)"
                        )
                except (ValueError, TypeError):
                    pass
            continue

        # Check 3: no PID + old → mark failed
        if launched_at:
            try:
                lt = datetime.fromisoformat(launched_at.replace("Z", "+00:00"))
                age = (now - lt).total_seconds()
                if age > max_age_seconds:
                    item["status"] = "failed"
                    item["failed_at"] = _now_utc()
                    reconciled += 1
                    changed = True
                    print(
                        f"zombie-reconcile: {item_id} ({task_id}) → failed (no pid, {int(age)}s old)"
                    )
            except (ValueError, TypeError):
                pass

    if changed:
        # Write changes directly to SQLite (post-migration)
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            for item in items:
                if isinstance(item, dict):
                    item_id = str(item.get("id", ""))
                    new_status = item.get("status", "")
                    if item_id and new_status:
                        try:
                            row = inbox_dao.get(conn, item_id)
                            if row:
                                inbox_dao.update_status(
                                    conn,
                                    item_id,
                                    from_status="launched",
                                    to_status=new_status,
                                    now=_now_utc(),
                                )
                        except Exception as e:
                            logger.warning(
                                "inbox_watch unexpected error: %s", e, exc_info=True
                            )
                            # Fallback: direct DAO single-field update
                            inbox_dao.set_field(conn, item_id, "status", new_status)
            conn.commit()
        finally:
            conn.close()

    return reconciled


def _reconcile_discussion_contract(project_dir: str) -> int:
    """Find in_progress tasks whose linked discussion is terminal.

    When a discussion is cancelled or closed via CLI (not dashboard), the contract
    task linked to it stays in_progress. This reconciler catches that gap.
    Reads discussion state from SQLite (post-YAML removal).
    Returns count of tasks updated.
    """
    from superharness.engine import state_reader, state_writer
    from superharness.engine.db import get_connection, init_db

    terminal = (
        "cancelled",
        "closed",
        "consensus",
        "deadlock",
        "failed",
        "failed_participant",
    )

    # Collect terminal discussion IDs from SQLite. Build placeholders
    # dynamically so adding/removing terminal statuses can't desync from
    # the literal "?, ?, ?" count (the previous version had 5 placeholders
    # for 6 values, silently failing inside the bare except).
    terminal_disc_ids: set[str] = set()
    conn = get_connection(project_dir)
    try:
        init_db(conn)
        placeholders = ",".join("?" * len(terminal))
        rows = conn.execute(
            f"SELECT id FROM discussions WHERE status IN ({placeholders})",
            terminal,
        ).fetchall()
        terminal_disc_ids = {r["id"] for r in rows}
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0
    finally:
        conn.close()

    if not terminal_disc_ids:
        return 0

    try:
        tasks = state_reader.get_tasks(project_dir)
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return 0

    updated = 0
    for task in tasks:
        if not isinstance(task, dict) or task.get("status") != "in_progress":
            continue
        tid = str(task.get("id", ""))
        for disc_id in terminal_disc_ids:
            if tid.startswith(disc_id + "/") or tid == disc_id:
                if state_writer.set_task_status(
                    project_dir, tid, "archived", from_status="in_progress", force=True
                ):
                    updated += 1
                    print(
                        f"discussion-reconcile: {tid} → archived (discussion {disc_id} is terminal)"
                    )
                break

    # Always clean up inbox items for terminal discussions (even if tasks already archived)
    if terminal_disc_ids:
        try:
            from superharness.engine.state_reader import get_inbox_items

            inbox = get_inbox_items(project_dir)
            inbox_cleaned = 0
            for item in inbox:
                if not isinstance(item, dict):
                    continue
                iid = str(item.get("id", ""))
                itask = str(item.get("task", item.get("task_id", "")))
                istatus = str(item.get("status", ""))
                if istatus not in ("pending", "launched", "running", "paused"):
                    continue
                for disc_id in terminal_disc_ids:
                    if itask.startswith(disc_id + "/") or itask == disc_id:
                        state_writer.set_inbox_status(project_dir, iid, "done")
                        inbox_cleaned += 1
                        break
            if inbox_cleaned > 0:
                print(
                    f"discussion-reconcile: cleaned {inbox_cleaned} inbox item(s) for terminal discussions"
                )
        except Exception as e:
            logger.warning(
                "_reconcile_discussion_contract: unexpected error: %s", e, exc_info=True
            )
            print(f"discussion-reconcile: inbox cleanup failed: {e}", file=sys.stderr)

    return updated


_CONSENSUS_GRACE_MINUTES = 60  # auto-close consensus discussions after 1h
_ORPHAN_ROUND_GRACE_MINUTES = (
    5  # advance orphaned rounds (inbox done, no verdicts) after 5m
)
_CONSENSUS_PENDING_REVIEW_PREFIX = "auto-pending-review:"
_CIRCUIT_BREAKER_THRESHOLD = 20  # consecutive failures in 5 minutes trips breaker

import re as _re  # noqa: E402

import logging  # noqa: E402

logger = logging.getLogger(__name__)
_ZERO_VERDICT_RE = _re.compile(r"\b0/\d+\b")


def _consensus_has_zero_verdicts(consensus_msg: str) -> bool:
    """Return True when the pending-review message indicates zero verdicts (0/N)."""
    return bool(_ZERO_VERDICT_RE.search(consensus_msg))


_CIRCUIT_BREAKER_WINDOW_MINUTES = 5

# === Auto-action cooldown tracking ===
# Each auto_* function has a minimum interval between runs.
# Prevents cascading loops when action A triggers action B.
#
# Persisted per (project, action) in SQLite (watcher_cooldowns, migration
# v29), not process memory. The watcher is by design a fresh Python process
# every tick (one-shot, respawned by the operator — see watch(): `if once
# or not foreground: _run_scripts(...)`). An in-memory dict is empty on
# every call from every process, so every cooldown gate silently no-oped on
# every tick regardless of the argument passed in. Confirmed live
# 2026-07-11: reinforce's claimed 300s cooldown fired on every ~5-7s tick,
# driving a full 181 MB trace.jsonl re-parse per tick instead of per 5
# minutes. time.time() (wall clock), not time.monotonic() — monotonic's
# epoch is arbitrary per-process and is not comparable across processes.
_AUTO_DEFAULT_COOLDOWN = 10  # seconds — most auto-actions need at least this


def _should_run(project_dir: str, action: str, cooldown: int = 0) -> bool:
    """Return True if enough time has passed since last run of this action
    for this project. On any DB error, returns False (skip this tick) rather
    than True — a auto-action that misses a tick is recoverable; one that
    runs unthrottled because its gate broke is the exact bug this exists to
    prevent."""
    import time as _time
    from superharness.engine.db import get_connection, init_db

    now = _time.time()
    threshold = cooldown or _AUTO_DEFAULT_COOLDOWN
    try:
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            row = conn.execute(
                "SELECT last_run_epoch FROM watcher_cooldowns WHERE action = ?",
                (action,),
            ).fetchone()
            last = row[0] if row else 0.0
            if now - last < threshold:
                return False
            conn.execute(
                "INSERT INTO watcher_cooldowns (action, last_run_epoch) VALUES (?, ?) "
                "ON CONFLICT(action) DO UPDATE SET last_run_epoch = excluded.last_run_epoch",
                (action, now),
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception:
        logger.warning(
            "_should_run(%s) cooldown check failed; skipping this tick",
            action,
            exc_info=True,
        )
        return False


_ACTIVE_DISCUSSION_TIMEOUT_HOURS = 24  # auto-close active discussions after 24h


def _circuit_breaker_tripped(project_dir: str) -> bool:
    try:
        from superharness.engine.db import get_connection, init_db, now_iso

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            now = now_iso()
            count = conn.execute(
                "SELECT COUNT(*) FROM inbox WHERE status='failed' AND failed_at > datetime(?, ? || ' minutes')",
                (now, f"-{_CIRCUIT_BREAKER_WINDOW_MINUTES}"),
            ).fetchone()[0]
            if count >= _CIRCUIT_BREAKER_THRESHOLD:
                print(
                    f"circuit-breaker: TRIPPED — {count} failures in {_CIRCUIT_BREAKER_WINDOW_MINUTES}min"
                )
                return True
            return False
        finally:
            conn.close()
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        return False


def _auto_advance_orphaned_rounds(project_dir: str) -> int:
    """Detect active discussions whose round-N inbox items all finished but no
    verdicts were submitted, and advance them to a 'pending review' consensus
    state so the operator gets a clear validation signal.

    This closes a real gap: an agent that completes its dispatched round task
    without calling `shux discuss submit` leaves the discussion stuck in
    'active' forever, because the verdict-driven auto-consensus path inside
    cmd_submit never runs. The reconciler treats unanimous inbox completion
    past a short grace period as a valid signal that the round is done, and
    surfaces it for explicit operator close (not silent auto-close).

    Returns count of discussions advanced.
    """
    from datetime import datetime, timezone, timedelta
    import re
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import discussions_dao

    now = datetime.now(timezone.utc)
    cutoff_iso = (now - timedelta(minutes=_ORPHAN_ROUND_GRACE_MINUTES)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    advanced = 0
    round_re = re.compile(r"/round-(\d+)$")

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        active = discussions_dao.get_all(conn, status="active")
        if not active:
            return 0

        for disc in active:
            owners = list(disc.owners or [])
            if not owners:
                continue

            # Find the highest round number with inbox activity for this discussion
            inbox_rows = conn.execute(
                """
                SELECT task_id, target_agent, status, done_at
                FROM inbox
                WHERE task_id LIKE ?
                """,
                (f"{disc.id}/round-%",),
            ).fetchall()
            if not inbox_rows:
                continue

            by_round: dict[int, list] = {}
            for r in inbox_rows:
                m = round_re.search(r["task_id"] or "")
                if not m:
                    continue
                by_round.setdefault(int(m.group(1)), []).append(r)
            if not by_round:
                continue

            round_n = max(by_round.keys())
            round_items = by_round[round_n]

            # Only agents who were actually dispatched via inbox need to be done.
            # Human participants (like "owner") never receive inbox items, so they
            # must not block orphan detection.
            dispatched = {r["target_agent"] for r in round_items}
            if not dispatched:
                continue
            agents_done = {
                r["target_agent"] for r in round_items if r["status"] == "done"
            }
            if not dispatched.issubset(agents_done):
                continue

            # Latest done_at across this round must be older than the grace window
            latest_done = max(
                (r["done_at"] for r in round_items if r["done_at"]),
                default=None,
            )
            if not latest_done or latest_done > cutoff_iso:
                continue

            # Register any YAML-only submissions into SQLite before checking
            # verdict coverage. Agents write their YAML file as their primary
            # output; getting it into SQLite is the harness's responsibility.
            # Without this, discussions whose agents wrote YAMLs but whose
            # DB rows are absent stay stuck forever (cmd_advance checks SQLite
            # only and exits "Round N is not complete").
            disc_dir = os.path.join(
                project_dir, ".superharness", "discussions", disc.id
            )
            _now_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            registered_any = False
            for agent in dispatched:
                if discussions_dao.register_yaml_submission(
                    conn, disc.id, round_n, agent, disc_dir, _now_ts
                ):
                    registered_any = True

            # Re-read verdict_agents from SQLite (now includes just-registered rows).
            verdict_agents = {
                rr.agent
                for rr in discussions_dao.get_rounds(conn, disc.id)
                if rr.round_number == round_n
            }
            if registered_any:
                conn.commit()

            if dispatched.issubset(verdict_agents):
                continue

            missing = [a for a in dispatched if a not in verdict_agents]

            # Compute the required number of submitted verdicts for this round.
            # Same formula as _gc_discussion_deadlock: n≤2 = all must submit,
            # n≥3 = n-1 suffices. (Fix: BUGREPORT-discussion-consensus-single-participant, root cause #1.)
            n_dispatched = len(dispatched)
            required = max(2, n_dispatched - 1) if n_dispatched > 1 else 2

            # Zero verdicts OR fewer than required submissions means participants
            # didn't engage meaningfully. This is a participant failure, not a
            # consensus — mark failed_participant so it does not masquerade.
            if not verdict_agents or len(verdict_agents) < min(required, n_dispatched):
                conn.execute(
                    "UPDATE discussions SET status='failed_participant' "
                    "WHERE id=? AND status='active'",
                    (disc.id,),
                )
                advanced += 1
                reason = (
                    "0 verdicts"
                    if not verdict_agents
                    else f"{len(verdict_agents)}/{n_dispatched} verdicts (need {required})"
                )
                print(
                    f"discussion-orphan-advance: {disc.id} → failed_participant "
                    f"(round {round_n} inbox complete, {reason})",
                    file=sys.stderr,
                )
                continue

            # Partial: some dispatched agents submitted, others didn't — surface for review.
            n_dispatched = len(dispatched)
            consensus_msg = (
                f"{_CONSENSUS_PENDING_REVIEW_PREFIX} round {round_n} inbox complete "
                f"({n_dispatched - len(missing)}/{n_dispatched} verdicts) — "
                f"operator review required"
            )
            conn.execute(
                "UPDATE discussions SET status='consensus', consensus=? "
                "WHERE id=? AND status='active'",
                (consensus_msg, disc.id),
            )
            advanced += 1
            print(
                f"discussion-orphan-advance: {disc.id} → consensus (pending review, "
                f"round {round_n}, missing verdicts: {missing})",
                file=sys.stderr,
            )

        if advanced:
            conn.commit()
    except Exception as e:
        logger.warning(
            "_auto_advance_orphaned_rounds: unexpected error: %s", e, exc_info=True
        )
        print(f"discussion-orphan-advance: error: {e}", file=sys.stderr)
    finally:
        conn.close()

    return advanced


def _auto_close_consensus_discussions(project_dir: str) -> int:
    """Close discussions that have been in consensus state beyond the grace period.

    Reads discussion state from SQLite (post-YAML removal).
    Returns count of discussions closed.
    """
    from datetime import datetime, timezone
    from superharness.engine.db import get_connection, init_db
    from superharness.engine import discussions_dao

    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    closed = 0

    conn = get_connection(project_dir)
    try:
        init_db(conn)
        rows = discussions_dao.get_all(conn, status="consensus")
        if not rows:
            return 0

        for row in rows:
            disc_id = row.id

            # Skip rows flagged for explicit operator review — these were auto-advanced
            # because the round's inbox completed with partial verdicts submitted.
            # Closing them silently would lose the validation step the operator wants.
            # Exception: zero verdicts (0/N) means agents didn't engage at all —
            # no human judgment is needed, auto-close as failed_participant instead.
            if (row.consensus or "").startswith(_CONSENSUS_PENDING_REVIEW_PREFIX):
                if not _consensus_has_zero_verdicts(row.consensus or ""):
                    continue
                # Zero-verdict pending-review: downgrade to failed_participant and close.
                conn.execute(
                    "UPDATE discussions SET status='failed_participant' WHERE id=?",
                    (disc_id,),
                )
                conn.commit()
                closed += 1
                print(
                    f"auto-close: {disc_id} → failed_participant "
                    f"(pending-review with 0 verdicts — no engagement)",
                    file=sys.stderr,
                )
                continue

            # If closed_at is already stamped but status wasn't updated, close immediately
            # (repairs SQLite-only drift where close() set closed_at but status stayed consensus)
            age_min: int | None = None
            if row.closed_at:
                pass  # fall through to close below
            else:
                # Use created_at for age (no consensus_at column in current schema)
                created = row.created_at
                if created:
                    try:
                        t = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                        age_min = int((now - t).total_seconds() / 60)
                    except (ValueError, TypeError):
                        age_min = 99999
                else:
                    age_min = 99999
                if age_min < _CONSENSUS_GRACE_MINUTES:
                    continue

            discussions_dao.close(
                conn,
                disc_id,
                consensus=row.consensus or "consensus",
                now=now_str,
            )
            closed += 1
            topic = (row.topic or "")[:60]
            _age_str = f"{age_min}m" if age_min is not None else "closed_at already set"
            print(f"discussion-auto-close: {disc_id} → closed ({_age_str}) — {topic}")

        conn.commit()
    except Exception as e:
        logger.warning(
            "_auto_close_consensus_discussions: unexpected error: %s", e, exc_info=True
        )
        print(f"discussion-auto-close: error: {e}", file=sys.stderr)
    finally:
        conn.close()

    return closed


_STALE_INBOX_DELETE_AGE_HOURS = (
    1  # delete items marked stale after 1h (they've had time for review)
)


def _auto_delete_stale_inbox(project_dir: str) -> int:
    """Delete stale inbox items older than _STALE_INBOX_DELETE_AGE_HOURS.

    Stale items are dead data — they've been marked as terminal by previous
    cleanup passes and serve no further purpose in the inbox. Deleting them
    keeps the inbox lean and prevents drift between status counts and actual
    issues.

    Returns count of items deleted.
    """
    from datetime import datetime, timezone, timedelta
    from superharness.engine.db import get_connection, init_db

    try:
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(hours=_STALE_INBOX_DELETE_AGE_HOURS)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            # Delete stale items where the last update was before the cutoff
            cursor = conn.execute(
                "DELETE FROM inbox WHERE status='stale' AND (failed_at IS NULL OR failed_at < ?)",
                (cutoff,),
            )
            deleted = cursor.rowcount or 0
            if deleted > 0:
                conn.commit()
                print(f"auto-delete-stale: removed {deleted} stale inbox item(s)")
            return deleted
        finally:
            conn.close()
    except Exception as e:
        logger.warning(
            "_auto_delete_stale_inbox: unexpected error: %s", e, exc_info=True
        )
        print(f"auto-delete-stale: failed: {e}", file=sys.stderr)
        return 0


# ---------------------------------------------------------------------------
# Reinforcement loop — learn from history and auto-adjust behavior
# ---------------------------------------------------------------------------

_REINFORCE_WINDOW_MINUTES = 30  # scan last 30 min of failures
_REINFORCE_FAILURE_THRESHOLD = 3  # auto-pause agent after N failures in window
_REINFORCE_TRACE_TAIL_LINES = 5000  # cap on trace.jsonl history scanned per tick


def _maybe_pause_agent(
    conn, agent: str, failure_count: int, now, project_dir: str
) -> None:
    """Pause all pending/launched items for an agent if not already paused."""
    from datetime import timedelta
    from superharness.engine.trace import trace_event

    window_start = (now - timedelta(minutes=_REINFORCE_WINDOW_MINUTES)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    already_paused = conn.execute(
        "SELECT 1 FROM inbox WHERE target_agent=? AND status='paused' "
        "AND paused_at >= ? LIMIT 1",
        (agent, window_start),
    ).fetchone()
    if already_paused:
        return
    paused_now = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    cursor = conn.execute(
        """UPDATE inbox SET status='paused', paused_at=?, failed_reason=?
           WHERE target_agent=? AND status IN ('pending','launched')
             AND run_id IS NULL""",
        (
            paused_now,
            f"reinforce: fleet-classified permanent_block after {failure_count} failures",
            agent,
        ),
    )
    if cursor.rowcount and cursor.rowcount > 0:
        conn.commit()
        trace_event(
            project_dir,
            "reinforce_agent_pause",
            {
                "agent": agent,
                "failure_count": failure_count,
                "classification": "permanent_block",
            },
        )
        print(
            f"reinforce: paused {agent} — fleet classified as permanent_block",
            file=sys.stderr,
        )


def _tail_lines(path, n: int) -> list[str]:
    """Return up to the last `n` non-empty lines of a text file, without
    loading the whole file into memory.

    Replaces `path.read_text().splitlines()` in _reinforce_loop, which
    loaded and JSON-parsed the ENTIRE trace.jsonl on every call — profiled
    at 1,379,991 json.loads calls in a single tick against a 181 MB file
    (live, 2026-07-11). trace.jsonl is append-only, so that cost only grows
    with the project's lifetime, forever.

    Reads backward from EOF in fixed-size chunks until at least `n` newlines
    are found or the start of the file is reached — bounded work regardless
    of how much history precedes the tail.
    """
    from pathlib import Path

    p = Path(path)
    if not p.exists():
        return []

    chunk_size = 65536
    with p.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        file_size = fh.tell()
        if file_size == 0:
            return []

        data = b""
        pos = file_size
        # +1: a file ending in a trailing newline splits into one extra
        # empty trailing element; look for one more line than needed so
        # that dropping it still leaves n real lines.
        newlines_needed = n + 1
        while pos > 0 and data.count(b"\n") <= newlines_needed:
            read_size = min(chunk_size, pos)
            pos -= read_size
            fh.seek(pos)
            data = fh.read(read_size) + data

    # rstrip \r before the emptiness check: files with CRLF line endings
    # (written in text mode on Windows) would otherwise leave every line
    # carrying a trailing \r into the caller's json.loads/string comparisons.
    lines = [
        line.rstrip("\r") for line in data.decode("utf-8", errors="replace").split("\n")
    ]
    if lines and lines[-1] == "":
        lines = lines[:-1]  # trailing newline produces a trailing empty element
    lines = [line for line in lines if line.strip()]
    return lines[-n:]


def _reinforce_loop(project_dir: str) -> None:
    """Self-reinforcement: read history, detect patterns, auto-correct.

    Runs every 5 minutes (cooldown-gated by caller). Actions taken:
      1. Smart failure analysis — fleet-powered root cause classification
      2. Agent learning — read trace.jsonl for long-term patterns
      3. Consensus extraction — create tasks from discussion outcomes

    Uses the local GPU fleet for intelligent failure analysis instead of
    hardcoded thresholds. Each decision includes the fleet's reasoning.
    """
    from datetime import datetime, timezone, timedelta
    from superharness.engine.db import get_connection, init_db
    from superharness.engine.trace import trace_event

    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(minutes=_REINFORCE_WINDOW_MINUTES)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    try:
        conn = get_connection(project_dir)
        try:
            init_db(conn)

            # ── 1. Smart failure analysis ─────────────────────────────
            # For each agent with failures in the window, ask the fleet
            # to classify the root cause. Only pause agents that the fleet
            # classifies as "permanent_block".
            agent_failures: dict[str, list[dict]] = {}
            rows = conn.execute(
                """
                SELECT target_agent, task_id, failed_reason, failed_at
                FROM inbox
                WHERE status = 'failed' AND failed_at >= ?
                ORDER BY failed_at DESC
                """,
                (window_start,),
            ).fetchall()
            for row in rows:
                agent = row["target_agent"]
                agent_failures.setdefault(agent, []).append(
                    {
                        "task": row["task_id"] or "",
                        "error": row["failed_reason"] or "unknown",
                        "at": row["failed_at"] or "",
                    }
                )

            for agent, failures in agent_failures.items():
                if len(failures) < 2:
                    continue  # not enough data to analyze

                # Build history summary
                history = "; ".join(
                    f"{f['at'][:16]}: {f['error'][:60]}" for f in failures[:5]
                )
                latest = failures[0]

                # Ask the fleet to analyze, then try to self-heal
                try:
                    from superharness.engine.model_router import analyze_failure

                    classification = analyze_failure(
                        agent=agent,
                        task=latest["task"],
                        error=latest["error"],
                        history=history,
                    )
                except Exception:
                    logger.warning(
                        "_reinforce_loop: unexpected error: %s",
                        sys.exc_info()[1],
                        exc_info=True,
                    )
                    classification = "unknown"

                # Try self-healing BEFORE pausing — fix the problem if we can
                healed, heal_msg = _self_heal(project_dir, agent, latest["error"])
                if healed:
                    print(
                        f"reinforce: self-healed {agent} — {heal_msg}",
                        file=sys.stderr,
                    )
                    trace_event(
                        project_dir,
                        "reinforce_self_heal",
                        {
                            "agent": agent,
                            "action": heal_msg,
                            "error": latest["error"][:200],
                        },
                    )
                    continue  # skip pause — agent should work now

                trace_event(
                    project_dir,
                    "reinforce_analysis",
                    {
                        "agent": agent,
                        "failures": len(failures),
                        "classification": classification,
                        "self_heal_attempted": True,
                        "self_heal_result": heal_msg,
                        "latest_error": latest["error"][:200],
                    },
                )

                if classification == "permanent_block":
                    _maybe_pause_agent(conn, agent, len(failures), now, project_dir)
                elif classification != "transient":
                    print(
                        f"reinforce: {agent} — fleet classified {len(failures)} "
                        f"failures as '{classification}', not pausing",
                        file=sys.stderr,
                    )

            # ── 2. Learn from trace.jsonl ──────────────────────────────
            # Reads only the tail (_REINFORCE_TRACE_TAIL_LINES), not the whole
            # file. Previously this read + JSON-parsed the ENTIRE file, TWICE
            # (once for the pause scan, once again for the dedup check) —
            # 1,379,991 json.loads calls in a single tick against a 181 MB
            # file (live, 2026-07-11). trace.jsonl is append-only, so that
            # cost only grows with the project's lifetime.
            #
            # Behavior change, deliberate: pause counting is now scoped to
            # the tail window instead of all-time history. All-time counting
            # meant an agent paused 3 times months ago and never since would
            # be flagged "chronically paused" forever — the tail window is
            # more correct, not just cheaper, and matches the time-windowed
            # design already used elsewhere in this function (see
            # _REINFORCE_WINDOW_MINUTES above).
            try:
                import json as _json
                from pathlib import Path

                trace_file = Path(project_dir) / ".superharness" / "trace.jsonl"
                tail = _tail_lines(trace_file, _REINFORCE_TRACE_TAIL_LINES)
                if tail:
                    agent_pause_counts: dict[str, int] = {}
                    for line in tail:
                        try:
                            event = _json.loads(line)
                            if event.get("type") == "reinforce_agent_pause":
                                agent = event.get("agent", "")
                                if agent:
                                    agent_pause_counts[agent] = (
                                        agent_pause_counts.get(agent, 0) + 1
                                    )
                        except _json.JSONDecodeError:
                            continue
                    recent_tail = tail[-50:]
                    for agent, count in agent_pause_counts.items():
                        if count >= 3:
                            already_logged = any(
                                f'"agent":"{agent}"' in line
                                and '"learning":"agent_deprioritized"' in line
                                for line in recent_tail
                            )
                            if not already_logged:
                                trace_event(
                                    project_dir,
                                    "reinforce_learning",
                                    {
                                        "learning": "agent_deprioritized",
                                        "agent": agent,
                                        "reason": f"paused {count} times",
                                        "action": "profile_update",
                                    },
                                )
            except Exception:
                logger.warning(
                    "_reinforce_loop: unexpected error: %s",
                    sys.exc_info()[1],
                    exc_info=True,
                )
                pass

            # ── 3. Consensus extraction ─────────────────────────────────
            try:
                from superharness.engine import discussions_dao

                consensus_discs = discussions_dao.get_all(conn, status="consensus")
                for disc in consensus_discs:
                    existing = conn.execute(
                        "SELECT 1 FROM tasks WHERE id LIKE ? LIMIT 1",
                        (f"auto-consensus-{disc.id}%",),
                    ).fetchone()
                    if existing:
                        continue
                    task_id = f"auto-consensus-{disc.id}-{now.strftime('%Y%m%d')}"
                    topic = (disc.topic or "discussion")[:80]
                    conn.execute(
                        "INSERT OR IGNORE INTO tasks (id, title, owner, status, "
                        "created_at, acceptance_criteria, workflow) "
                        "VALUES (?, ?, ?, 'todo', ?, '[]', 'implementation')",
                        (
                            task_id,
                            f"Consensus: {topic}",
                            "owner",
                            now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        ),
                    )
                    conn.commit()
                    trace_event(
                        project_dir,
                        "reinforce_consensus_task",
                        {
                            "discussion_id": disc.id,
                            "task_id": task_id,
                            "topic": topic,
                        },
                    )
            except Exception:
                logger.warning(
                    "_reinforce_loop: unexpected error: %s",
                    sys.exc_info()[1],
                    exc_info=True,
                )
                pass

        finally:
            conn.close()
    except Exception as e:
        logger.warning("reinforce_loop failed: %s", e, exc_info=True)


# ── Self-healing: match known error patterns → attempt safe fix ──────────────

_HEAL_PATTERNS: list[tuple[str, str, str]] = [
    # (regex pattern, fix description, fix action keyword)
    (r"No module named '([^']+)'", "module_not_found", "pip_install"),
    (r"No module named \"([^\"]+)\"", "module_not_found", "pip_install"),
    (r"No such file or directory: '([^']+)'", "file_not_found", "check_path"),
    (
        r"Address already in use|port.*already in use|Errno 48",
        "port_conflict",
        "kill_orphan",
    ),
    (r"database is locked|database locked", "db_locked", "retry_db"),
    (r"email\.message|importlib\.metadata", "py314_incompat", "downgrade_python"),
]


def _self_heal(project_dir: str, agent: str, error: str) -> tuple[bool, str]:
    """Attempt to auto-heal a known failure pattern.

    Returns (healed, action_description). Never raises — all errors caught.
    Only attempts safe fixes: install missing modules, kill orphan processes,
    clean up stale locks. Does NOT modify code or configuration.
    """
    import re
    from superharness.engine.trace import trace_event

    for pattern, desc, action in _HEAL_PATTERNS:
        m = re.search(pattern, error, re.IGNORECASE)
        if not m:
            continue

        if action == "pip_install":
            module_name = m.group(1).split(".")[0]  # top-level module only
            if module_name in (
                "email",
                "importlib",
                "os",
                "sys",
                "json",
                "re",
                "time",
                "pathlib",
                "subprocess",
                "logging",
                "typing",
                "collections",
            ):
                # stdlib module — not fixable via pip
                trace_event(
                    project_dir,
                    "self_heal_skip",
                    {
                        "error": desc,
                        "detail": f"stdlib module '{module_name}' — not pip-installable",
                    },
                )
                return False, f"stdlib module '{module_name}' cannot be pip-installed"

            # Try pip install
            try:
                import subprocess as _sp

                python = os.environ.get("PYTHON", "python3")
                result = _sp.run(
                    [python, "-m", "pip", "install", module_name],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode == 0:
                    trace_event(
                        project_dir,
                        "self_heal_success",
                        {
                            "action": "pip_install",
                            "module": module_name,
                            "agent": agent,
                        },
                    )
                    return True, f"pip installed '{module_name}'"
                else:
                    trace_event(
                        project_dir,
                        "self_heal_fail",
                        {
                            "action": "pip_install",
                            "module": module_name,
                            "stderr": result.stderr[:200],
                        },
                    )
                    return (
                        False,
                        f"pip install '{module_name}' failed: {result.stderr[:100]}",
                    )
            except Exception as e:
                logger.warning("_self_heal: unexpected error: %s", e, exc_info=True)
                return False, f"pip install failed: {e}"

        if action == "kill_orphan":
            # Find port from error or use a common port
            port_match = re.search(r":(\d{4,5})", error)
            port = int(port_match.group(1)) if port_match else None
            killed = 0
            if port:
                try:
                    import subprocess as _sp

                    # lsof -ti :port → PIDs using that port
                    result = _sp.run(
                        ["lsof", "-ti", f":{port}"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                    )
                    for pid_str in result.stdout.strip().split():
                        try:
                            os.kill(int(pid_str), 9)
                            killed += 1
                        except (OSError, ProcessLookupError, ValueError):
                            pass
                except Exception:
                    logger.warning(
                        "_self_heal: unexpected error: %s",
                        sys.exc_info()[1],
                        exc_info=True,
                    )
                    pass
            if killed > 0:
                trace_event(
                    project_dir,
                    "self_heal_success",
                    {
                        "action": "kill_orphan",
                        "port": port,
                        "killed": killed,
                    },
                )
                return True, f"killed {killed} process(es) on port {port}"
            return False, "no orphan processes found to kill"

        if action == "retry_db":
            # Try PRAGMA to unlock SQLite
            try:
                from superharness.engine.db import get_connection

                conn = get_connection(project_dir)
                try:
                    conn.execute("PRAGMA busy_timeout=10000")
                    conn.execute("ROLLBACK")
                    trace_event(
                        project_dir,
                        "self_heal_success",
                        {
                            "action": "db_unlock",
                        },
                    )
                    return True, "SQLite pragma executed — retry may succeed"
                finally:
                    conn.close()
            except Exception:
                logger.warning(
                    "_self_heal: unexpected error: %s", sys.exc_info()[1], exc_info=True
                )
                return False, "SQLite pragma failed"

        if action == "downgrade_python":
            trace_event(
                project_dir,
                "self_heal_skip",
                {
                    "error": desc,
                    "detail": "Python 3.14 incompatibility — cannot auto-fix",
                },
            )
            return False, "Python 3.14 incompatibility — use Python 3.11/3.12 instead"

        if action == "check_path":
            path = m.group(1)
            if os.path.exists(path):
                return False, f"path '{path}' exists — not a missing file issue"
            # Check if it's a stale pipx venv path
            if "pipx/venvs" in path:
                trace_event(
                    project_dir,
                    "self_heal_skip",
                    {
                        "error": desc,
                        "detail": "stale pipx venv path — reinstalling fixes this",
                    },
                )
                return False, "stale pipx venv path — run: pipx install superharness"
            return False, f"file '{path}' not found — check path"

    # Unknown error pattern — don't attempt fix
    trace_event(
        project_dir,
        "self_heal_unknown",
        {
            "error": error[:200],
            "agent": agent,
        },
    )
    return False, "unknown error pattern — no auto-fix available"


# ---------------------------------------------------------------------------
# Comprehensive GC pass — runs once per tick, handles all cleanup in one sweep.
# ---------------------------------------------------------------------------

_last_gc_time: dict[str, float] = {}  # project_dir → last GC timestamp
_GC_MIN_INTERVAL_SECONDS = 60  # run at most once per minute


def _comprehensive_gc(project_dir: str) -> dict[str, int]:
    """Run all GC passes in one sweep. Returns counts per action."""
    now_ts = __import__("time").time()
    if project_dir in _last_gc_time:
        elapsed = now_ts - _last_gc_time[project_dir]
        if elapsed < _GC_MIN_INTERVAL_SECONDS:
            return {}
    _last_gc_time[project_dir] = now_ts

    counts: dict[str, int] = {}
    counts["duplicates"] = _gc_duplicate_inbox(project_dir)
    counts["zombies_running"] = _gc_zombie_running(project_dir)
    counts["zombies_pending"] = _gc_zombie_pending(project_dir)
    counts["deadlocked_discussions"] = _gc_discussion_deadlock(project_dir)
    counts["orphaned_discussion_inbox"] = _gc_orphaned_discussion_inbox(project_dir)
    counts["stuck_waiting_input"] = _gc_stuck_waiting_input(project_dir)
    return counts


# ── Gap 2: duplicate inbox cleanup ────────────────────────────────────────────


def _gc_duplicate_inbox(project_dir: str) -> int:
    """Merge duplicate pending inbox items: keep newest, cancel older ones."""
    try:
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            # Find tasks with multiple pending items for the same agent
            dupes = conn.execute("""
                SELECT task_id, target_agent, COUNT(*) as cnt
                FROM inbox
                WHERE status = 'pending' AND run_id IS NULL
                GROUP BY task_id, target_agent
                HAVING cnt > 1
            """).fetchall()
            cleaned = 0
            for dupe in dupes:
                task_id, agent = dupe["task_id"], dupe["target_agent"]
                # Keep newest, cancel older
                rows = conn.execute(
                    "SELECT id FROM inbox WHERE task_id=? AND target_agent=? "
                    "AND status='pending' AND run_id IS NULL ORDER BY created_at DESC",
                    (task_id, agent),
                ).fetchall()
                for row in rows[1:]:  # skip newest
                    conn.execute(
                        "UPDATE inbox SET status='done', failed_reason='gc: duplicate merged' WHERE id=?",
                        (row["id"],),
                    )
                    cleaned += 1
            if cleaned:
                conn.commit()
            return cleaned
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_duplicate_inbox failed: %s", e, exc_info=True)
        return 0


# ── Gap 3: zombie reconciler for running/pending ──────────────────────────────


def _gc_zombie_running(project_dir: str) -> int:
    """Mark running items as failed if no dispatcher process is active."""
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            rows = conn.execute(
                "SELECT id, pid FROM inbox WHERE status='running' AND run_id IS NULL"
            ).fetchall()
            cleaned = 0
            now = _now_utc()
            for row in rows:
                pid = row["pid"]
                if pid and not _pid_alive(int(pid)):
                    inbox_dao.update_status(
                        conn,
                        row["id"],
                        from_status="running",
                        to_status="failed",
                        now=now,
                        reason="gc: dispatcher process died",
                    )
                    cleaned += 1
            if cleaned:
                conn.commit()
            return cleaned
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_zombie_running failed: %s", e, exc_info=True)
        return 0


def _gc_zombie_pending(project_dir: str) -> int:
    """Cancel pending items that have been waiting > 15 minutes with no dispatch."""
    try:
        from datetime import datetime, timezone, timedelta
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=15)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            cursor = conn.execute(
                "UPDATE inbox SET status='done', failed_reason='gc: pending timeout (>15min)' "
                "WHERE status='pending' AND run_id IS NULL AND created_at < ?",
                (cutoff,),
            )
            cleaned = cursor.rowcount or 0
            if cleaned:
                conn.commit()
            return cleaned
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_zombie_pending failed: %s", e, exc_info=True)
        return 0


# ── Gap 1: discussion deadlock detection ──────────────────────────────────────


def _gc_discussion_deadlock(project_dir: str) -> int:
    """Auto-close discussion rounds where required agents can't submit.

    A round is deadlocked when:
    - Round age > 30 min
    - At least 1 agent submitted
    - Remaining required agents all failed (retry_count >= max_retries)
    """
    try:
        from datetime import datetime, timezone, timedelta
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import discussions_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            now = datetime.now(timezone.utc)
            cutoff = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Find active discussions with rounds older than cutoff
            active = discussions_dao.get_all(conn, status="active")
            closed_count = 0
            for disc in active:
                # Get current round (max round_number from discussion_rounds)
                max_round_row = conn.execute(
                    "SELECT MAX(round_number) as current_round FROM discussion_rounds WHERE discussion_id=?",
                    (disc.id,),
                ).fetchone()
                current_round = int(max_round_row["current_round"] or 0)
                if current_round < 1:
                    # No rounds submitted — check for no-engagement timeout (>2 hours)
                    if disc.created_at and disc.created_at < cutoff:
                        conn.execute(
                            "UPDATE discussions SET status='failed_participant', closed_at=? WHERE id=?",
                            (now.strftime("%Y-%m-%dT%H:%M:%SZ"), disc.id),
                        )
                        conn.commit()
                        closed_count += 1
                        print(
                            f"gc: discussion {disc.id} closed as failed_participant "
                            f"(no engagement after 2+ hours)",
                            file=sys.stderr,
                        )
                    continue

                # Check if the round task is old enough
                task_id = f"{disc.id}/round-{current_round}"
                task_row = conn.execute(
                    "SELECT created_at FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
                if not task_row:
                    continue
                created = str(task_row["created_at"] or "")
                if not created or created >= cutoff:
                    continue  # too new

                # Count submitted vs required
                submissions = discussions_dao.get_rounds(conn, disc.id)
                submitted = len(
                    [s for s in submissions if s.round_number == current_round]
                )
                # Parse participants from owners JSON
                import json as _json

                participants = (
                    _json.loads(disc.owners)
                    if isinstance(disc.owners, str)
                    else (disc.owners or [])
                )
                total_participants = len(participants)
                required = (
                    max(2, total_participants - 1) if total_participants > 1 else 2
                )

                if submitted >= required:
                    continue  # enough submitted, normal advance

                # Check if remaining agents are dead
                submitted_agents = {
                    s.agent for s in submissions if s.round_number == current_round
                }
                missing_agents = set(participants) - submitted_agents

                # Fast-close: if all missing agents have no daemon heartbeat, close now.
                # Don't wait for retry exhaustion — they can't respond.
                # BUT: still require `required` submissions, not just 1.
                # Opening the gate at 1 submission produces misleading consensus
                # verdicts when only one agent actually voted.
                # (Fix: BUGREPORT-discussion-consensus-single-participant, root cause #3.)
                all_daemon_dead = True
                for agent in missing_agents:
                    hb = conn.execute(
                        "SELECT status FROM agent_heartbeats WHERE agent=?",
                        (agent,),
                    ).fetchone()
                    if hb and hb["status"] not in ("zombie", None):
                        all_daemon_dead = False
                        break
                if all_daemon_dead and submitted >= required:
                    conn.execute(
                        "UPDATE discussions SET status='failed_participant', closed_at=? WHERE id=?",
                        (now.strftime("%Y-%m-%dT%H:%M:%SZ"), disc.id),
                    )
                    conn.commit()
                    closed_count += 1
                    print(
                        f"gc: discussion {disc.id} closed as failed_participant "
                        f"(round {current_round}: {submitted} submitted, "
                        f"{len(missing_agents)} agents have no daemon)",
                        file=sys.stderr,
                    )
                    continue

                all_dead = True
                for agent in missing_agents:
                    agent_task = f"{disc.id}/round-{current_round}"
                    failed_row = conn.execute(
                        "SELECT retry_count, max_retries FROM inbox "
                        "WHERE target_agent=? AND task_id=? AND status='failed' "
                        "ORDER BY created_at DESC LIMIT 1",
                        (agent, agent_task),
                    ).fetchone()
                    if not failed_row:
                        all_dead = False
                        break
                    retry = int(failed_row["retry_count"] or 0)
                    max_retries = int(failed_row["max_retries"] or 3)
                    if retry < max_retries:
                        all_dead = False
                        break

                if all_dead and submitted >= required:
                    conn.execute(
                        "UPDATE discussions SET status='failed_participant', closed_at=? WHERE id=?",
                        (now.strftime("%Y-%m-%dT%H:%M:%SZ"), disc.id),
                    )
                    conn.commit()
                    closed_count += 1
                    print(
                        f"gc: deadlocked discussion {disc.id} closed as failed_participant "
                        f"(round {disc.current_round}: {submitted} submitted, "
                        f"{len(missing_agents)} agents exhausted)",
                        file=sys.stderr,
                    )
            return closed_count
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_discussion_deadlock failed: %s", e, exc_info=True)
        return 0


# ── Gap 4: discussion inbox cleanup ───────────────────────────────────────────


def _gc_orphaned_discussion_inbox(project_dir: str) -> int:
    """Cancel inbox items for discussions that are already closed/failed."""
    try:
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            # Find closed/failed discussions (anything not active or consensus)
            closed_ids = [
                r["id"]
                for r in conn.execute(
                    "SELECT id FROM discussions WHERE status NOT IN ('active','consensus')"
                ).fetchall()
            ]
            if not closed_ids:
                return 0

            cleaned = 0
            for disc_id in closed_ids:
                # Cancel pending/launched/running/failed inbox items for this discussion
                cursor = conn.execute(
                    "UPDATE inbox SET status='done', failed_reason='gc: discussion closed' "
                    "WHERE task_id LIKE ? AND status IN ('pending','launched','running','failed')",
                    (f"{disc_id}/%",),
                )
                cleaned += cursor.rowcount or 0
            if cleaned:
                conn.commit()
            return cleaned
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_orphaned_discussion_inbox failed: %s", e, exc_info=True)
        return 0


# ── Gap 7: stuck waiting_input timeout ────────────────────────────────────────


def _gc_stuck_waiting_input(project_dir: str) -> int:
    """Auto-archive tasks stuck in waiting_input > 30 minutes."""
    try:
        from datetime import datetime, timezone, timedelta
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            # Bulk-select the candidate ids first — the actual status write
            # goes through _with_task_lock (one row at a time) so each write
            # is version-checked instead of a single unguarded bulk UPDATE.
            stuck_rows = conn.execute(
                "SELECT id FROM tasks WHERE status='waiting_input' AND ("
                "(in_progress_at IS NOT NULL AND in_progress_at < ?) "
                "OR (created_at IS NOT NULL AND created_at < ? AND in_progress_at IS NULL)"
                ")",
                (cutoff, cutoff),
            ).fetchall()
            cleaned = 0
            for row in stuck_rows:
                if _is_reliable_task_id(conn, row["id"]):
                    continue
                result = _with_task_lock(
                    conn,
                    row["id"],
                    {
                        "status": "archived",
                        "archived_reason": "gc: waiting_input timeout (>30min)",
                    },
                    guard=lambda t: t.status == "waiting_input",
                    context="gc_stuck_waiting_input",
                )
                if result is not None:
                    cleaned += 1
            if cleaned:
                conn.commit()
            return cleaned
        finally:
            conn.close()
    except Exception as e:
        logger.warning("_gc_stuck_waiting_input failed: %s", e, exc_info=True)
        return 0


def _pid_alive(pid: int) -> bool:
    """Return True if process with given PID exists.

    Delegates to the single seam in engine/process.py — the previous body
    (a raw signal-0 probe with no Windows branch) reported every live
    process as dead on Windows.
    """
    from superharness.engine.process import pid_alive

    return pid_alive(pid)


def _cancel_undispatchable_agents(project_dir: str) -> int:
    """Cancel pending inbox items for agents that have no dispatch script.

    Uses the adapter registry as the canonical source of valid agent names.
    Falls back to script-glob + hardcoded names if the registry is unavailable.
    Returns count of items canceled.
    """
    known_agents = set()
    try:
        from superharness.engine.adapter_registry import list_adapters

        known_agents.update(list_adapters())
    except Exception as e:
        logger.warning("inbox_watch unexpected error: %s", e, exc_info=True)
        pass
    if not known_agents:
        import glob as _glob

        scripts_dir = _find_scripts_dir()
        for script in _glob.glob(os.path.join(scripts_dir, "delegate-to-*.sh")):
            name = (
                os.path.basename(script).replace("delegate-to-", "").replace(".sh", "")
            )
            known_agents.add(name)
        known_agents.update(["claude-code", "codex-cli", "gemini-cli", "opencode"])

    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import inbox_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            rows = conn.execute(
                "SELECT id, target_agent FROM inbox WHERE status='pending' AND run_id IS NULL "
                "AND target_agent NOT IN ({})".format(
                    ",".join(f"'{a}'" for a in known_agents)
                )
            ).fetchall()
            canceled = 0
            for row in rows:
                inbox_dao.mark_stale(
                    conn, row[0], reason=f"agent '{row[1]}' has no dispatch adapter"
                )
                canceled += 1
            if canceled > 0:
                conn.commit()
                agents = set(r[1] for r in rows)
                print(
                    f"undispatchable-cleanup: canceled {canceled} item(s) for unknown agent(s): {agents}"
                )
            return canceled
        finally:
            conn.close()
    except Exception as e:
        logger.warning(
            "_cancel_undispatchable_agents: unexpected error: %s", e, exc_info=True
        )
        print(f"undispatchable-cleanup: failed: {e}", file=sys.stderr)
        return 0


def _find_scripts_dir() -> str:
    """Locate the scripts/ directory via package data, or env var override."""
    return os.environ.get("SUPERHARNESS_SCRIPTS_DIR") or str(
        _importlib_resources.files("superharness").joinpath("scripts")
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def watch(
    project_dir: str,
    *,
    target: str = "both",
    foreground: bool = False,
    # In one-shot mode (once or not foreground — the launchd/systemd path)
    # this only feeds heartbeat_stale_seconds=max(interval*2, 60) below; it
    # does NOT control tick cadence there (that's the caller's respawn
    # interval — launchd's StartInterval or operator.py's spawn loop). For
    # any interval <= 30 (the launchd/systemd default is 15) the max(...,60)
    # floor makes this value a no-op in practice. Only the foreground loop
    # below uses it as an actual poll interval.
    interval: int = 30,
    print_only: bool = False,
    non_interactive: bool = True,  # default non-interactive for auto-mode
    codex_bypass: bool = False,
    recover_timeout_minutes: int = 20,
    recover_action: str = "stale",
    launcher_timeout: int = 0,
    lock_stale_minutes: int = 30,
    once: bool = False,
) -> int:
    project_dir = os.path.realpath(project_dir)

    if not os.path.isdir(project_dir):
        _abort(f"Project directory does not exist: {project_dir}")

    harness_dir = os.path.join(project_dir, ".superharness")
    if not os.path.isdir(harness_dir):
        _abort(f"Not a superharness project (missing .superharness/): {project_dir}")
    if not os.access(harness_dir, os.W_OK):
        _abort(
            f"error: .superharness/ is not writable — check permissions: {harness_dir}"
        )

    lock_dir = _lock_dir_path(project_dir)

    # Auto-break stale lock
    _auto_break_stale_lock(
        lock_dir,
        lock_stale_minutes,
        project_dir=project_dir,
        heartbeat_stale_seconds=max(interval * 2, 60),
    )

    # Try to acquire lock
    if not _acquire_watcher_lock(lock_dir):
        print(f"Watcher already running for project: {project_dir}")
        return 0

    # Acquire SQLite singleton lease alongside filesystem lock
    _sqlite_singleton_acquire(project_dir)

    def _on_exit(signum: int = 0, frame: object = None) -> None:
        _reliable_release(project_dir)
        _sqlite_singleton_release(project_dir)
        _release_watcher_lock(lock_dir)
        if signum:
            sys.exit(0)

    import atexit

    atexit.register(_on_exit)
    signal.signal(signal.SIGTERM, _on_exit)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _on_exit)

    try:
        dispatch_kwargs = dict(
            target=target,
            print_only=print_only,
            non_interactive=non_interactive,
            codex_bypass=codex_bypass,
            launcher_timeout=launcher_timeout,
            recover_timeout_minutes=recover_timeout_minutes,
            recover_action=recover_action,
        )

        if once or not foreground:
            # Single cycle (launchd / --once)
            _run_scripts(project_dir, **dispatch_kwargs)
        else:
            # Foreground mode
            running = [True]

            def _stop(signum: int, frame: object) -> None:
                running[0] = False
                print("\nWatcher stopped.")
                _reliable_release(project_dir)
                _sqlite_singleton_release(project_dir)
                _release_watcher_lock(lock_dir)
                sys.exit(0)

            signal.signal(signal.SIGINT, _stop)
            signal.signal(signal.SIGTERM, _stop)
            if hasattr(signal, "SIGHUP"):
                signal.signal(signal.SIGHUP, _stop)

            print(
                f"superharness watcher (foreground) — project: {project_dir}",
                flush=True,
            )
            print(f"Polling every {interval}s. Press Ctrl+C to stop.", flush=True)

            while running[0]:
                _run_scripts(project_dir, **dispatch_kwargs)
                end_time = time.time() + interval
                while running[0] and time.time() < end_time:
                    time.sleep(0.1)

    finally:
        _release_watcher_lock(lock_dir)

    return 0


def main(argv: list[str] | None = None) -> None:
    import argparse

    if argv is None:
        argv = sys.argv[1:]

    class _CapUsage(argparse.HelpFormatter):
        def _format_usage(self, usage, actions, groups, prefix):
            return super()._format_usage(usage, actions, groups, "Usage: ")

    parser = argparse.ArgumentParser(
        prog="inbox_watch",
        description="Watch inbox and dispatch pending items",
        formatter_class=_CapUsage,
        add_help=True,
    )
    parser.add_argument("--project", "-p", required=True)
    parser.add_argument("--to", default="both", dest="target")
    parser.add_argument("--print-only", action="store_true", default=False)
    parser.add_argument("--non-interactive", action="store_true", default=True)
    parser.add_argument("--codex-bypass", action="store_true", default=False)
    parser.add_argument(
        "--recover-timeout-minutes", default="20", dest="recover_timeout_minutes"
    )
    parser.add_argument("--recover-action", default="stale")
    parser.add_argument("--launcher-timeout", default="0")
    parser.add_argument("--lock-stale-minutes", default="30")
    parser.add_argument("--foreground", "-f", action="store_true", default=False)
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Run in a foreground loop (alias for --foreground)",
    )
    parser.add_argument("--interval", "-i", default="30")
    parser.add_argument(
        "--once",
        action="store_true",
        default=False,
        help="Run a single cycle and exit (same as single-cycle / launchd mode)",
    )

    opts = parser.parse_args(argv)

    # Validate integer args
    def _parse_nonneg_int(name: str, val: str) -> int:
        try:
            n = int(val)
            if n < 0:
                raise ValueError
            return n
        except (ValueError, TypeError):
            _abort(f"{name} must be a non-negative integer", 2)

    def _parse_pos_int(name: str, val: str) -> int:
        try:
            n = int(val)
            if n <= 0:
                raise ValueError
            return n
        except (ValueError, TypeError):
            _abort(f"{name} must be a positive integer", 2)

    from superharness.harnesses import KNOWN_HARNESSES

    valid_targets = ["both", *KNOWN_HARNESSES]
    if opts.target not in valid_targets:
        _abort(f"--to must be one of: {', '.join(valid_targets)}", 2)

    if opts.recover_action not in ("stale", "retry"):
        _abort("--recover-action must be one of: stale, retry", 2)

    recover_timeout_minutes = _parse_nonneg_int(
        "--recover-timeout-minutes", opts.recover_timeout_minutes
    )
    launcher_timeout = _parse_nonneg_int("--launcher-timeout", opts.launcher_timeout)
    lock_stale_minutes = _parse_nonneg_int(
        "--lock-stale-minutes", opts.lock_stale_minutes
    )
    interval = _parse_pos_int("--interval", opts.interval)

    rc = watch(
        project_dir=opts.project,
        target=opts.target,
        foreground=opts.foreground or opts.loop,
        interval=interval,
        print_only=opts.print_only,
        non_interactive=opts.non_interactive,
        codex_bypass=opts.codex_bypass,
        recover_timeout_minutes=recover_timeout_minutes,
        recover_action=opts.recover_action,
        launcher_timeout=launcher_timeout,
        lock_stale_minutes=lock_stale_minutes,
        once=opts.once,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()

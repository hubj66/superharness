"""Python port of inbox-dispatch.sh.

Dispatches the next pending inbox item to its target launcher.
"""

from __future__ import annotations

import hashlib
import importlib.resources as _importlib_resources
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from superharness.engine.process import pid_alive, signal_process_group

_log = logging.getLogger(__name__)

DIRTY_WORKTREE_REASON = "dirty_worktree_requires_user_confirmation"

# Effort → timeout mapping (in seconds)
TIMEOUT_LOW_EFFORT = 900  # 15 minutes
TIMEOUT_MEDIUM_EFFORT = 1800  # 30 minutes
TIMEOUT_HIGH_EFFORT = 3600  # 60 minutes

DISCUSSION_ROUND_TIMEOUT_SECONDS = (
    900  # 15 min hard cap per discussion round (fallback)
)
DISCUSSION_TIMEOUT_LOW = 600  # 10 min
DISCUSSION_TIMEOUT_MEDIUM = 1200  # 20 min
DISCUSSION_TIMEOUT_HIGH = 1800  # 30 min


@dataclass
class DispatchContext:
    project_dir: str
    inbox_file: str
    contract_file: str
    print_only: bool
    non_interactive: bool
    codex_bypass: bool
    launcher_timeout: int
    script_dir: str
    sqlite_primary: bool
    target_filter: str | None = None
    # Mutated during stages
    item: dict = field(default_factory=dict)
    item_id: str = ""
    item_to: str = ""
    item_task: str = ""
    item_project: str = ""
    exec_project: str = ""
    effective_timeout: int = 0
    launcher: str = ""
    worktree_dir: str | None = None
    worktree_source_dir: str | None = None
    launch_args: list[str] = field(default_factory=list)
    spawn_env: dict[str, str] = field(default_factory=dict)
    task_log: str = ""
    wrapped_args: list[str] = field(default_factory=list)
    launcher_rc: int = 0
    launch_start: float = 0.0
    classification_category: str = "unknown"
    classification_explain: str = ""
    is_discussion: bool = False
    prelaunch_failure_reason: str = ""
    run_id: str | None = None


def _get_python() -> str:
    """Return the superharness pipx Python binary path."""
    venv = os.path.expanduser("~/.local/pipx/venvs/superharness/bin/python3")
    return venv if os.path.isfile(venv) else sys.executable


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _abort(msg: str, code: int = 1) -> None:
    _log.error("abort: %s", msg)
    print(msg, file=sys.stderr)
    sys.exit(code)


def _result_artifact_path(ctx: DispatchContext) -> str:
    """Return a deterministic path writable by an isolated child process."""
    project_root = os.path.realpath(ctx.project_dir)
    execution_root = os.path.realpath(ctx.exec_project or ctx.project_dir)
    if execution_root != project_root:
        project_key = hashlib.sha256(
            project_root.encode(encoding="utf-8")
        ).hexdigest()
        result_dir = os.path.join(
            tempfile.gettempdir(), "superharness-run-results", project_key
        )
    else:
        result_dir = os.path.join(ctx.project_dir, ".superharness", "run-results")
    os.makedirs(result_dir, exist_ok=True)
    filename = _safe_task_id_for_path(ctx.run_id or "run") + ".json"
    return os.path.join(result_dir, filename)


def _safe_task_id_for_path(task_id: str) -> str:
    """Sanitize a task id for use in a single-file path component.

    Task ids such as `discuss-<uuid>/round-N` contain `/` which would make
    `os.path.join(dir, f"{task_id}-...")` resolve into a nonexistent
    subdirectory, silently breaking log writes (script(1) exits 1).

    Mirrors `superharness.commands.discuss.cmd_summary` safe_id handling.
    """
    return task_id.replace("/", "_").replace("..", "_")


# ---------------------------------------------------------------------------
# Lock helpers (mkdir-based, same semantics as shell version)
# ---------------------------------------------------------------------------


class _MkdirLock:
    """Non-blocking mutex using a directory with PID-based orphan detection."""

    def __init__(self, path: str, stale_seconds: int = 300) -> None:
        self.path = path
        self._held = False
        self._stale_seconds = stale_seconds

    def _pid_file(self) -> str:
        return os.path.join(self.path, "owner.pid")

    def _write_pid(self) -> None:
        try:
            with open(self._pid_file(), "w", encoding="utf-8") as f:
                f.write(f"{os.getpid()}\n")
        except OSError:
            pass

    def _read_pid(self) -> int | None:
        try:
            with open(self._pid_file(), encoding="utf-8") as f:
                return int(f.readline().strip())
        except (OSError, ValueError):
            return None

    @staticmethod
    def _pid_alive(pid: int | None) -> bool:
        if pid is None:
            return False
        return pid_alive(pid)

    def _break_orphan(self) -> bool:
        """Remove lock if owning PID is dead or lock is stale with no PID."""
        if not os.path.isdir(self.path):
            return False
        pid = self._read_pid()
        if pid is not None and not self._pid_alive(pid):
            print(
                f"Auto-breaking orphaned dispatch lock (pid {pid} not running): {self.path}"
            )
            self._remove()
            return True
        if pid is None:
            try:
                age = time.time() - os.stat(self.path).st_mtime
            except OSError:
                return False
            if age >= self._stale_seconds:
                print(
                    f"Auto-breaking stale dispatch lock (age: {int(age)}s, no pid): {self.path}"
                )
                self._remove()
                return True
        return False

    def _remove(self) -> None:
        try:
            os.unlink(self._pid_file())
        except OSError:
            pass
        try:
            os.rmdir(self.path)
        except OSError:
            pass

    def acquire(self) -> bool:
        try:
            os.mkdir(self.path)
            self._write_pid()
            self._held = True
            return True
        except FileExistsError:
            if self._break_orphan():
                return self.acquire()
            return False

    def acquire_with_retry(self, attempts: int = 50, delay: float = 0.1) -> bool:
        for _ in range(attempts):
            if self.acquire():
                return True
            time.sleep(delay)
        return False

    def release(self) -> None:
        if self._held:
            self._remove()
            self._held = False


# ---------------------------------------------------------------------------
# Git dirty worktree detection + worktree isolation
# ---------------------------------------------------------------------------


def _git_worktree_add(project_dir: str, task_id: str) -> str | None:
    """Create a temporary git worktree for isolated dispatch. Returns worktree path or None."""
    import tempfile
    import uuid
    from superharness.engine.worktree_ops import sanitize_task_id

    # Sanitize at the sink, not just at the CLI boundary: mcp/tools/contract.py
    # create_task() INSERTs an arbitrary id straight into tasks with no
    # validation, and rows predating the _validate_token traversal guard may
    # already be in the DB. sanitize_task_id collapses path separators and ".."
    # so the join below cannot escape the worktree root.
    safe_task_id = sanitize_task_id(task_id)
    worktree_dir = os.path.join(
        tempfile.gettempdir(),
        "superharness-worktrees",
        f"{safe_task_id}-{uuid.uuid4().hex[:8]}",
    )
    os.makedirs(os.path.dirname(worktree_dir), exist_ok=True)
    r = subprocess.run(
        ["git", "-C", project_dir, "worktree", "add", "--detach", worktree_dir, "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        print(f"git worktree add failed: {r.stderr.strip()}", file=sys.stderr)
        return None
    # Symlink .superharness/ so the agent sees contract, inbox, handoffs.
    # Be defensive: replace any pre-existing dst that isn't already a symlink
    # to the correct source. A non-symlink dst can happen if a hook (e.g.
    # session-start.sh) raced ahead and mkdir'd .superharness/, which would
    # otherwise leave the worktree with an empty .superharness/ and break
    # the lifecycle gate (it'd see empty status and reject every dispatch).
    src_harness = os.path.join(project_dir, ".superharness")
    dst_harness = os.path.join(worktree_dir, ".superharness")
    if os.path.isdir(src_harness):
        try:
            if os.path.islink(dst_harness):
                if os.readlink(dst_harness) != src_harness:
                    os.unlink(dst_harness)
                    os.symlink(src_harness, dst_harness)
            elif os.path.isdir(dst_harness):
                # Replace an unintended real dir (likely created by a hook).
                # Only safe because worktrees are always under tempfile.gettempdir().
                import shutil as _shutil

                _shutil.rmtree(dst_harness, ignore_errors=True)
                os.symlink(src_harness, dst_harness)
            elif not os.path.exists(dst_harness):
                os.symlink(src_harness, dst_harness)
        except OSError as e:
            print(
                f"warning: failed to symlink .superharness/ in worktree: {e}",
                file=sys.stderr,
            )
    return worktree_dir


def _git_worktree_remove(project_dir: str, worktree_dir: str) -> bool:
    """Remove a temporary git worktree. Returns True on success."""
    # Remove .superharness symlink first (don't let git prune delete the real dir)
    dst_harness = os.path.join(worktree_dir, ".superharness")
    if os.path.islink(dst_harness):
        os.unlink(dst_harness)
    r = subprocess.run(
        ["git", "-C", project_dir, "worktree", "remove", "--force", worktree_dir],
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        print(f"git worktree remove failed: {r.stderr.strip()}", file=sys.stderr)
        return False
    return True


def _has_dirty_worktree(project_dir: str) -> bool:
    try:
        r = subprocess.run(
            ["git", "-C", project_dir, "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode != 0:
            return False
        r2 = subprocess.run(
            [
                "git",
                "-C",
                project_dir,
                "status",
                "--porcelain",
                "--untracked-files=normal",
                "--",
                ":!.superharness/",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return bool(r2.stdout.strip())
    except FileNotFoundError:
        return False


def _pi_worktree_preflight_error(project_dir: str) -> str | None:
    """Return an actionable reason when Pi isolation cannot use ``HEAD``."""
    try:
        inside = subprocess.run(
            ["git", "-C", project_dir, "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return "Git executable is unavailable"
    if inside.returncode != 0 or inside.stdout.strip() != "true":
        return "project is not a Git repository"

    try:
        head = subprocess.run(
            ["git", "-C", project_dir, "rev-parse", "--verify", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return f"Git HEAD check failed: {exc}"
    if head.returncode != 0:
        return "Git repository has no committed HEAD"
    return None


# ---------------------------------------------------------------------------
# Inbox engine helpers
# ---------------------------------------------------------------------------


def _inbox_cmd(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_get_python(), "-m", "superharness.engine.inbox"] + args,
        capture_output=True,
        text=True,
        check=False,
    )


def _contract_cmd(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [_get_python(), "-m", "superharness.engine.contract"] + args,
        capture_output=True,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------------
# Task effort → timeout calculation
# ---------------------------------------------------------------------------


def _get_task_effort_timeout(project_dir: str, task_id: str) -> int:
    """Calculate launcher timeout based on task effort estimate.

    Reads from SQLite (state.db). Returns timeout in seconds. Precedence:
    1. estimated_minutes field (if present)
    2. effort field mapped to standard timeouts (low=15min, medium=30min, high=60min)
    3. 0 (no timeout) if neither is set
    """
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import tasks_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            task = tasks_dao.get(conn, task_id)
        finally:
            conn.close()
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        return 0

    if task is None:
        return 0

    # Priority 1: explicit estimated_minutes
    if task.estimated_minutes is not None:
        try:
            return int(task.estimated_minutes) * 60
        except (ValueError, TypeError):
            pass

    # Priority 2: effort mapping
    if task.effort == "low":
        return TIMEOUT_LOW_EFFORT
    elif task.effort == "medium":
        return TIMEOUT_MEDIUM_EFFORT
    elif task.effort == "high":
        return TIMEOUT_HIGH_EFFORT

    return 0


def _set_inbox_field(inbox_file: str, item_id: str, key: str, value: str) -> None:
    _inbox_cmd(
        [
            "set_field",
            "--file",
            inbox_file,
            "--id",
            item_id,
            "--key",
            key,
            "--value",
            value,
        ]
    )


def _set_inbox_status(
    inbox_file: str, item_id: str, from_: str, to: str, now: str, stamp_key: str
) -> bool:
    r = _inbox_cmd(
        [
            "set_status",
            "--file",
            inbox_file,
            "--id",
            item_id,
            "--from",
            from_,
            "--to",
            to,
            "--now",
            now,
            "--stamp-key",
            stamp_key,
        ]
    )
    return r.returncode == 0


# ---------------------------------------------------------------------------
# Timeout subprocess runner
# ---------------------------------------------------------------------------


def _run_with_timeout(
    timeout_secs: int,
    cmd: list[str],
    inbox_file: str = "",
    item_id: str = "",
    env: dict | None = None,
    stdout=None,
    stderr=None,
) -> int:
    """Run a command with a timeout; returns exit code (124 = timed out).

    Uses SIGALRM on POSIX (macOS/Linux). Falls back to a threading.Timer on
    Windows and any platform where SIGALRM is unavailable (Phase 4 reliability).
    """
    _use_sigalrm = hasattr(signal, "SIGALRM")

    # preexec_fn is POSIX-only; skip on Windows
    preexec = os.setsid if hasattr(os, "setsid") else None
    proc = subprocess.Popen(
        cmd, preexec_fn=preexec, env=env, stdout=stdout, stderr=stderr
    )
    if inbox_file and item_id:
        _inbox_cmd(
            [
                "set_field",
                "--file",
                inbox_file,
                "--id",
                item_id,
                "--key",
                "pid",
                "--value",
                str(proc.pid),
            ]
        )

    timed_out = [False]

    if _use_sigalrm:

        def _on_alarm(signum: int, frame: object) -> None:
            timed_out[0] = True
            signal_process_group(proc.pid, signal.SIGTERM)

        def _on_term(signum: int, frame: object) -> None:
            signal_process_group(proc.pid, signal.SIGTERM)
            sys.exit(1)

        old_alarm = signal.signal(signal.SIGALRM, _on_alarm)  # type: ignore[attr-defined]
        old_term = signal.signal(signal.SIGTERM, _on_term)
        signal.alarm(timeout_secs)  # type: ignore[attr-defined]
        try:
            rc = proc.wait()
        finally:
            signal.alarm(0)  # type: ignore[attr-defined]
            signal.signal(signal.SIGALRM, old_alarm)  # type: ignore[attr-defined]
            signal.signal(signal.SIGTERM, old_term)
            if inbox_file and item_id:
                _inbox_cmd(
                    [
                        "set_field",
                        "--file",
                        inbox_file,
                        "--id",
                        item_id,
                        "--key",
                        "pid",
                        "--value",
                        "",
                    ]
                )
    else:
        # Windows / SIGALRM-unavailable fallback: threading.Timer
        import threading

        def _on_timer() -> None:
            timed_out[0] = True
            try:
                proc.terminate()
            except OSError:
                pass

        timer = threading.Timer(timeout_secs, _on_timer)
        timer.start()
        try:
            rc = proc.wait()
        finally:
            timer.cancel()
            if inbox_file and item_id:
                _inbox_cmd(
                    [
                        "set_field",
                        "--file",
                        inbox_file,
                        "--id",
                        item_id,
                        "--key",
                        "pid",
                        "--value",
                        "",
                    ]
                )

    if timed_out[0]:
        return 124
    return rc


# ---------------------------------------------------------------------------
# mark_item_failed / paused
# ---------------------------------------------------------------------------


def _mark_item_failed(
    inbox_file: str, item_id: str, failed_at: str, lock: _MkdirLock, reason: str = ""
) -> bool:
    if not lock.acquire_with_retry(50, 0.1):
        print(
            f"Failed to acquire inbox lock while marking failure for {item_id}",
            file=sys.stderr,
        )
        return False

    ok = _set_inbox_status(
        inbox_file, item_id, "launched", "failed", failed_at, "failed_at"
    ) or _set_inbox_status(
        inbox_file, item_id, "running", "failed", failed_at, "failed_at"
    )
    if ok and reason:
        _set_inbox_field(inbox_file, item_id, "failed_reason", reason)
    lock.release()
    if ok:
        print(
            f"Inbox item updated: {item_id} -> failed{' (' + reason + ')' if reason else ''}"
        )
    else:
        print(f"Failed to mark inbox item as failed for {item_id}", file=sys.stderr)
    return ok


def _mark_item_paused_dirty(inbox_file: str, item_id: str, paused_at: str) -> bool:
    if _set_inbox_status(
        inbox_file, item_id, "pending", "paused", paused_at, "paused_at"
    ):
        _set_inbox_field(inbox_file, item_id, "pause_reason", DIRTY_WORKTREE_REASON)
        print(
            f"Inbox item updated: {item_id} -> paused (dirty worktree requires interactive confirmation)"
        )
        return True
    return False


def _sqlite_record_review(
    project_dir: str,
    owner: str,
    task_type: str,
    duration_s: float,
    score: float,
    failed: bool,
    now: str,
) -> None:
    """Record dispatch outcome in review_dao for agent performance tracking. Never raises."""
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import review_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            review_dao.record(
                conn,
                owner=owner,
                task_type=task_type,
                duration_s=duration_s,
                score=score,
                failed=failed,
                now=now,
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass


def _sqlite_mirror_dispatch(
    project_dir: str,
    item_id: str,
    task_id: str,
    agent: str,
    to_status: str,
    now: str,
    *,
    reason: str = "",
) -> None:
    """Mirror inbox dispatch status change to SQLite. Never raises, logs errors."""
    try:
        from superharness.engine.db import get_connection, init_db, transaction
        from superharness.engine import inbox_dao, ledger_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                ledger_dao.record(
                    conn,
                    agent=agent,
                    action=f"dispatch_{to_status}",
                    task_id=task_id,
                    now=now,
                )
                updated = False
                for _from in ("pending", "launched", "running"):
                    if inbox_dao.update_status(
                        conn,
                        item_id,
                        from_status=_from,
                        to_status=to_status,
                        now=now,
                        reason=reason or None,
                    ):
                        updated = True
                        break
                if not updated:
                    _log.warning(
                        "_sqlite_mirror_dispatch: no row matched for item=%s to_status=%s",
                        item_id,
                        to_status,
                    )
        finally:
            conn.close()
    except Exception as e:
        # Was two stacked broad-catch clauses on the same try — the second
        # (with exc_info=True) could never run, since the first already
        # matches every Exception subclass. Merged into one, keeping the
        # more specific message and adding exc_info=True.
        _log.warning(
            "_sqlite_mirror_dispatch failed for %s: %s", item_id, e, exc_info=True
        )


def _clear_claimed_inbox_pid(project_dir: str, item_id: str) -> None:
    """Clear the dispatcher PID when a claimed item fails before launch."""
    try:
        from superharness.engine import inbox_dao
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            inbox_dao.set_field(conn, item_id, "pid", None)
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        _log.warning(
            "failed to clear pre-launch inbox PID for %s: %s",
            item_id,
            exc,
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Cost extraction helper
# ---------------------------------------------------------------------------


def _read_context_cache_cost(project_dir: str, task_id: str) -> float:
    """Read cost_usd from the context-cache snapshot written by delegate after SDK dispatch."""
    try:
        import yaml

        cache_path = os.path.join(
            project_dir, ".superharness", "context-cache", f"{task_id}.yaml"
        )
        if not os.path.isfile(cache_path):
            return 0.0
        with open(cache_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return float(data.get("cost_usd", 0.0))
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        return 0.0


# ---------------------------------------------------------------------------
# Main dispatch logic
# ---------------------------------------------------------------------------


def _sqlite_claim_next(project_dir: str, target_agent: str, now: str) -> dict | None:
    """Claim the next pending inbox item via inbox_dao (SQLite-native). Never raises.

    Returns a dict with YAML-shape keys, or None if nothing to claim.
    """
    try:
        from dataclasses import asdict
        from superharness.engine.db import get_connection, init_db, transaction
        from superharness.engine import inbox_dao
        from superharness.engine import runs_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                row = inbox_dao.claim_next(
                    conn, target_agent=target_agent, pid=os.getpid(), now=now
                )
                if row is None:
                    return None
                if row.run_id:
                    run = runs_dao.get_run(conn, row.run_id)
                    if run is None:
                        raise RuntimeError(
                            f"linked inbox row {row.id} references missing run {row.run_id}"
                        )
                    if run.status == "queued":
                        runs_dao.transition_run(
                            conn, run.id, to_status="claimed", now=now
                        )
            d = asdict(row)
            return {
                "id": d["id"],
                "to": d["target_agent"],
                "task": d["task_id"],
                "project": d["project_path"] or "",
                "retry_count": d["retry_count"],
                "max_retries": d["max_retries"],
                "priority": d["priority"],
                "plan_only": d["plan_only"],
                "run_id": d["run_id"],
            }
        finally:
            conn.close()
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        return None


def dispatch(
    project_dir: str,
    target_filter: str | None = None,
    print_only: bool = False,
    non_interactive: bool = False,
    codex_bypass: bool = False,
    launcher_timeout: int = 0,
) -> int:
    project_dir = os.path.realpath(project_dir)
    harness_dir = os.path.join(project_dir, ".superharness")
    inbox_file = os.path.join(harness_dir, "inbox.yaml")
    contract_file = os.path.join(harness_dir, "contract.yaml")

    _backend = os.environ.get("STATE_BACKEND", "").strip().lower()
    _sqlite_primary = _backend == "sqlite_only"
    # Post-migration: SQLite is always the source of truth.
    if not _sqlite_primary:
        try:
            from superharness.engine.sqlite_only import is_sqlite_only

            # Must pass project_dir so detection can fall back to checking
            # for state.sqlite3 — without it, returns False and dispatch
            # silently takes the broken YAML path.
            _sqlite_primary = is_sqlite_only(project_dir=project_dir)
        except Exception as e:
            _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
            pass
    if not _sqlite_primary and not os.path.exists(inbox_file):
        print(f"Inbox file not found: {inbox_file}", file=sys.stderr)
        return 1

    # Locate launcher scripts — env var allows tests/CI to inject a different dir
    script_dir = os.environ.get("SUPERHARNESS_SCRIPTS_DIR") or str(
        _importlib_resources.files("superharness").joinpath("scripts")
    )

    # Lock
    lock = _MkdirLock(inbox_file + ".lock.d")
    if not lock.acquire():
        print(f"Another inbox dispatcher is active for {inbox_file}; skipping.")
        return 0

    try:
        return _do_dispatch(
            inbox_file=inbox_file,
            contract_file=contract_file,
            project_dir=project_dir,
            target_filter=target_filter,
            print_only=print_only,
            non_interactive=non_interactive,
            codex_bypass=codex_bypass,
            launcher_timeout=launcher_timeout,
            script_dir=script_dir,
            lock=lock,
            sqlite_primary=_sqlite_primary,
        )
    finally:
        lock.release()


def _do_dispatch(
    inbox_file: str,
    contract_file: str,
    project_dir: str,
    target_filter: str | None,
    print_only: bool,
    non_interactive: bool,
    codex_bypass: bool,
    launcher_timeout: int,
    script_dir: str,
    lock: _MkdirLock,
    sqlite_primary: bool = False,
) -> int:
    ctx = DispatchContext(
        project_dir=project_dir,
        inbox_file=inbox_file,
        contract_file=contract_file,
        target_filter=target_filter,
        print_only=print_only,
        non_interactive=non_interactive,
        codex_bypass=codex_bypass,
        launcher_timeout=launcher_timeout,
        script_dir=script_dir,
        sqlite_primary=sqlite_primary,
    )

    # 1. Claim
    rc = _claim_next_item(ctx)
    if rc is not None:
        return rc

    # 2. Refresh a worker only after work was actually claimed.  The worker
    # shares state with the source project but deliberately omits .git.
    _sync_claimed_worker_copy(ctx)

    # 3. Resolve
    rc = _resolve_execution_context(ctx)
    if rc is not None:
        if _reliable_run(ctx):
            ctx.launcher_rc = 2
            _reliable_run_finished(ctx)
            _clear_claimed_inbox_pid(ctx.project_dir, ctx.item_id)
            return 1
        if ctx.prelaunch_failure_reason:
            ctx.launcher_rc = 2
            ctx.launch_start = time.time()
            lock.release()
            try:
                return _handle_failure(ctx)
            finally:
                _clear_claimed_inbox_pid(ctx.project_dir, ctx.item_id)
        return rc

    # 4. Transition
    rc = _transition_to_launched(ctx, lock)
    if rc is not None:
        return rc

    # 5. Prepare
    _prepare_launch_context(ctx)

    # 5b. Pre-launch idempotence guard for discussion rounds.
    # Bug G (docs/bugs/2026-05-11_discuss_dispatch_bugs.md §8): when the
    # watcher restarts, leftover pending inbox items for a round whose
    # YAML is already on disk (or whose discussion is closed) get
    # claimed and dispatched via the normal inbox path — not through
    # discussion_dispatch — so the discussion_dispatch idempotence
    # guard never sees them. Skip the launch and mark the item done.
    if _skip_already_done_discussion_round(ctx):
        return 0

    # 6. Execute
    _execute_agent(ctx)

    # Reliable runs report execution facts directly.  Legacy dispatch continues
    # through its existing task-state reconciliation below.
    if _reliable_run(ctx):
        _reliable_run_finished(ctx)

    # 7. Cleanup
    if ctx.worktree_dir:
        worktree_source = ctx.worktree_source_dir or ctx.project_dir
        if _git_worktree_remove(worktree_source, ctx.worktree_dir):
            print(f"Worktree removed: {ctx.worktree_dir}")
        else:
            print(
                f"Warning: worktree cleanup failed for {ctx.worktree_dir} — remove manually",
                file=sys.stderr,
            )

    # 8. Post-process
    if _reliable_run(ctx):
        return 0 if ctx.launcher_rc == 0 else 1

    if ctx.launcher_rc != 0:
        return _handle_failure(ctx)

    return _reconcile_state(ctx)


def _sync_claimed_worker_copy(ctx: DispatchContext) -> None:
    """Refresh a shared-state worker after a real inbox claim.

    ``watcher-worker`` launches the dispatcher from a worker directory whose
    ``.superharness`` symlink points at the source project.  Synchronizing only
    here avoids recursive scans during idle watcher cycles while ensuring a
    newly claimed task sees current source files before execution is resolved.
    """
    source_project = os.path.realpath(ctx.item_project)
    worker_project = os.path.realpath(ctx.project_dir)
    if source_project == worker_project:
        return
    if not os.path.isdir(os.path.join(source_project, ".git")):
        return
    if os.path.isdir(os.path.join(worker_project, ".git")):
        return
    source_state = os.path.realpath(os.path.join(source_project, ".superharness"))
    worker_state = os.path.realpath(os.path.join(worker_project, ".superharness"))
    if source_state != worker_state or not os.path.isdir(source_state):
        return

    from superharness.engine.platform_runtime import sync_worker_copy

    sync_worker_copy(source_project, worker_project)


def _claim_next_item(ctx: DispatchContext) -> int | None:
    now_claim = _now_utc()

    if ctx.sqlite_primary:
        if not ctx.target_filter:
            print(
                "sqlite_only mode requires --to <agent>; no YAML fallback available.",
                file=sys.stderr,
            )
            return 1
        # SQLite-native path: atomically claim next pending item via inbox_dao
        item = _sqlite_claim_next(ctx.project_dir, ctx.target_filter, now_claim)
        if item is None:
            return 0
        # Log the claim (inbox.yaml transition skipped in sqlite_only)
        print(
            f"Inbox item claimed from SQLite: {item['id']} → {item['task']} (sqlite_only mode)"
        )
    else:
        print(
            "inbox_dispatch: SQLite required but not active. "
            "Run 'shux init' to initialise state.sqlite3, or set STATE_BACKEND=sqlite_only.",
            file=sys.stderr,
        )
        return 1

    ctx.item = item
    ctx.item_id = str(item.get("id", ""))
    ctx.item_to = str(item.get("to", ""))
    ctx.item_task = str(item.get("task", ""))
    ctx.item_project = str(item.get("project", "") or ctx.project_dir)
    ctx.run_id = str(item.get("run_id") or "") or None
    if not ctx.item_project:
        ctx.item_project = ctx.project_dir

    return None


def _reliable_run(ctx: DispatchContext) -> bool:
    return bool(ctx.run_id)


def _pid_starttime(pid: int) -> str | None:
    from superharness.engine.process import process_starttime

    return process_starttime(pid)


def _git_snapshot(path: str) -> dict[str, object]:
    def run(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", path, *args],
                capture_output=True,
                text=True,
                check=False,
            )
            return result.stdout.strip() if result.returncode == 0 else None
        except OSError:
            return None

    head = run("rev-parse", "HEAD")
    branch = run("symbolic-ref", "--short", "HEAD") or run(
        "rev-parse", "--short", "HEAD"
    )
    dirty_output = run("status", "--porcelain", "--untracked-files=normal")
    return {
        "branch_name": branch,
        "base_sha": head,
        "head_sha": head,
        "dirty": bool(dirty_output),
    }


def _reliable_run_started(ctx: DispatchContext, pid: int | None = None) -> None:
    if not _reliable_run(ctx):
        return
    from superharness.engine import runs_dao
    from superharness.engine.db import get_connection, init_db, transaction

    conn = get_connection(ctx.project_dir)
    try:
        init_db(conn)
        with transaction(conn):
            run = runs_dao.get_run(conn, ctx.run_id or "")
            if run is None:
                raise RuntimeError(f"run {ctx.run_id} disappeared before launch")
            if run.status == "claimed":
                runs_dao.transition_run(
                    conn, run.id, to_status="running", now=_now_utc()
                )
            runs_dao.record_run_execution(
                conn,
                run.id,
                pid=pid,
                pid_starttime=_pid_starttime(pid) if pid else None,
                worktree_path=ctx.exec_project or ctx.project_dir,
                log_path=ctx.task_log or None,
            )
    finally:
        conn.close()


def _reliable_run_heartbeat(ctx: DispatchContext, pid: int | None) -> None:
    """Refresh the durable observation while the owned child is running."""
    if not _reliable_run(ctx):
        return
    from superharness.engine import runs_dao
    from superharness.engine.db import get_connection, init_db, transaction

    conn = get_connection(ctx.project_dir)
    try:
        init_db(conn)
        with transaction(conn):
            runs_dao.touch_run_heartbeat(
                conn,
                ctx.run_id or "",
                now=_now_utc(),
                pid=pid,
                pid_starttime=_pid_starttime(pid) if pid else None,
            )
    finally:
        conn.close()


def _reliable_run_finished(ctx: DispatchContext) -> None:
    """Persist dispatcher facts and close the linked inbox row."""
    if not _reliable_run(ctx) or ctx.print_only:
        return
    from superharness.engine import inbox_dao, runs_dao
    from superharness.engine.db import get_connection, init_db, transaction
    from superharness.engine.state_errors import BoundaryError

    snapshot = _git_snapshot(ctx.exec_project or ctx.project_dir)
    conn = get_connection(ctx.project_dir)
    try:
        init_db(conn)
        with transaction(conn):
            run = runs_dao.get_run(conn, ctx.run_id or "")
            if run is None:
                raise RuntimeError(f"run {ctx.run_id} disappeared before completion")
            success = ctx.launcher_rc == 0
            if success and run.status == "claimed":
                runs_dao.transition_run(
                    conn, run.id, to_status="running", now=_now_utc()
                )
                run = runs_dao.get_run(conn, run.id) or run
            result_snapshot = dict(snapshot)
            if success:
                result_snapshot["worktree_path"] = ctx.exec_project or ctx.project_dir
                for key in ("branch_name", "base_sha", "head_sha"):
                    if not result_snapshot.get(key):
                        result_snapshot[key] = "unknown"
                result_snapshot.setdefault("dirty", False)
            artifact_payload = _load_run_result_artifact(ctx)
            payload = artifact_payload or {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": run.task_id,
                "kind": run.kind,
                "agent": run.agent,
                "exit_code": ctx.launcher_rc,
                "completion_status": "completed" if success else "failed",
                **result_snapshot,
            }
            try:
                runs_dao.record_run_result(conn, run.id, payload, now=_now_utc())
                result_valid = True
            except (BoundaryError, ValueError, TypeError) as exc:
                result_valid = False
                runs_dao.record_run_diagnostic(
                    conn,
                    run.id,
                    failure_category="invalid_run_result",
                    failure_detail=str(exc),
                )
            terminal_success = success and result_valid
            if terminal_success:
                terminal_status = "succeeded"
                terminal_category = None
                terminal_detail = None
            else:
                from superharness.engine.failure_classifier import classify_reliable

                classification = classify_reliable(
                    launcher_rc=ctx.launcher_rc,
                    log_tail=_reliable_log_tail(ctx.task_log),
                    invalid_result=success and not result_valid,
                    timed_out=ctx.launcher_rc == 124,
                )
                terminal_category = classification.category
                terminal_detail = (
                    classification.explain
                    if success
                    else f"{classification.explain}: exit code {ctx.launcher_rc}"
                )
                terminal_status = {
                    "agent_crash": "crashed",
                    "lost_process": "crashed",
                    "timeout": "timed_out",
                    "hang": "timed_out",
                    "quota": "quota_blocked",
                }.get(classification.category, "failed")
            if run.status in {"claimed", "running"}:
                runs_dao.transition_run(
                    conn,
                    run.id,
                    to_status=terminal_status,
                    now=_now_utc(),
                    failure_category=terminal_category,
                    failure_detail=terminal_detail,
                )
            if run.inbox_id:
                inbox_dao.update_status(
                    conn,
                    run.inbox_id,
                    from_status="launched",
                    to_status="done" if terminal_success else "failed",
                    now=_now_utc(),
                    reason=None
                    if terminal_success
                    else (terminal_detail if not terminal_success else None),
                ) or inbox_dao.update_status(
                    conn,
                    run.inbox_id,
                    from_status="running",
                    to_status="done" if terminal_success else "failed",
                    now=_now_utc(),
                    reason=None
                    if terminal_success
                    else (terminal_detail if not terminal_success else None),
                )
    finally:
        conn.close()


def _reliable_log_tail(path: str, lines: int = 50) -> str:
    if not path or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return "\n".join(handle.read().splitlines()[-lines:])
    except OSError:
        return ""


def _load_run_result_artifact(ctx: DispatchContext) -> dict[str, object] | None:
    path = ctx.spawn_env.get("SUPERHARNESS_RUN_RESULT_PATH", "")
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _transition_to_launched(ctx: DispatchContext, lock: _MkdirLock) -> int | None:
    # Launch transition: in sqlite_only mode, inbox_dao.claim_next() already set status=launched.
    # In YAML mode, explicitly transition via YAML CLI.
    launch_now = _now_utc()
    item_priority = int(ctx.item.get("priority", 2))
    item_max_retries = int(ctx.item.get("max_retries", 3))

    if ctx.sqlite_primary:
        lock.release()
        new_retry_count = int(ctx.item.get("retry_count", 0))
        print(
            f"Inbox item updated: {ctx.item_id} -> launched (priority={item_priority}, retries={new_retry_count}/{item_max_retries})"
        )
    else:
        lr = subprocess.run(
            [
                _get_python(),
                "-m",
                "superharness.engine.inbox",
                "launch",
                "--file",
                ctx.inbox_file,
                "--id",
                ctx.item_id,
                "--now",
                launch_now,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        launch_rc = lr.returncode

        lock.release()  # Release before spawning launcher

        if launch_rc == 4:
            print(
                f"Inbox item updated: {ctx.item_id} -> failed (retry limit reached: {ctx.item.get('retry_count', 0)}/{item_max_retries})"
            )
            return 1

        if launch_rc != 0:
            print(
                f"Failed to launch inbox item transition for {ctx.item_id}: {lr.stdout.strip()}",
                file=sys.stderr,
            )
            return 1

        import re

        m = re.search(r"retry_count=(\d+)", lr.stdout)
        new_retry_count = int(m.group(1)) if m else int(ctx.item.get("retry_count", 0))
        print(
            f"Inbox item updated: {ctx.item_id} -> launched (priority={item_priority}, retries={new_retry_count}/{item_max_retries})"
        )
        _sqlite_mirror_dispatch(
            ctx.project_dir,
            ctx.item_id,
            ctx.item_task,
            ctx.item_to,
            "launched",
            launch_now,
        )

    return None


def _reconcile_state(ctx: DispatchContext) -> int:
    # Reconcile in non-interactive mode
    if ctx.non_interactive and not ctx.print_only:
        reconcile_now = _now_utc()
        new_lock = _MkdirLock(ctx.inbox_file + ".lock.d")
        if not new_lock.acquire_with_retry(50, 0.1):
            print(
                f"Failed to acquire inbox lock while reconciling status for {ctx.item_id}",
                file=sys.stderr,
            )
            return 1

        final_state = ""
        if ctx.is_discussion:
            # For discussion rounds the task isn't in the contract.
            # Check whether the agent wrote its submission YAML instead.
            # item_task = "discuss-<id>/round-N"
            parts = ctx.item_task.split("/")
            if len(parts) == 2:
                discuss_id, round_slug = parts
                submission_path = os.path.join(
                    ctx.exec_project,
                    ".superharness",
                    "discussions",
                    discuss_id,
                    f"{round_slug}-{ctx.item_to}.yaml",
                )
                if os.path.exists(submission_path):
                    final_state = "done"
                else:
                    # Bug S (rc=0 path): agent printed YAML to stdout but couldn't
                    # write to disk (e.g. gemini-cli write_file permission block).
                    # Launcher exits rc=0, so _handle_failure is not called; try to
                    # recover the YAML from the launcher log before giving up.
                    if ctx.task_log and os.path.isfile(ctx.task_log):
                        try:
                            round_num = int(round_slug.split("-", 1)[1])
                        except (IndexError, ValueError):
                            round_num = -1
                        if round_num > 0 and _recover_yaml_from_log(
                            ctx.task_log,
                            submission_path,
                            discuss_id,
                            round_num,
                            ctx.item_to,
                        ):
                            print(
                                f"  [recover-yaml] recovered submission from log -> {submission_path}"
                            )
                            final_state = "done"
                    # Bug T fix: if YAML is still absent, fail immediately.
                    # Dirty worktree is irrelevant for discussion agents — they don't
                    # commit code, so pausing for 30 min before the lifecycle timeout
                    # fires is pure waste. Go straight to failed so the retry budget
                    # is consumed and the watcher can re-dispatch promptly.
                    if final_state != "done":
                        final_state = "failed"
        else:
            try:
                from superharness.engine import state_reader as _sr

                _task_row = _sr.get_task(ctx.project_dir, ctx.item_task)
                if _task_row:
                    final_state = str(_task_row.get("status", ""))
            except Exception as _e:
                _log.warning(
                    "_reconcile_state: could not read SQLite task status: %s",
                    _e,
                    exc_info=True,
                )

        reconciled = 0

        if final_state == "done":
            if _set_inbox_status(
                ctx.inbox_file,
                ctx.item_id,
                "launched",
                "done",
                reconcile_now,
                "done_at",
            ) or _set_inbox_status(
                ctx.inbox_file, ctx.item_id, "running", "done", reconcile_now, "done_at"
            ):
                reconciled = 1
        elif final_state == "failed":
            if _set_inbox_status(
                ctx.inbox_file,
                ctx.item_id,
                "launched",
                "failed",
                reconcile_now,
                "failed_at",
            ) or _set_inbox_status(
                ctx.inbox_file,
                ctx.item_id,
                "running",
                "failed",
                reconcile_now,
                "failed_at",
            ):
                reconciled = 1
        elif final_state == "pending_user_approval":
            if _set_inbox_status(
                ctx.inbox_file,
                ctx.item_id,
                "launched",
                "paused",
                reconcile_now,
                "paused_at",
            ) or _set_inbox_status(
                ctx.inbox_file,
                ctx.item_id,
                "running",
                "paused",
                reconcile_now,
                "paused_at",
            ):
                _set_inbox_field(
                    ctx.inbox_file,
                    ctx.item_id,
                    "pause_reason",
                    "awaiting_user_approval",
                )
                reconciled = 3
        else:
            if _has_dirty_worktree(ctx.exec_project):
                if _set_inbox_status(
                    ctx.inbox_file,
                    ctx.item_id,
                    "launched",
                    "paused",
                    reconcile_now,
                    "paused_at",
                ) or _set_inbox_status(
                    ctx.inbox_file,
                    ctx.item_id,
                    "running",
                    "paused",
                    reconcile_now,
                    "paused_at",
                ):
                    _set_inbox_field(
                        ctx.inbox_file,
                        ctx.item_id,
                        "pause_reason",
                        DIRTY_WORKTREE_REASON,
                    )
                    reconciled = 2
            else:
                if _set_inbox_status(
                    ctx.inbox_file,
                    ctx.item_id,
                    "launched",
                    "failed",
                    reconcile_now,
                    "failed_at",
                ) or _set_inbox_status(
                    ctx.inbox_file,
                    ctx.item_id,
                    "running",
                    "failed",
                    reconcile_now,
                    "failed_at",
                ):
                    reconciled = 1

        new_lock.release()

        if reconciled > 0:
            _r_status = (
                "paused"
                if reconciled in (2, 3)
                else ("done" if final_state == "done" else "failed")
            )
            _sqlite_mirror_dispatch(
                ctx.project_dir,
                ctx.item_id,
                ctx.item_task,
                ctx.item_to,
                _r_status,
                reconcile_now,
            )
        elif ctx.sqlite_primary and final_state in (
            "done",
            "failed",
            "pending_user_approval",
        ):
            # sqlite-only mode: the item was claimed from SQLite, not inbox.yaml, so
            # _set_inbox_status returned False and reconciled stayed 0. Mirror to
            # SQLite directly so the item doesn't stay stuck in 'launched' forever.
            _r_status = (
                "paused" if final_state == "pending_user_approval" else final_state
            )
            _sqlite_mirror_dispatch(
                ctx.project_dir,
                ctx.item_id,
                ctx.item_task,
                ctx.item_to,
                _r_status,
                reconcile_now,
            )
            reconciled = 3 if final_state == "pending_user_approval" else 1
        elif (
            ctx.sqlite_primary
            and final_state not in ("done", "failed", "pending_user_approval")
            and final_state
        ):
            # sqlite-only: fallback state (dirty worktree → paused, else failed).
            _r_status = "paused" if _has_dirty_worktree(ctx.exec_project) else "failed"
            _sqlite_mirror_dispatch(
                ctx.project_dir,
                ctx.item_id,
                ctx.item_task,
                ctx.item_to,
                _r_status,
                reconcile_now,
            )
            reconciled = 2 if _r_status == "paused" else 1

        if reconciled == 2:
            print(
                f"Inbox item updated: {ctx.item_id} -> paused ({DIRTY_WORKTREE_REASON})"
            )
            return 0
        if reconciled == 3:
            print(
                f"Inbox item updated: {ctx.item_id} -> paused (awaiting_user_approval)"
            )
            try:
                from superharness.commands.notify_desktop import notify_task_event

                notify_task_event(ctx.item_task, "waiting_input", ctx.item_to)
            except Exception as e:
                _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
                pass
            return 0
        if reconciled == 1:
            import time as _time

            _elapsed = _time.time() - ctx.launch_start
            if final_state == "done":
                print(
                    f"Inbox item updated: {ctx.item_id} -> done (reconciled from contract task status)"
                )
                try:
                    from superharness.commands.notify_desktop import notify_task_event

                    notify_task_event(ctx.item_task, "done", ctx.item_to)
                except Exception as e:
                    _log.warning(
                        "inbox_dispatch.py unexpected error: %s", e, exc_info=True
                    )
                    pass
                try:
                    from superharness.engine.benchmark import record_dispatch

                    _cost = _read_context_cache_cost(ctx.exec_project, ctx.item_task)
                    record_dispatch(
                        ctx.exec_project,
                        ctx.item_task,
                        ctx.item_to,
                        "done",
                        _elapsed,
                        cost_usd=_cost,
                    )
                except Exception as e:
                    _log.warning(
                        "inbox_dispatch.py unexpected error: %s", e, exc_info=True
                    )
                    pass
                _sqlite_record_review(
                    ctx.project_dir,
                    owner=ctx.item_to,
                    task_type="dispatch",
                    duration_s=_elapsed,
                    score=1.0,
                    failed=False,
                    now=reconcile_now,
                )
                return 0
            print(
                f"Inbox item updated: {ctx.item_id} -> failed (non-interactive launch exited without done/failed)"
            )
            try:
                from superharness.engine.benchmark import record_dispatch

                _cost = _read_context_cache_cost(ctx.exec_project, ctx.item_task)
                record_dispatch(
                    ctx.exec_project,
                    ctx.item_task,
                    ctx.item_to,
                    "failed",
                    _elapsed,
                    cost_usd=_cost,
                )
            except Exception as e:
                _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
                pass
            _sqlite_record_review(
                ctx.project_dir,
                owner=ctx.item_to,
                task_type="dispatch",
                duration_s=_elapsed,
                score=0.0,
                failed=True,
                now=reconcile_now,
            )
            return 1

    return 0


def _recover_yaml_from_log(
    log_path: str,
    submission_path: str,
    disc_id: str,
    round_num: int,
    agent: str,
) -> bool:
    """Bug S fix: scan launcher log for YAML the agent printed but could not write.

    gemini-cli emits its submission as a fenced ```yaml block (or raw YAML starting
    with 'discussion_id:') when write_file is blocked. This function extracts and
    validates that content, then writes it to submission_path so the normal done-path
    in _handle_failure can proceed.

    Returns True if a valid submission was recovered and written."""
    import re

    try:
        import yaml as _yaml
    except ImportError:
        return False
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            content = fh.read()
    except OSError:
        return False

    candidates: list[str] = []

    # Primary: fenced ```yaml or ```yml blocks
    for m in re.finditer(r"```ya?ml\s*\n(.*?)```", content, re.DOTALL | re.IGNORECASE):
        candidates.append(m.group(1))

    # Fallback: last contiguous YAML-like block in the log. Agents that cannot
    # write files print raw YAML to stdout; key order depends on the serialiser
    # (may not start with 'discussion_id:'). Scan backward collecting lines that
    # match top-level YAML key-value syntax; stop at the first blank or
    # non-YAML line once any YAML lines have been accumulated.
    lines = content.splitlines()
    block_rev: list[str] = []
    for line in reversed(lines):
        stripped = line.rstrip()
        if re.match(r"^[a-z_][a-z_-]*:", stripped) or re.match(r"^\s+\S", stripped):
            block_rev.append(stripped)
        elif block_rev:
            break
    if block_rev:
        candidates.append("\n".join(reversed(block_rev)))

    for candidate in reversed(candidates):
        try:
            data = _yaml.safe_load(candidate)
        except Exception:
            _log.warning(
                "_recover_yaml_from_log: unexpected error: %s",
                sys.exc_info()[1],
                exc_info=True,
            )
            continue
        if not isinstance(data, dict):
            continue
        if str(data.get("discussion_id", "")) != disc_id:
            continue
        try:
            if int(data.get("round", -1)) != round_num:
                continue
        except (TypeError, ValueError):
            continue
        if str(data.get("agent", "")) != agent:
            continue
        if not str(data.get("verdict", "")).strip():
            continue
        os.makedirs(os.path.dirname(submission_path), exist_ok=True)
        try:
            with open(submission_path, "w", encoding="utf-8") as fh:
                _yaml.dump(
                    data,
                    fh,
                    allow_unicode=True,
                    default_flow_style=False,
                    sort_keys=False,
                )
            return True
        except OSError:
            return False
    return False


def _handle_failure(ctx: DispatchContext) -> int:
    fail_now = _now_utc()

    # Bug E (docs/bugs/2026-05-11_discuss_dispatch_bugs.md): for
    # discussion rounds, the agent sometimes writes a complete
    # round-N-<agent>.yaml then exits with terminal-escape garbage
    # that the shell interprets as a non-zero status. If the YAML is
    # on disk the round IS successful; trust the artifact over the
    # exit code. Trump card: the reconciler path already does this
    # on rc==0; we mirror it here for rc!=0 so a trailing
    # control-sequence death doesn't lose a real submission.
    if ctx.is_discussion and ctx.launcher_rc != 2:
        parts = ctx.item_task.split("/")
        if len(parts) == 2:
            discuss_id, round_slug = parts
            submission_path = os.path.join(
                ctx.exec_project,
                ".superharness",
                "discussions",
                discuss_id,
                f"{round_slug}-{ctx.item_to}.yaml",
            )
            yaml_on_disk = os.path.isfile(submission_path)
            if not yaml_on_disk and ctx.task_log and os.path.isfile(ctx.task_log):
                try:
                    round_num = int(round_slug.split("-", 1)[1])
                except (IndexError, ValueError):
                    round_num = -1
                if round_num > 0 and _recover_yaml_from_log(
                    ctx.task_log, submission_path, discuss_id, round_num, ctx.item_to
                ):
                    yaml_on_disk = True
                    print(
                        f"  [recover-yaml] recovered submission from log -> {submission_path}"
                    )
            if yaml_on_disk:
                new_lock = _MkdirLock(ctx.inbox_file + ".lock.d")
                if new_lock.acquire_with_retry(50, 0.1):
                    try:
                        if _set_inbox_status(
                            ctx.inbox_file,
                            ctx.item_id,
                            "launched",
                            "done",
                            fail_now,
                            "done_at",
                        ) or _set_inbox_status(
                            ctx.inbox_file,
                            ctx.item_id,
                            "running",
                            "done",
                            fail_now,
                            "done_at",
                        ):
                            _sqlite_mirror_dispatch(
                                ctx.project_dir,
                                ctx.item_id,
                                ctx.item_task,
                                ctx.item_to,
                                "done",
                                fail_now,
                            )
                            print(
                                f"Inbox item updated: {ctx.item_id} -> done "
                                f"(launcher rc={ctx.launcher_rc} but submission YAML "
                                f"present at {submission_path})"
                            )
                            return 0
                    finally:
                        new_lock.release()

    # Exit code 2 == permanent block. Pre-launch Pi isolation failures use the
    # same non-retryable path because no agent process ran and retrying without
    # correcting Git state would fail identically.
    permanent_block = ctx.launcher_rc == 2 or bool(ctx.prelaunch_failure_reason)

    # Read log tail for classifier
    log_tail_text = ""
    if os.path.isfile(ctx.task_log):
        try:
            from pathlib import Path as _P

            _lines = (
                _P(ctx.task_log)
                .read_text(encoding="utf-8", errors="replace")
                .splitlines()
            )
            log_tail_text = "\n".join(_lines[-50:])
        except Exception as e:
            _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
            log_tail_text = ""

    # Classify the failure. Isolation failures already have their precise,
    # actionable classification before any launcher exists.
    if ctx.prelaunch_failure_reason:
        failure_class = "worktree_isolation"
        failure_explain = ctx.prelaunch_failure_reason
    else:
        try:
            from superharness.engine.failure_classifier import (
                classify as _classify_failure,
            )

            _classification = _classify_failure(
                launcher_rc=ctx.launcher_rc, error_text="", log_tail=log_tail_text
            )
            failure_class = _classification.category
            failure_explain = _classification.explain
        except Exception as e:
            _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
            failure_class = "unknown"
            failure_explain = f"launcher exited with code {ctx.launcher_rc}"

    if ctx.prelaunch_failure_reason:
        fail_reason = ctx.prelaunch_failure_reason
        print(
            f"Pre-launch isolation failure for {ctx.item_id}: {fail_reason}",
            file=sys.stderr,
        )
    elif ctx.launcher_rc == 124:
        fail_reason = f"launcher timed out after {ctx.effective_timeout}s"
        print(
            f"Launcher timed out after {ctx.effective_timeout}s for {ctx.item_id}",
            file=sys.stderr,
        )
        # Discussion round timeouts are silent failures today — log FATAL
        # so operators can trace which agent/round was affected.
        if ctx.is_discussion:
            _log.error(
                "FATAL: discussion round dispatch timed out — "
                "agent=%s discussion=%s round_task=%s timeout=%ds",
                ctx.item_to,
                ctx.item_task.split("/round-")[0],
                ctx.item_task,
                ctx.effective_timeout,
            )
    elif ctx.launcher_rc < 0:
        # Signal death: killed by SIGKILL (-9), SIGTERM (-15), etc.
        import signal as _signal_mod

        sig_name = ""
        try:
            sig_name = _signal_mod.Signals(-ctx.launcher_rc).name
        except (ValueError, AttributeError):
            sig_name = str(ctx.launcher_rc)
        fail_reason = f"launcher killed by signal {sig_name}"
        print(
            f"Launcher killed by signal {sig_name} for {ctx.item_id}", file=sys.stderr
        )
    elif permanent_block:
        fail_reason = f"permanent block (lifecycle gate): {failure_explain}"
        print(
            f"Permanent block for {ctx.item_id}: lifecycle gate rejected. Not retrying.",
            file=sys.stderr,
        )
    else:
        fail_reason = f"{failure_class}: {failure_explain}"

    # auth_mismatch: reset cached auth state so the next dispatch re-evaluates
    # credentials and applies the correct model overrides.  For codex-cli we can
    # actively re-detect the account type via `codex login status`.  For other
    # agents (gemini-cli, opencode) we record the failure and let the retry use
    # whatever credentials are currently configured.
    if failure_class == "auth_mismatch":
        try:
            from superharness.engine.model_router import persist_agent_auth_state

            if ctx.item_to == "codex-cli":
                from superharness.engine.model_router import (
                    reset_codex_auth_cache,
                    detect_codex_auth_mode,
                )

                reset_codex_auth_cache()
                new_auth_mode = detect_codex_auth_mode()
                persist_agent_auth_state(
                    str(ctx.project_dir), "codex-cli", new_auth_mode
                )
                _log.warning(
                    "inbox_dispatch: auth_mismatch for codex-cli — cache reset, "
                    "re-detected auth_mode=%s, will retry with override model",
                    new_auth_mode,
                )
            else:
                persist_agent_auth_state(
                    str(ctx.project_dir), ctx.item_to, "auth_failure"
                )
                _log.warning(
                    "inbox_dispatch: auth_mismatch for %s — persisted failure state; "
                    "ensure credentials are valid for this agent and retry",
                    ctx.item_to,
                )
        except Exception as _auth_err:
            _log.warning(
                "inbox_dispatch: auth state reset failed: %s", _auth_err, exc_info=True
            )

    # quota: record a cooldown window so the watcher skips this agent during
    # fallback routing until the quota resets.  Default cooldown is 60 minutes.
    if failure_class == "quota":
        try:
            from superharness.engine.model_router import set_agent_quota_limited

            set_agent_quota_limited(str(ctx.project_dir), ctx.item_to, reset_minutes=60)
            _log.warning(
                "inbox_dispatch: quota exceeded for %s — marked quota-limited for 60 min; "
                "watcher will skip this agent in fallback routing until cooldown expires",
                ctx.item_to,
            )
        except Exception as _quota_err:
            _log.warning(
                "inbox_dispatch: quota state write failed: %s",
                _quota_err,
                exc_info=True,
            )

    if failure_class == "unknown" and ctx.launcher_rc == 1:
        try:
            from superharness.engine.db import get_connection, init_db

            conn = get_connection(ctx.project_dir)
            try:
                init_db(conn)
                hb = conn.execute(
                    "SELECT status FROM agent_heartbeats WHERE agent=?",
                    (ctx.item_to,),
                ).fetchone()
                if hb is None:
                    fail_reason = "agent daemon not running (no heartbeat)"
                elif hb["status"] == "zombie":
                    fail_reason = "agent daemon zombie (stale heartbeat)"
            finally:
                conn.close()
        except Exception:
            _log.warning(
                "_handle_failure: unexpected error: %s",
                sys.exc_info()[1],
                exc_info=True,
            )
            pass
    # Append structured diagnostic to the task log file so operators can trace
    # which agent/round failed and why — the Python logger is not visible to them.
    if ctx.is_discussion and ctx.task_log:
        try:
            with open(ctx.task_log, "a", encoding="utf-8") as _lf:
                _lf.write(
                    f"\n--- superharness failure diagnostic ---\n"
                    f"agent={ctx.item_to}\n"
                    f"round_task={ctx.item_task}\n"
                    f"exit_code={ctx.launcher_rc}\n"
                    f"reason={fail_reason}\n"
                    f"classification={failure_class}\n"
                    f"failed_at={fail_now}\n"
                    f"--- end diagnostic ---\n"
                )
        except Exception as _diag_err:
            _log.warning(
                "inbox_dispatch.py: could not write failure diagnostic: %s",
                _diag_err,
                exc_info=True,
            )
    new_lock = _MkdirLock(ctx.inbox_file + ".lock.d")
    # Record failure in decision ledger for debugging
    try:
        from superharness.engine.ledger_dao import decision_log

        decision_log(
            ctx.project_dir,
            "dispatch_failed",
            task_id=ctx.item_task,
            agent=ctx.item_to,
            reason=fail_reason,
            details={
                "exit_code": ctx.launcher_rc,
                "item_id": ctx.item_id,
                "classification": failure_class,
            },
        )
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    _mark_item_failed(
        ctx.inbox_file, ctx.item_id, fail_now, new_lock, reason=fail_reason
    )
    # Stamp structured failure metadata for auto_retry and dashboard surface
    try:
        _inbox_cmd(
            [
                "set_field",
                "--file",
                ctx.inbox_file,
                "--id",
                ctx.item_id,
                "--key",
                "failure_class",
                "--value",
                failure_class,
            ]
        )
        _inbox_cmd(
            [
                "set_field",
                "--file",
                ctx.inbox_file,
                "--id",
                ctx.item_id,
                "--key",
                "failure_explain",
                "--value",
                failure_explain,
            ]
        )
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    _sqlite_mirror_dispatch(
        ctx.project_dir,
        ctx.item_id,
        ctx.item_task,
        ctx.item_to,
        "failed",
        fail_now,
        reason=fail_reason,
    )

    item_max_retries = int(ctx.item.get("max_retries", 3))
    try:
        from superharness.engine.failure_classifier import classify as _classify_failure

        _classification = _classify_failure(
            launcher_rc=ctx.launcher_rc, error_text="", log_tail=log_tail_text
        )
        if permanent_block or not _classification.retryable:
            # Push retry_count to max_retries so the watcher's next pass sees it as
            # retry-exhausted and does not pick it up again.
            _inbox_cmd(
                [
                    "set_field",
                    "--file",
                    ctx.inbox_file,
                    "--id",
                    ctx.item_id,
                    "--key",
                    "retry_count",
                    "--value",
                    str(item_max_retries),
                ]
            )
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    try:
        from superharness.commands.notify_desktop import notify_task_event

        notify_task_event(ctx.item_task, "failed", ctx.item_to)
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    # Record failure pattern for next dispatch
    try:
        from superharness.engine.failure_patterns import record_failure

        error_snippet = log_tail_text
        if ctx.launcher_rc == 124:
            error_snippet = f"timed out\n{error_snippet}"
        if error_snippet:
            record_failure(
                ctx.exec_project, ctx.item_task, error_snippet, agent=ctx.item_to
            )
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    import time as _time

    _elapsed = _time.time() - ctx.launch_start

    # Record benchmark for failed launch
    try:
        from superharness.engine.benchmark import record_dispatch

        outcome = "timeout" if ctx.launcher_rc == 124 else "failed"
        record_dispatch(ctx.exec_project, ctx.item_task, ctx.item_to, outcome, _elapsed)
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass
    _sqlite_record_review(
        ctx.project_dir,
        owner=ctx.item_to,
        task_type="dispatch",
        duration_s=_elapsed,
        score=0.0,
        failed=True,
        now=fail_now,
    )

    return 1


def _skip_already_done_discussion_round(ctx: DispatchContext) -> bool:
    """Return True (and short-circuit launch) if this inbox item is a
    discussion-round task whose round-N-<agent>.yaml already exists OR
    whose discussion has been closed.

    The leftover-pending storm vector from §8 of the discuss-dispatch
    bug report: the watcher restarts, finds stale pending inbox items
    for rounds the agents already finished, and dispatches them via
    the normal claim_next loop (which knows nothing about discussion
    state). This guard runs after _prepare_launch_context so the item
    is already transitioned to 'launched'; we flip it back to 'done'
    and let the caller short-circuit.
    """
    if not ctx.is_discussion or ctx.print_only:
        return False
    parts = ctx.item_task.split("/")
    if len(parts) != 2:
        return False
    discuss_id, round_slug = parts
    submission_path = os.path.join(
        ctx.exec_project,
        ".superharness",
        "discussions",
        discuss_id,
        f"{round_slug}-{ctx.item_to}.yaml",
    )
    yaml_exists = os.path.isfile(submission_path)

    discussion_closed = False
    try:
        from superharness.engine.db import get_connection, init_db
        from superharness.engine import discussions_dao

        conn = get_connection(ctx.project_dir)
        try:
            init_db(conn)
            disc = discussions_dao.get(conn, discuss_id)
            if disc is not None and disc.status not in ("active",):
                discussion_closed = True
        finally:
            conn.close()
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        # If we can't check, fall through — never block legitimate dispatch.
        pass

    if not (yaml_exists or discussion_closed):
        return False

    skip_reason = (
        "submission YAML already present"
        if yaml_exists
        else "discussion not active (status check)"
    )
    now = _now_utc()
    new_lock = _MkdirLock(ctx.inbox_file + ".lock.d")
    if not new_lock.acquire_with_retry(50, 0.1):
        return False
    try:
        if (
            _set_inbox_status(
                ctx.inbox_file, ctx.item_id, "launched", "done", now, "done_at"
            )
            or _set_inbox_status(
                ctx.inbox_file, ctx.item_id, "running", "done", now, "done_at"
            )
            or _set_inbox_status(
                ctx.inbox_file, ctx.item_id, "pending", "done", now, "done_at"
            )
        ):
            _set_inbox_field(
                ctx.inbox_file, ctx.item_id, "failed_reason", f"skipped: {skip_reason}"
            )
            _sqlite_mirror_dispatch(
                ctx.project_dir,
                ctx.item_id,
                ctx.item_task,
                ctx.item_to,
                "done",
                now,
                reason=f"skipped: {skip_reason}",
            )
            print(
                f"Inbox item updated: {ctx.item_id} -> done "
                f"(skipped launch: {skip_reason})"
            )
            return True
    finally:
        new_lock.release()
    return False


def _execute_agent(ctx: DispatchContext) -> None:
    # Spawn launcher. --print-only short-circuits here: the caller only wants
    # to see the dispatch decision, not actually invoke the underlying agent
    # CLI (which may not be installed in test/CI environments).
    if ctx.print_only:
        ctx.launcher_rc = 0
        print(f"[print-only] would launch: {' '.join(ctx.launch_args)}")
    else:
        import time as _time

        ctx.launch_start = _time.time()
        if ctx.effective_timeout > 0:
            _reliable_run_started(ctx)
            ctx.launcher_rc = _run_with_timeout(
                ctx.effective_timeout,
                ctx.wrapped_args,
                inbox_file=ctx.inbox_file,
                item_id=ctx.item_id,
                env=ctx.spawn_env,
            )
        else:
            _preexec = os.setsid if hasattr(os, "setsid") else None
            proc = subprocess.Popen(
                ctx.wrapped_args, preexec_fn=_preexec, env=ctx.spawn_env
            )
            _reliable_run_started(ctx, proc.pid)
            _inbox_cmd(
                [
                    "set_field",
                    "--file",
                    ctx.inbox_file,
                    "--id",
                    ctx.item_id,
                    "--key",
                    "pid",
                    "--value",
                    str(proc.pid),
                ]
            )
            while proc.poll() is None:
                if _reliable_run(ctx):
                    _reliable_run_heartbeat(ctx, proc.pid)
                _time.sleep(10)
            ctx.launcher_rc = proc.returncode
        _inbox_cmd(
            [
                "set_field",
                "--file",
                ctx.inbox_file,
                "--id",
                ctx.item_id,
                "--key",
                "pid",
                "--value",
                "",
            ]
        )

    return None


def _prepare_execution(ctx: DispatchContext) -> None:
    # Build launcher args: Delegate to the Python `superharness delegate` command which
    # builds the proper prompt from the task context, then resolves the target's
    # launcher script via adapter_registry.
    launch_args = [
        _get_python(),
        "-m",
        "superharness.commands.delegate",
        "--to",
        ctx.item_to,
        "--project",
        ctx.exec_project,
        "--task",
        ctx.item_task,
    ]
    task_status = ""
    try:
        from superharness.engine import state_reader as _sr

        _task_row = _sr.get_task(ctx.project_dir, ctx.item_task)
        if _task_row:
            task_status = str(_task_row.get("status", ""))
    except Exception as _e:
        _log.warning(
            "_prepare_execution: could not read SQLite task status: %s",
            _e,
            exc_info=True,
        )
    if task_status == "review_requested":
        launch_args.append("--for-review")
    run_prompt = ""
    run_model = ""
    if ctx.run_id:
        from superharness.engine.state_errors import StateError

        try:
            from superharness.engine import runs_dao
            from superharness.engine.db import managed_connection

            with managed_connection(ctx.project_dir) as conn:
                run = runs_dao.get_run(conn, ctx.run_id)
            if run is not None:
                run_model = run.model or ""
                prompt_value = run.result_json.get("prompt")
                run_prompt = prompt_value if isinstance(prompt_value, str) else ""
        except (OSError, sqlite3.Error, StateError) as _e:
            _log.warning(
                "_prepare_execution: could not read linked run metadata: %s",
                _e,
                exc_info=True,
            )
    if run_model:
        launch_args.extend(["--model", run_model])
    if bool(ctx.item.get("plan_only", False)):
        launch_args.append("--plan-only")
    elif ctx.non_interactive:
        # Implementation tasks in autonomous mode need YOLO permissions
        launch_args.append("--yolo")

    if ctx.print_only:
        launch_args.append("--print-only")
    if ctx.non_interactive:
        launch_args.append("--non-interactive")
    if ctx.codex_bypass:
        launch_args.append("--codex-bypass")

    ctx.launch_args = launch_args

    # Per-task log file for live monitoring in launcher-logs/
    launcher_log_dir = os.path.join(ctx.project_dir, ".superharness", "launcher-logs")
    os.makedirs(launcher_log_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_item_task = _safe_task_id_for_path(ctx.item_task)
    ctx.task_log = os.path.join(
        launcher_log_dir, f"{safe_item_task}-{ctx.item_to}-{timestamp}.log"
    )

    # Rotate old logs (keep last 5 per task+agent)
    from pathlib import Path
    from superharness.commands.delegate import _rotate_launcher_logs

    _rotate_launcher_logs(Path(launcher_log_dir), safe_item_task, ctx.item_to, keep=5)

    # Pass log path to delegate so SDK runner's JSONL tailer writes to the same file
    spawn_env = os.environ.copy()
    spawn_env["SUPERHARNESS_LAUNCHER_LOG"] = ctx.task_log
    # Force Python launcher to flush stdout/stderr immediately — prevents line-dropping
    # when the process is wrapped in a PTY (script command) under load.
    spawn_env["PYTHONUNBUFFERED"] = "1"
    if ctx.run_id:
        spawn_env["SUPERHARNESS_RUN_ID"] = ctx.run_id
        spawn_env["SUPERHARNESS_RUN_RESULT_PATH"] = _result_artifact_path(ctx)
        try:
            os.unlink(spawn_env["SUPERHARNESS_RUN_RESULT_PATH"])
        except FileNotFoundError:
            pass
    if run_prompt:
        spawn_env["SUPERHARNESS_RUN_PROMPT"] = run_prompt
    if ctx.non_interactive:
        spawn_env["SUPERHARNESS_CONFIRM_NON_INTERACTIVE"] = "YES"
    # When dispatching from a git worktree, preserve the original project path
    # so delegate reads state from the correct XDG database.
    if ctx.project_dir and (
        ctx.worktree_dir or (ctx.run_id and ctx.exec_project != ctx.project_dir)
    ):
        spawn_env["SUPERHARNESS_STATE_PROJECT"] = ctx.project_dir
    ctx.spawn_env = spawn_env

    # Wrap in `script` to capture PTY output (bash launcher needs a terminal).
    import platform

    if os.environ.get("SUPERHARNESS_NO_PTY_WRAP", "").strip() in ("1", "true", "yes"):
        ctx.wrapped_args = launch_args
    elif platform.system() == "Darwin":
        ctx.wrapped_args = ["script", "-q", "-F", ctx.task_log] + launch_args
    else:
        import shlex

        ctx.wrapped_args = [
            "script",
            "-q",
            "-e",
            "-f",
            "-c",
            shlex.join(launch_args),
            ctx.task_log,
        ]

    return None


def _resolve_execution_context(ctx: DispatchContext) -> int | None:
    # Determine execution project (worker mode)
    exec_project = ctx.item_project
    try:
        proj_harness_real = os.path.realpath(
            os.path.join(ctx.project_dir, ".superharness")
        )
        item_harness_real = os.path.realpath(
            os.path.join(ctx.item_project, ".superharness")
        )
        if (
            not ctx.run_id
            and proj_harness_real == item_harness_real
            and ctx.project_dir != ctx.item_project
            and os.path.isdir(proj_harness_real)
        ):
            exec_project = ctx.project_dir
    except OSError:
        pass
    ctx.exec_project = exec_project

    # Auto-calculate timeout from task effort if not explicitly set
    if ctx.run_id:
        # Reliable Runs are governed by Run heartbeat/reconciliation.  The
        # legacy effort/default timeout is not a valid liveness signal.  A
        # hard cap is opt-in and explicit so a quiet but healthy child lives.
        configured = os.environ.get(
            "SUPERHARNESS_RELIABLE_HARD_TIMEOUT_SECONDS", ""
        ).strip()
        try:
            ctx.effective_timeout = max(0, int(configured)) if configured else 0
        except ValueError:
            ctx.effective_timeout = 0
    else:
        ctx.effective_timeout = ctx.launcher_timeout
        if ctx.launcher_timeout == 0:
            ctx.effective_timeout = _get_task_effort_timeout(
                ctx.project_dir, ctx.item_task
            )

    # Worktree isolation: Pi always gets an isolated checkout for autonomous
    # inbox work, including discussions. Other agents retain the existing
    # dirty-worktree-only policy and discussion exemption.
    ctx.is_discussion = "/round-" in ctx.item_task or ctx.item_task.startswith(
        "discuss-"
    )
    automated_dispatch = ctx.non_interactive and not ctx.print_only
    pi_requires_worktree = automated_dispatch and ctx.item_to == "pi" and not ctx.run_id
    dirty_requires_worktree = (
        automated_dispatch
        and not ctx.is_discussion
        and not pi_requires_worktree
        and not ctx.run_id
        and _has_dirty_worktree(ctx.exec_project)
    )
    if pi_requires_worktree:
        # Watcher workers deliberately omit .git. Pi must branch from the task's
        # canonical project checkout, then execute only inside the new worktree.
        worktree_source = ctx.item_project
        preflight_error = _pi_worktree_preflight_error(worktree_source)
        if preflight_error:
            ctx.prelaunch_failure_reason = (
                "Pi non-interactive dispatch requires an isolated Git worktree: "
                f"{preflight_error}. Commit an initial revision and retry."
            )
            print(
                ctx.prelaunch_failure_reason,
                file=sys.stderr,
            )
            return 1
    else:
        worktree_source = ctx.exec_project

    if pi_requires_worktree or dirty_requires_worktree:
        ctx.worktree_dir = _git_worktree_add(worktree_source, ctx.item_task)
        if ctx.worktree_dir:
            ctx.worktree_source_dir = worktree_source
            reason = (
                "Pi non-interactive isolation"
                if pi_requires_worktree
                else "main worktree is dirty"
            )
            print(f"Dispatching in worktree: {ctx.worktree_dir} ({reason})")
            ctx.exec_project = ctx.worktree_dir
            # Record worktree path on task for dashboard visibility
            try:
                from superharness.engine.db import get_connection, init_db

                conn = get_connection(ctx.project_dir)
                init_db(conn)
                conn.execute(
                    "UPDATE tasks SET worktree_path=? WHERE id=?",
                    (ctx.worktree_dir, ctx.item_task),
                )
                conn.commit()
                conn.close()
            except Exception as e:
                _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
                pass
        else:
            if pi_requires_worktree:
                ctx.prelaunch_failure_reason = (
                    "Pi non-interactive dispatch requires an isolated Git worktree, "
                    "but superharness could not create one. Resolve the Git worktree "
                    "error and retry; dispatch did not fall back to the main checkout."
                )
                print(
                    ctx.prelaunch_failure_reason,
                    file=sys.stderr,
                )
                return 1
            # Existing behavior for other agents: pause a dirty dispatch.
            pause_now = _now_utc()
            if _mark_item_paused_dirty(ctx.inbox_file, ctx.item_id, pause_now):
                return 0

    return None


def _log_dispatch_error(project_dir: str, error: str) -> None:
    """Log a dispatch error to the project's watcher error log. Never raises."""
    try:
        from datetime import datetime, timezone

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        log_path = os.path.join(project_dir, ".superharness", "watcher-errors.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] dispatch: {error}\n")
    except Exception as e:
        _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
        pass


# ---------------------------------------------------------------------------
# Discussion dispatch helpers
# ---------------------------------------------------------------------------


def _classify_launch_failure(exit_code: int, log_tail: str) -> dict:
    """Classify a launcher failure by exit code and log output.

    Returns {"action": "pause"|"fail", "retry_after_minutes": int}.
    Quota exhaustion (exit 1 + quota keyword) → pause so the watcher retries
    after the quota resets instead of burning the retry budget immediately.
    """
    import re as _re

    _QUOTA_KEYWORDS = (
        "quota_exhausted",
        "quota will reset",
        "terminalquotaerror",
        "exhausted your capacity",
        # Gemini / Google API signals
        "resource_exhausted",
        "usage limit",
        "you've reached your",
        "quota has been exceeded",
        "free tier",
    )
    log_lower = log_tail.lower()
    is_quota = exit_code == 1 and any(kw in log_lower for kw in _QUOTA_KEYWORDS)
    if is_quota:
        m = _re.search(r"reset after\s+(\d+)m", log_lower)
        retry_minutes = int(m.group(1)) if m else 30
        return {"action": "pause", "retry_after_minutes": retry_minutes}
    return {"action": "fail", "retry_after_minutes": 0}


def _prepare_launch_context(ctx: DispatchContext) -> None:
    """Build launch args, apply discussion-specific timeout and model overrides."""
    _prepare_execution(ctx)

    # Discussion timeout and model resolution: read the discussion task from
    # SQLite once, then derive both timeout (from effort) and model (from
    # model_tier). Both are set by classify_task at discussion creation time.
    #
    # Timeout order: env var > task effort > fallback (900s)
    # Model order:   env var > task model_tier > resolve_model > fallback
    #
    # Applies to ALL agents uniformly — no more agent-by-agent hardcoding.
    if ctx.is_discussion:
        tier = "standard"
        effort = "medium"
        try:
            from superharness.engine.db import get_connection, init_db
            from superharness.engine import tasks_dao

            conn = get_connection(ctx.project_dir)
            try:
                init_db(conn)
                task = tasks_dao.get(conn, ctx.item_task)
                if task:
                    if task.model_tier and task.model_tier != "standard":
                        tier = task.model_tier
                    if task.effort:
                        effort = task.effort
            finally:
                conn.close()
        except Exception as e:
            _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)

        # Fall back to profile config if task has no model_tier set
        if tier == "standard":
            try:
                from superharness.commands.config import get_config_value

                profile_tier = get_config_value(
                    ctx.project_dir, "discussion_model_tier"
                )
                if profile_tier and str(profile_tier) in ("mini", "standard", "max"):
                    tier = str(profile_tier)
            except Exception as e:
                _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)

        # Timeout from effort (env var override wins)
        if ctx.effective_timeout == 0:
            _override = os.environ.get(
                "SUPERHARNESS_DISCUSSION_ROUND_TIMEOUT_SECONDS", ""
            ).strip()
            if _override:
                try:
                    ctx.effective_timeout = int(_override)
                except ValueError:
                    ctx.effective_timeout = DISCUSSION_ROUND_TIMEOUT_SECONDS
            elif effort == "low":
                ctx.effective_timeout = DISCUSSION_TIMEOUT_LOW
            elif effort == "high":
                ctx.effective_timeout = DISCUSSION_TIMEOUT_HIGH
            else:
                ctx.effective_timeout = DISCUSSION_TIMEOUT_MEDIUM  # medium or unknown

        # Resolve tier → agent-specific model, with per-agent routing.
        # For discussions, primary reasoners (claude, opencode) get the
        # full classified tier; secondary agents (gemini, codex) are capped
        # at standard for cost efficiency on max-tier topics.
        try:
            from superharness.engine.model_router import (
                resolve_model_for_tier,
                route_discussion_tier,
            )

            agent_tier = route_discussion_tier(tier, ctx.item_to)
            model = resolve_model_for_tier(ctx.item_to, agent_tier, ctx.project_dir)
        except Exception as e:
            _log.warning("inbox_dispatch.py unexpected error: %s", e, exc_info=True)
            model = "claude-sonnet-4-6"  # absolute last-resort fallback

        # Env var override (agent-specific, back compat)
        env_key = f"SUPERHARNESS_{ctx.item_to.upper().replace('-', '_')}_MODEL"
        if ctx.item_to == "claude-code":
            env_key = "SUPERHARNESS_CLAUDE_MODEL"  # legacy short name
        elif ctx.item_to == "gemini-cli":
            env_key = "SUPERHARNESS_GEMINI_MODEL"  # legacy short name
        model = os.environ.get(env_key, model)

        ctx.launch_args = ctx.launch_args + ["--model", model]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    import argparse

    if argv is None:
        argv = sys.argv[1:]

    class _CapUsage(argparse.HelpFormatter):
        def _format_usage(self, usage, actions, groups, prefix):
            return super()._format_usage(usage, actions, groups, "Usage: ")

    parser = argparse.ArgumentParser(
        prog="inbox_dispatch",
        description="Dispatch next pending inbox item to target launcher",
        formatter_class=_CapUsage,
        add_help=True,
    )
    parser.add_argument("--project", "-p", required=True)
    parser.add_argument("--to", default=None, dest="target_filter")
    parser.add_argument("--print-only", action="store_true", default=False)
    parser.add_argument("--non-interactive", action="store_true", default=False)
    parser.add_argument("--codex-bypass", action="store_true", default=False)
    parser.add_argument("--launcher-timeout", default="0")

    opts = parser.parse_args(argv)

    # Validate launcher-timeout as non-negative integer
    try:
        launcher_timeout = int(opts.launcher_timeout)
        if launcher_timeout < 0:
            raise ValueError
    except (ValueError, TypeError):
        print("--launcher-timeout must be a non-negative integer", file=sys.stderr)
        sys.exit(2)

    if opts.target_filter:
        from superharness.engine.adapter_registry import list_adapters

        valid_targets = list_adapters()
        if opts.target_filter not in valid_targets:
            print(
                f"--to must be one of: {', '.join(valid_targets) or 'none'}",
                file=sys.stderr,
            )
            sys.exit(2)

    try:
        rc = dispatch(
            project_dir=opts.project,
            target_filter=opts.target_filter or None,
            print_only=opts.print_only,
            non_interactive=opts.non_interactive,
            codex_bypass=opts.codex_bypass,
            launcher_timeout=launcher_timeout,
        )
    except Exception as e:
        _log.warning("dispatch: unexpected error: %s", e, exc_info=True)
        _log_dispatch_error(opts.project, f"dispatch raised: {e}")
        raise
    sys.exit(rc)


if __name__ == "__main__":
    main()

"""Watcher helpers for the feature-gated reliable orchestrator."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable

from superharness.engine.reliable_orchestrator_gate import (
    is_reliable_orchestrated_task,
)


def reliable_task_ids(project_dir: str) -> set[str]:
    from superharness.engine.state_reader import get_tasks

    try:
        tasks = get_tasks(project_dir)
    except (OSError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error):
        return set()
    return {
        str(task.get("id", ""))
        for task in tasks
        if isinstance(task, dict)
        and task.get("id")
        and is_reliable_orchestrated_task(task)
    }


def is_reliable_task_id(conn: sqlite3.Connection, task_id: str) -> bool:
    from superharness.engine import tasks_dao

    task = tasks_dao.get(conn, task_id)
    return task is not None and is_reliable_orchestrated_task(task)


def tick(
    project_dir: str,
    now: str,
    log_error: Callable[[str, str, str], None],
) -> bool:
    """Acquire/renew the lease and run one reliable-orchestrator tick."""
    try:
        from superharness.engine import orchestrator_lease
        from superharness.engine.db import get_connection, init_db

        host, pid, pid_starttime, owner_id = (
            orchestrator_lease.current_process_identity()
        )
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            if not orchestrator_lease.acquire(
                conn,
                owner_id=owner_id,
                host=host,
                pid=pid,
                pid_starttime=pid_starttime,
                now=now,
            ):
                return False
        finally:
            conn.close()

        from superharness.engine.lifecycle_orchestrator import LifecycleOrchestrator

        LifecycleOrchestrator(project_dir).tick()
        return True
    except (
        OSError,
        RuntimeError,
        ValueError,
        TypeError,
        KeyError,
        ImportError,
        sqlite3.Error,
    ) as exc:
        log_error(project_dir, "reliable_orchestrator", str(exc))
        return False


def release(project_dir: str) -> None:
    try:
        from superharness.engine import orchestrator_lease
        from superharness.engine.db import get_connection, init_db

        _, _, _, owner_id = orchestrator_lease.current_process_identity()
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            orchestrator_lease.release(conn, owner_id=owner_id)
        finally:
            conn.close()
    except (
        OSError,
        RuntimeError,
        ValueError,
        TypeError,
        KeyError,
        ImportError,
        sqlite3.Error,
    ):
        return

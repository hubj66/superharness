"""Inbox garbage collector — reconcile stale inbox items against contract status.

Reads from SQLite via state_reader; writes to SQLite via inbox_dao/tasks_dao.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import logging

logger = logging.getLogger(__name__)

# Inbox statuses eligible for GC (terminal-but-stale)
GC_ELIGIBLE = {"stopped", "failed", "stale", "paused"}
TASK_PAST_DISPATCH = {
    "done",
    "report_ready",
    "review_requested",
    "review_passed",
    "review_failed",
}


def run_gc(project_dir: str | Path, dry_run: bool = False) -> dict:
    project_dir = Path(project_dir)

    # Read tasks from SQLite
    from superharness.engine.state_reader import get_tasks

    tasks = get_tasks(str(project_dir))
    task_statuses = {
        str(t.get("id", "")): str(t.get("status", ""))
        for t in tasks
        if isinstance(t, dict)
    }
    reliable_task_ids = {
        str(t.get("id"))
        for t in tasks
        if isinstance(t, dict) and t.get("workflow") == "reliable-orchestrator"
    }

    # Read inbox from SQLite
    from superharness.engine.state_reader import get_inbox_items

    items = get_inbox_items(str(project_dir))
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    reconciled = 0
    would_reconcile = 0
    details = []

    for item in items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status", ""))
        task_id = str(item.get("task", item.get("task_id", "")))
        item_id = str(item.get("id", ""))

        if task_id in reliable_task_ids:
            continue

        if status not in GC_ELIGIBLE:
            continue

        contract_status = task_statuses.get(task_id, "")
        if (
            contract_status in ("done", "archived")
            or contract_status in TASK_PAST_DISPATCH
        ):
            if dry_run:
                details.append(
                    {"id": item_id, "task": task_id, "from": status, "to": "done"}
                )
                would_reconcile += 1
            else:
                # Write to SQLite directly
                try:
                    from superharness.engine.db import get_connection, init_db
                    from superharness.engine import inbox_dao, ledger_dao

                    conn = get_connection(str(project_dir))
                    try:
                        init_db(conn)
                        inbox_dao.update_status(
                            conn, item_id, from_status=status, to_status="done", now=now
                        )
                        # Audit each reconcile in the ledger so consumers and
                        # tests can see what GC touched.
                        try:
                            ledger_dao.record(
                                conn,
                                agent="inbox_gc",
                                action="gc_reconcile",
                                task_id=task_id,
                                details={
                                    "item_id": item_id,
                                    "from": status,
                                    "to": "done",
                                },
                                now=now,
                            )
                        except Exception as e:
                            logger.warning(
                                "inbox_gc.py unexpected error: %s", e, exc_info=True
                            )
                            pass
                        conn.commit()
                        reconciled += 1
                        details.append(
                            {
                                "id": item_id,
                                "task": task_id,
                                "from": status,
                                "to": "done",
                            }
                        )
                    finally:
                        conn.close()
                except Exception as e:
                    logger.warning("inbox_gc.py unexpected error: %s", e, exc_info=True)
                    pass
    if reconciled > 0:
        print(f"Inbox GC: {reconciled} reconciled, {would_reconcile} would reconcile")
    return {
        "reconciled": reconciled,
        "would_reconcile": would_reconcile,
        "items": details,
    }

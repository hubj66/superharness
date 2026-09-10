from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any, cast

RELIABLE_ORCHESTRATOR_WORKFLOW = "reliable-orchestrator"


def is_reliable_orchestrated_task(task: object) -> bool:
    """Return True for tasks opted into the future reliable orchestrator path."""

    workflow = _get_task_value(task, "workflow")
    if isinstance(workflow, str) and workflow.strip() == RELIABLE_ORCHESTRATOR_WORKFLOW:
        return True

    extras = _get_task_value(task, "extras_json")
    if isinstance(extras, str) and extras.strip():
        try:
            parsed = json.loads(extras)
        except json.JSONDecodeError:
            return False
        if isinstance(parsed, dict):
            return bool(parsed.get("reliable_orchestrator"))
    if isinstance(extras, Mapping):
        return bool(extras.get("reliable_orchestrator"))
    return False


def reliable_run_lifecycle_violation(conn: Any, task: object) -> str | None:
    """Return an error when a reliable agent process attempts task mutation.

    The public CLI may still mutate tasks for humans and legacy workflows. A
    durable reliable Run is the narrow boundary that makes lifecycle writes
    agent-forbidden; the orchestrator uses DAO writes directly and does not set
    this process environment marker.
    """
    run_id = os.environ.get("SUPERHARNESS_RUN_ID", "").strip()
    if not run_id:
        return None
    if not is_reliable_orchestrated_task(task):
        return None

    from superharness.engine import runs_dao, tasks_dao

    run = runs_dao.get_run(conn, run_id)
    if run is None:
        return (
            f"forbidden: reliable Run '{run_id}' was not found; "
            "agent lifecycle mutation is denied"
        )
    run_task = tasks_dao.get(conn, run.task_id)
    if run_task is not None and is_reliable_orchestrated_task(run_task):
        return (
            "forbidden: LifecycleOrchestrator owns task lifecycle for "
            f"reliable Run '{run_id}'"
        )
    return None


def _get_task_value(task: object, key: str) -> Any:
    if isinstance(task, Mapping):
        return task.get(key)
    if hasattr(task, "keys"):
        keys = task.keys()
        if key in keys:
            return cast(Any, task)[key]
    return getattr(task, key, None)

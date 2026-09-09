from __future__ import annotations

import json
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


def _get_task_value(task: object, key: str) -> Any:
    if isinstance(task, Mapping):
        return task.get(key)
    if hasattr(task, "keys"):
        keys = task.keys()
        if key in keys:
            return cast(Any, task)[key]
    return getattr(task, key, None)

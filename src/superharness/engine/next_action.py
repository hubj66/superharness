"""next_action — derive the recommended next move for a task.

Pure leaf module: no imports from other superharness engine modules.
Called by adapter_payload to populate next_action per task in schema v1.3.
Centralizes lifecycle and workflow logic (previously in lifecycle.py).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Canonical status enum (all statuses the state machine recognises)
# ---------------------------------------------------------------------------

ALL_STATUSES: list[str] = [
    "pending",
    "todo",
    "plan_proposed",
    "plan_approved",
    "in_progress",
    "pending_user_approval",
    "report_ready",
    "review_requested",
    "review_passed",
    "review_failed",
    "done",
    "failed",
    "stopped",
    "blocked",
    "waiting_input",
    "paused",
    "archived",
    "pr_open",
]

TERMINAL_STATUSES = frozenset({"done", "failed", "stopped"})
RELIABLE_ORCHESTRATOR_WORKFLOW = "reliable-orchestrator"

_RELIABLE_RUN_KIND_DISPATCH_STATUSES: dict[str, frozenset[str]] = {
    "plan": frozenset({"todo", "plan_proposed"}),
    "implement": frozenset({"plan_approved", "in_progress"}),
    "fallback": frozenset({"in_progress"}),
    "review": frozenset({"review_requested"}),
    "repair": frozenset({"review_failed", "in_progress"}),
    "ship": frozenset(),
}

_DISC_ROUND_RE = re.compile(r"^(discuss-[^/]+)/round-(\d+)$")


# ---------------------------------------------------------------------------
# Mapping: status -> (recommended, legal_transitions, reason)
#
# recommended: one member of legal[], or None when terminal / waiting
# legal: valid next statuses the owner can trigger from this state
# reason: short human-readable hint shown in the UI
# ---------------------------------------------------------------------------

_MAPPING: dict[str, tuple[Optional[str], list[str], str]] = {
    # "pending" is the initial status for a decomposed-subtask row (see
    # engine/subtask.py and commands/delegate.py's _record_decomposition) —
    # not part of the top-level task lifecycle proper, but a real value
    # written to tasks.status, so it needs the same floor as every other
    # status (migration v35's CHECK constraint) and the same transitions as
    # "todo", which is the closest equivalent ("not yet started").
    "pending": (
        "plan_proposed",
        ["plan_proposed", "waiting_input"],
        "author a plan handoff before dispatch (subtask default status)",
    ),
    "todo": (
        "plan_proposed",
        ["plan_proposed", "waiting_input"],
        "author a plan handoff before dispatch",
    ),
    "plan_proposed": (
        "plan_approved",
        ["plan_approved", "todo"],
        "review the plan; approve or send back to todo",
    ),
    "plan_approved": (
        "in_progress",
        ["in_progress", "plan_proposed"],
        "dispatch to the agent",
    ),
    "in_progress": (
        None,
        [
            "report_ready",
            "pr_open",
            "pending_user_approval",
            "stopped",
            "failed",
            "blocked",
            "waiting_input",
        ],
        "agent is working; wait for report_ready or pending_user_approval",
    ),
    "pending_user_approval": (
        "in_progress",
        ["in_progress", "stopped"],
        "answer the agent question then resume",
    ),
    "report_ready": (
        "review_passed",
        ["review_passed", "review_failed", "review_requested"],
        "review diff and handoff, then mark passed or failed",
    ),
    "review_requested": (
        "review_passed",
        ["review_passed", "review_failed"],
        "complete the review",
    ),
    "review_passed": (
        "done",
        ["done", "review_failed"],
        "close the task",
    ),
    "review_failed": (
        "plan_proposed",
        ["in_progress", "plan_proposed", "todo"],
        "revise the plan and re-propose",
    ),
    "done": (
        None,
        [],
        "task is complete",
    ),
    "failed": (
        "plan_proposed",
        ["plan_proposed", "todo", "stopped"],
        "revise the plan and retry, or stop",
    ),
    "stopped": (
        "in_progress",
        ["in_progress", "plan_proposed", "todo"],
        "resume or reopen with a revised plan",
    ),
    "blocked": (
        None,
        ["todo", "plan_proposed"],
        "resolve the blocker first",
    ),
    "waiting_input": (
        None,
        ["in_progress", "pending_user_approval", "todo", "plan_proposed"],
        "waiting for external input",
    ),
    "paused": (
        "in_progress",
        ["in_progress", "stopped"],
        "resume or stop",
    ),
    "archived": (
        None,
        [],
        "task has been archived",
    ),
    "pr_open": (
        "review_requested",
        ["review_requested", "review_passed", "review_failed"],
        "review and merge the open PR",
    ),
}


# ---------------------------------------------------------------------------
# Human-readable labels for every task status (used by dashboard + tests)
# ---------------------------------------------------------------------------

_STATUS_LABELS: dict[str, str] = {
    "todo": "To Do",
    "plan_proposed": "Plan Proposed",
    "plan_approved": "Plan Approved",
    "in_progress": "In Progress",
    "pending_user_approval": "Pending User Approval",
    "report_ready": "Report Ready",
    "review_requested": "Review Requested",
    "review_passed": "Review Passed",
    "review_failed": "Review Failed",
    "pr_open": "PR Open",
    "done": "Done",
    "failed": "Failed",
    "stopped": "Stopped",
    "blocked": "Blocked",
    "waiting_input": "Waiting Input",
    "paused": "Paused",
    "archived": "Archived",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass
class NextAction:
    recommended: Optional[str]
    legal: list[str]
    reason: str

    def as_dict(self) -> dict:
        return {
            "recommended": self.recommended,
            "legal": self.legal,
            "reason": self.reason,
        }

    @classmethod
    def compute(cls, status: str) -> "NextAction":
        """Return the NextAction for the given status."""
        return next_action(status)


def next_action(status: str) -> NextAction:
    """Return the NextAction for a task in the given status.

    Unknown statuses return an empty legal list with a generic reason so
    consumers degrade gracefully when the state machine evolves.
    """
    entry = _MAPPING.get(status)
    if entry is None:
        return NextAction(
            recommended=None, legal=[], reason=f"unknown status: {status}"
        )
    recommended, legal, reason = entry
    return NextAction(recommended=recommended, legal=legal, reason=reason)


def infer_workflow(task_id: str, task_obj: dict | None) -> str:
    """Return the workflow name for a task.

    Priority: explicit `workflow` field on the task → discussion-round pattern
    → default `implementation`.
    """
    workflow = ""
    if isinstance(task_obj, dict):
        workflow = str(task_obj.get("workflow", "") or "").strip().lower()
    if workflow:
        return workflow
    if _DISC_ROUND_RE.match(task_id):
        return "discussion"
    return "implementation"


def allowed_statuses_for_workflow(
    workflow: str, *, for_review: bool = False
) -> set[str]:
    """Return the set of statuses at which a task is dispatchable.

    Terminal statuses (done/failed/stopped) are not included here — callers
    must handle them separately because reconcile logic differs (failed and
    stopped are re-dispatchable; done is not).
    """
    if workflow == "implementation":
        allowed = {
            "plan_approved",
            "in_progress",
            "report_ready",
            "review_passed",
            "pr_open",
            "review_failed",
            "pending_user_approval",
        }
        if for_review:
            allowed.add("review_requested")
        return allowed
    if workflow == "quick":
        return {"todo", "in_progress", "report_ready", "failed", "stopped"}
    if workflow == "note":
        return {"todo", "in_progress", "failed", "stopped"}
    if workflow == "discussion":
        return {"todo", "in_progress"}
    if workflow == "review":
        allowed = {"todo", "in_progress", "review_requested", "review_failed"}
        if for_review:
            allowed.add("review_passed")
        return allowed
    if workflow == "approval":
        return {"pending_user_approval"}
    if workflow == RELIABLE_ORCHESTRATOR_WORKFLOW:
        if for_review:
            return set(_RELIABLE_RUN_KIND_DISPATCH_STATUSES["review"])
        return set(
            _RELIABLE_RUN_KIND_DISPATCH_STATUSES["implement"]
            | _RELIABLE_RUN_KIND_DISPATCH_STATUSES["fallback"]
            | _RELIABLE_RUN_KIND_DISPATCH_STATUSES["repair"]
        )
    return {"plan_approved", "in_progress"}


def plan_only_allowed_statuses(workflow: str) -> set[str]:
    """Statuses at which a `--plan-only` dispatch is acceptable.

    For the implementation workflow, this additionally allows `todo` and
    `plan_proposed` — the agent is expected to write (or revise) a plan and
    stop before touching any implementation code. Non-implementation
    workflows inherit their normal allowed set (plan-only is a no-op there).
    """
    if workflow == "implementation":
        return {
            "todo",
            "plan_proposed",
            "plan_approved",
            "review_failed",
            "in_progress",
        }
    if workflow == RELIABLE_ORCHESTRATOR_WORKFLOW:
        return set(_RELIABLE_RUN_KIND_DISPATCH_STATUSES["plan"])
    return allowed_statuses_for_workflow(workflow)


def reliable_dispatch_statuses_for_run_kind(kind: str) -> set[str]:
    """Return task statuses where a durable reliable Run kind may dispatch."""

    return set(_RELIABLE_RUN_KIND_DISPATCH_STATUSES.get(kind, frozenset()))


# ---------------------------------------------------------------------------
# Canonical status → dashboard column mapping
#
# Single source of truth for all dashboard views and JS rendering.
# When adding a new status to ALL_STATUSES, also add it to STATUS_TO_COL
# and STATUS_TO_GROUP below. The E2E test `test_dashboard_status_mapping_*`
# enforces this at CI time.
# ---------------------------------------------------------------------------

# Maps each status to its board column (board_view / board_tasks / JS columns)
STATUS_TO_COL: dict[str, str] = {
    "pending": "todo",
    "todo": "todo",
    "plan_proposed": "plan",
    "plan_approved": "plan",
    "plan_confirmed": "plan",
    "in_progress": "in_progress",
    "launched": "in_progress",
    "running": "in_progress",
    "waiting_input": "in_progress",
    "pending_user_approval": "in_progress",
    "paused": "in_progress",
    "report_ready": "review",
    "review_requested": "review",
    "review_passed": "review",
    "review_failed": "review",
    "pr_open": "review",
    "done": "done",
    "stopped": "done",
    "failed": "done",
    "archived": "done",
    "blocked": "todo",
}

# Maps column name → list of statuses (inverse of STATUS_TO_COL)
# Also serves as the JS STATUS_GROUPS definition
STATUS_GROUPS: list[dict] = [
    {"key": "archived", "label": "📁 archived", "statuses": ["archived"]},
    {"key": "done", "label": "✅ done", "statuses": ["done"]},
    {"key": "failed", "label": "❌ failed", "statuses": ["failed"]},
    {"key": "stopped", "label": "⛔ stopped", "statuses": ["stopped"]},
    {"key": "pr_open", "label": "🔀 pr open", "statuses": ["pr_open"]},
    {
        "key": "review",
        "label": "🔍 review",
        "statuses": [
            "report_ready",
            "review_requested",
            "review_passed",
            "review_failed",
        ],
    },
    {
        "key": "in_progress",
        "label": "🔄 in progress",
        "statuses": [
            "in_progress",
            "launched",
            "running",
            "waiting_input",
            "paused",
            "pending_user_approval",
        ],
    },
    {"key": "plan", "label": "📋 plan", "statuses": ["plan_proposed", "plan_approved"]},
    {"key": "todo", "label": "🕐 todo", "statuses": ["todo", "blocked", "pending"]},
]


def dashboard_status_mapping() -> dict:
    """Return the canonical status mapping for dashboard use.

    Returns a dict with ``cols`` (status→column) and ``groups`` (column→statuses)
    so dashboard code never hardcodes its own copy.
    """
    return {
        "cols": STATUS_TO_COL,
        "groups": [{**g} for g in STATUS_GROUPS],  # shallow copy
    }


def validate_status_transition(current: str, new: str) -> None:
    """Raise ValueError if `new` is not a legal transition from `current`.

    Uses the authoritative _MAPPING from this module.
    Terminal statuses accept no transitions.
    Unknown current statuses accept any transition (fail open).
    """
    if not current:
        raise ValueError("current status must not be empty")
    if new not in ALL_STATUSES:
        raise ValueError(
            f"Invalid status '{new}'. Valid statuses: {', '.join(ALL_STATUSES)}"
        )
    info = _MAPPING.get(current)
    if info is None:
        return  # unknown current status — fail open
    legal = info[1]
    if new not in legal:
        raise ValueError(
            f"Illegal transition: '{current}' → '{new}'. "
            f"Legal transitions from '{current}': {', '.join(legal)}"
        )

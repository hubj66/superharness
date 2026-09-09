"""State machine tests — validate all lifecycle transitions.

Generated from the transition graph in engine/next_action.py.
Every legal transition must succeed. Every illegal transition must fail.
"""

from __future__ import annotations

import pytest

from superharness.engine.next_action import (
    validate_status_transition,
    ALL_STATUSES,
)


# ── Legal transitions (from the graph in next_action.py) ─────────────────────

LEGAL_TRANSITIONS = {
    "pending": ["plan_proposed", "waiting_input"],
    "todo": ["plan_proposed", "waiting_input"],
    "plan_proposed": ["plan_approved", "todo"],
    "plan_approved": ["in_progress", "plan_proposed"],
    "in_progress": [
        "report_ready",
        "pr_open",
        "pending_user_approval",
        "stopped",
        "failed",
        "blocked",
        "waiting_input",
    ],
    "pending_user_approval": ["in_progress", "stopped"],
    "report_ready": ["review_passed", "review_failed", "review_requested"],
    "review_requested": ["review_passed", "review_failed"],
    "review_passed": ["done", "review_failed"],
    "review_failed": ["in_progress", "plan_proposed", "todo"],
    "done": [],
    "failed": ["plan_proposed", "todo", "stopped"],
    "stopped": ["in_progress", "plan_proposed", "todo"],
    "blocked": ["todo", "plan_proposed"],
    "waiting_input": ["in_progress", "pending_user_approval", "todo", "plan_proposed"],
    "paused": ["in_progress", "stopped"],
    "archived": [],
    "pr_open": ["review_requested", "review_passed", "review_failed"],
}


def _all_statuses() -> set[str]:
    """All 16 task statuses."""
    return {
        s for s in ALL_STATUSES if s != "launched" and s != "running"
    }  # exclude inbox-only


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestLegalTransitions:
    """Every legal transition must succeed without raising."""

    @pytest.mark.parametrize(
        "from_status,to_status",
        [(f, t) for f, targets in LEGAL_TRANSITIONS.items() for t in targets],
    )
    def test_legal_transition(self, from_status, to_status):
        validate_status_transition(from_status, to_status)


class TestIllegalTransitions:
    """Every illegal transition must raise ValueError."""

    @pytest.mark.parametrize(
        "from_status,to_status",
        [
            (f, t)
            for f in LEGAL_TRANSITIONS
            for t in _all_statuses()
            if t not in LEGAL_TRANSITIONS[f] and t != f
        ],
    )
    def test_illegal_transition(self, from_status, to_status):
        with pytest.raises(ValueError, match="transition"):
            validate_status_transition(from_status, to_status)


class TestTerminalStates:
    """Terminal states should have no outgoing transitions."""

    def test_archived_has_no_transitions(self):
        """Archived is terminal — no outgoing transitions."""
        legal = LEGAL_TRANSITIONS.get("archived", [])
        assert legal == [], f"archived should have no transitions, got {legal}"


class TestDiscussionTransitions:
    """Discussion round tasks have their own lifecycle considerations."""

    def test_waiting_input_to_in_progress(self):
        """Operator can move waiting_input back to in_progress."""
        validate_status_transition("waiting_input", "in_progress")

    def test_in_progress_to_waiting_input(self):
        """in_progress → waiting_input is a legal transition."""
        validate_status_transition("in_progress", "waiting_input")


class TestCounts:
    """Verify the transition graph is complete."""

    def test_all_statuses_in_graph(self):
        """Every status in ALL_STATUSES must be in LEGAL_TRANSITIONS."""
        for status in _all_statuses():
            assert status in LEGAL_TRANSITIONS, f"Missing transitions for: {status}"

    def test_all_legal_targets_are_valid(self):
        """Every target status in LEGAL_TRANSITIONS must be a real status."""
        all_statuses_set = _all_statuses()
        for from_s, targets in LEGAL_TRANSITIONS.items():
            for t in targets:
                assert t in all_statuses_set, (
                    f"Illegal target: {from_s} → {t} (not a valid status)"
                )

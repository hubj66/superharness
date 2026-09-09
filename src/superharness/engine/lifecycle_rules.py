"""Unified lifecycle reconciler — replaces 4 ad-hoc reconcilers with one rule table.

The data-driven rule table makes adding a new state-with-timeout a one-line change.
Each LifecycleRule declares:
  - state:           the source state to scan for
  - timeout_minutes: how long an entity may sit in this state before action
  - on_timeout:      what to do (fail, archive, revert)
  - revert_to:       for "revert" action, the destination state
  - skip_if_field:   if set, items with this field non-empty are exempt
                     (e.g. paused items with a manual `reason` field)
  - profile_key:     optional profile.yaml override for timeout_minutes
  - source:          "inbox" (scan inbox.yaml) or "contract" (scan contract.yaml)
  - timestamp_field: which field on the item carries the entry timestamp

Adding a new rule only requires adding a row to LIFECYCLE_RULES.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

import yaml

import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LifecycleRule:
    state: str
    timeout_minutes: int
    on_timeout: Literal["fail", "archive", "revert"]
    source: Literal["inbox", "contract"]
    timestamp_field: str
    revert_to: str | None = None
    skip_if_field: str | None = None
    profile_key: str | None = None  # profile.yaml override key
    fail_reason_template: str = (
        "{state} timeout ({age_minutes}m >= {limit_minutes}m limit)"
    )


LIFECYCLE_RULES: list[LifecycleRule] = [
    LifecycleRule(
        state="paused",
        timeout_minutes=30,
        on_timeout="fail",
        source="inbox",
        timestamp_field="paused_at",
        skip_if_field="reason",
        profile_key="paused_timeout_minutes",
    ),
    LifecycleRule(
        state="review_requested",
        timeout_minutes=120,
        on_timeout="revert",
        revert_to="report_ready",
        source="contract",
        timestamp_field="review_requested_at",
        skip_if_field="escalated_to",  # iter 7: leave escalated reviews alone
        profile_key="review_timeout_minutes",
    ),
    LifecycleRule(
        state="in_progress",
        timeout_minutes=180,
        on_timeout="archive",
        source="contract",
        timestamp_field="updated_at",
        profile_key="in_progress_timeout_minutes",
    ),
    LifecycleRule(
        state="waiting_input",
        timeout_minutes=480,  # 8 hours
        on_timeout="fail",
        source="contract",
        timestamp_field="updated_at",
        profile_key="waiting_input_timeout_minutes",
        fail_reason_template=(
            "waiting_input timeout ({age_minutes}m >= {limit_minutes}m) — "
            "no human response received"
        ),
    ),
    LifecycleRule(
        state="report_ready",
        timeout_minutes=1440,  # 24 hours
        on_timeout="archive",
        source="contract",
        timestamp_field="report_ready_at",
        profile_key="report_ready_timeout_minutes",
        fail_reason_template=(
            "report_ready timeout ({age_minutes}m >= {limit_minutes}m) — "
            "no review activity"
        ),
    ),
    LifecycleRule(
        state="todo",
        timeout_minutes=120,  # 2 hours
        on_timeout="archive",
        source="contract",
        timestamp_field="created_at",
        profile_key="todo_timeout_minutes",
        fail_reason_template=(
            "todo timeout ({age_minutes}m >= {limit_minutes}m) — "
            "task was never dispatched"
        ),
    ),
    # Operator-stopped tasks stay in `stopped` forever otherwise — they
    # accumulate in `shux contract` and the dashboard's Active Tasks view.
    # 7 days gives the operator plenty of room to resume; after that the
    # task gets archived (still recoverable, just out of the active list).
    LifecycleRule(
        state="stopped",
        timeout_minutes=10080,  # 7 days
        on_timeout="archive",
        source="contract",
        timestamp_field="stopped_at",
        profile_key="stopped_timeout_minutes",
        fail_reason_template=(
            "stopped timeout ({age_minutes}m >= {limit_minutes}m) — "
            "operator-halted task auto-archived"
        ),
    ),
    # Plan-state timeouts: tasks that get stuck awaiting dispatch or operator response
    LifecycleRule(
        state="plan_approved",
        timeout_minutes=240,  # 4 hours — approved but agent never picked up
        on_timeout="fail",
        source="contract",
        timestamp_field="plan_approved_at",
        profile_key="plan_approved_timeout_minutes",
        fail_reason_template=(
            "plan_approved timeout ({age_minutes}m >= {limit_minutes}m) — "
            "task was approved but never dispatched"
        ),
    ),
    LifecycleRule(
        state="plan_proposed",
        timeout_minutes=480,  # 8 hours — operator never responded to plan proposal
        on_timeout="fail",
        source="contract",
        timestamp_field="plan_proposed_at",
        profile_key="plan_proposed_timeout_minutes",
        fail_reason_template=(
            "plan_proposed timeout ({age_minutes}m >= {limit_minutes}m) — "
            "no operator response to plan proposal"
        ),
    ),
    LifecycleRule(
        state="pending_user_approval",
        timeout_minutes=480,  # 8 hours — operator never approved
        on_timeout="fail",
        source="contract",
        timestamp_field="updated_at",
        profile_key="pending_user_approval_timeout_minutes",
        fail_reason_template=(
            "pending_user_approval timeout ({age_minutes}m >= {limit_minutes}m) — "
            "no operator approval received"
        ),
    ),
]

# Non-terminal states that are eligible for deadline enforcement.
# Terminal/archived states are excluded because deadline has already passed or
# the task was intentionally closed.
_DEADLINE_ELIGIBLE_STATES = frozenset(
    {
        "todo",
        "plan_proposed",
        "plan_approved",
        "in_progress",
        "report_ready",
        "review_requested",
        "review_passed",
        "review_failed",
        "pending_user_approval",
        "waiting_input",
        "pr_open",
        "blocked",
        "paused",
        "stopped",
    }
)


def _now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_profile(project_dir: str) -> dict:
    path = os.path.join(project_dir, ".superharness", "profile.yaml")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f.read()) or {}
    except Exception as e:
        logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
        return {}


def _effective_timeout(rule: LifecycleRule, profile: dict) -> int:
    if rule.profile_key:
        try:
            return int(profile.get(rule.profile_key, rule.timeout_minutes))
        except (ValueError, TypeError):
            pass
    return rule.timeout_minutes


def _age_minutes(ts_str: str) -> float | None:
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        # A tz-naive timestamp (no offset) raises TypeError when subtracted
        # from an aware datetime, which would crash the entire reconcile pass.
        # Treat naive timestamps as UTC.
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - ts).total_seconds() / 60
    except (ValueError, TypeError):
        return None


def _apply_action(item: dict, rule: LifecycleRule, age: float, limit: int) -> bool:
    """Mutate item in-place per rule. Returns True if a change was made."""
    reason = rule.fail_reason_template.format(
        state=rule.state, age_minutes=int(age), limit_minutes=limit
    )
    now = _now_utc_str()
    if rule.on_timeout == "fail":
        item["status"] = "failed"
        item["failed_reason"] = reason
        item["failed_at"] = now
        return True
    if rule.on_timeout == "archive":
        item["status"] = "archived"
        item["archived_at"] = now
        item["archived_reason"] = reason
        return True
    if rule.on_timeout == "revert":
        if not rule.revert_to:
            return False
        item["status"] = rule.revert_to
        item.pop(rule.timestamp_field, None)
        return True
    return False


def _scan_inbox(project_dir: str, rules: list[LifecycleRule], profile: dict) -> int:
    from superharness.engine import state_reader, state_writer
    from superharness.engine.reliable_watcher import reliable_task_ids

    try:
        items = state_reader.get_inbox_items(project_dir)
    except Exception as e:
        logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
        return 0

    reliable_ids = reliable_task_ids(project_dir)

    changed = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("task", item.get("task_id", ""))) in reliable_ids:
            continue
        original_status = item.get("status")
        for rule in rules:
            if rule.source != "inbox":
                continue
            if original_status != rule.state:
                continue
            if rule.skip_if_field and item.get(rule.skip_if_field):
                continue
            age = _age_minutes(item.get(rule.timestamp_field, ""))
            if age is None:
                continue
            limit = _effective_timeout(rule, profile)
            if limit <= 0:
                continue
            if age >= limit:
                if _apply_action(item, rule, age, limit):
                    new_status = item["status"]
                    item_id = str(item.get("id", ""))
                    # Write update via state_writer — pass only lifecycle-relevant fields
                    _lifecycle_fields = {
                        k: v
                        for k, v in item.items()
                        if k
                        in (
                            "failed_reason",
                            "failed_at",
                            "archived_reason",
                            "archived_at",
                        )
                    }
                    if state_writer.set_inbox_status(
                        project_dir, item_id, new_status, **_lifecycle_fields
                    ):
                        print(
                            f"lifecycle: inbox item {item_id} "
                            f"{rule.state} → {new_status} ({int(age)}m >= {limit}m)"
                        )
                        changed += 1
                    break  # one rule per item per pass

    return changed


def _scan_contract(project_dir: str, rules: list[LifecycleRule], profile: dict) -> int:
    from superharness.engine import state_reader, state_writer
    from superharness.engine.reliable_orchestrator_gate import (
        is_reliable_orchestrated_task,
    )

    try:
        tasks = state_reader.get_tasks(project_dir)
    except Exception as e:
        logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
        return 0

    changed = 0
    for task in tasks:
        if not isinstance(task, dict):
            continue
        if is_reliable_orchestrated_task(task):
            continue
        original_status = task.get("status")
        for rule in rules:
            if rule.source != "contract":
                continue
            if original_status != rule.state:
                continue
            if rule.skip_if_field and task.get(rule.skip_if_field):
                continue
            age = _age_minutes(task.get(rule.timestamp_field, ""))
            if age is None:
                continue
            limit = _effective_timeout(rule, profile)
            if limit <= 0:
                continue
            if age >= limit:
                if _apply_action(task, rule, age, limit):
                    new_status = task["status"]
                    task_id = str(task.get("id", ""))
                    # Write update via state_writer — pass only lifecycle-relevant fields
                    _lifecycle_fields = {
                        k: v
                        for k, v in task.items()
                        if k
                        in (
                            "failed_reason",
                            "failed_at",
                            "archived_reason",
                            "archived_at",
                        )
                    }
                    # System-driven transition (timeout): bypass the
                    # interactive transition graph since e.g. in_progress→archived
                    # is not a legal user move but is the whole point of
                    # the reconciler.
                    if state_writer.set_task_status(
                        project_dir,
                        task_id,
                        new_status,
                        from_status=original_status,
                        force=True,
                        **_lifecycle_fields,
                    ):
                        print(
                            f"lifecycle: task {task_id} "
                            f"{rule.state} → {new_status} ({int(age)}m >= {limit}m)"
                        )
                        changed += 1
                    break

    return changed


def _last_event_age_minutes(conn, task_id: str) -> float | None:
    """Minutes since the most recent `events` row (engine/events.py,
    migration v31) for task_id, or None if no events exist for it.

    See docs/PLAN-adopt-omnigent.md iteration 8.
    """
    try:
        row = conn.execute(
            "SELECT MAX(ts) AS last_ts FROM events WHERE task_id = ?", (task_id,)
        ).fetchone()
    except Exception as e:
        logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
        return None
    if row is None or row["last_ts"] is None:
        return None
    return _age_minutes(row["last_ts"])


def _check_deadlines(project_dir: str, profile: dict) -> int:
    """Enforce deadlines on non-terminal tasks: dual watchdog (idle timeout +
    absolute ceiling) when the task has event history, PR #43's
    in_progress_at-budgeted deadline_minutes check otherwise.

    Dual watchdog (docs/PLAN-adopt-omnigent.md iteration 8), opt-in via
    profile keys `idle_timeout_minutes` and `absolute_ceiling_minutes`
    (both default 0 = disabled — with both unset, behavior is byte-identical
    to the PR #43 fix this extends). When enabled AND the task has at least
    one row in the `events` table (engine/events.py, migration v31):
      - age >= absolute_ceiling_minutes  -> fail (reason contains "ceiling")
      - else, minutes-since-last-event >= idle_timeout_minutes -> fail
        (reason contains "idle")
      - else -> survives, even past its own (legacy) deadline_minutes —
        fresh events prove the task isn't wedged.
    Tasks with no event history (or when both keys are disabled) fall
    through to the exact legacy deadline_minutes / default_deadline_minutes
    check, unmodified.

    Age is measured from in_progress_at (when work started) when present,
    else from created_at, in both paths — so a long-queued task is not
    failed the moment it starts.

    Returns count of tasks failed due to deadline expiry.
    """
    from superharness.engine import state_reader, state_writer
    from superharness.engine.reliable_orchestrator_gate import (
        is_reliable_orchestrated_task,
    )

    # Allow profile override for a project-wide default deadline
    default_deadline = None
    try:
        default_deadline = int(profile.get("default_deadline_minutes", 0))
    except (ValueError, TypeError):
        pass

    idle_timeout = 0
    try:
        idle_timeout = int(profile.get("idle_timeout_minutes", 0) or 0)
    except (ValueError, TypeError):
        pass
    absolute_ceiling = 0
    try:
        absolute_ceiling = int(profile.get("absolute_ceiling_minutes", 0) or 0)
    except (ValueError, TypeError):
        pass
    watchdog_active = idle_timeout > 0 or absolute_ceiling > 0

    events_conn = None
    if watchdog_active:
        try:
            from superharness.engine.db import get_connection, init_db

            events_conn = get_connection(project_dir)
            init_db(events_conn)
        except Exception as e:
            logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
            events_conn = None

    try:
        tasks = state_reader.get_tasks(project_dir)
    except Exception as e:
        logger.warning("lifecycle_rules.py unexpected error: %s", e, exc_info=True)
        if events_conn is not None:
            events_conn.close()
        return 0

    changed = 0
    try:
        for task in tasks:
            if not isinstance(task, dict):
                continue
            if is_reliable_orchestrated_task(task):
                continue
            status = str(task.get("status", ""))
            if status not in _DEADLINE_ELIGIBLE_STATES:
                continue

            task_id = str(task.get("id", ""))

            # Budget from when work actually started (in_progress_at), not
            # from task creation — a task that queued in the backlog for
            # hours must not be force-failed the moment it is dispatched.
            # Pre-work states (no in_progress_at) fall back to created_at.
            ref_ts = task.get("in_progress_at") or task.get("created_at", "")
            age = _age_minutes(ref_ts)
            if age is None:
                continue

            if watchdog_active and events_conn is not None:
                last_event_age = _last_event_age_minutes(events_conn, task_id)
                if last_event_age is not None:
                    # Event history exists: idle/ceiling semantics fully
                    # replace the legacy deadline_minutes check below for
                    # this task.
                    if absolute_ceiling > 0 and age >= absolute_ceiling:
                        reason = (
                            f"absolute ceiling exceeded ({int(age)}m elapsed >= "
                            f"{absolute_ceiling}m ceiling) — task was in status '{status}'"
                        )
                        if state_writer.set_task_status(
                            project_dir,
                            task_id,
                            "failed",
                            from_status=status,
                            force=True,
                            failed_reason=reason,
                            failed_at=_now_utc_str(),
                        ):
                            print(
                                f"lifecycle: task {task_id} absolute ceiling exceeded "
                                f"({int(age)}m >= {absolute_ceiling}m) → failed"
                            )
                            changed += 1
                        continue
                    if idle_timeout > 0 and last_event_age >= idle_timeout:
                        reason = (
                            f"idle timeout exceeded (no events for {int(last_event_age)}m >= "
                            f"{idle_timeout}m idle limit) — task was in status '{status}'"
                        )
                        if state_writer.set_task_status(
                            project_dir,
                            task_id,
                            "failed",
                            from_status=status,
                            force=True,
                            failed_reason=reason,
                            failed_at=_now_utc_str(),
                        ):
                            print(
                                f"lifecycle: task {task_id} idle timeout exceeded "
                                f"({int(last_event_age)}m >= {idle_timeout}m) → failed"
                            )
                            changed += 1
                        continue
                    # Events fresh and within ceiling: survives regardless
                    # of the legacy deadline_minutes field.
                    continue

            # Legacy PR #43 path (unmodified): per-task/profile deadline_minutes,
            # only reached when the watchdog is disabled or this task has no
            # event history yet.
            deadline = None
            raw_deadline = task.get("deadline_minutes")
            if raw_deadline is not None:
                try:
                    deadline = int(raw_deadline)
                except (ValueError, TypeError):
                    pass
            if not deadline:
                deadline = default_deadline
            if not deadline or deadline <= 0:
                continue
            if age < deadline:
                continue

            reason = (
                f"deadline exceeded ({int(age)}m elapsed >= {deadline}m limit) — "
                f"task was in status '{status}'"
            )
            if state_writer.set_task_status(
                project_dir,
                task_id,
                "failed",
                from_status=status,
                force=True,
                failed_reason=reason,
                failed_at=_now_utc_str(),
            ):
                print(
                    f"lifecycle: task {task_id} "
                    f"deadline exceeded ({int(age)}m >= {deadline}m) → failed"
                )
                changed += 1
    finally:
        if events_conn is not None:
            events_conn.close()

    return changed


def reconcile_lifecycle(project_dir: str) -> int:
    """Run all lifecycle rules + deadline enforcement.

    Returns total count of items/tasks changed.
    """
    profile = _load_profile(project_dir)
    inbox_rules = [r for r in LIFECYCLE_RULES if r.source == "inbox"]
    contract_rules = [r for r in LIFECYCLE_RULES if r.source == "contract"]
    return (
        _scan_inbox(project_dir, inbox_rules, profile)
        + _scan_contract(project_dir, contract_rules, profile)
        + _check_deadlines(project_dir, profile)
    )

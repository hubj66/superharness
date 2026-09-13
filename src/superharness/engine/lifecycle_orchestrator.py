"""The feature-gated lifecycle owner for reliable-orchestrator tasks.

Phase 6 adds availability-aware agent selection and independent review while
still leaving merge decisions manual.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import yaml

from superharness.engine import agent_availability, inbox_dao, runs_dao, tasks_dao
from superharness.engine.agent_selector import (
    CLAUDE_AGENT,
    CODEX_AGENT,
    DEFAULT_CODEX_MODEL,
    AgentAssignment,
    AgentSelector,
)
from superharness.engine.db import get_connection, init_db, transaction
from superharness.engine.failure_classifier import (
    ReliableFailureCategory,
    ReliableFailureClassification,
    classify_reliable,
)
from superharness.engine.next_action import validate_status_transition
from superharness.engine.process import probe_process
from superharness.engine.reliable_orchestrator_gate import (
    is_reliable_orchestrated_task,
)
from superharness.engine.reliable_worktree import (
    create_fallback_worktree,
    create_managed_worktree,
    create_repair_worktree,
    create_review_worktree,
    current_branch_name,
    is_managed_worktree_path,
    reliable_task_branch,
    rev_parse,
)
from superharness.engine.run_results import validate_result_for_run
from superharness.engine.shipper import SYSTEM_AGENT, SystemShipper
from superharness.engine.state_errors import BoundaryError, StateError

_ACTIVE_INBOX_STATUSES = ("pending", "launched", "running", "paused")
RELIABLE_HEARTBEAT_GRACE_SECONDS = 15 * 60
RELIABLE_DISPATCH_FINALIZATION_GRACE_SECONDS = 60
AGENT_FAILURE_CATEGORIES = frozenset(
    {
        "quota",
        "session_limit",
        "agent_crash",
        "timeout",
        "hang",
        "auth",
        "network",
        "invalid_result",
        "lost_process",
        "unknown",
    }
)
REVIEW_RETRY_CATEGORIES = frozenset(
    {
        "quota",
        "session_limit",
        "agent_crash",
        "timeout",
        "hang",
        "auth",
        "network",
        "invalid_result",
        "lost_process",
        "unknown",
    }
)
REVIEW_VALID_VERDICTS = frozenset({"LGTM", "REJECTED"})


def _now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class TickResult:
    inspected: int = 0
    runs_created: int = 0
    results_consumed: int = 0
    transitions: int = 0


class LifecycleOrchestrator:
    """Own plan/implementation transitions for gated tasks only."""

    def __init__(
        self,
        project_dir: str,
        *,
        now: Callable[[], str] = _now_utc,
        primary_agent: str | None = None,
        auto_approve_plans: bool | None = None,
        review_agent: str | None = None,
        review_model: str | None = None,
        shipper_factory: Callable[[str], SystemShipper] | None = None,
    ) -> None:
        self.project_dir = os.path.realpath(project_dir)
        self._now = now
        self._primary_agent = primary_agent
        self._auto_approve = auto_approve_plans
        self._review_agent = review_agent
        self._review_model = review_model
        self._profile_cache: dict[str, Any] | None = None
        self._shipper_factory = shipper_factory or (
            lambda project: SystemShipper(project)
        )

    def tick(self, task_id: str | None = None) -> TickResult:
        """Reconcile gated tasks once, atomically and safely on repeat calls."""
        conn = get_connection(self.project_dir)
        try:
            init_db(conn)
            result = TickResult()
            ship_runs_to_execute: list[str] = []
            with transaction(conn):
                self._reconcile_active_runs(conn, task_id=task_id)
                tasks = [
                    tasks_dao.get(conn, task_id) if task_id else task
                    for task in ([None] if task_id else tasks_dao.get_all(conn))
                ]
                if task_id:
                    tasks = [tasks[0]]
                for task in tasks:
                    if task is None or not is_reliable_orchestrated_task(task):
                        continue
                    result = TickResult(
                        inspected=result.inspected + 1,
                        runs_created=result.runs_created,
                        results_consumed=result.results_consumed,
                        transitions=result.transitions,
                    )
                    consumed, transitions, created = self._consume_finished(conn, task)
                    result = TickResult(
                        inspected=result.inspected,
                        runs_created=result.runs_created + created,
                        results_consumed=result.results_consumed + consumed,
                        transitions=result.transitions + transitions,
                    )
                    task = tasks_dao.get(conn, task.id)
                    if task is None:
                        continue

                    if task.status == "todo":
                        existing = runs_dao.list_runs_for_task(
                            conn, task.id, kind="plan"
                        )
                        if not existing:
                            assignment = self._select_mutator(conn, task)
                            if assignment is None:
                                continue
                            self._create_dispatch_run(
                                conn,
                                task,
                                kind="plan",
                                dedupe_key=f"plan:{task.id}",
                                agent=assignment.agent,
                                model=assignment.model,
                            )
                            result = TickResult(
                                inspected=result.inspected,
                                runs_created=result.runs_created + 1,
                                results_consumed=result.results_consumed,
                                transitions=result.transitions,
                            )

                    task = tasks_dao.get(conn, task.id)
                    if task is None:
                        continue
                    if task.status == "plan_proposed" and self._auto_approve_enabled():
                        self._transition_task(conn, task, "plan_approved")
                        result = TickResult(
                            inspected=result.inspected,
                            runs_created=result.runs_created,
                            results_consumed=result.results_consumed,
                            transitions=result.transitions + 1,
                        )
                        task = tasks_dao.get(conn, task.id)
                    if task is not None and task.status == "plan_approved":
                        implementation_runs = runs_dao.list_runs_for_task(
                            conn, task.id, kind="implement"
                        )
                        if not implementation_runs:
                            plan_runs = runs_dao.list_runs_for_task(
                                conn, task.id, kind="plan"
                            )
                            plan_token = plan_runs[-1].id if plan_runs else "manual"
                            assignment = self._select_mutator(conn, task)
                            if assignment is None:
                                continue
                            self._create_dispatch_run(
                                conn,
                                task,
                                kind="implement",
                                dedupe_key=f"implement:{task.id}:{plan_token}",
                                agent=assignment.agent,
                                model=assignment.model,
                                parent_run_id=plan_runs[-1].id if plan_runs else None,
                            )
                            self._transition_task(conn, task, "in_progress")
                            result = TickResult(
                                inspected=result.inspected,
                                runs_created=result.runs_created + 1,
                                results_consumed=result.results_consumed,
                                transitions=result.transitions + 1,
                            )

                    task = tasks_dao.get(conn, task.id) if task is not None else None
                    if task is None:
                        continue
                    review_created, review_transition = self._ensure_review_run(
                        conn, task
                    )
                    if review_created or review_transition:
                        result = TickResult(
                            inspected=result.inspected,
                            runs_created=result.runs_created + review_created,
                            results_consumed=result.results_consumed,
                            transitions=result.transitions + review_transition,
                        )
                ship_runs_to_execute = self._queued_ship_runs(conn, task_id=task_id)
            for ship_run_id in ship_runs_to_execute:
                self._execute_ship_run(ship_run_id)
            if ship_runs_to_execute:
                with transaction(conn):
                    for ship_run_id in ship_runs_to_execute:
                        run = runs_dao.get_run(conn, ship_run_id)
                        if run is None:
                            continue
                        task = tasks_dao.get(conn, run.task_id)
                        if task is None or not is_reliable_orchestrated_task(task):
                            continue
                        consumed, transitions, _created = self._consume_finished(
                            conn, task
                        )
                        task = tasks_dao.get(conn, run.task_id)
                        review_created = 0
                        review_transition = 0
                        if task is not None and is_reliable_orchestrated_task(task):
                            review_created, review_transition = self._ensure_review_run(
                                conn, task
                            )
                        result = TickResult(
                            inspected=result.inspected,
                            runs_created=result.runs_created + review_created,
                            results_consumed=result.results_consumed + consumed,
                            transitions=(
                                result.transitions + transitions + review_transition
                            ),
                        )
            return result
        finally:
            conn.close()

    def _consume_finished(self, conn, task: tasks_dao.TaskRow) -> tuple[int, int, int]:
        consumed = 0
        transitions = 0
        created = 0
        for run in runs_dao.list_unconsumed_finished_runs(conn, task_id=task.id):
            if run.kind not in {
                "plan",
                "implement",
                "repair",
                "fallback",
                "ship",
                "review",
            }:
                continue
            if run.status == "succeeded":
                try:
                    result = (
                        None
                        if run.kind == "ship"
                        else validate_result_for_run(run.result_json, run)
                    )
                except (BoundaryError, ValueError, TypeError, json.JSONDecodeError):
                    if run.kind == "review":
                        runs_dao.record_run_diagnostic(
                            conn,
                            run.id,
                            failure_category="invalid_review_result",
                            failure_detail="review result failed run-bound validation",
                        )
                        self._terminalize_inbox(conn, run, failed=True)
                        runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                        consumed += 1
                    # A malformed result remains visible for operator repair and
                    # cannot move task state.
                    continue
                if result is not None and result.completion_status != "completed":
                    if run.kind == "review":
                        runs_dao.record_run_diagnostic(
                            conn,
                            run.id,
                            failure_category="review_not_completed",
                            failure_detail=f"completion_status={result.completion_status}",
                        )
                        self._terminalize_inbox(conn, run, failed=False)
                        runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                        consumed += 1
                    continue
                if run.kind != "ship":
                    self._record_agent_success(conn, run)
                if run.kind == "plan" and task.status == "todo":
                    self._transition_task(conn, task, "plan_proposed")
                    transitions += 1
                    task = tasks_dao.get(conn, task.id) or task
                elif (
                    run.kind in {"implement", "repair", "fallback"}
                    and task.status == "in_progress"
                ):
                    self._create_ship_run(conn, task, run)
                    created += 1
                elif (
                    run.kind == "ship"
                    and task.status == "in_progress"
                    and run.remote_head_sha
                    and run.head_sha
                    and run.remote_head_sha == run.head_sha
                    and run.pr_number is not None
                    and run.pr_url
                ):
                    self._record_task_pr(conn, task, run)
                    task = tasks_dao.get(conn, task.id) or task
                    self._transition_task(conn, task, "pr_open")
                    transitions += 1
                elif run.kind == "review":
                    self._terminalize_inbox(conn, run, failed=False)
                    review_transitions, review_created = self._consume_review_result(
                        conn, task, run, result
                    )
                    transitions += review_transitions
                    created += review_created
                    task = tasks_dao.get(conn, task.id) or task
            if run.status != "succeeded":
                if self._run_owner_may_be_live(run):
                    continue
                self._terminalize_inbox(conn, run, failed=True)
                failure_created, failure_transitions = self._route_failed_run(
                    conn, task, run
                )
                created += failure_created
                transitions += failure_transitions
            if run.status != "succeeded" or run.kind in {"implement", "repair", "ship"}:
                self._terminalize_inbox(conn, run, failed=run.status != "succeeded")
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
            elif run.kind in {"plan", "review", "fallback"}:
                self._terminalize_inbox(conn, run, failed=False)
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
        return consumed, transitions, created

    def _run_owner_may_be_live(self, run: runs_dao.RunRow) -> bool:
        if run.pid is None:
            return False
        return probe_process(run.pid, run.pid_starttime) in {"live", "unknown"}

    def _reconcile_active_runs(self, conn, *, task_id: str | None = None) -> None:
        """Reconcile reliable Run facts after a watcher restart."""
        for run in runs_dao.list_active_runs(conn, task_id=task_id):
            task = tasks_dao.get(conn, run.task_id)
            if task is None or not is_reliable_orchestrated_task(task):
                continue
            if run.status == "queued":
                continue
            if run.status == "claimed" and run.started_at is None:
                row = inbox_dao.get(conn, run.inbox_id) if run.inbox_id else None
                if row and row.status in {"launched", "running"}:
                    runs_dao.record_run_diagnostic(
                        conn,
                        run.id,
                        failure_category=run.failure_category or "lost_process",
                        failure_detail=run.failure_detail
                        or "dispatch claimed run but process identity was not persisted",
                    )
                    continue
                runs_dao.transition_run(
                    conn, run.id, to_status="queued", now=self._now()
                )
                continue
            if run.status != "running":
                continue

            # A dispatcher may have persisted a complete result immediately
            # before the watcher disappeared.
            result_payload_present = any(
                key in run.result_json
                for key in ("schema_version", "run_id", "completion_status")
            )
            try:
                result = validate_result_for_run(run.result_json, run)
            except (BoundaryError, ValueError, TypeError, json.JSONDecodeError):
                result = None
                if result_payload_present:
                    runs_dao.transition_run(
                        conn,
                        run.id,
                        to_status="failed",
                        now=self._now(),
                        failure_category="invalid_result",
                        failure_detail="persisted structured result is invalid",
                    )
                    continue
            if result is not None and result.completion_status == "completed":
                runs_dao.transition_run(
                    conn, run.id, to_status="succeeded", now=self._now()
                )
                continue

            process_state = probe_process(run.pid, run.pid_starttime)
            if process_state == "live":
                runs_dao.touch_run_heartbeat(
                    conn,
                    run.id,
                    now=self._now(),
                    pid=run.pid,
                    pid_starttime=run.pid_starttime,
                )
                continue
            if process_state in {
                "dead",
                "reused",
            } and self._dispatcher_may_be_finalizing(conn, run):
                continue
            if process_state in {"dead", "reused"} or self._heartbeat_expired(run):
                category = (
                    "lost_process"
                    if process_state in {"reused", "unknown"}
                    else "agent_crash"
                )
                runs_dao.transition_run(
                    conn,
                    run.id,
                    to_status="crashed",
                    now=self._now(),
                    failure_category=category,
                    failure_detail=f"process probe: {process_state}",
                )

    def _dispatcher_may_be_finalizing(self, conn, run: runs_dao.RunRow) -> bool:
        """Avoid racing the dispatcher between child reap and terminal Run write."""
        if not run.inbox_id:
            return False
        row = inbox_dao.get(conn, run.inbox_id)
        if row is None or row.status not in {"launched", "running"}:
            return False
        stamp = run.heartbeat_at or run.started_at
        if not stamp:
            return False
        try:
            current = datetime.fromisoformat(self._now())
            previous = datetime.fromisoformat(str(stamp))
        except (TypeError, ValueError):
            return False
        return (
            current - previous
        ).total_seconds() <= RELIABLE_DISPATCH_FINALIZATION_GRACE_SECONDS

    def _heartbeat_expired(self, run: runs_dao.RunRow) -> bool:
        stamp = run.heartbeat_at or run.started_at
        if not stamp:
            return False
        try:
            current = datetime.fromisoformat(self._now())
            previous = datetime.fromisoformat(str(stamp))
            return (
                current - previous
            ).total_seconds() > RELIABLE_HEARTBEAT_GRACE_SECONDS
        except (TypeError, ValueError):
            return False

    def _route_failed_run(
        self, conn, task: tasks_dao.TaskRow, run: runs_dao.RunRow
    ) -> tuple[int, int]:
        classification = self._classify_run_failure(run)
        if run.kind == "ship":
            return 0, 0
        runs_dao.record_run_diagnostic(
            conn,
            run.id,
            failure_category=classification.category,
            failure_detail=classification.explain,
        )
        if run.kind != "ship":
            self._record_agent_failure(conn, run, classification)
        if run.kind == "plan":
            return 0, 0

        if run.kind == "review":
            metadata = self._pr_metadata(task)
            current_sha = metadata.get("pr_head_sha") if metadata else None
            if (
                metadata
                and run.review_target_sha
                and current_sha != run.review_target_sha
            ):
                runs_dao.record_run_diagnostic(
                    conn,
                    run.id,
                    failure_category="stale_review",
                    failure_detail=(
                        f"review_target_sha={run.review_target_sha!r}, "
                        f"task_pr_head_sha={current_sha!r}"
                    ),
                )
                return 0, 0
            if (
                run.attempt < 2
                and classification.category in REVIEW_RETRY_CATEGORIES
                and self._agent_selectable(conn, run.agent)
                and self._create_retry_run(conn, task, run)
            ):
                return 1, 0
            return 0, 0

        if run.kind == "fallback":
            if classification.category in {"quota", "session_limit", "auth"}:
                return 0, self._move_task(conn, task, "blocked")
            if (
                run.attempt < 2
                and classification.category
                in {
                    "agent_crash",
                    "network",
                    "unknown",
                    "timeout",
                    "hang",
                    "lost_process",
                }
                and self._agent_selectable(conn, run.agent)
                and self._create_retry_run(conn, task, run)
            ):
                return 1, 0
            return 0, self._move_task(conn, task, "failed")

        if run.kind in {"implement", "repair"}:
            if classification.category == "auth":
                return 0, self._move_task(conn, task, "blocked")
            if classification.category in {"network", "unknown"}:
                if (
                    run.attempt < 2
                    and self._agent_selectable(conn, run.agent)
                    and self._create_retry_run(conn, task, run)
                ):
                    return 1, 0
                return 0, self._move_task(conn, task, "blocked")
            if self._create_fallback_run(conn, task, run, classification.category):
                return 1, 0
            if classification.category in {"quota", "session_limit"}:
                return 0, 0
            return 0, self._move_task(conn, task, "blocked")
        return 0, 0

    def _classify_run_failure(self, run: runs_dao.RunRow):
        if run.status == "quota_blocked":
            return ReliableFailureClassification(
                "quota", run.failure_detail or "Run was blocked by agent quota"
            )
        explicit = {
            "quota",
            "session_limit",
            "agent_crash",
            "timeout",
            "hang",
            "auth",
            "network",
            "ship_failure",
            "invalid_result",
            "lost_process",
        }
        if run.failure_category in explicit:
            category = cast(ReliableFailureCategory, run.failure_category)
            return ReliableFailureClassification(
                category, run.failure_detail or category
            )
        return classify_reliable(
            launcher_rc=run.exit_code,
            error_text=run.failure_detail or "",
            timed_out=run.status == "timed_out",
            lost_process=run.status == "crashed",
            invalid_result=run.failure_category == "dispatcher_failure",
        )

    def _move_task(self, conn, task: tasks_dao.TaskRow, status: str) -> int:
        current = tasks_dao.get(conn, task.id) or task
        if current.status != status:
            self._transition_task(conn, current, status)
            return 1
        return 0

    def _create_retry_run(
        self, conn, task: tasks_dao.TaskRow, failed_run: runs_dao.RunRow
    ) -> bool:
        metadata = self._pr_metadata(task)
        worktree_path = failed_run.worktree_path
        branch_name = failed_run.branch_name
        base_sha = failed_run.base_sha
        head_sha = failed_run.head_sha
        target_sha = failed_run.review_target_sha
        if failed_run.kind == "review" and target_sha:
            if metadata and metadata.get("source_agent") == failed_run.agent:
                return False
            if not metadata:
                metadata = {
                    "branch_name": failed_run.branch_name or "",
                    "pr_head_sha": target_sha,
                    "pr_number": failed_run.pr_number or 0,
                    "pr_url": failed_run.pr_url or "",
                }
                worktree_path, branch_name, base_sha, head_sha = (
                    failed_run.worktree_path,
                    failed_run.branch_name,
                    failed_run.base_sha,
                    target_sha,
                )
            elif self._is_git_repo():
                try:
                    worktree = create_review_worktree(
                        self.project_dir,
                        task.id,
                        branch_name=metadata["branch_name"],
                        review_target_sha=target_sha,
                    )
                except StateError:
                    return False
                worktree_path, branch_name, base_sha, head_sha = (
                    worktree.path,
                    None,
                    target_sha,
                    target_sha,
                )
            else:
                worktree_path, branch_name, base_sha, head_sha = (
                    failed_run.worktree_path,
                    metadata["branch_name"],
                    target_sha,
                    target_sha,
                )
        elif (
            metadata
            and failed_run.kind in {"repair", "fallback"}
            and self._is_git_repo()
        ):
            try:
                worktree = create_repair_worktree(
                    self.project_dir,
                    task.id,
                    branch_name=metadata["branch_name"],
                    expected_head_sha=metadata["pr_head_sha"],
                    allow_dirty_reset=True,
                )
            except StateError:
                return False
            worktree_path, branch_name, base_sha, head_sha = (
                worktree.path,
                worktree.branch_name,
                worktree.base_sha,
                worktree.base_sha,
            )
        elif not worktree_path or not self._safe_task_worktree(task, worktree_path):
            if not base_sha:
                return False
            if self._is_git_repo():
                try:
                    worktree = create_fallback_worktree(
                        self.project_dir, task.id, base_sha=base_sha
                    )
                except StateError:
                    return False
                worktree_path, branch_name = worktree.path, worktree.branch_name
        prompt = str(failed_run.result_json.get("prompt") or "")
        prompt += (
            f"\n\nRetry this same {failed_run.kind} Run after {failed_run.id}. "
            "Do not push or ship manually."
        )
        dedupe = f"retry:{task.id}:{failed_run.kind}:{failed_run.id}:{failed_run.attempt + 1}"
        self._create_dispatch_run(
            conn,
            task,
            kind=failed_run.kind,
            dedupe_key=dedupe,
            agent=failed_run.agent,
            model=failed_run.model,
            parent_run_id=failed_run.id,
            trigger_run_id=failed_run.id,
            review_target_sha=target_sha,
            prompt=prompt,
            worktree_path=worktree_path,
            branch_name=branch_name,
            base_sha=base_sha,
            head_sha=head_sha,
            pr_number=metadata["pr_number"] if metadata else failed_run.pr_number,
            pr_url=metadata["pr_url"] if metadata else failed_run.pr_url,
            attempt=failed_run.attempt + 1,
        )
        return True

    def _create_fallback_run(
        self, conn, task: tasks_dao.TaskRow, failed_run: runs_dao.RunRow, category: str
    ) -> bool:
        assignment = self._select_mutator(
            conn, task, preferred_agent=None, exclude_agents={failed_run.agent}
        )
        if assignment is None:
            runs_dao.record_run_diagnostic(
                conn,
                failed_run.id,
                failure_category=category,
                failure_detail="no eligible alternate mutating agent is currently available",
            )
            return False
        metadata = self._pr_metadata(task)
        worktree_path = None
        branch_name = None
        base_sha = failed_run.base_sha
        head_sha = failed_run.head_sha
        if metadata and self._is_git_repo():
            try:
                worktree = create_repair_worktree(
                    self.project_dir,
                    task.id,
                    branch_name=metadata["branch_name"],
                    expected_head_sha=metadata["pr_head_sha"],
                    allow_dirty_reset=True,
                )
            except StateError:
                return False
            worktree_path, branch_name, base_sha, head_sha = (
                worktree.path,
                worktree.branch_name,
                worktree.base_sha,
                metadata["pr_head_sha"],
            )
        elif metadata:
            branch_name = metadata["branch_name"]
            base_sha = metadata["pr_head_sha"]
            head_sha = metadata["pr_head_sha"]
        else:
            if not base_sha:
                return False
            if self._is_git_repo():
                try:
                    worktree = create_fallback_worktree(
                        self.project_dir, task.id, base_sha=base_sha
                    )
                except StateError:
                    return False
                worktree_path, branch_name = worktree.path, worktree.branch_name
        prompt = self._fallback_prompt(conn, task, failed_run, category, metadata)
        self._create_dispatch_run(
            conn,
            task,
            kind="fallback",
            dedupe_key=f"fallback:{task.id}:{failed_run.id}:{assignment.agent}",
            agent=assignment.agent,
            model=assignment.model,
            parent_run_id=failed_run.id,
            trigger_run_id=failed_run.id,
            prompt=prompt,
            worktree_path=worktree_path,
            branch_name=branch_name,
            base_sha=base_sha,
            head_sha=head_sha,
            pr_number=metadata["pr_number"] if metadata else failed_run.pr_number,
            pr_url=metadata["pr_url"] if metadata else failed_run.pr_url,
        )
        return True

    def _safe_task_worktree(self, task: tasks_dao.TaskRow, path: str) -> bool:
        if not self._is_git_repo() or not is_managed_worktree_path(
            self.project_dir, path
        ):
            return False
        try:
            return current_branch_name(path) == reliable_task_branch(task.id) and bool(
                rev_parse(path, "HEAD")
            )
        except StateError:
            return False

    def _fallback_prompt(self, conn, task, failed_run, category, metadata):
        scope = "\n".join(f"- {item}" for item in task.acceptance_criteria) or "- none"
        lines = [
            "=== RELIABLE ORCHESTRATOR FALLBACK ===",
            "Continue the SAME task. Do not make unrelated changes.",
            "Do not commit, push, ship, merge, enable auto-merge, or close the task.",
            "System shipping owns commit, push, PR update, and SHA confirmation.",
            f"Task: {task.id} - {task.title}",
            "Authoritative acceptance criteria:",
            scope,
            f"Failed source Run: {failed_run.id}",
            f"Failed source agent: {failed_run.agent}",
            f"Failure category: {category}",
            f"Current branch: {failed_run.branch_name or (metadata or {}).get('branch_name') or 'task branch'}",
            f"Current/base SHA: {failed_run.head_sha or failed_run.base_sha or (metadata or {}).get('pr_head_sha') or 'recorded Run state'}",
        ]
        if metadata:
            lines.extend(
                [
                    f"PR: {metadata['pr_url']} (#{metadata['pr_number']})",
                    f"Current PR head SHA: {metadata['pr_head_sha']}",
                ]
            )
        if failed_run.trigger_run_id:
            review = runs_dao.get_run(conn, failed_run.trigger_run_id)
            if review:
                findings = review.result_json.get("findings", [])
                lines.extend(
                    [
                        f"Rejected review Run: {review.id}",
                        "Rejected review findings:",
                        *(f"- {item}" for item in findings),
                    ]
                )
        lines.append(
            "Run appropriate focused tests and write the structured result JSON to SUPERHARNESS_RUN_RESULT_PATH."
        )
        return "\n".join(lines)

    def _terminalize_inbox(self, conn, run: runs_dao.RunRow, *, failed: bool) -> None:
        if not run.inbox_id:
            return
        row = inbox_dao.get(conn, run.inbox_id)
        if row is None or row.status not in _ACTIVE_INBOX_STATUSES:
            return
        inbox_dao.update_status(
            conn,
            row.id,
            from_status=row.status,
            to_status="failed" if failed else "done",
            now=self._now(),
            reason="run failed" if failed else None,
        )

    def _ensure_review_run(self, conn, task: tasks_dao.TaskRow) -> tuple[int, int]:
        if task.status not in {"pr_open", "review_requested"}:
            return 0, 0
        metadata = self._pr_metadata(task)
        if metadata is None:
            return 0, 0
        review_target_sha = metadata["pr_head_sha"]
        existing = [
            run
            for run in runs_dao.list_runs_for_task(conn, task.id, kind="review")
            if run.review_target_sha == review_target_sha
        ]
        existing_result = self._handle_same_sha_review(conn, task, existing)
        if existing_result is not None:
            return existing_result
        assignment = self._select_reviewer(
            conn,
            task,
            source_agent=metadata.get("source_agent"),
            preferred_agent=self._review_agent,
        )
        if assignment is None:
            return 0, 0
        worktree = None
        if self._is_git_repo():
            try:
                worktree = create_review_worktree(
                    self.project_dir,
                    task.id,
                    branch_name=metadata["branch_name"],
                    review_target_sha=review_target_sha,
                )
            except StateError:
                return 0, 0
        prompt = self._review_prompt(task, metadata)
        self._create_dispatch_run(
            conn,
            task,
            kind="review",
            dedupe_key=f"review:{task.id}:{review_target_sha}",
            agent=assignment.agent,
            model=self._review_model if self._review_model else assignment.model,
            review_target_sha=review_target_sha,
            prompt=prompt,
            worktree_path=worktree.path if worktree else None,
            branch_name=metadata["branch_name"],
            base_sha=review_target_sha,
            head_sha=review_target_sha,
            pr_number=metadata["pr_number"],
            pr_url=metadata["pr_url"],
        )
        if task.status == "pr_open":
            self._transition_task(conn, task, "review_requested")
            return 1, 1
        return 1, 0

    def _handle_same_sha_review(
        self,
        conn,
        task: tasks_dao.TaskRow,
        existing: list[runs_dao.RunRow],
    ) -> tuple[int, int] | None:
        if not existing:
            return None
        if any(run.status in runs_dao.ACTIVE_RUN_STATUSES for run in existing):
            return 0, 0
        if any(self._review_has_consumed_verdict(run) for run in existing):
            return 0, 0
        latest_attempt = max(run.attempt for run in existing)
        if latest_attempt >= 2:
            return 0, 0
        retry_source = max(existing, key=lambda run: (run.attempt, run.created_at))
        if retry_source.status == "succeeded" and retry_source.failure_category not in {
            "invalid_review_result",
            "review_not_completed",
        }:
            return 0, 0
        classification = self._classify_run_failure(retry_source)
        if (
            classification.category in REVIEW_RETRY_CATEGORIES
            and self._agent_selectable(conn, retry_source.agent)
            and self._create_retry_run(conn, task, retry_source)
        ):
            transition = 0
            if task.status == "pr_open":
                self._transition_task(conn, task, "review_requested")
                transition = 1
            return 1, transition
        return 0, 0

    def _review_has_consumed_verdict(self, run: runs_dao.RunRow) -> bool:
        return (
            run.status == "succeeded"
            and run.orchestrator_consumed_at is not None
            and run.review_verdict in REVIEW_VALID_VERDICTS
            and not run.failure_category
        )

    def _consume_review_result(
        self,
        conn,
        task: tasks_dao.TaskRow,
        run: runs_dao.RunRow,
        result: Any,
    ) -> tuple[int, int]:
        metadata = self._pr_metadata(task)
        current_sha = metadata["pr_head_sha"] if metadata else None
        reviewed_sha = getattr(result, "reviewed_sha", None)
        verdict = getattr(result, "review_verdict", None)
        if (
            not current_sha
            or not run.review_target_sha
            or reviewed_sha != run.review_target_sha
            or current_sha != run.review_target_sha
        ):
            detail = (
                f"reviewed_sha={reviewed_sha!r}, "
                f"review_target_sha={run.review_target_sha!r}, "
                f"task_pr_head_sha={current_sha!r}"
            )
            runs_dao.record_run_diagnostic(
                conn,
                run.id,
                failure_category="stale_review",
                failure_detail=detail,
            )
            self._terminalize_inbox(conn, run, failed=False)
            created, transitions = self._ensure_review_run(conn, task)
            return transitions, created

        self._record_review_metadata(conn, task, run, result)
        task = tasks_dao.get(conn, task.id) or task
        if verdict == "LGTM" and task.status == "review_requested":
            self._transition_task(conn, task, "review_passed")
            return 1, 0
        if verdict == "REJECTED" and task.status == "review_requested":
            self._transition_task(conn, task, "review_failed")
            transitions = 1
            task = tasks_dao.get(conn, task.id) or task
            created = self._ensure_repair_run(conn, task, run, result)
            task = tasks_dao.get(conn, task.id) or task
            has_repair = any(
                candidate.trigger_run_id == run.id
                for candidate in runs_dao.list_runs_for_task(
                    conn, task.id, kind="repair"
                )
            )
            if has_repair and task.status == "review_failed":
                self._transition_task(conn, task, "in_progress")
                transitions += 1
            return transitions, created
        if verdict == "BLOCKED":
            runs_dao.record_run_diagnostic(
                conn,
                run.id,
                failure_category="review_blocked",
                failure_detail="Codex review returned BLOCKED",
            )
        return 0, 0

    def _create_dispatch_run(
        self,
        conn,
        task: tasks_dao.TaskRow,
        *,
        kind: str,
        dedupe_key: str,
        agent: str | None = None,
        model: str | None = None,
        parent_run_id: str | None = None,
        trigger_run_id: str | None = None,
        review_target_sha: str | None = None,
        prompt: str | None = None,
        worktree_path: str | None = None,
        branch_name: str | None = None,
        base_sha: str | None = None,
        head_sha: str | None = None,
        pr_number: int | None = None,
        pr_url: str | None = None,
        attempt: int = 1,
    ) -> runs_dao.RunRow:
        agent = agent or self._agent_for(task)
        worktree = None
        if kind == "implement" and self._is_git_repo() and worktree_path is None:
            worktree = create_managed_worktree(self.project_dir, task.id)
            worktree_path = worktree.path
            branch_name = worktree.branch_name
            base_sha = worktree.base_sha
        run_id = "run-" + hashlib.sha256(dedupe_key.encode()).hexdigest()[:24]
        run = runs_dao.create_run(
            conn,
            id=run_id,
            task_id=task.id,
            kind=kind,
            agent=agent,
            model=model,
            attempt=attempt,
            dedupe_key=dedupe_key,
            parent_run_id=parent_run_id,
            trigger_run_id=trigger_run_id,
            worktree_path=worktree_path,
            branch_name=branch_name,
            base_sha=base_sha,
            head_sha=head_sha,
            pr_number=pr_number,
            pr_url=pr_url,
            review_target_sha=review_target_sha,
            result_json={"prompt": prompt} if prompt else None,
            now=self._now(),
        )
        if run.inbox_id is not None:
            return run

        inbox = None
        if kind in {"plan", "implement"}:
            inbox = conn.execute(
                """
                SELECT id FROM inbox
                 WHERE task_id=? AND target_agent=? AND run_id IS NULL
                   AND status IN ('pending','launched','running','paused')
                 ORDER BY created_at ASC, id ASC LIMIT 1
                """,
                (task.id, agent),
            ).fetchone()
        inbox_id = inbox["id"] if inbox else f"orchestrator-{run_id}"
        if inbox is None:
            inbox_dao.enqueue(
                conn,
                id=inbox_id,
                task_id=task.id,
                target_agent=agent,
                project_path=worktree_path or self.project_dir,
                plan_only=kind == "plan",
                run_id=run.id,
                now=self._now(),
            )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox_id)
        return runs_dao.get_run(conn, run.id) or run

    def _ensure_repair_run(
        self,
        conn,
        task: tasks_dao.TaskRow,
        rejected_review_run: runs_dao.RunRow,
        review_result: Any,
    ) -> int:
        existing = [
            run
            for run in runs_dao.list_runs_for_task(conn, task.id, kind="repair")
            if run.trigger_run_id == rejected_review_run.id
        ]
        if existing:
            return 0
        metadata = self._pr_metadata(task)
        if metadata is None:
            runs_dao.record_run_diagnostic(
                conn,
                rejected_review_run.id,
                failure_category="repair_blocked",
                failure_detail="missing PR metadata for repair",
            )
            return 0
        current_sha = metadata["pr_head_sha"]
        if rejected_review_run.review_target_sha != current_sha:
            runs_dao.record_run_diagnostic(
                conn,
                rejected_review_run.id,
                failure_category="stale_review",
                failure_detail=(
                    f"rejected SHA {rejected_review_run.review_target_sha!r} "
                    f"!= current PR head {current_sha!r}"
                ),
            )
            return 0
        assignment = self._select_mutator(
            conn,
            task,
            preferred_agent=metadata.get("source_agent"),
        )
        if assignment is None:
            runs_dao.record_run_diagnostic(
                conn,
                rejected_review_run.id,
                failure_category="repair_blocked",
                failure_detail="no eligible mutating agent is currently available",
            )
            return 0
        worktree = None
        if self._is_git_repo():
            try:
                worktree = create_repair_worktree(
                    self.project_dir,
                    task.id,
                    branch_name=metadata["branch_name"],
                    expected_head_sha=current_sha,
                )
            except StateError:
                runs_dao.record_run_diagnostic(
                    conn,
                    rejected_review_run.id,
                    failure_category="repair_blocked",
                    failure_detail="repair worktree is not at the confirmed PR head",
                )
                return 0
        prompt = self._repair_prompt(task, metadata, rejected_review_run, review_result)
        self._create_dispatch_run(
            conn,
            task,
            kind="repair",
            dedupe_key=f"repair:{task.id}:{rejected_review_run.id}",
            agent=assignment.agent,
            model=assignment.model,
            trigger_run_id=rejected_review_run.id,
            worktree_path=worktree.path if worktree else None,
            branch_name=metadata["branch_name"],
            base_sha=current_sha,
            head_sha=current_sha,
            pr_number=metadata["pr_number"],
            pr_url=metadata["pr_url"],
            prompt=prompt,
        )
        return 1

    def _create_ship_run(
        self, conn, task: tasks_dao.TaskRow, source_run: runs_dao.RunRow
    ) -> runs_dao.RunRow:
        head_token = source_run.head_sha or "unknown"
        dedupe_key = f"ship:{task.id}:{source_run.id}:{head_token}"
        run_id = "run-" + hashlib.sha256(dedupe_key.encode()).hexdigest()[:24]
        return runs_dao.create_run(
            conn,
            id=run_id,
            task_id=task.id,
            kind="ship",
            agent=SYSTEM_AGENT,
            dedupe_key=dedupe_key,
            parent_run_id=source_run.id,
            worktree_path=source_run.worktree_path,
            branch_name=source_run.branch_name,
            base_sha=source_run.head_sha,
            now=self._now(),
        )

    def _pr_metadata(self, task: tasks_dao.TaskRow) -> dict[str, Any] | None:
        try:
            extras = json.loads(task.extras_json or "{}")
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(extras, dict):
            return None
        metadata = extras.get("reliable_orchestrator")
        if not isinstance(metadata, dict):
            return None
        branch = metadata.get("branch_name")
        pr_head_sha = (
            metadata.get("pr_head_sha")
            or metadata.get("remote_head_sha")
            or metadata.get("head_sha")
        )
        pr_url = metadata.get("pr_url")
        pr_number = metadata.get("pr_number")
        if not isinstance(branch, str) or not branch:
            return None
        if not isinstance(pr_head_sha, str) or not pr_head_sha:
            return None
        if not isinstance(pr_url, str) or not pr_url:
            return None
        if not isinstance(pr_number, int):
            return None
        return {
            "branch_name": branch,
            "pr_head_sha": pr_head_sha,
            "pr_number": pr_number,
            "pr_url": pr_url,
            "ship_run_id": metadata.get("ship_run_id"),
            "source_run_id": metadata.get("source_run_id"),
            "source_run_kind": metadata.get("source_run_kind"),
            "source_agent": metadata.get("source_agent"),
        }

    def _review_prompt(self, task: tasks_dao.TaskRow, metadata: dict[str, Any]) -> str:
        return "\n".join(
            [
                "=== RELIABLE ORCHESTRATOR CODE REVIEW ===",
                "Review only. Do not modify code. Do not commit. Do not push.",
                "Do not ship, merge, enable auto-merge, or close the task.",
                "Inspect the full persisted task scope and the actual checked-out diff.",
                "Treat the task row, issue URL, acceptance criteria, context, and locked contract as authoritative.",
                "Do not treat the PR description as authoritative task scope.",
                f"Task: {task.id} - {task.title}",
                f"PR: {metadata['pr_url']} (#{metadata['pr_number']})",
                f"Review target SHA: {metadata['pr_head_sha']}",
                "Write the structured Superharness execution result JSON to SUPERHARNESS_RUN_RESULT_PATH.",
                "The review_verdict must be LGTM, REJECTED, or BLOCKED.",
                "The reviewed_sha must exactly equal the review target SHA.",
                "If REJECTED, include concrete findings in the findings list.",
            ]
        )

    def _repair_prompt(
        self,
        task: tasks_dao.TaskRow,
        metadata: dict[str, Any],
        review_run: runs_dao.RunRow,
        review_result: Any,
    ) -> str:
        findings = getattr(review_result, "findings", []) or []
        findings_text = "\n".join(f"- {item}" for item in findings) or "- none provided"
        scope = "\n".join(f"- {item}" for item in task.acceptance_criteria) or "- none"
        return "\n".join(
            [
                "=== RELIABLE ORCHESTRATOR REPAIR ===",
                "Fix the same PR/task only. Do not make unrelated changes.",
                "Do not manually push, ship, merge, enable auto-merge, or close the task.",
                "System shipping owns commit, push, PR update, and SHA confirmation.",
                f"Task: {task.id} - {task.title}",
                f"Rejected review Run: {review_run.id}",
                f"Reviewed SHA: {getattr(review_result, 'reviewed_sha', '')}",
                f"Current PR head SHA: {metadata['pr_head_sha']}",
                f"PR: {metadata['pr_url']} (#{metadata['pr_number']})",
                "Authoritative acceptance criteria:",
                scope,
                "Rejected review findings:",
                findings_text,
                "Run appropriate focused tests and write the structured mutating Run result JSON to SUPERHARNESS_RUN_RESULT_PATH.",
            ]
        )

    def _queued_ship_runs(self, conn, *, task_id: str | None = None) -> list[str]:
        query = """
            SELECT r.id
            FROM runs r
            JOIN tasks t ON t.id = r.task_id
            WHERE r.kind='ship'
              AND r.agent=?
              AND r.status='queued'
              AND t.workflow='reliable-orchestrator'
        """
        params: list[Any] = [SYSTEM_AGENT]
        if task_id is not None:
            query += " AND r.task_id=?"
            params.append(task_id)
        query += " ORDER BY r.created_at ASC, r.id ASC"
        return [row["id"] for row in conn.execute(query, params).fetchall()]

    def _execute_ship_run(self, ship_run_id: str) -> None:
        conn = get_connection(self.project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                ship_run = runs_dao.get_run(conn, ship_run_id)
                if ship_run is None or ship_run.status != "queued":
                    return
                if ship_run.parent_run_id is None:
                    self._fail_ship_run(
                        conn, ship_run, "invalid_worktree", "missing source Run"
                    )
                    return
                source_run = runs_dao.get_run(conn, ship_run.parent_run_id)
                task = tasks_dao.get(conn, ship_run.task_id)
                if source_run is None or task is None:
                    self._fail_ship_run(
                        conn, ship_run, "invalid_worktree", "missing source Run or task"
                    )
                    return
                runs_dao.transition_run(
                    conn, ship_run.id, to_status="claimed", now=self._now()
                )
                runs_dao.transition_run(
                    conn, ship_run.id, to_status="running", now=self._now()
                )

            outcome = self._shipper_factory(self.project_dir).ship(
                ship_run=ship_run, source_run=source_run, task=task
            )
            with transaction(conn):
                ship_run = runs_dao.get_run(conn, ship_run_id)
                if ship_run is None or ship_run.status != "running":
                    return
                runs_dao.record_ship_outcome(
                    conn,
                    ship_run.id,
                    branch_name=outcome.branch_name,
                    worktree_path=outcome.worktree_path,
                    base_sha=outcome.base_sha,
                    head_sha=outcome.head_sha,
                    remote_head_sha=outcome.remote_head_sha,
                    pr_number=outcome.pr_number,
                    pr_url=outcome.pr_url,
                    failure_category=outcome.failure_category,
                    failure_detail=outcome.failure_detail,
                    result_json={
                        "ok": outcome.ok,
                        "failure_category": outcome.failure_category,
                        "failure_detail": outcome.failure_detail,
                    },
                )
                runs_dao.transition_run(
                    conn,
                    ship_run.id,
                    to_status="succeeded" if outcome.ok else "failed",
                    now=self._now(),
                    failure_category=outcome.failure_category,
                    failure_detail=outcome.failure_detail,
                )
        finally:
            conn.close()

    def _fail_ship_run(
        self,
        conn,
        ship_run: runs_dao.RunRow,
        category: str,
        detail: str,
    ) -> None:
        if ship_run.status == "queued":
            runs_dao.transition_run(
                conn, ship_run.id, to_status="claimed", now=self._now()
            )
        current = runs_dao.get_run(conn, ship_run.id)
        if current is not None and current.status == "claimed":
            runs_dao.transition_run(
                conn, ship_run.id, to_status="running", now=self._now()
            )
        runs_dao.record_ship_outcome(
            conn,
            ship_run.id,
            failure_category=category,
            failure_detail=detail,
            result_json={
                "ok": False,
                "failure_category": category,
                "failure_detail": detail,
            },
        )
        runs_dao.transition_run(
            conn,
            ship_run.id,
            to_status="failed",
            now=self._now(),
            failure_category=category,
            failure_detail=detail,
        )

    def _record_task_pr(
        self, conn, task: tasks_dao.TaskRow, ship_run: runs_dao.RunRow
    ) -> None:
        try:
            extras = json.loads(task.extras_json or "{}")
        except (json.JSONDecodeError, TypeError):
            extras = {}
        if not isinstance(extras, dict):
            extras = {}
        source_run = (
            runs_dao.get_run(conn, ship_run.parent_run_id)
            if ship_run.parent_run_id
            else None
        )
        existing = extras.get("reliable_orchestrator")
        existing_metadata = existing if isinstance(existing, dict) else {}
        existing_metadata.update(
            {
                "branch_name": ship_run.branch_name,
                "base_sha": ship_run.base_sha,
                "head_sha": ship_run.head_sha,
                "remote_head_sha": ship_run.remote_head_sha,
                "pr_head_sha": ship_run.remote_head_sha,
                "pr_number": ship_run.pr_number,
                "pr_url": ship_run.pr_url,
                "ship_run_id": ship_run.id,
                "source_run_id": source_run.id
                if source_run
                else ship_run.parent_run_id,
                "source_run_kind": source_run.kind if source_run else None,
                "source_agent": source_run.agent if source_run else None,
            }
        )
        extras["reliable_orchestrator"] = existing_metadata
        tasks_dao.update(
            conn,
            task.id,
            task.version,
            {
                "extras_json": json.dumps(extras),
                "worktree_path": ship_run.worktree_path,
            },
        )

    def _record_review_metadata(
        self,
        conn,
        task: tasks_dao.TaskRow,
        review_run: runs_dao.RunRow,
        review_result: Any,
    ) -> None:
        try:
            extras = json.loads(task.extras_json or "{}")
        except (json.JSONDecodeError, TypeError):
            extras = {}
        if not isinstance(extras, dict):
            extras = {}
        existing = extras.get("reliable_orchestrator")
        metadata = existing if isinstance(existing, dict) else {}
        metadata.update(
            {
                "last_review_run_id": review_run.id,
                "last_review_verdict": getattr(review_result, "review_verdict", None),
                "last_reviewed_head_sha": getattr(review_result, "reviewed_sha", None),
            }
        )
        extras["reliable_orchestrator"] = metadata
        tasks_dao.update(
            conn,
            task.id,
            task.version,
            {"extras_json": json.dumps(extras)},
        )

    def _is_git_repo(self) -> bool:
        return os.path.isdir(os.path.join(self.project_dir, ".git"))

    def _transition_task(self, conn, task: tasks_dao.TaskRow, new_status: str) -> None:
        if task.status == new_status:
            return
        validate_status_transition(task.status, new_status)
        now = self._now()
        changes: dict[str, Any] = {"status": new_status, "updated_at": now}
        timestamp_columns = {
            "plan_proposed": "plan_proposed_at",
            "plan_approved": "plan_approved_at",
            "in_progress": "in_progress_at",
            "review_requested": "review_requested_at",
        }
        if new_status in timestamp_columns:
            changes[timestamp_columns[new_status]] = now
        if new_status == "plan_approved" and not task.contract_locked_at:
            changes["locked_contract"] = json.dumps(
                {"acceptance_criteria": task.acceptance_criteria, "tdd": task.tdd}
            )
            changes["contract_locked_at"] = now
        tasks_dao.update(conn, task.id, task.version, changes)

    def _select_mutator(
        self,
        conn,
        task: tasks_dao.TaskRow,
        *,
        preferred_agent: str | None = None,
        exclude_agents: set[str] | None = None,
    ) -> AgentAssignment | None:
        preferred = preferred_agent or self._agent_for(task)
        return self._selector().select_mutator(
            conn,
            now=self._now(),
            preferred_agent=preferred,
            exclude_agents=exclude_agents,
        )

    def _select_reviewer(
        self,
        conn,
        task: tasks_dao.TaskRow,
        *,
        source_agent: str | None,
        preferred_agent: str | None = None,
    ) -> AgentAssignment | None:
        del task
        return self._selector().select_reviewer(
            conn,
            now=self._now(),
            source_agent=source_agent,
            preferred_agent=preferred_agent,
        )

    def _agent_selectable(self, conn, agent: str) -> bool:
        return agent_availability.is_selectable(conn, agent, now=self._now())

    def _record_agent_success(self, conn, run: runs_dao.RunRow) -> None:
        if run.agent == SYSTEM_AGENT:
            return
        agent_availability.mark_success(
            conn, run.agent, now=self._now(), source_run_id=run.id
        )

    def _record_agent_failure(
        self,
        conn,
        run: runs_dao.RunRow,
        classification: ReliableFailureClassification,
    ) -> None:
        if (
            run.agent == SYSTEM_AGENT
            or classification.category not in AGENT_FAILURE_CATEGORIES
        ):
            return
        agent_availability.mark_failure(
            conn,
            run.agent,
            category=classification.category,
            detail=classification.explain,
            now=self._now(),
            source_run_id=run.id,
        )

    def _selector(self) -> AgentSelector:
        return AgentSelector(self._profile())

    def _agent_for(self, task: tasks_dao.TaskRow) -> str:
        if self._primary_agent:
            return self._primary_agent
        if task.owner:
            return task.owner
        profile = self._profile()
        return str(profile.get("primary_agent") or CLAUDE_AGENT)

    def _auto_approve_enabled(self) -> bool:
        if self._auto_approve is not None:
            return self._auto_approve
        return bool(self._profile().get("auto_approve_plans", False))

    def _review_agent_name(self) -> str:
        if self._review_agent:
            return self._review_agent
        return str(self._profile().get("review_agent") or CODEX_AGENT)

    def _review_model_name(self) -> str:
        if self._review_model:
            return self._review_model
        return str(
            self._profile().get("codex_review_model")
            or self._profile().get("review_model")
            or DEFAULT_CODEX_MODEL
        )

    def _profile(self) -> dict[str, Any]:
        if self._profile_cache is not None:
            return self._profile_cache
        path = os.path.join(self.project_dir, ".superharness", "profile.yaml")
        if not os.path.isfile(path):
            self._profile_cache = {}
            return self._profile_cache
        try:
            with open(path, encoding="utf-8") as handle:
                value = yaml.safe_load(handle) or {}
            self._profile_cache = value if isinstance(value, dict) else {}
            return self._profile_cache
        except (OSError, yaml.YAMLError):
            self._profile_cache = {}
            return self._profile_cache

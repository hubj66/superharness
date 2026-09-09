"""The feature-gated lifecycle owner for reliable-orchestrator tasks.

Phase 3 ships successful implementation Runs to a confirmed PR head SHA, then
stops at ``pr_open``.  It does not review, repair, fall back, or merge work.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import yaml

from superharness.engine import inbox_dao, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db, transaction
from superharness.engine.next_action import validate_status_transition
from superharness.engine.reliable_orchestrator_gate import (
    is_reliable_orchestrated_task,
)
from superharness.engine.reliable_worktree import create_managed_worktree
from superharness.engine.run_results import validate_result_for_run
from superharness.engine.shipper import SYSTEM_AGENT, SystemShipper
from superharness.engine.state_errors import BoundaryError

_ACTIVE_INBOX_STATUSES = ("pending", "launched", "running", "paused")


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
        shipper_factory: Callable[[str], SystemShipper] | None = None,
    ) -> None:
        self.project_dir = os.path.realpath(project_dir)
        self._now = now
        self._primary_agent = primary_agent
        self._auto_approve = auto_approve_plans
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
                            self._create_dispatch_run(
                                conn, task, kind="plan", dedupe_key=f"plan:{task.id}"
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
                    if task is None or task.status != "plan_approved":
                        continue

                    implementation_runs = runs_dao.list_runs_for_task(
                        conn, task.id, kind="implement"
                    )
                    if not implementation_runs:
                        plan_runs = runs_dao.list_runs_for_task(
                            conn, task.id, kind="plan"
                        )
                        plan_token = plan_runs[-1].id if plan_runs else "manual"
                        self._create_dispatch_run(
                            conn,
                            task,
                            kind="implement",
                            dedupe_key=f"implement:{task.id}:{plan_token}",
                            parent_run_id=plan_runs[-1].id if plan_runs else None,
                        )
                        self._transition_task(conn, task, "in_progress")
                        result = TickResult(
                            inspected=result.inspected,
                            runs_created=result.runs_created + 1,
                            results_consumed=result.results_consumed,
                            transitions=result.transitions + 1,
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
                        result = TickResult(
                            inspected=result.inspected,
                            runs_created=result.runs_created,
                            results_consumed=result.results_consumed + consumed,
                            transitions=result.transitions + transitions,
                        )
            return result
        finally:
            conn.close()

    def _consume_finished(self, conn, task: tasks_dao.TaskRow) -> tuple[int, int, int]:
        consumed = 0
        transitions = 0
        created = 0
        for run in runs_dao.list_unconsumed_finished_runs(conn, task_id=task.id):
            if run.kind not in {"plan", "implement", "ship"}:
                continue
            if run.status == "succeeded":
                try:
                    result = (
                        None
                        if run.kind == "ship"
                        else validate_result_for_run(run.result_json, run)
                    )
                except (BoundaryError, ValueError, TypeError, json.JSONDecodeError):
                    # A malformed result remains visible for operator repair and
                    # cannot move task state.
                    continue
                if result is not None and result.completion_status != "completed":
                    continue
                if run.kind == "plan" and task.status == "todo":
                    self._transition_task(conn, task, "plan_proposed")
                    transitions += 1
                    task = tasks_dao.get(conn, task.id) or task
                elif run.kind == "implement" and task.status == "in_progress":
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
            # Terminal failures are execution facts, not a task retry policy.
            # They are consumed so a later tick cannot repeatedly act on them.
            if run.status != "succeeded" or run.kind in {"implement", "ship"}:
                self._terminalize_inbox(conn, run, failed=run.status != "succeeded")
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
            elif run.kind == "plan":
                self._terminalize_inbox(conn, run, failed=False)
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
        return consumed, transitions, created

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

    def _create_dispatch_run(
        self,
        conn,
        task: tasks_dao.TaskRow,
        *,
        kind: str,
        dedupe_key: str,
        parent_run_id: str | None = None,
    ) -> runs_dao.RunRow:
        agent = self._agent_for(task)
        worktree = None
        if kind == "implement" and self._is_git_repo():
            worktree = create_managed_worktree(self.project_dir, task.id)
        run_id = "run-" + hashlib.sha256(dedupe_key.encode()).hexdigest()[:24]
        run = runs_dao.create_run(
            conn,
            id=run_id,
            task_id=task.id,
            kind=kind,
            agent=agent,
            dedupe_key=dedupe_key,
            parent_run_id=parent_run_id,
            worktree_path=worktree.path if worktree else None,
            branch_name=worktree.branch_name if worktree else None,
            base_sha=worktree.base_sha if worktree else None,
            now=self._now(),
        )
        if run.inbox_id is not None:
            return run

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
                project_path=worktree.path if worktree else self.project_dir,
                plan_only=kind == "plan",
                run_id=run.id,
                now=self._now(),
            )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox_id)
        return runs_dao.get_run(conn, run.id) or run

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
        extras["reliable_orchestrator"] = {
            "branch_name": ship_run.branch_name,
            "base_sha": ship_run.base_sha,
            "head_sha": ship_run.head_sha,
            "remote_head_sha": ship_run.remote_head_sha,
            "pr_number": ship_run.pr_number,
            "pr_url": ship_run.pr_url,
            "ship_run_id": ship_run.id,
        }
        tasks_dao.update(
            conn,
            task.id,
            task.version,
            {
                "extras_json": json.dumps(extras),
                "worktree_path": ship_run.worktree_path,
            },
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
        }
        if new_status in timestamp_columns:
            changes[timestamp_columns[new_status]] = now
        if new_status == "plan_approved" and not task.contract_locked_at:
            changes["locked_contract"] = json.dumps(
                {"acceptance_criteria": task.acceptance_criteria, "tdd": task.tdd}
            )
            changes["contract_locked_at"] = now
        tasks_dao.update(conn, task.id, task.version, changes)

    def _agent_for(self, task: tasks_dao.TaskRow) -> str:
        if self._primary_agent:
            return self._primary_agent
        if task.owner:
            return task.owner
        profile = self._profile()
        return str(profile.get("primary_agent") or "claude-code")

    def _auto_approve_enabled(self) -> bool:
        if self._auto_approve is not None:
            return self._auto_approve
        return bool(self._profile().get("auto_approve_plans", False))

    def _profile(self) -> dict[str, Any]:
        path = os.path.join(self.project_dir, ".superharness", "profile.yaml")
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, encoding="utf-8") as handle:
                value = yaml.safe_load(handle) or {}
            return value if isinstance(value, dict) else {}
        except (OSError, yaml.YAMLError):
            return {}

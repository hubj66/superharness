"""The feature-gated lifecycle owner for reliable-orchestrator tasks.

Phase 2 deliberately stops at ``in_progress``.  It creates and consumes plan
and implementation Runs, but does not ship, review, repair, or merge work.
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
from superharness.engine.run_results import validate_result_for_run
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
    ) -> None:
        self.project_dir = os.path.realpath(project_dir)
        self._now = now
        self._primary_agent = primary_agent
        self._auto_approve = auto_approve_plans

    def tick(self, task_id: str | None = None) -> TickResult:
        """Reconcile gated tasks once, atomically and safely on repeat calls."""
        conn = get_connection(self.project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                tasks = [
                    tasks_dao.get(conn, task_id) if task_id else task
                    for task in ([None] if task_id else tasks_dao.get_all(conn))
                ]
                if task_id:
                    tasks = [tasks[0]]
                result = TickResult()
                for task in tasks:
                    if task is None or not is_reliable_orchestrated_task(task):
                        continue
                    result = TickResult(
                        inspected=result.inspected + 1,
                        runs_created=result.runs_created,
                        results_consumed=result.results_consumed,
                        transitions=result.transitions,
                    )
                    consumed, transitions = self._consume_finished(conn, task)
                    result = TickResult(
                        inspected=result.inspected,
                        runs_created=result.runs_created,
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
                return result
        finally:
            conn.close()

    def _consume_finished(self, conn, task: tasks_dao.TaskRow) -> tuple[int, int]:
        consumed = 0
        transitions = 0
        for run in runs_dao.list_unconsumed_finished_runs(conn, task_id=task.id):
            if run.kind not in {"plan", "implement"}:
                continue
            if run.status == "succeeded":
                try:
                    result = validate_result_for_run(run.result_json, run)
                except (BoundaryError, ValueError, TypeError, json.JSONDecodeError):
                    # A malformed result remains visible for operator repair and
                    # cannot move task state.
                    continue
                if result.completion_status != "completed":
                    continue
                if run.kind == "plan" and task.status == "todo":
                    self._transition_task(conn, task, "plan_proposed")
                    transitions += 1
                    task = tasks_dao.get(conn, task.id) or task
            # Terminal failures are execution facts, not a task retry policy.
            # They are consumed so a later tick cannot repeatedly act on them.
            if run.status != "succeeded" or run.kind == "implement":
                self._terminalize_inbox(conn, run, failed=run.status != "succeeded")
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
            elif run.kind == "plan":
                self._terminalize_inbox(conn, run, failed=False)
                runs_dao.mark_run_consumed(conn, run.id, now=self._now())
                consumed += 1
        return consumed, transitions

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
        run_id = "run-" + hashlib.sha256(dedupe_key.encode()).hexdigest()[:24]
        run = runs_dao.create_run(
            conn,
            id=run_id,
            task_id=task.id,
            kind=kind,
            agent=agent,
            dedupe_key=dedupe_key,
            parent_run_id=parent_run_id,
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
                project_path=self.project_dir,
                plan_only=kind == "plan",
                run_id=run.id,
                now=self._now(),
            )
        runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox_id)
        return runs_dao.get_run(conn, run.id) or run

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

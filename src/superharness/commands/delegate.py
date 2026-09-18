"""Python port of delegate.sh.

Builds a prompt for the target agent (claude-code or codex-cli) and either
prints it (--print-only) or launches the CLI or SDK.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from superharness.engine.adapter_registry import flagship_1m
from superharness.engine.context_hint import build_context_hint
from superharness.engine.sdk_runner import sdk_available, SDKRunner
from superharness.engine.orchestrator import DecompositionResult
from superharness.engine.taxonomy import VALID_EFFORTS
from superharness.utils.paths import is_project_initialized
import logging

from superharness.engine.state_errors import StateError

logger = logging.getLogger(__name__)


_JSON_MODE = False
_JSON_CTX: dict = {}


def _abort(msg: str, code: int = 1) -> None:
    if _JSON_MODE:
        from superharness.utils.json_output import emit_error

        emit_error(msg, exit_code=code, **_JSON_CTX)
    print(msg, file=sys.stderr)
    sys.exit(code)


def _rotate_launcher_logs(
    log_dir: Path, task_id: str, agent: str, keep: int = 5
) -> None:
    """Keep only the most recent N log files for a given task+agent combination.

    Args:
        log_dir: Directory containing launcher log files
        task_id: Task ID to filter logs
        agent: Agent name to filter logs
        keep: Number of most recent logs to keep (default: 5)
    """
    pattern = f"{task_id}-{agent}-*.log"
    log_files = sorted(
        log_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True
    )

    # Delete all but the most recent `keep` files
    for old_log in log_files[keep:]:
        try:
            old_log.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Profile helpers
# ---------------------------------------------------------------------------


def _read_profile_field(project_dir: str, field: str, default: str) -> str:
    profile_file = os.path.join(project_dir, ".superharness", "profile.yaml")
    if not os.path.isfile(profile_file):
        return default
    try:
        import yaml

        with open(profile_file) as f:
            doc = yaml.safe_load(f) or {}
        val = doc.get(field)
        return str(val) if val is not None else default
    except Exception as e:
        logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        return default


# ---------------------------------------------------------------------------
# Contract helpers (direct YAML reads — avoids subprocess for simple fields)
# ---------------------------------------------------------------------------


def _get_contract_id(project_dir: str) -> str:
    from superharness.engine import state_reader

    doc = state_reader.get_contract_doc(project_dir)
    return str(doc.get("id") or "") or "unknown-contract"


def _get_task_acceptance_criteria(project_dir: str, task_id: str) -> list[str]:
    from superharness.engine import state_reader

    task = state_reader.get_task(project_dir, task_id)
    if task:
        ac = task.get("acceptance_criteria")
        if isinstance(ac, list):
            return [str(c) for c in ac]
    return []


def _get_task_title(project_dir: str, task_id: str) -> str:
    from superharness.engine import state_reader

    task = state_reader.get_task(project_dir, task_id)
    return str((task or {}).get("title") or "")


def _get_task_field(project_dir: str, task_id: str, field: str) -> str | None:
    """Return a string field from a task, or None if absent."""
    from superharness.engine import state_reader

    task = state_reader.get_task(project_dir, task_id)
    if not task:
        return None
    val = task.get(field)
    return str(val) if val is not None else None


def _build_task_execution_prompt(
    *,
    target: str,
    task_id: str,
    contract_id: str,
    latest_handoff: bool,
    acceptance_criteria: str,
    context_hint: str,
    user_instructions: str,
    auto_directive: str,
) -> str:
    """Build the shared task prompt, addressed to the selected harness."""
    if latest_handoff:
        handoff_instruction = (
            f"Read the latest handoff addressed to {target} and execute task {task_id}.\n"
            "Use scope, commands, and acceptance criteria from the handoff.\n"
            "Run `shux rules` to see project constraints before starting.\n"
            "Use `shux contract` to update task status. Append .superharness/ledger.md, "
            "and refresh the handoff with outcomes.\n"
        )
    else:
        handoff_instruction = (
            f"No handoff exists yet for task {task_id}; you are {target}.\n"
            "Run `shux rules` to see project constraints before starting.\n"
            f"Use `shux contract` and `shux context {task_id}` to understand the task.\n"
            "Use `shux contract` to update task status. Append .superharness/ledger.md, "
            "and create a new handoff with outcomes.\n"
        )
    return (
        "continue contract\n"
        f"{handoff_instruction}"
        f"Contract id: {contract_id}."
        f"{acceptance_criteria}{context_hint}{user_instructions}{auto_directive}"
    )


def _build_reliable_task_execution_prompt(
    *,
    target: str,
    task_id: str,
    run_id: str,
    run_kind: str,
    review_target_sha: str | None,
    worktree_path: str | None,
    branch_name: str | None,
    base_sha: str | None,
    head_sha: str | None,
    acceptance_criteria: str,
    context_hint: str,
    user_instructions: str,
    auto_directive: str,
    result_path: str | None = None,
) -> str:
    """Build the agent contract for one durable reliable-orchestrator Run.

    Reliable agents report facts for the Run; they never own task lifecycle or
    repository shipping. Keep this separate from the legacy prompt because
    legacy handoff/contract instructions remain supported for other workflows.
    """
    common = (
        "continue reliable-orchestrator Run\n"
        f"Run id: {run_id}. Task: {task_id}. Run kind: {run_kind}.\n"
        "SQLite Run state is authoritative. Report execution facts for this Run; "
        "do not mutate task lifecycle state.\n"
        "Do not run `shux task status` or `shux contract` to change status. "
        "Do not create or update ledger/handoff lifecycle records.\n"
        "Do not commit, push, create or update a PR, merge, enable auto-merge, "
        "invoke /ship, or close the task.\n"
    )
    from superharness.engine.run_results import reliable_result_instructions

    result_contract = reliable_result_instructions(
        run_id=run_id,
        task_id=task_id,
        kind=run_kind,
        agent=target,
        artifact_path=result_path,
        review_target_sha=review_target_sha,
        worktree_path=worktree_path,
        branch_name=branch_name,
        base_sha=base_sha,
        head_sha=head_sha,
    )

    if run_kind == "plan":
        role = (
            "Planning only: inspect the task and repository as needed, then provide "
            "a concise implementation plan and acceptance/test approach. Do not "
            "modify source, tests, or configuration. The orchestrator will consume "
            "your successful Run result and advance the task.\n"
        )
    elif run_kind == "review":
        role = (
            "Review only: inspect the exact immutable review checkout and report a "
            "structured LGTM or REJECTED verdict. Do not modify any file or task.\n"
            f"The required review target SHA is {review_target_sha or 'missing'}.\n"
        )
    else:
        role = (
            "Mutation execution: edit only the source and tests needed for the task, "
            "run the relevant tests, and leave the intended changes in this managed "
            "worktree for the system shipper. Do not perform lifecycle or shipping "
            "actions yourself.\n"
        )
    return (
        common
        + role
        + result_contract
        + acceptance_criteria
        + context_hint
        + user_instructions
        + auto_directive
    )


# Effort → max budget USD (per-task caps to prevent runaway spend)
EFFORT_BUDGET_MAP = {
    "low": 0.50,
    "medium": 2.00,
    "high": 5.00,
    "max": 15.00,
}


def _get_task_budget(project_dir: str, task_id: str, effort: str) -> float | None:
    """Return task budget: explicit budget_usd from contract, or effort-based default."""
    explicit = _get_task_field(project_dir, task_id, "budget_usd")
    if explicit:
        try:
            return float(explicit)
        except (ValueError, TypeError):
            pass
    return EFFORT_BUDGET_MAP.get(effort)


def _save_context_snapshot(
    project_dir: str, task_id: str, result: dict, model: str | None = None
) -> None:
    """Save a context snapshot after dispatch for future warm-start reference."""
    import subprocess

    snapshot_dir = os.path.join(project_dir, ".superharness", "context-cache")
    os.makedirs(snapshot_dir, exist_ok=True)
    snapshot = {
        "task_id": task_id,
        "input_tokens": result.get("input_tokens", 0),
        "output_tokens": result.get("output_tokens", 0),
        "cost_usd": result.get("cost_usd", 0),
    }
    # Capture which files were modified during dispatch
    try:
        r = subprocess.run(
            ["git", "diff", "--name-only", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
            cwd=project_dir,
        )
        snapshot["files_touched"] = [f for f in r.stdout.strip().splitlines() if f]
    except Exception as e:
        logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        snapshot["files_touched"] = []
    try:
        from superharness.engine.yaml_helpers import round_trip_dump

        round_trip_dump(snapshot, os.path.join(snapshot_dir, f"{task_id}.yaml"))
    except Exception as e:
        logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        pass

    # Persist measured usage to the durable task_usage table (additive to the
    # YAML sidecar above). A DB write failure must never break dispatch.
    try:
        from superharness.engine.db import managed_connection
        from superharness.engine import usage_dao

        with managed_connection(project_dir) as conn:
            usage_dao.record(
                conn,
                task_id=task_id,
                agent="claude-code",
                source="sdk",
                model=model,
                input_tokens=result.get("input_tokens"),
                output_tokens=result.get("output_tokens"),
                cost_usd=result.get("cost_usd"),
            )
    except Exception as e:
        logger.warning("delegate.py failed to record task_usage: %s", e, exc_info=True)


def _get_task_previously_failed(project_dir: str, task_id: str) -> bool:
    status = _get_task_field(project_dir, task_id, "status")
    return status == "failed"


def _get_latest_handoff_task(handoff_dir: str, to: str) -> tuple[str, str]:
    """Returns (task_id, handoff_file) or ("", "") — reads from SQLite (source of truth)."""
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(handoff_dir)))
    try:
        from superharness.engine.db import managed_connection
        from superharness.engine import handoffs_dao

        with managed_connection(project_dir) as conn:
            rows = handoffs_dao.get_for_agent(conn, to_agent=to)
        for row in rows:
            task_val = str(row.task_id or "")
            if task_val:
                fpath = os.path.join(handoff_dir, f"{task_val}-{row.phase}.yaml")
                return task_val, fpath
        return "", ""
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            "_get_latest_handoff_task SQLite error: %s", e
        )
        return "", ""


# ---------------------------------------------------------------------------
# Discussion round context helpers
# ---------------------------------------------------------------------------

# Thin re-exports so existing call sites in this module and any external
# imports keep working. Canonical implementations live in engine.next_action.


def _check_dispatch_gates(project_dir: str, task_id: str, target: str) -> int | None:
    """Check scheduling and dependency gates before dispatch.

    Returns:
        None if all gates pass (continue dispatch).
        1 if a gate blocks (caller should abort dispatch).
    """
    from datetime import datetime, date as _date

    today = _date.today()

    # Gate 1: scheduled_after
    scheduled_after = _get_task_field(project_dir, task_id, "scheduled_after")
    if scheduled_after:
        try:
            sched_date = datetime.strptime(scheduled_after.strip(), "%Y-%m-%d").date()
            if today < sched_date:
                days_left = (sched_date - today).days
                print(
                    f"⛔ Task '{task_id}' is not ready — scheduled after {scheduled_after} ({days_left} day(s) from now).",
                    file=sys.stderr,
                )
                return 1
        except ValueError:
            pass

    # Gate 2: due_by (warn only, never block)
    due_by = _get_task_field(project_dir, task_id, "due_by")
    if due_by:
        try:
            due_date = datetime.strptime(due_by.strip(), "%Y-%m-%d").date()
            if today > due_date:
                days_overdue = (today - due_date).days
                print(
                    f"⚠️ Task '{task_id}' is overdue — due by {due_by} ({days_overdue} day(s) ago).",
                    file=sys.stderr,
                )
        except ValueError:
            pass

    # Gate 3: depends_on / blocked_by
    from superharness.engine import state_reader as _sr

    task_obj = _sr.get_task(project_dir, task_id)
    _dep_id_set: list[str] = []
    if task_obj:
        _dep_id_set = task_obj.get("blocked_by") or []
    if _dep_id_set:
        blockers = []
        for dep_id in _dep_id_set:
            dep_status = _get_task_field(project_dir, dep_id, "status")
            if dep_status not in ("done", "archived"):
                blockers.append(f"{dep_id} (status: {dep_status or 'not found'})")
        if blockers:
            print(
                f"blocked: task '{task_id}' depends on unfinished tasks:",
                file=sys.stderr,
            )
            for b in blockers:
                print(f"   - {b}", file=sys.stderr)
            try:
                from superharness.engine.ledger_dao import decision_log

                decision_log(
                    project_dir,
                    "gate_block",
                    task_id=task_id,
                    agent=target,
                    reason=f"unfinished dependencies: {', '.join(blockers)}",
                )
            except Exception as e:
                logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            return 1

    return None  # all gates pass


from superharness.engine.next_action import (  # noqa: E402
    _DISC_ROUND_RE,
    infer_workflow as _infer_workflow,
    allowed_statuses_for_workflow as _allowed_statuses_for_workflow,
    plan_only_allowed_statuses as _plan_only_allowed_statuses,
)

# Exit-code contract for permanent-block launcher failures.
# Callers (launcher / inbox dispatch) treat exit 2 as non-retryable so the
# inbox doesn't waste its retry budget on a lifecycle violation that will
# fail identically every time.
EXIT_PERMANENT_BLOCK = 2


def _reliable_run_dispatch_statuses(
    project_dir: str,
    *,
    task_id: str,
    target: str,
    model_override: str,
    plan_only: bool,
    for_review: bool,
) -> tuple[set[str] | None, str | None, str | None]:
    """Validate a reliable-orchestrator dispatch against its durable Run.

    Reliable lifecycle dispatch is owned by LifecycleOrchestrator. When a
    dispatcher launches a linked inbox item it supplies SUPERHARNESS_RUN_ID; use
    that Run's kind as the authority for which task statuses are legal.
    """

    run_id = os.environ.get("SUPERHARNESS_RUN_ID", "").strip()
    if not run_id:
        return None, None, None

    from superharness.engine import runs_dao
    from superharness.engine.db import managed_connection

    try:
        with managed_connection(project_dir) as conn:
            run = runs_dao.get_run(conn, run_id)
    except (OSError, sqlite3.Error, StateError) as exc:
        return None, f"could not read reliable Run '{run_id}': {exc}", None

    if run is None:
        return None, f"reliable Run '{run_id}' not found", None
    if run.task_id != task_id:
        return (
            None,
            f"reliable Run '{run_id}' belongs to task '{run.task_id}', not '{task_id}'",
            None,
        )
    if run.agent != target:
        return (
            None,
            f"reliable Run '{run_id}' targets agent '{run.agent}', not '{target}'",
            None,
        )
    if not run.model:
        return None, f"reliable Run '{run_id}' has no persisted model", None
    if model_override and model_override != run.model:
        return (
            None,
            f"reliable Run '{run_id}' uses model '{run.model}', not '{model_override}'",
            None,
        )

    if run.kind == "plan":
        if not plan_only or for_review:
            return (
                None,
                f"reliable plan Run '{run_id}' must dispatch with --plan-only",
                None,
            )
    elif run.kind == "review":
        if plan_only or not for_review:
            return (
                None,
                f"reliable review Run '{run_id}' must dispatch with --for-review",
                None,
            )
    elif run.kind in {"implement", "fallback", "repair"}:
        if plan_only or for_review:
            return (
                None,
                f"reliable {run.kind} Run '{run_id}' must dispatch as worker execution",
                None,
            )
    elif run.kind == "ship":
        return (
            None,
            f"reliable ship Run '{run_id}' is system-owned and not delegable",
            None,
        )
    else:
        return None, f"reliable Run '{run_id}' has unsupported kind '{run.kind}'", None

    from superharness.engine.next_action import reliable_dispatch_statuses_for_run_kind

    statuses = reliable_dispatch_statuses_for_run_kind(run.kind)
    if not statuses:
        return None, f"reliable Run kind '{run.kind}' is not agent-dispatchable", None
    return statuses, None, run.model


def _reliable_run_for_prompt(project_dir: str):
    """Return the durable Run bound to this agent process, if any."""
    run_id = os.environ.get("SUPERHARNESS_RUN_ID", "").strip()
    if not run_id:
        return None
    from superharness.engine import runs_dao
    from superharness.engine.db import managed_connection

    with managed_connection(project_dir) as conn:
        return runs_dao.get_run(conn, run_id)


# ---------------------------------------------------------------------------
# Orchestrator helpers
# ---------------------------------------------------------------------------


def _record_decomposition(
    project_dir: str,
    task_id: str,
    decomposition: DecompositionResult,
) -> None:
    """Record orchestration event, update parent metadata, and upsert subtasks to SQLite.
    Never raises.
    """
    try:
        from datetime import datetime, timezone
        from superharness.engine.db import get_connection, init_db, transaction
        from superharness.engine import tasks_dao, ledger_dao
        from superharness.engine.tasks_dao import TaskRow
        import json as _j

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn = get_connection(project_dir)
        try:
            init_db(conn)
            with transaction(conn):
                # 1. Update parent task metadata in extras_json (v11)
                parent = tasks_dao.get(conn, task_id)
                if parent:
                    extras = _j.loads(parent.extras_json) if parent.extras_json else {}

                    # Normalize subtasks for the parent's extras blob (matches legacy YAML shape)
                    normalised_st = []
                    for st in decomposition.subtasks:
                        if not isinstance(st, dict):
                            continue
                        st = dict(st)
                        st.setdefault("status", "pending")
                        st.setdefault("owner", "claude-code")
                        normalised_st.append(st)

                    extras["subtasks"] = normalised_st
                    extras["estimated_cost_usd"] = round(
                        decomposition.total_estimated_cost_usd, 4
                    )
                    extras["budget_usd"] = round(
                        decomposition.recommended_budget_usd, 4
                    )

                    tasks_dao.update(
                        conn, task_id, parent.version, {"extras_json": _j.dumps(extras)}
                    )

                # 2. Record ledger event
                ledger_dao.record(
                    conn,
                    task_id=task_id,
                    agent="orchestrator",
                    action="decompose",
                    details={"subtask_count": len(decomposition.subtasks)},
                    now=now,
                )

                # 3. Upsert subtasks as separate rows (v2 parent_id)
                for st in decomposition.subtasks:
                    if not isinstance(st, dict):
                        continue
                    st_id = str(st.get("id", ""))
                    if not st_id:
                        continue

                    # v2: parent_id links subtask to its parent
                    # v10: stamped defaults
                    row = TaskRow(
                        id=st_id,
                        title=str(st.get("title", st_id)),
                        owner=str(st.get("owner", "claude-code")),
                        status=str(st.get("status") or "pending"),
                        effort=st.get("effort"),
                        project_path=project_dir,
                        parent_id=task_id,
                        development_method=None,
                        acceptance_criteria=[],
                        test_types=[],
                        out_of_scope=[],
                        definition_of_done=[],
                        context=None,
                        tdd=None,
                        version=1,
                        created_at=now,
                        blocked_by=list(st.get("blocked_by") or []),
                    )
                    tasks_dao.upsert(conn, row)
        finally:
            conn.close()
    except Exception as e:
        logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        # Orchestration record is non-critical; don't crash delegation if DB is locked
        pass


def build_discussion_prompt(
    target: str,
    discussion_id: str,
    discussion_round: int,
    disc_topic: str,
    disc_max: str,
    submit_path: str,
    auto_directive: str = "",
    prior_context: str = "",
) -> str:
    """Build the instruction prompt for a discussion round."""
    if discussion_round == 1:
        return (
            f"You are participating in a multi-agent discussion.\n"
            f"Topic: {disc_topic}\n"
            f"You are: {target}\n"
            f"This is round {discussion_round} of {disc_max}.\n"
            f"\n"
            f"Review the topic and write YOUR OWN position. Be specific about what you agree or disagree with.\n"
            f"\n"
            f"IMPORTANT: You MUST write your own submission file. Other agents write their own files.\n"
            f"Do NOT skip writing because another agent already submitted. Every agent's perspective matters.\n"
            f"\n"
            f"When done, write a YAML file to: {submit_path}\n"
            f"The file must have these fields:\n"
            f"  discussion_id: {discussion_id}\n"
            f"  round: {discussion_round}\n"
            f"  agent: {target}\n"
            f"  verdict: agree OR disagree OR partial\n"
            f"  position: your free-form analysis\n"
            f"  points: list of {{id, verdict, rationale}} for each sub-point\n"
            f"  submitted_at: (current UTC ISO 8601 timestamp)\n"
            f"\n"
            f"If you agree with everything, set verdict: agree. "
            f"Otherwise set verdict: disagree or partial and explain in points.\n"
            f"Run `shux context {discussion_id}` for the full discussion context.\n"
            f"Read the handoff referenced in the project contract for the task details."
            f"{auto_directive}"
        )
    else:
        return (
            f"You are participating in a multi-agent discussion.\n"
            f"Topic: {disc_topic}\n"
            f"You are: {target}\n"
            f"This is round {discussion_round} of {disc_max}.\n"
            f"\n"
            f"Here are the positions from prior rounds:\n"
            f"{prior_context}\n"
            f"\n"
            f"Consider the other agent's position carefully. "
            f"If you now agree with all points, set verdict: agree.\n"
            f"If you still disagree, explain specifically what remains unresolved.\n"
            f"\n"
            f"Write your response to: {submit_path}\n"
            f"The file must have these fields:\n"
            f"  discussion_id: {discussion_id}\n"
            f"  round: {discussion_round}\n"
            f"  agent: {target}\n"
            f"  verdict: agree OR disagree OR partial\n"
            f"  position: your free-form analysis\n"
            f"  points: list of {{id, verdict, rationale}} for each sub-point\n"
            f"  submitted_at: (current UTC ISO 8601 timestamp)\n"
            f"\n"
            f"Run `shux context {discussion_id}` for full context."
            f"{auto_directive}"
        )


def _get_round_context(disc_dir: str, round_: int, agent: str) -> dict:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "superharness.engine.discussion",
            "round_context",
            "--discussion-dir",
            disc_dir,
            "--round",
            str(round_),
            "--agent",
            agent,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        _abort("Failed to get discussion context")
    import json

    return json.loads(result.stdout)


def _build_prior_context(ctx: dict) -> str:
    lines = []
    for r in ctx.get("prior_rounds") or []:
        lines.append(f"--- Round {r['round']} ---")
        for p in r.get("positions") or []:
            lines.append(f"Agent: {p.get('agent', '')}")
            lines.append(f"Verdict: {p.get('verdict', '')}")
            lines.append(f"Position: {p.get('position', '')}")
            pts = p.get("points") or []
            if isinstance(pts, list) and pts:
                lines.append("Points:")
                for pt in pts:
                    if isinstance(pt, dict):
                        lines.append(
                            f"  - {pt.get('id', '')}: {pt.get('verdict', '')} — {pt.get('rationale', '')}"
                        )
            lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Confirmation helpers (mirrors Bash confirm_* functions)
# ---------------------------------------------------------------------------


def _confirm_non_interactive_risk(target: str, codex_bypass: bool) -> None:
    if target == "codex-cli" and codex_bypass:
        risk_msg = "Risk: non-interactive Codex bypass disables sandbox and approval prompts. Continue?"
    else:
        risk_msg = (
            "Risk: non-interactive mode runs without live user supervision. Continue?"
        )

    env_val = os.environ.get("SUPERHARNESS_CONFIRM_NON_INTERACTIVE", "")
    if env_val in ("YES", "yes", "Y", "y"):
        return

    if not sys.stdin.isatty():
        print(
            "Refusing non-interactive launch without explicit confirmation.",
            file=sys.stderr,
        )
        print(
            "Set SUPERHARNESS_CONFIRM_NON_INTERACTIVE=YES to allow unattended launch.",
            file=sys.stderr,
        )
        sys.exit(1)

    sys.stderr.write(f"{risk_msg} [y/N]: ")
    sys.stderr.flush()
    ans = sys.stdin.readline().strip()
    if ans not in ("y", "Y", "yes", "YES"):
        print("Aborted by user.", file=sys.stderr)
        sys.exit(1)


def _confirm_dangerous_flag_risk(env_name: str, risk_msg: str) -> None:
    env_val = os.environ.get(env_name, "")
    if env_val in ("YES", "yes", "Y", "y"):
        return

    if not sys.stdin.isatty():
        print(
            "Refusing dangerous launch without explicit confirmation.", file=sys.stderr
        )
        print(f"Set {env_name}=YES to allow this specific bypass.", file=sys.stderr)
        sys.exit(1)

    sys.stderr.write(f"{risk_msg} [y/N]: ")
    sys.stderr.flush()
    ans = sys.stdin.readline().strip()
    if ans not in ("y", "Y", "yes", "YES"):
        print("Aborted by user.", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Launch
# ---------------------------------------------------------------------------


def _launch_agent(
    target: str,
    prompt: str,
    project_dir: str,
    non_interactive: bool,
    codex_bypass: bool,
    label: str = "",
    model: str = "",
    effort: str = "",
    task_id: str = "",
    print_only: bool = False,
    yolo: bool = False,
) -> None:
    from superharness.engine.platform_runtime import launch_agent, expand_agent_path
    from superharness.logging_utils import get_logger, get_audit_logger, redact

    log = get_logger("delegate")
    audit = get_audit_logger()
    expand_agent_path()

    audit.info(
        "dispatch: target=%s project=%s non_interactive=%s codex_bypass=%s model=%s",
        target,
        project_dir,
        non_interactive,
        codex_bypass,
        model or "<default>",
    )
    log.info("launch_agent target=%s prompt_len=%d", target, len(prompt))
    log.debug("prompt redacted=%s", redact(prompt[:300]))

    # Harness registry (docs/PLAN-adopt-omnigent.md iterations 5-6): every
    # agent's invocation is built by its Harness adapter, proven
    # byte-identical to the legacy inline construction by
    # tests/unit/test_harness_registry.py and test_harness_adapters.py.
    # An unrecognized target fails cleanly here (KeyError-with-known-list)
    # instead of a dispatch that hangs or gets stuck 'launched'.
    from superharness.harnesses import get_harness

    try:
        harness = get_harness(target)
    except KeyError as e:
        _abort(str(e))

    invocation = harness.build_invocation(
        task={
            "prompt": prompt,
            "model": model,
            "effort": effort,
            "yolo": yolo,
            "codex_bypass": codex_bypass,
        },
        project_dir=project_dir,
        non_interactive=non_interactive,
    )
    launch_args = list(invocation.argv)
    launcher = invocation.argv[1]

    if print_only:
        print(f"would launch: {launcher}")
        return

    # Register agent daemon heartbeat before dispatching.
    # Agent daemons don't self-register — this keeps the heartbeat table accurate.
    try:
        from superharness.engine.db import get_connection, init_db, now_iso
        from superharness.engine import heartbeat_dao

        conn = get_connection(project_dir)
        try:
            init_db(conn)
            heartbeat_dao.upsert(
                conn,
                agent=target,
                task_id=task_id,
                status="launched",
                pid=os.getpid(),
                now=now_iso(),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass  # best-effort, don't block dispatch

    display_label = f" {label}" if label else ""
    agent_name = target.replace("-cli", "").replace("-code", "").capitalize()

    # Print launcher path to satisfy integration tests
    print(f"Launching {agent_name}{display_label} ({os.path.basename(launcher)})...")

    rc = launch_agent(launch_args, cwd=project_dir)
    sys.exit(rc)


def _expand_path() -> None:
    """Augment PATH with common user-local bin dirs — launchd starts with a minimal PATH."""
    extra = [
        os.path.expanduser("~/.local/bin"),
        "/usr/local/bin",
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/sbin",
    ]
    current = os.environ.get("PATH", "")
    additions = [
        p for p in extra if p not in current.split(os.pathsep) and os.path.isdir(p)
    ]
    if additions:
        os.environ["PATH"] = current + os.pathsep + os.pathsep.join(additions)


def _cmd_exists(name: str) -> bool:
    import shutil

    _expand_path()
    return shutil.which(name) is not None


# ---------------------------------------------------------------------------
# Main delegate logic
# ---------------------------------------------------------------------------


def _fire_on_delegate(project_dir: str, target: str, task_id: str) -> None:
    """Fire on_delegate lifecycle hooks (openclaw routing, telegram notify, …).

    Best-effort: a hook failure must never block a dispatch. Conditional hooks
    (e.g. openclaw's ``target == 'openclaw'``) are gated inside run_hooks.
    """
    try:
        from pathlib import Path
        from superharness.modules.runner import run_hooks

        run_hooks(
            "on_delegate",
            {
                "task_id": task_id,
                "target": target,
                "task_title": _get_task_title(project_dir, task_id),
                "task_description": _get_task_field(project_dir, task_id, "context")
                or "",
                "event": "on_delegate",
            },
            Path(project_dir),
        )
    except Exception as e:
        logger.warning("delegate.py on_delegate hooks failed: %s", e, exc_info=True)


def delegate(
    project_dir: str,
    target: str,
    task_id: str,
    print_only: bool,
    non_interactive: bool,
    codex_bypass: bool,
    for_review: bool = False,
    model_override: str = "",
    effort_override: str = "",
    no_auto_model: bool = False,
    via_sdk: bool | None = None,
    orchestrate: bool = False,
    no_orchestrate: bool = False,
    skip_preflight: bool = False,
    force: bool = False,
    plan_only: bool = False,
    ship_on_complete: bool = False,
    role: str = "worker",
    yolo: bool = False,
) -> int:
    project_dir = os.path.realpath(project_dir)

    harness_dir = os.path.join(project_dir, ".superharness")
    handoff_dir = os.path.join(harness_dir, "handoffs")

    if not is_project_initialized(project_dir):
        _abort(f"Missing project state at {project_dir}. Run 'shux init' first.")
    contract_id = _get_contract_id(project_dir)

    # Discussion-round detection
    m = _DISC_ROUND_RE.match(task_id)
    discussion_id = m.group(1) if m else ""
    discussion_round = int(m.group(2)) if m else 0
    label = f"for discussion round {discussion_round}" if discussion_id else ""

    latest_handoff = ""
    if not task_id:
        task_id, latest_handoff = _get_latest_handoff_task(handoff_dir, target)
        if not task_id:
            print(
                f"Could not determine task id. Provide --task TASK_ID or create a "  # shipguard:ignore PY-007
                f"{target} handoff in {handoff_dir}",
                file=sys.stderr,
            )
            return 1

    # ── Scheduling + dependency gates ──────────────────────────────────
    gate_result = _check_dispatch_gates(project_dir, task_id, target)
    if gate_result is not None:
        return gate_result

    from superharness.engine import state_reader as _sr

    task_obj = _sr.get_task(project_dir, task_id)

    # Gate 3b: existence — a task ID that doesn't resolve at all must fail
    # closed with a clear "not found" message, not fall through the status
    # gate below as if it were a real task with an empty status (which
    # produced a misleading "plan must be approved" message and triggered a
    # spurious ledger write for a task_id that was never real).
    if task_obj is None:
        print(f"blocked: task '{task_id}' not found", file=sys.stderr)
        return EXIT_PERMANENT_BLOCK

    _workflow = _infer_workflow(task_id, task_obj)
    # Reliable Runs use SQLite Run/task context and do not require legacy
    # handoff files. All other dispatches retain the legacy precondition.
    if not os.path.isdir(handoff_dir) and _workflow != "reliable-orchestrator":
        _abort(f"Missing handoff directory: {handoff_dir}")

    # Gate 4: minimum content — plan-only dispatch requires acceptance criteria
    # or definition of done. Empty tasks produce empty plans, wasting an agent cycle.
    if plan_only and task_obj:
        ac = task_obj.get("acceptance_criteria") or []
        dod = task_obj.get("definition_of_done") or []
        context = task_obj.get("context") or ""
        if not ac and not dod and not context:
            print(
                f"blocked: task '{task_id}' has no acceptance criteria, "
                f"definition of done, or context — nothing to plan. "
                f"Add at least one before dispatching.",
                file=sys.stderr,
            )
            try:
                from superharness.engine.ledger_dao import decision_log

                decision_log(
                    project_dir,
                    "gate_block",
                    task_id=task_id,
                    agent=target,
                    reason="empty task: no AC, DoD, or context for plan-only dispatch",
                )
            except Exception as e:
                logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
                pass
            return EXIT_PERMANENT_BLOCK  # Gate 4: minimum content

    # Gate 5: status lifecycle — dispatch is workflow-aware.
    # Terminal statuses (done/failed/stopped) pass through — reconcile handles them.
    # --plan-only: relax the allowed set so the agent can propose a plan on a
    # todo/implementation task without first needing plan_approved.
    _task_status = task_obj.get(
        "status", ""
    )  # task_obj is guaranteed non-None past Gate 3b
    _DISPATCH_TERMINAL_STATUSES = {"done", "failed", "stopped"}
    # Auto-route: todo+implementation → plan-only (soft-route, not a permanent block)
    if _workflow == "implementation" and _task_status == "todo" and not plan_only:
        plan_only = True
        print(
            f"Note: auto-applying --plan-only for '{task_id}' "
            "(status: todo, workflow: implementation) — agent will propose a plan.",
            file=sys.stderr,
        )
    _reliable_run_gate_error = None
    _reliable_run_model = None
    if _workflow == "reliable-orchestrator":
        (
            _DISPATCH_ALLOWED_STATUSES,
            _reliable_run_gate_error,
            _reliable_run_model,
        ) = _reliable_run_dispatch_statuses(
            project_dir,
            task_id=task_id,
            target=target,
            model_override=model_override,
            plan_only=plan_only,
            for_review=for_review,
        )
        if _DISPATCH_ALLOWED_STATUSES is None and _reliable_run_gate_error is None:
            _reliable_run_gate_error = (
                "reliable-orchestrator dispatch requires SUPERHARNESS_RUN_ID"
            )
    else:
        _DISPATCH_ALLOWED_STATUSES = None
    if _reliable_run_gate_error:
        print(
            f"blocked: {_reliable_run_gate_error}",
            file=sys.stderr,
        )
        try:
            from superharness.engine.ledger_dao import decision_log

            decision_log(
                project_dir,
                "gate_block",
                task_id=task_id,
                agent=target,
                reason=_reliable_run_gate_error,
            )
        except (OSError, sqlite3.Error, StateError) as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        return EXIT_PERMANENT_BLOCK
    if _DISPATCH_ALLOWED_STATUSES is None:
        if plan_only:
            _DISPATCH_ALLOWED_STATUSES = _plan_only_allowed_statuses(_workflow)
        else:
            _DISPATCH_ALLOWED_STATUSES = _allowed_statuses_for_workflow(
                _workflow, for_review=for_review
            )
    if (
        _task_status not in _DISPATCH_ALLOWED_STATUSES
        and _task_status not in _DISPATCH_TERMINAL_STATUSES
    ):
        if _workflow == "implementation":
            print(
                f"blocked: task '{task_id}' status is '{_task_status}' — "
                f"plan must be approved before delegating "
                f"(run: shux task status --id {task_id} --status plan_proposed, "
                f"or re-dispatch with --plan-only to let the agent propose the plan)",
                file=sys.stderr,
            )
        else:
            allowed = ", ".join(sorted(_DISPATCH_ALLOWED_STATUSES))
            print(
                f"blocked: task '{task_id}' status is '{_task_status}' for workflow '{_workflow}' "
                f"(allowed: {allowed})",
                file=sys.stderr,
            )
        # Permanent lifecycle block — non-retryable.
        try:
            from superharness.engine.ledger_dao import decision_log

            _allowed = ", ".join(sorted(_DISPATCH_ALLOWED_STATUSES))
            decision_log(
                project_dir,
                "gate_block",
                task_id=task_id,
                agent=target,
                reason=f"status '{_task_status}' not allowed for workflow '{_workflow}' (allowed: {_allowed})",
            )
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
        return EXIT_PERMANENT_BLOCK

    # -----------------------------------------------------------------------
    # Pre-flight analysis (fast, local-only)
    # Runs after all gates so task_obj is resolved. Warns or blocks if needed.
    # -----------------------------------------------------------------------
    if not print_only and not skip_preflight and task_obj is not None:
        try:
            from superharness.engine.preflight import run_preflight

            pf = run_preflight(
                project_dir=project_dir,
                task=dict(task_obj),
                skip_git=non_interactive,  # skip git check in non-interactive mode
            )
            if pf.status != "pass":
                print(pf.format_summary(verbose=False), file=sys.stderr)
            if not pf.can_dispatch:
                try:
                    from superharness.engine.ledger_dao import decision_log

                    decision_log(
                        project_dir,
                        "gate_block",
                        task_id=task_id,
                        agent=target,
                        reason=f"preflight: {pf.format_summary(verbose=False)}",
                    )
                except Exception as e:
                    logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
                    pass
                return 1
            # Surface fanout hint if complexity suggests parallel
            if pf.suggested_mode != "single":
                print(
                    f"  Hint: consider `--fanout {pf.suggested_fanout_n}` or swarm mode for this task.",
                    file=sys.stderr,
                )
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass  # Preflight is advisory — never block on its own errors

    # -----------------------------------------------------------------------
    # Model / effort resolution
    # Order: CLI flag > task field > classifier > profile default > standard/medium
    # -----------------------------------------------------------------------
    resolved_model = ""
    resolved_effort = ""
    model_source = "fallback"
    reliable_run_bound = bool(_reliable_run_model)
    reliable_run = _reliable_run_for_prompt(project_dir) if reliable_run_bound else None
    if reliable_run_bound and reliable_run is None:
        _abort("reliable Run disappeared before model resolution", EXIT_PERMANENT_BLOCK)
    if _reliable_run_model:
        resolved_model = _reliable_run_model
        model_source = "reliable-run"

    # 0. Role-based routing (lower priority than explicit CLI flag)
    if not reliable_run_bound and role and role != "worker" and not model_override:
        try:
            from superharness.engine.model_router_roles import ModelRouter

            _router = ModelRouter.from_project(project_dir)
            resolved_model = _router.model_for(role)
            model_source = f"role:{role}"
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
    # 1. CLI flag (overrides role routing)
    if not reliable_run_bound and model_override:
        resolved_model = model_override
        model_source = "manual"
    if effort_override:
        resolved_effort = effort_override

    # 2. Task field (if not already set by CLI)
    if not resolved_model:
        task_model = _get_task_field(project_dir, task_id, "model")
        if task_model:
            resolved_model = task_model
            model_source = "task"
    if not resolved_effort:
        task_effort = _get_task_field(project_dir, task_id, "effort")
        if task_effort:
            resolved_effort = task_effort

    # 3. Auto-classification (skip if print_only — don't call Claude for a preview)
    if (
        not print_only
        and not reliable_run_bound
        and not no_auto_model
        and (not resolved_model or not resolved_effort)
    ):
        try:
            from superharness.engine.adapter_registry import clear_manifest_cache
            from superharness.engine.model_router import (
                classify_task,
                resolve_model_for_tier,
                resolve_tier,
            )

            # Force reload of manifests (prevents stale "sonnet" fallback)
            clear_manifest_cache()

            task_title = _get_task_title(project_dir, task_id)
            ac_for_classify = _get_task_acceptance_criteria(project_dir, task_id)
            previously_failed = _get_task_previously_failed(project_dir, task_id)

            classified_tier, classified_effort = classify_task(
                title=task_title or task_id,
                criteria=ac_for_classify or None,
                previously_failed=previously_failed,
            )
            if not resolved_model:
                resolved_model = resolve_model_for_tier(
                    target, classified_tier, project_dir
                )
                model_source = "auto-classified"
            if not resolved_effort:
                resolved_effort = classified_effort
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass  # classification failure is non-fatal

    # 4. Profile defaults
    if not resolved_model:
        profile_model = _read_profile_field(project_dir, "default_model", "")
        if profile_model:
            try:
                from superharness.engine.model_router import (
                    resolve_model_for_tier,
                    resolve_tier,
                )

                tier = resolve_tier(profile_model)
                if tier:
                    resolved_model = resolve_model_for_tier(target, tier, project_dir)
                else:
                    resolved_model = profile_model
            except Exception as e:
                logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
                resolved_model = profile_model
            model_source = "profile"
    if not resolved_effort:
        profile_effort = _read_profile_field(project_dir, "default_effort", "")
        if profile_effort:
            resolved_effort = profile_effort

    # 5. Hardcoded fallback
    if not resolved_model:
        try:
            from superharness.engine.model_router import resolve_model_for_tier

            resolved_model = resolve_model_for_tier(target, "standard", project_dir)
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            resolved_model = "sonnet"
        model_source = "fallback"
    if not resolved_effort:
        resolved_effort = "medium"

    # If model_override was a tier name, resolve to agent-specific model
    if not reliable_run_bound:
        try:
            from superharness.engine.model_router import (
                resolve_tier,
                resolve_model_for_tier,
            )

            tier = resolve_tier(resolved_model)
            if tier:
                resolved_model = resolve_model_for_tier(target, tier, project_dir)
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
    # Apply ChatGPT-account overrides last so every resolution path
    # (CLI, task field, auto-classify via adapter_registry, profile,
    # fallback, tier-reroute) gets remapped when codex is signed in via
    # ChatGPT. Without this, gpt-5.3-codex reaches the codex CLI and 400s.
    if not reliable_run_bound:
        try:
            from superharness.engine.model_router import _apply_chatgpt_auth_override

            resolved_model = _apply_chatgpt_auth_override(
                target, resolved_model, project_dir
            )
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
    # -----------------------------------------------------------------------
    # Auto-orchestrate: let the best model decide owner+tier+effort+decompose.
    # Default path. Skip with --no-orchestrate for trivial tasks.
    # --orchestrate flag forces this path (backward compat, same behavior).
    # -----------------------------------------------------------------------
    should_orchestrate = not no_orchestrate and not reliable_run_bound
    # Discussion rounds need multi-agent perspectives — skip orchestrator.
    # The orchestrator would reroute every agent to the same "best" model,
    # defeating the purpose of multi-agent deliberation.
    is_discussion_round = "/round-" in task_id
    if (
        should_orchestrate
        and not is_discussion_round
        and (not print_only or orchestrate)
    ):
        try:
            from superharness.engine.orchestrator import Orchestrator

            orch = Orchestrator(project_dir=project_dir)
            task_data = {
                "id": task_id,
                "title": _get_task_title(project_dir, task_id) or task_id,
                "owner": target,
                "acceptance_criteria": _get_task_acceptance_criteria(
                    project_dir, task_id
                ),
            }
            routing = orch.route(task_data)

            print()
            print(f"Orchestrator routing for {task_id}:")
            print(f"  Owner:    {routing.owner}")
            print(f"  Tier:     {routing.tier}")
            print(f"  Effort:   {routing.effort}")
            print(f"  Rationale: {routing.rationale}")

            if routing.decompose and routing.subtasks:
                print(f"  Decomposed into {len(routing.subtasks)} subtasks:")
                for st in routing.subtasks:
                    print(
                        f"    - {st['id']}: {st['title']} [{st['owner']} / {st['model_tier']}]"
                    )
                print(f"  Estimated cost: ${routing.total_estimated_cost_usd:.4f}")
                print(f"  Recommended budget: ${routing.recommended_budget_usd:.4f}")

                if print_only:
                    return 0

                # Override target/model/effort from routing plan
                target = routing.owner
                if not resolved_model:
                    resolved_model = resolve_model_for_tier(
                        target, routing.tier, project_dir
                    )
                if not resolved_effort:
                    resolved_effort = routing.effort
                model_source = "orchestrator"
            else:
                print("  Plan:     direct dispatch (no decomposition)")
                # Apply routing: override target/model/effort
                target = routing.owner
                resolved_model = resolve_model_for_tier(
                    target, routing.tier, project_dir
                )
                resolved_effort = routing.effort
                model_source = "orchestrator"
        except Exception as e:
            logger.warning(
                "delegate.py orchestrator failed, falling back to standard dispatch: %s",
                e,
                exc_info=True,
            )
            # Fall through to existing dispatch path

    # Acceptance criteria
    ac_lines = _get_task_acceptance_criteria(project_dir, task_id)
    acceptance_criteria = ""
    if ac_lines:
        ac_block = "\n".join(f"- {c}" for c in ac_lines)
        acceptance_criteria = f"\n\nAcceptance criteria for this task:\n{ac_block}"

    auto_directive = ""
    if non_interactive:
        auto_directive = (
            "\nThis is an automated non-interactive run. "
            "Do not ask for confirmation or approval. "
            "Proceed and apply all changes immediately."
        )
    if reliable_run is not None and reliable_run.kind in {"plan", "review"}:
        auto_directive = (
            "\nThis is an automated read-only run. Do not ask for confirmation. "
            "Return the required structured result without changing source or "
            "task state."
        )

    if plan_only and not reliable_run_bound:
        # Plan-only dispatch: agent must write a plan handoff and stop. No
        # implementation code should be touched on this turn. The owner will
        # review the plan and re-dispatch (without --plan-only) to execute.
        auto_directive += (
            "\n\n=== PLAN-ONLY MODE ===\n"
            f"Your only job on this turn is to propose a TDD plan for task '{task_id}'.\n"
            "1. Do NOT write, modify, or delete any source, test, or configuration file.\n"
            "2. Write a plan handoff YAML to .superharness/handoffs/ with:\n"
            f"   task: {task_id}\n"
            "   phase: plan\n"
            "   status: plan_proposed\n"
            f"   from: {target}\n"
            "   to: owner\n"
            "   date: <ISO-8601 UTC timestamp>\n"
            "   plan: <scope and approach — 1–3 short paragraphs>\n"
            "   tdd:\n"
            "     red: <the failing tests you will add first — specific names/locations>\n"
            "     green: <the minimal implementation that will make them pass>\n"
            "     refactor: <cleanup planned after green, or 'none'>\n"
            "   risks: <open questions, unknowns, dependencies>\n"
            "3. Run `shux task status --id "
            + task_id
            + " --status plan_proposed` to transition the task.\n"
            "4. Stop. Do not proceed to implementation. The owner must review and approve the plan first.\n"
        )

    # ship_on_complete: inject directive so the agent runs /ship commit before report_ready
    _ship_on_complete = ship_on_complete or str(
        _get_task_field(project_dir, task_id, "ship_on_complete") or ""
    ).lower() in ("true", "1", "yes")
    if _ship_on_complete and not reliable_run_bound:
        auto_directive += (
            "\n\n=== SHIP-ON-COMPLETE ===\n"
            "This task has ship_on_complete: true.\n"
            "After all acceptance criteria are met and BEFORE writing report_ready:\n"
            "1. Run `ALLOW_PUSH=1 /ship commit` (non-interactive) inside this worktree.\n"
            "2. If /ship commit exits non-zero, do NOT write report_ready.\n"
            "   Instead write a failed handoff and exit.\n"
            "3. Include the PR URL in your handoff outcomes list "
            "(e.g. 'PR: https://github.com/org/repo/pull/N').\n"
            "4. Only after /ship commit succeeds, set task status to report_ready.\n"
        )

    # Discussion-round detection (moved up)
    discussions_dir = os.path.join(harness_dir, "discussions")
    prompt = ""
    # Prompt components captured as the prompt is assembled below, keyed by
    # context_dao.COMPONENT_TYPES name — recorded (best-effort) just before
    # SDK dispatch so `shux diff --context` can show what changed between
    # dispatches. See docs/PLAN-typed-boundaries-context-hashing.md, Iter 4.
    components: list[tuple[str, str]] = []

    if discussion_id and discussion_round:
        disc_dir = os.path.join(discussions_dir, discussion_id)
        if not os.path.isdir(disc_dir):
            _abort(f"Discussion directory not found: {disc_dir}")

        ctx = _get_round_context(disc_dir, discussion_round, target)
        disc_topic = str(ctx.get("topic") or "")
        disc_max = str(ctx.get("max_rounds") or "")
        submit_path = os.path.join(disc_dir, f"round-{discussion_round}-{target}.yaml")
        prior_context = _build_prior_context(ctx) if discussion_round > 1 else ""

        prompt = build_discussion_prompt(
            target=target,
            discussion_id=discussion_id,
            discussion_round=discussion_round,
            disc_topic=disc_topic,
            disc_max=disc_max,
            submit_path=submit_path,
            auto_directive=auto_directive,
            prior_context=prior_context,
        )
        components.append(("discussion_prompt", prompt))

        print(f"Project: {project_dir}")
        print(f"Discussion: {discussion_id}")
        print(f"Round: {discussion_round}")
        print(f"Agent: {target}")
        print(f"Topic: {disc_topic}")

    else:
        # Check for user-provided instructions file (not a managed runtime artifact).
        instructions_file = os.path.join(handoff_dir, f"{task_id}-instructions.md")
        user_instructions = ""
        if os.path.isfile(instructions_file):
            # Keep the suppression on the read call for the state-read ratchet.
            # fmt: off
            user_instructions = Path(instructions_file).read_text(encoding="utf-8")  # shipguard:ignore state-read: user-supplied task instructions; fmt: skip
            # fmt: on
            user_instructions = user_instructions.strip()
            if user_instructions:
                user_instructions = (
                    f"\n\nUser instructions for this task:\n{user_instructions}"
                )
        run_prompt = os.environ.get("SUPERHARNESS_RUN_PROMPT", "").strip()
        if run_prompt and os.environ.get("SUPERHARNESS_RUN_ID", "").strip():
            user_instructions += f"\n\nRun instructions:\n{run_prompt}"
        result_path = os.environ.get("SUPERHARNESS_RUN_RESULT_PATH", "").strip()
        if reliable_run_bound and result_path:
            user_instructions += (
                f"\n\nResolved structured result artifact path: {result_path}\n"
                "Write the JSON artifact to this exact path; stdout alone is not a result handoff."
            )

        # Build context hint to reduce cold-start exploration time
        context_hint = build_context_hint(project_dir, task_obj or {})

        if reliable_run_bound:
            assert reliable_run is not None
            prompt = _build_reliable_task_execution_prompt(
                target=target,
                task_id=task_id,
                run_id=reliable_run.id,
                run_kind=reliable_run.kind,
                review_target_sha=reliable_run.review_target_sha,
                worktree_path=reliable_run.worktree_path,
                branch_name=reliable_run.branch_name,
                base_sha=reliable_run.base_sha,
                head_sha=reliable_run.head_sha,
                result_path=result_path or None,
                acceptance_criteria=acceptance_criteria,
                context_hint=context_hint,
                user_instructions=user_instructions,
                auto_directive=auto_directive,
            )
        else:
            prompt = _build_task_execution_prompt(
                target=target,
                task_id=task_id,
                contract_id=contract_id,
                latest_handoff=bool(latest_handoff),
                acceptance_criteria=acceptance_criteria,
                context_hint=context_hint,
                user_instructions=user_instructions,
                auto_directive=auto_directive,
            )
        components.append(("task_instructions", prompt))

        # Enrich prompt with vault context
        task_title = _get_task_title(project_dir, task_id)
        try:
            from superharness.engine.osm import vault_search

            vault_results = vault_search(task_title or task_id)
            if vault_results:
                vault_block = (
                    "\n\nVault notes relevant to this task (via obsidian-semantic):\n"
                )
                for r in vault_results:
                    preview = r.get("preview", "")[:120].replace("\n", " ")
                    vault_block += f"  - {r['path']} (similarity: {r['similarity']})\n    {preview}\n"
                prompt += vault_block
                components.append(("vault_block", vault_block))
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
        # Inject project rules so the agent knows constraints before starting
        try:
            from superharness.commands.rules import all_rules_text

            rules_text = all_rules_text(project_dir)
            if rules_text:
                prompt += f"\n\nProject rules (run `shux rules` to see full list):\n{rules_text}"
                components.append(("project_rules", rules_text))
        except Exception as e:
            logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
            pass
        print(f"Project: {project_dir}")
        print(f"Contract: {contract_id}")
        print(f"Task: {task_id}")
        if latest_handoff:
            print(f"Handoff: {latest_handoff}")

    print(f"Model: {resolved_model} ({model_source})")
    print(f"Effort: {resolved_effort}")

    # SDK vs CLI dispatch — auto-detect: use SDK if available, fall back to CLI
    # Current limit: only claude-code supports the SDK runner
    supports_sdk = target == "claude-code"
    use_sdk = via_sdk if via_sdk is not None else (sdk_available() and supports_sdk)
    if use_sdk and not sdk_available():
        print("⚠️  SDK not available — falling back to CLI", file=sys.stderr)
        use_sdk = False

    print(f"Via: {'sdk' if use_sdk else 'cli'}")

    # Stamp model tier onto task in SQLite so reviewers know author's tier

    if print_only:
        print()
        print("Generated prompt:")
        print("-----------------")
        print(prompt)
        _launch_agent(
            target,
            prompt,
            project_dir,
            non_interactive,
            codex_bypass,
            label=label,
            model=resolved_model,
            effort=resolved_effort,
            task_id=task_id,
            print_only=True,
            yolo=yolo,
        )
        return 0

    # Budget guard — warn or block before any dispatch
    try:
        from superharness.engine.model_budget import check_budget, BudgetStatus

        budget_result = check_budget(project_dir)
        if budget_result.status == BudgetStatus.WARN:
            print(f"\n⚠️  {budget_result.message}")
        elif budget_result.status == BudgetStatus.BLOCK:
            print(f"\n⛔ {budget_result.message}", file=sys.stderr)
            print("  Use --force to override.", file=sys.stderr)
            if not force:
                return 1
    except Exception as e:
        logger.warning("delegate.py unexpected error: %s", e, exc_info=True)
        pass  # budget check is best-effort — never block dispatch on error

    # Dispatch is committed past this point. Fire on_delegate hooks before the
    # launch — the CLI path execs and never returns, so hooks must run now.
    # print_only returned above, so notifications/routing fire only on real
    # dispatch.
    _fire_on_delegate(project_dir, target, task_id)

    # SDK dispatch path
    if use_sdk:
        print()
        print(f"Launching via SDK (model: {resolved_model})...")
        try:
            # Use dispatcher-provided log path if available, otherwise create our own
            env_log = os.environ.get("SUPERHARNESS_LAUNCHER_LOG")
            if env_log:
                log_file = Path(env_log)
                log_file.parent.mkdir(parents=True, exist_ok=True)
            else:
                from datetime import datetime

                log_dir = Path(harness_dir) / "launcher-logs"
                log_dir.mkdir(exist_ok=True)
                timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                log_file = log_dir / f"{task_id}-{target}-{timestamp}.log"
                _rotate_launcher_logs(log_dir, task_id, target, keep=5)

            task_budget = _get_task_budget(project_dir, task_id, resolved_effort)
            if task_budget:
                print(f"Budget: ${task_budget:.2f}")

            # Record this dispatch's prompt components (content-addressed,
            # sha256-deduplicated) so `shux diff <id> --context` can show
            # what changed between dispatches. Best-effort: recording is
            # observability, not a gate — a failure here must never block
            # the actual dispatch below.
            try:
                from superharness.engine import context_dao
                from superharness.engine.db import managed_connection, now_iso

                with managed_connection(project_dir) as _ctx_conn:
                    context_dao.record_dispatch(
                        _ctx_conn,
                        task_id=task_id,
                        agent=target,
                        components=components,
                        now=now_iso(),
                    )
            except (StateError, sqlite3.Error, OSError) as e:
                logger.warning("delegate.py failed to record dispatch context: %s", e)

            runner = SDKRunner(
                project_dir=Path(project_dir),
                model=resolved_model,
                max_budget_usd=task_budget,
            )
            result = runner.run(prompt, log_file=log_file)
            print()
            print("SDK execution completed:")
            print(result.get("output", ""))
            print(f"\nLauncher log: {log_file}")

            # Save context snapshot for warm-start on related future tasks
            _save_context_snapshot(project_dir, task_id, result, model=resolved_model)
            return 0
        except Exception as e:
            print(f"⚠️  SDK execution failed: {e}", file=sys.stderr)
            print("Falling back to CLI...", file=sys.stderr)
            use_sdk = False

    # CLI dispatch path (original behavior)
    if non_interactive:
        _confirm_non_interactive_risk(target, codex_bypass)

    _launch_agent(
        target,
        prompt,
        project_dir,
        non_interactive,
        codex_bypass,
        label=label,
        model=resolved_model,
        effort=resolved_effort,
        task_id=task_id,
        yolo=yolo,
    )
    return 0  # unreachable after exec, but satisfies type checker


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> "argparse.ArgumentParser":
    import argparse

    class _CapUsage(argparse.HelpFormatter):
        def _format_usage(self, usage, actions, groups, prefix):
            return super()._format_usage(usage, actions, groups, "Usage: ")

    parser = argparse.ArgumentParser(
        prog="delegate",
        description="Build prompt for target agent and launch or print it",
        formatter_class=_CapUsage,
        add_help=True,
    )
    from superharness.engine.adapter_registry import (
        list_adapters as _list_adapters_for_help,
    )

    parser.add_argument(
        "--to",
        required=False,
        dest="target",
        default=None,
        help=f"Target agent: {', '.join(_list_adapters_for_help()) or 'claude-code, codex-cli, gemini-cli, opencode'}",
    )
    parser.add_argument(
        "--project",
        "-p",
        default=None,
        help="Project directory (default: current directory)",
    )
    parser.add_argument(
        "--task",
        "-t",
        default="",
        help="Task ID from project contract (default: latest handoff for target)",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        default=False,
        help="Print the generated prompt without launching the agent",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        default=False,
        help="Launch agent without live supervision (requires confirmation or env var)",
    )
    parser.add_argument(
        "--yolo",
        action="store_true",
        default=False,
        help="Dangerously skip permissions and apply changes without asking",
    )
    parser.add_argument(
        "--codex-bypass",
        action="store_true",
        default=False,
        help="Codex only: use --dangerously-bypass-approvals-and-sandbox",
    )
    parser.add_argument(
        "--for-review",
        action="store_true",
        default=False,
        help="Allow dispatch of review_requested tasks for review workflow only",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Override model. Accepts tier (mini/standard/max) or model name (sonnet, gpt-5.3-codex, etc.)",
    )
    parser.add_argument(
        "--effort",
        default=None,
        choices=list(VALID_EFFORTS),
        help="Override thinking effort (low/medium/high)",
    )
    parser.add_argument(
        "--no-auto-model",
        action="store_true",
        default=False,
        help="Skip auto-classification, use profile defaults or standard/medium",
    )
    parser.add_argument(
        "--via",
        default=None,
        choices=["cli", "sdk"],
        help="Force dispatch method (default: auto-detect — SDK if installed, CLI otherwise)",
    )
    parser.add_argument(
        "--role",
        default="worker",
        choices=["orchestrator", "worker", "validator", "code_reviewer"],
        help="Agent role: drives model selection and dispatch payload policy. "
        "validator/code_reviewer enforce fresh-worktree + minimal payload.",
    )
    parser.add_argument(
        "--orchestrate",
        action="store_true",
        default=False,
        help="Force orchestrator mode: decompose task into subtasks (also the default — use --no-orchestrate to skip)",
    )
    parser.add_argument(
        "--no-orchestrate",
        action="store_true",
        default=False,
        help="Skip orchestrator — dispatch directly without analysis (for trivial fix/chore tasks)",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        default=False,
        help="Skip pre-flight analysis (useful when you know the task is ready)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Override budget block for this dispatch",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        default=False,
        dest="plan_only",
        help="Agent proposes a TDD plan and stops (no implementation). "
        "Relaxes gate 4 so todo+implementation tasks are dispatchable.",
    )
    parser.add_argument(
        "--1m-context",
        action="store_true",
        default=False,
        dest="context_1m",
        help=f"Force max-1m tier ({flagship_1m()}) for this dispatch. "
        "Implies effort=max. Use when prompt exceeds ~200K tokens.",
    )
    parser.add_argument(
        "--ship-on-complete",
        action="store_true",
        default=False,
        dest="ship_on_complete",
        help="Override: inject SHIP-ON-COMPLETE directive for this dispatch even "
        "if the contract task does not have ship_on_complete: true.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=False,
        help="Emit machine-readable JSON on stdout instead of human text. "
        "Implies --print-only when no --via is forced.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]

    parser = _build_parser()
    opts = parser.parse_args(argv)

    global _JSON_MODE, _JSON_CTX
    if getattr(opts, "json", False):
        _JSON_MODE = True
        _JSON_CTX = {"task_id": opts.task, "to": opts.target}
        # --json implies --print-only so we get a deterministic exit without
        # launching an interactive agent.
        opts.print_only = True

    from superharness.engine.adapter_registry import list_adapters

    valid_agents = list_adapters()
    if opts.target is None:
        parser.error("--to is required")
    if opts.target not in valid_agents:
        if _JSON_MODE:
            from superharness.utils.json_output import emit_error

            emit_error(
                f"--to must be one of: {', '.join(valid_agents)}",
                exit_code=2,
                **_JSON_CTX,
            )
        print(f"--to must be one of: {', '.join(valid_agents)}", file=sys.stderr)
        sys.exit(2)

    project_dir = os.path.realpath(opts.project or os.getcwd())
    if not os.path.isdir(project_dir):
        print(f"Project directory does not exist: {project_dir}", file=sys.stderr)
        sys.exit(1)

    # Apply autonomy from profile (mirrors Bash export logic)
    autonomy = _read_profile_field(project_dir, "autonomy", "approval-gated")
    if autonomy == "autonomous":
        os.environ.setdefault("SUPERHARNESS_CONFIRM_NON_INTERACTIVE", "YES")
        os.environ.setdefault("SUPERHARNESS_CONFIRM_SKIP_PERMISSIONS", "YES")
    elif autonomy == "supervised":
        os.environ.setdefault("SUPERHARNESS_CONFIRM_NON_INTERACTIVE", "YES")

    if _JSON_MODE:
        import io

        _orig_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            rc = delegate(
                project_dir=project_dir,
                target=opts.target,
                task_id=opts.task,
                print_only=opts.print_only,
                non_interactive=opts.non_interactive,
                codex_bypass=opts.codex_bypass,
                for_review=opts.for_review,
                model_override=opts.model or "",
                effort_override=opts.effort or "",
                no_auto_model=opts.no_auto_model,
                via_sdk=True
                if opts.via == "sdk"
                else (False if opts.via == "cli" else None),
                orchestrate=opts.orchestrate,
                no_orchestrate=opts.no_orchestrate,
                skip_preflight=opts.skip_preflight,
                force=opts.force,
                plan_only=opts.plan_only,
                ship_on_complete=opts.ship_on_complete,
                role=getattr(opts, "role", "worker") or "worker",
                yolo=opts.yolo,
            )
            captured = sys.stdout.getvalue()
        finally:
            sys.stdout = _orig_stdout
        prompt_text = ""
        if "Generated prompt:" in captured:
            prompt_text = captured.split("-----------------", 1)[-1].strip()
        from superharness.utils.json_output import emit_json

        emit_json(
            {
                "task_id": opts.task,
                "to": opts.target,
                "print_only": bool(opts.print_only),
                "plan_only": bool(opts.plan_only),
                "orchestrate": bool(opts.orchestrate),
                "role": getattr(opts, "role", "worker") or "worker",
                "prompt": prompt_text,
                "prompt_length": len(prompt_text),
            },
            ok=(rc == 0),
            exit_code=rc,
        )

    rc = delegate(
        project_dir=project_dir,
        target=opts.target,
        task_id=opts.task,
        print_only=opts.print_only,
        non_interactive=opts.non_interactive,
        codex_bypass=opts.codex_bypass,
        for_review=opts.for_review,
        model_override=opts.model or "",
        effort_override=opts.effort or "",
        no_auto_model=opts.no_auto_model,
        via_sdk=True if opts.via == "sdk" else (False if opts.via == "cli" else None),
        orchestrate=opts.orchestrate,
        skip_preflight=opts.skip_preflight,
        force=opts.force,
        plan_only=opts.plan_only,
        ship_on_complete=opts.ship_on_complete,
        role=getattr(opts, "role", "worker") or "worker",
        yolo=opts.yolo,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()

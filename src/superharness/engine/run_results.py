from __future__ import annotations

import json

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from superharness.engine.state_errors import BoundaryError

RUN_KINDS = frozenset({"plan", "implement", "repair", "fallback", "ship", "review"})
REVIEW_VERDICTS = frozenset({"LGTM", "REJECTED"})
COMPLETION_STATUSES = frozenset({"completed", "blocked", "needs_input", "failed"})

RunKind = Literal["plan", "implement", "repair", "fallback", "ship", "review"]
CompletionStatus = Literal["completed", "blocked", "needs_input", "failed"]
ReviewVerdict = Literal["LGTM", "REJECTED"]


class ExecutionResult(BaseModel):
    """Structured result captured for one durable Run.

    This is not a task transition contract yet. Phase 1 only validates and
    persists the execution result so the future orchestrator can consume it
    without relying on filename or mtime discovery.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    run_id: str
    task_id: str
    kind: RunKind
    agent: str
    exit_code: int
    completion_status: CompletionStatus
    worktree_path: str | None = None
    branch_name: str | None = None
    base_sha: str | None = None
    head_sha: str | None = None
    dirty: bool | None = None
    changed_files: list[str] = Field(default_factory=list)
    review_verdict: ReviewVerdict | None = None
    reviewed_sha: str | None = None
    findings: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_review_fields(self) -> ExecutionResult:
        if self.review_verdict is not None and self.kind != "review":
            raise ValueError("review_verdict is only valid for review runs")
        if self.reviewed_sha is not None and self.kind != "review":
            raise ValueError("reviewed_sha is only valid for review runs")
        if self.review_verdict is not None and not self.reviewed_sha:
            raise ValueError("review_verdict requires reviewed_sha")
        if self.completion_status == "completed":
            if self.kind == "review":
                if not self.reviewed_sha:
                    raise ValueError("completed review results require reviewed_sha")
                if self.review_verdict is None:
                    raise ValueError("completed review results require review_verdict")
            else:
                missing = [
                    field
                    for field in (
                        "worktree_path",
                        "branch_name",
                        "base_sha",
                        "head_sha",
                        "dirty",
                    )
                    if getattr(self, field) is None
                ]
                if missing:
                    raise ValueError(
                        "completed execution results require: " + ", ".join(missing)
                    )
        return self


# Keep JSON type wording next to the model so prompt and validation documentation
# cannot drift independently.
RESULT_FIELD_JSON_TYPES = {
    "schema_version": "JSON integer, exactly 1",
    "run_id": "JSON string",
    "task_id": "JSON string",
    "kind": "JSON string, one of plan/implement/repair/fallback/ship/review",
    "agent": "JSON string",
    "exit_code": "JSON integer",
    "completion_status": "JSON string, one of completed/blocked/needs_input/failed",
    "worktree_path": "JSON string or null",
    "branch_name": "JSON string or null",
    "base_sha": "JSON string or null",
    "head_sha": "JSON string or null",
    "dirty": "JSON boolean or null",
    "changed_files": "JSON array of strings",
    "review_verdict": "JSON string LGTM or REJECTED, or null",
    "reviewed_sha": "JSON string or null",
    "findings": "JSON array of strings",
}


def minimal_execution_result_example(
    *,
    run_id: str,
    task_id: str,
    kind: str,
    agent: str,
    review_target_sha: str | None = None,
) -> dict[str, Any]:
    """Build a minimal valid completed result from the ExecutionResult contract."""
    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "task_id": task_id,
        "kind": kind,
        "agent": agent,
        "exit_code": 0,
        "completion_status": "completed",
        "changed_files": [],
        "findings": [],
    }
    if kind == "review":
        payload.update(
            {
                "review_verdict": "LGTM",
                "reviewed_sha": review_target_sha or "required-review-target-sha",
            }
        )
    else:
        payload.update(
            {
                "worktree_path": "/path/to/managed/worktree",
                "branch_name": "shux/reliable/example",
                "base_sha": "base-sha",
                "head_sha": "head-sha",
                "dirty": False,
            }
        )
    return payload


def reliable_result_instructions(
    *,
    run_id: str,
    task_id: str,
    kind: str,
    agent: str,
    artifact_path: str | None,
    review_target_sha: str | None = None,
) -> str:
    """Render the prompt contract from the authoritative result model."""
    fields = ", ".join(ExecutionResult.model_fields)
    field_types = "; ".join(
        f"{name}: {RESULT_FIELD_JSON_TYPES[name]}"
        for name in ExecutionResult.model_fields
    )
    example = minimal_execution_result_example(
        run_id=run_id,
        task_id=task_id,
        kind=kind,
        agent=agent,
        review_target_sha=review_target_sha,
    )
    lines = [
        "Structured result contract:",
        f"Write exactly one JSON object to {artifact_path or 'SUPERHARNESS_RUN_RESULT_PATH'}.",
        f"The JSON fields are: {fields}.",
        f"JSON field types: {field_types}.",
        f"Set run_id exactly to {run_id}.",
        f"Set task_id exactly to {task_id}.",
        f"Set kind exactly to {kind}.",
        f"Set agent exactly to {agent}.",
        'schema_version must be the JSON integer 1, written as: "schema_version": 1. Do not quote it.',
        "completion_status must be one of: completed, blocked, needs_input, failed; it is a JSON string.",
        'For a successful completed Run, set completion_status to the JSON string "completed" and exit_code to the JSON integer 0.',
        "For a non-successful Run, record the actual JSON integer non-zero exit_code and matching completion_status.",
        "Do not use custom fields such as status, summary, files_changed, or test_results.",
        "Minimal valid JSON example for this Run kind:",
        json.dumps(example, indent=2),
    ]
    if kind == "plan":
        lines.append(
            "ExecutionResult has no plan, summary, details, or metadata field; do not put a plan object in findings. Plan detail is outside the durable Run result contract."
        )
    if kind == "review":
        lines.extend(
            [
                f"Set reviewed_sha exactly to {review_target_sha or 'the required review target SHA'}.",
                'For completed reviews, review_verdict must be LGTM or REJECTED, as a JSON string.',
                "findings must be a JSON array of strings; use [] for LGTM and concrete string entries for REJECTED.",
            ]
        )
    elif kind in {"plan", "implement", "repair", "fallback"}:
        lines.append(
            "For completed Runs, worktree_path and branch_name must be JSON strings; base_sha and head_sha must be JSON strings; dirty must be a JSON boolean. changed_files and findings must be JSON arrays of strings."
        )
    lines.append("The artifact is the only reliable result handoff; stdout alone is not sufficient.")
    return "\n".join(lines) + "\n"


def parse_execution_result(
    payload: ExecutionResult | dict[str, Any],
) -> ExecutionResult:
    if isinstance(payload, ExecutionResult):
        return payload
    return ExecutionResult.model_validate(payload)


def validate_result_for_run(
    payload: ExecutionResult | dict[str, Any], run: object
) -> ExecutionResult:
    result = parse_execution_result(payload)
    expected = {
        "run_id": getattr(run, "id", None),
        "task_id": getattr(run, "task_id", None),
        "kind": getattr(run, "kind", None),
        "agent": getattr(run, "agent", None),
    }
    actual = {
        "run_id": result.run_id,
        "task_id": result.task_id,
        "kind": result.kind,
        "agent": result.agent,
    }
    mismatches = [
        f"{field}: expected {expected[field]!r}, got {actual[field]!r}"
        for field in expected
        if expected[field] is not None and actual[field] != expected[field]
    ]
    if mismatches:
        raise BoundaryError(
            "Execution result does not match run: " + "; ".join(mismatches)
        )

    expected_review_sha = getattr(run, "review_target_sha", None)
    if (
        result.reviewed_sha is not None
        and expected_review_sha
        and result.reviewed_sha != expected_review_sha
    ):
        raise BoundaryError(
            "Execution result reviewed_sha does not match run review_target_sha: "
            f"expected {expected_review_sha!r}, got {result.reviewed_sha!r}"
        )
    return result

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from superharness.engine.state_errors import BoundaryError

RUN_KINDS = frozenset({"plan", "implement", "repair", "fallback", "ship", "review"})
REVIEW_VERDICTS = frozenset({"LGTM", "REJECTED", "BLOCKED"})
COMPLETION_STATUSES = frozenset({"completed", "blocked", "needs_input", "failed"})

RunKind = Literal["plan", "implement", "repair", "fallback", "ship", "review"]
CompletionStatus = Literal["completed", "blocked", "needs_input", "failed"]
ReviewVerdict = Literal["LGTM", "REJECTED", "BLOCKED"]


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


def parse_execution_result(payload: ExecutionResult | dict[str, Any]) -> ExecutionResult:
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
        raise BoundaryError("Execution result does not match run: " + "; ".join(mismatches))

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

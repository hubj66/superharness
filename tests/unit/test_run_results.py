import json
from types import SimpleNamespace

import pytest

from superharness.engine.run_results import (
    RESULT_FIELD_JSON_TYPES,
    ExecutionResult,
    minimal_execution_result_example,
    reliable_result_instructions,
    validate_result_for_run,
)
from superharness.engine.state_errors import BoundaryError


def test_r9_string_schema_version_and_object_findings_are_rejected():
    run = SimpleNamespace(id="run-r9", task_id="gh-1-r9", kind="plan", agent="claude-code")
    artifact = {
        "schema_version": "1",
        "run_id": "run-r9",
        "task_id": "gh-1-r9",
        "kind": "plan",
        "agent": "claude-code",
        "exit_code": 0,
        "completion_status": "completed",
        "worktree_path": "/tmp/worktree",
        "branch_name": "shux/reliable/gh-1-r9",
        "base_sha": "base",
        "head_sha": "head",
        "dirty": False,
        "changed_files": [],
        "findings": {"plan": {"steps": []}},
    }
    with pytest.raises(ValueError):
        validate_result_for_run(artifact, run)


@pytest.mark.parametrize("kind", ["plan", "implement", "repair", "fallback", "review"])
def test_generated_minimal_example_validates_for_each_reliable_kind(kind):
    artifact = minimal_execution_result_example(
        run_id=f"run-{kind}",
        task_id="task-1",
        kind=kind,
        agent="codex-cli" if kind == "review" else "claude-code",
        review_target_sha="sha-review" if kind == "review" else None,
        worktree_path="/tmp/worktree",
        branch_name="shux/reliable/task-1",
        base_sha="sha-base",
        head_sha="sha-head",
    )
    assert ExecutionResult.model_validate(json.loads(json.dumps(artifact)))
    assert set(RESULT_FIELD_JSON_TYPES) == set(ExecutionResult.model_fields)
    prompt = reliable_result_instructions(
        run_id=f"run-{kind}",
        task_id="task-1",
        kind=kind,
        agent="codex-cli" if kind == "review" else "claude-code",
        artifact_path="/tmp/result.json",
        review_target_sha="sha-review" if kind == "review" else None,
        worktree_path="/tmp/worktree",
        branch_name="shux/reliable/task-1",
        base_sha="sha-base",
        head_sha="sha-head",
    )
    assert json.dumps(artifact, indent=2) in prompt
    # All run kinds with known base_sha must emit the authoritative identity block.
    assert "Run git identity is authoritative" in prompt
    assert '"base_sha": "sha-base"' in prompt
    # base_sha appears once in the example JSON and once in the authoritative identity block.
    assert prompt.count('"base_sha": "sha-base"') == 2
    if kind == "review":
        assert "DO NOT replace them with values from git" in prompt


# ── gh-253 regression: review Run git identity contract ─────────────────────
# Production failure: Codex attempt 3 reviewed task gh-253 (PR #258) and
# derived base_sha from git merge-base (bf0d7e5...) instead of echoing the
# authoritative Run identity (base_sha = head_sha = review_target_sha = 909a401...).
# Strict validation correctly rejected it; this suite reproduces the scenario.

REVIEW_SHA = "909a40137612cc0b5a4063d5674c14dd16cc978e"
PR_BASE_SHA = "bf0d7e5074b08cd12da1244114c471abdca4365c"


def _review_run(
    *,
    run_id: str = "run-review-gh253",
    review_sha: str = REVIEW_SHA,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=run_id,
        task_id="gh-253",
        kind="review",
        agent="codex-cli",
        worktree_path="/worktrees/review-gh253",
        branch_name="shux/reliable/gh-253",
        base_sha=review_sha,
        head_sha=review_sha,
        review_target_sha=review_sha,
    )


def _valid_review_payload(
    *,
    run_id: str = "run-review-gh253",
    review_sha: str = REVIEW_SHA,
    base_sha: str = REVIEW_SHA,
    head_sha: str = REVIEW_SHA,
    verdict: str = "LGTM",
    findings: list | None = None,
) -> dict:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "task_id": "gh-253",
        "kind": "review",
        "agent": "codex-cli",
        "exit_code": 0,
        "completion_status": "completed",
        "changed_files": [],
        "findings": findings or [],
        "review_verdict": verdict,
        "reviewed_sha": review_sha,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "worktree_path": "/worktrees/review-gh253",
        "branch_name": "shux/reliable/gh-253",
    }


def test_review_prompt_includes_authoritative_git_identity():
    """Prompt for review must explicitly provide base_sha/head_sha so the model
    cannot derive them from git history or the PR base."""
    prompt = reliable_result_instructions(
        run_id="run-review-gh253",
        task_id="gh-253",
        kind="review",
        agent="codex-cli",
        artifact_path="/tmp/result.json",
        review_target_sha=REVIEW_SHA,
        worktree_path="/worktrees/review-gh253",
        branch_name="shux/reliable/gh-253",
        base_sha=REVIEW_SHA,
        head_sha=REVIEW_SHA,
    )
    assert "Run git identity is authoritative" in prompt
    assert REVIEW_SHA in prompt
    assert '"base_sha"' in prompt
    assert '"head_sha"' in prompt
    assert "DO NOT replace them with values from git" in prompt
    assert "git merge-base" in prompt


def test_review_prompt_example_contains_authoritative_shas():
    """The example JSON shown to the model must use the actual authoritative SHAs."""
    example = minimal_execution_result_example(
        run_id="run-review-gh253",
        task_id="gh-253",
        kind="review",
        agent="codex-cli",
        review_target_sha=REVIEW_SHA,
        worktree_path="/worktrees/review-gh253",
        branch_name="shux/reliable/gh-253",
        base_sha=REVIEW_SHA,
        head_sha=REVIEW_SHA,
    )
    assert example["base_sha"] == REVIEW_SHA
    assert example["head_sha"] == REVIEW_SHA
    assert example["reviewed_sha"] == REVIEW_SHA
    # Must validate as a proper ExecutionResult.
    assert ExecutionResult.model_validate(example)


def test_review_lgtm_with_authoritative_sha_passes_validation():
    """Codex echoing the authoritative review SHA for base/head passes."""
    run = _review_run()
    result = validate_result_for_run(_valid_review_payload(), run)
    assert result.review_verdict == "LGTM"
    assert result.reviewed_sha == REVIEW_SHA


def test_review_rejected_with_authoritative_sha_passes_validation():
    """REJECTED verdict with authoritative SHA and findings passes."""
    run = _review_run()
    payload = _valid_review_payload(
        verdict="REJECTED",
        findings=["Missing test for the new payment path"],
    )
    result = validate_result_for_run(payload, run)
    assert result.review_verdict == "REJECTED"
    assert result.findings == ["Missing test for the new payment path"]


def test_review_with_pr_base_sha_fails_strict_validation():
    """Production failure reproduced: using PR base SHA instead of review SHA."""
    run = _review_run()
    payload = _valid_review_payload(base_sha=PR_BASE_SHA)
    with pytest.raises(BoundaryError, match="base_sha"):
        validate_result_for_run(payload, run)


@pytest.mark.parametrize("field", ["base_sha", "head_sha", "worktree_path", "branch_name"])
def test_review_null_result_field_fails_when_run_has_authoritative_value(field: str):
    """Nulling an identity field the Run has non-null is a trust-boundary bypass."""
    run = _review_run()
    payload = _valid_review_payload()
    payload[field] = None
    with pytest.raises(BoundaryError, match=field):
        validate_result_for_run(payload, run)


def test_null_result_field_accepted_when_run_field_is_also_null():
    """Null result is only accepted when the authoritative Run value is itself null."""
    run_id = "run-review-null-branch"
    run = SimpleNamespace(
        id=run_id,
        task_id="gh-253",
        kind="review",
        agent="codex-cli",
        worktree_path="/worktrees/review-gh253",
        branch_name=None,  # orchestrator left branch_name null
        base_sha=REVIEW_SHA,
        head_sha=REVIEW_SHA,
        review_target_sha=REVIEW_SHA,
    )
    payload = _valid_review_payload(run_id=run_id)
    payload["branch_name"] = None
    result = validate_result_for_run(payload, run)
    assert result.review_verdict == "LGTM"


def test_review_with_wrong_reviewed_sha_fails_validation():
    """reviewed_sha must equal review_target_sha; using PR base fails."""
    run = _review_run()
    payload = _valid_review_payload(review_sha=PR_BASE_SHA)
    with pytest.raises(BoundaryError, match="reviewed_sha"):
        validate_result_for_run(payload, run)


def test_recovery_review_preserves_authoritative_identity():
    """Recovery review (attempt 3) uses same base_sha/head_sha = review_target_sha."""
    recovery_run_id = "run-review-gh253-recovery"
    run = _review_run(run_id=recovery_run_id)
    # Same authoritative identity: base = head = review_target_sha
    prompt = reliable_result_instructions(
        run_id=recovery_run_id,
        task_id="gh-253",
        kind="review",
        agent="codex-cli",
        artifact_path="/tmp/result.json",
        review_target_sha=REVIEW_SHA,
        worktree_path="/worktrees/review-gh253",
        branch_name="shux/reliable/gh-253",
        base_sha=REVIEW_SHA,
        head_sha=REVIEW_SHA,
    )
    assert "Run git identity is authoritative" in prompt
    assert REVIEW_SHA in prompt
    # Correct result validates successfully.
    result = validate_result_for_run(
        _valid_review_payload(run_id=recovery_run_id),
        run,
    )
    assert result.review_verdict == "LGTM"

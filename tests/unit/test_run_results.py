from types import SimpleNamespace
import json

import pytest

from superharness.engine.run_results import (
    ExecutionResult,
    RESULT_FIELD_JSON_TYPES,
    minimal_execution_result_example,
    reliable_result_instructions,
    validate_result_for_run,
)


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
    if kind != "review":
        assert "Run git identity is authoritative" in prompt
        assert '"base_sha": "sha-base"' in prompt
        assert prompt.count('"base_sha": "sha-base"') == 2

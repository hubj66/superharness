from __future__ import annotations

import json
import platform
import subprocess
from pathlib import Path

import pytest

from superharness.commands.inbox_dispatch import (
    DispatchContext,
    _prepare_execution,
    _reliable_run_finished,
    _reliable_run_started,
    _resolve_execution_context,
    _sqlite_claim_next,
)
from superharness.engine import inbox_dao, runs_dao
from superharness.engine.db import get_connection, init_db

NOW = "2026-01-01T00:00:00Z"


def _project(tmp_path: Path):
    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES ('t1', 't1', 'claude-code', 'in_progress', 1, ?, 'reliable-orchestrator')",
        (NOW,),
    )
    run = runs_dao.create_run(
        conn,
        id="run-1",
        task_id="t1",
        kind="implement",
        agent="claude-code",
        dedupe_key="implement:t1:plan-1",
        now=NOW,
    )
    inbox = inbox_dao.enqueue(
        conn,
        id="inbox-1",
        task_id="t1",
        target_agent="claude-code",
        project_path=str(project),
        run_id=run.id,
        now=NOW,
    )
    runs_dao.link_inbox(conn, run_id=run.id, inbox_id=inbox.id)
    conn.commit()
    return project, conn


def test_dispatch_claim_advances_linked_run_atomically(tmp_path):
    project, conn = _project(tmp_path)
    try:
        claimed = _sqlite_claim_next(str(project), "claude-code", NOW)
        assert claimed is not None and claimed["run_id"] == "run-1"
        assert runs_dao.get_run(conn, "run-1").status == "claimed"
        assert inbox_dao.get(conn, "inbox-1").status == "launched"
    finally:
        conn.close()


def test_dispatch_passes_run_id_to_agent_and_records_completion(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        assert ctx.spawn_env["SUPERHARNESS_RUN_ID"] == "run-1"
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None and run.status == "succeeded"
        assert run.result_json["run_id"] == "run-1"
        assert inbox_dao.get(conn, "inbox-1").status == "done"
    finally:
        conn.close()


def test_linux_pty_wrapper_propagates_child_exit_code(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        monkeypatch.delenv("SUPERHARNESS_NO_PTY_WRAP", raising=False)
        monkeypatch.setattr(platform, "system", lambda: "Linux")
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        assert ctx.wrapped_args[:3] == ["script", "-q", "-e"]
    finally:
        conn.close()


def test_darwin_pty_wrapper_keeps_bsd_script_invocation(tmp_path, monkeypatch):
    project, conn = _project(tmp_path)
    try:
        monkeypatch.delenv("SUPERHARNESS_NO_PTY_WRAP", raising=False)
        monkeypatch.setattr(platform, "system", lambda: "Darwin")
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        assert ctx.wrapped_args[:3] == ["script", "-q", "-F"]
        assert "-e" not in ctx.wrapped_args[:4]
    finally:
        conn.close()


def test_util_linux_script_return_flag_propagates_distinctive_exit_code(tmp_path):
    log_path = tmp_path / "script.log"
    result = subprocess.run(
        ["script", "-q", "-e", "-f", "-c", "sh -c 'exit 7'", str(log_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 127 and "No such file" in result.stderr:
        pytest.skip("script command unavailable")
    assert result.returncode == 7


@pytest.mark.parametrize("exit_code", [1, 7])
def test_nonzero_delegate_exit_cannot_succeed_reliable_run(tmp_path, exit_code):
    project, conn = _project(tmp_path)
    try:
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = exit_code
        _reliable_run_finished(ctx)

        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status != "succeeded"
        assert run.status == "failed"
        assert run.result_json["exit_code"] == exit_code
        assert run.result_json["completion_status"] == "failed"
        assert inbox_dao.get(conn, "inbox-1").status == "failed"
        assert runs_dao.list_runs_for_task(conn, "t1", kind="ship") == []
    finally:
        conn.close()


def test_sigsegv_delegate_exit_records_reliable_crash(tmp_path):
    project, conn = _project(tmp_path)
    try:
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 139
        _reliable_run_finished(ctx)

        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status == "crashed"
        assert run.failure_category == "agent_crash"
        assert run.result_json["exit_code"] == 139
        assert inbox_dao.get(conn, "inbox-1").status == "failed"
        assert runs_dao.list_runs_for_task(conn, "t1", kind="ship") == []
    finally:
        conn.close()


def test_linked_run_preserves_orchestrator_worktree_path(tmp_path):
    project, conn = _project(tmp_path)
    worktree = tmp_path / "superharness-worktrees" / "wt"
    worktree.mkdir(parents=True)
    (worktree / ".superharness").symlink_to(project / ".superharness")
    try:
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(worktree),
            run_id="run-1",
            item={"plan_only": False},
        )
        assert _resolve_execution_context(ctx) is None
        assert ctx.exec_project == str(worktree)
        _prepare_execution(ctx)
        assert ctx.spawn_env["SUPERHARNESS_STATE_PROJECT"] == str(project)
    finally:
        conn.close()


def test_linked_review_run_supplies_model_and_run_prompt(tmp_path):
    project, conn = _project(tmp_path)
    try:
        conn.execute("UPDATE tasks SET status='review_requested' WHERE id='t1'")
        conn.execute(
            """
            UPDATE runs
               SET kind='review',
                   agent='codex-cli',
                   model='gpt-5.5',
                   review_target_sha='sha-a',
                   result_json=?
             WHERE id='run-1'
            """,
            ('{"prompt": "Review only. Return reviewed SHA sha-a."}',),
        )
        conn.execute("UPDATE inbox SET target_agent='codex-cli' WHERE id='inbox-1'")
        conn.commit()
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        assert "--for-review" in ctx.launch_args
        assert ctx.launch_args[ctx.launch_args.index("--model") + 1] == "gpt-5.5"
        assert ctx.spawn_env["SUPERHARNESS_RUN_ID"] == "run-1"
        assert ctx.spawn_env["SUPERHARNESS_RUN_PROMPT"].startswith("Review only")
        assert ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"].endswith("run-1.json")
    finally:
        conn.close()


@pytest.mark.regression
@pytest.mark.parametrize(
    ("verdict", "findings"),
    [("LGTM", []), ("REJECTED", ["Ruff violations remain"])],
)
def test_linked_review_run_ingests_structured_result_artifact(
    tmp_path, verdict, findings
):
    project, conn = _project(tmp_path)
    try:
        conn.execute("UPDATE tasks SET status='review_requested' WHERE id='t1'")
        conn.execute(
            """
            UPDATE runs
               SET kind='review',
                   agent='codex-cli',
                   model='gpt-5.5',
                   review_target_sha='sha-a'
             WHERE id='run-1'
            """
        )
        conn.commit()
        _sqlite_claim_next(str(project), "claude-code", NOW)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"]).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": "run-1",
                    "task_id": "t1",
                    "kind": "review",
                    "agent": "codex-cli",
                    "exit_code": 0,
                    "completion_status": "completed",
                    "review_verdict": verdict,
                    "reviewed_sha": "sha-a",
                    "findings": findings,
                    # Echo authoritative worktree_path stamped by _reliable_run_started.
                    "worktree_path": ctx.exec_project,
                }
            ),
            encoding="utf-8",
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None and run.status == "succeeded"
        assert run.review_verdict == verdict
        assert run.result_json["reviewed_sha"] == "sha-a"
        assert run.result_json["findings"] == findings
    finally:
        conn.close()


def test_review_exit_zero_without_artifact_fails_run(tmp_path):
    project, conn = _project(tmp_path)
    try:
        conn.execute("UPDATE tasks SET status='review_requested' WHERE id='t1'")
        conn.execute(
            """
            UPDATE runs
               SET kind='review',
                   agent='codex-cli',
                   review_target_sha='sha-a'
             WHERE id='run-1'
            """
        )
        conn.commit()
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status == "failed"
        assert run.failure_category == "invalid_result"
        assert "was not created" in (run.failure_detail or "")
    finally:
        conn.close()


def test_review_artifact_with_wrong_run_id_fails_run(tmp_path):
    project, conn = _project(tmp_path)
    try:
        conn.execute("UPDATE tasks SET status='review_requested' WHERE id='t1'")
        conn.execute(
            """
            UPDATE runs
               SET kind='review',
                   agent='codex-cli',
                   review_target_sha='sha-a'
             WHERE id='run-1'
            """
        )
        conn.commit()
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"]).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": "other-run",
                    "task_id": "t1",
                    "kind": "review",
                    "agent": "codex-cli",
                    "exit_code": 0,
                    "completion_status": "completed",
                    "review_verdict": "LGTM",
                    "reviewed_sha": "sha-a",
                }
            ),
            encoding="utf-8",
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status == "failed"
        assert run.failure_category == "invalid_result"
    finally:
        conn.close()


def test_review_malformed_artifact_fails_closed_with_diagnostic(tmp_path):
    project, conn = _project(tmp_path)
    try:
        conn.execute("UPDATE tasks SET status='review_requested' WHERE id='t1'")
        conn.execute(
            """
            UPDATE runs
               SET kind='review',
                   agent='codex-cli',
                   review_target_sha='sha-a'
             WHERE id='run-1'
            """
        )
        conn.commit()
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"]).write_text(
            "not-json", encoding="utf-8"
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)

        run = runs_dao.get_run(conn, "run-1")
        assert run is not None and run.status == "failed"
        assert run.failure_category == "invalid_result"
        assert "malformed JSON" in (run.failure_detail or "")
    finally:
        conn.close()


def test_isolated_run_uses_writable_result_artifact_path(tmp_path):
    project, conn = _project(tmp_path)
    worktree = tmp_path / "review-worktree"
    worktree.mkdir()
    try:
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="codex-cli",
            item_project=str(worktree),
            exec_project=str(worktree),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        result_path = Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"])
        assert not result_path.is_relative_to(project / ".superharness")
        result_path.write_text("stale", encoding="utf-8")
        _prepare_execution(ctx)
        assert not result_path.exists()
        result_path.write_text("{}", encoding="utf-8")
        assert result_path.is_file()
    finally:
        conn.close()



def test_result_artifact_path_is_deterministic_and_project_isolated(tmp_path):
    import hashlib
    from types import SimpleNamespace

    from superharness.commands.inbox_dispatch import _result_artifact_path

    project_a = tmp_path / "project-a"
    context_a = SimpleNamespace(
        project_dir=str(project_a),
        exec_project=str(tmp_path / "worktree-a"),
        run_id="run-1",
    )
    path_a = _result_artifact_path(context_a)
    assert _result_artifact_path(context_a) == path_a
    assert Path(path_a).name == "run-1.json"
    assert Path(path_a).parent.name == hashlib.sha256(
        str(project_a.resolve()).encode("utf-8")
    ).hexdigest()

    context_b = SimpleNamespace(
        project_dir=str(tmp_path / "project-b"),
        exec_project=str(tmp_path / "worktree-b"),
        run_id="run-1",
    )
    path_b = _result_artifact_path(context_b)
    assert Path(path_b).parent != Path(path_a).parent

    context_a.run_id = "run-2"
    path_a2 = _result_artifact_path(context_a)
    assert Path(path_a2).parent == Path(path_a).parent
    assert Path(path_a2).name == "run-2.json"
    assert path_a2 != path_a



@pytest.mark.parametrize(
    ("artifact", "expected_status"),
    [
        (
            {
                "run_id": "run-1",
                "task_id": "t1",
                "status": "success",
                "summary": "implemented",
                "files_changed": ["calculator.py"],
                "test_results": {"passed": 4, "failed": 0, "errors": 0},
            },
            "failed",
        ),
        (
            {
                "schema_version": 1,
                "run_id": "run-1",
                "task_id": "t1",
                "kind": "implement",
                "agent": "claude-code",
                "exit_code": 0,
                "completion_status": "completed",
                "worktree_path": "/tmp/worktree",
                "branch_name": "shux/reliable/t1",
                "base_sha": "sha-base",
                "head_sha": "sha-head",
                "dirty": True,
                "changed_files": ["calculator.py"],
            },
            "succeeded",
        ),
    ],
)
def test_implement_artifact_must_match_authoritative_result_contract(
    tmp_path, artifact, expected_status
):
    project, conn = _project(tmp_path)
    try:
        artifact = dict(artifact)
        if expected_status == "succeeded":
            artifact["worktree_path"] = str(project)
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"]).write_text(
            json.dumps(artifact), encoding="utf-8"
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status == expected_status
        if expected_status == "failed":
            assert run.failure_category == "invalid_result"
    finally:
        conn.close()


@pytest.mark.regression
def test_repair_artifact_with_unrelated_base_cannot_corrupt_run_or_ship(tmp_path):
    """gh-253: repair base B cannot replace authoritative PR-head base A."""
    project, conn = _project(tmp_path)
    try:
        conn.execute(
            """
            UPDATE runs
               SET kind='repair',
                   branch_name='shux/reliable/t1',
                   base_sha='sha-a',
                   head_sha='sha-a'
             WHERE id='run-1'
            """
        )
        conn.commit()
        _sqlite_claim_next(str(project), "claude-code", NOW)
        ctx = DispatchContext(
            project_dir=str(project),
            inbox_file=str(project / ".superharness" / "inbox.yaml"),
            contract_file=str(project / ".superharness" / "contract.yaml"),
            print_only=False,
            non_interactive=True,
            codex_bypass=False,
            launcher_timeout=0,
            script_dir=str(project),
            sqlite_primary=True,
            item_id="inbox-1",
            item_task="t1",
            item_to="claude-code",
            item_project=str(project),
            exec_project=str(project),
            run_id="run-1",
            item={"plan_only": False},
        )
        _prepare_execution(ctx)
        Path(ctx.spawn_env["SUPERHARNESS_RUN_RESULT_PATH"]).write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": "run-1",
                    "task_id": "t1",
                    "kind": "repair",
                    "agent": "claude-code",
                    "exit_code": 0,
                    "completion_status": "completed",
                    "worktree_path": str(project),
                    "branch_name": "shux/reliable/t1",
                    "base_sha": "sha-b",
                    "head_sha": "sha-a",
                    "dirty": True,
                    "changed_files": ["src/fixed.py"],
                }
            ),
            encoding="utf-8",
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)

        run = runs_dao.get_run(conn, "run-1")
        assert run is not None
        assert run.status == "failed"
        assert run.failure_category == "invalid_result"
        assert run.base_sha == "sha-a"
        assert run.head_sha == "sha-a"
        assert runs_dao.list_runs_for_task(conn, "t1", kind="ship") == []
    finally:
        conn.close()

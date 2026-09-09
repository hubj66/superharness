from __future__ import annotations

import json
from pathlib import Path

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


def test_linked_review_run_ingests_structured_result_artifact(tmp_path):
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
                    "review_verdict": "LGTM",
                    "reviewed_sha": "sha-a",
                    "findings": [],
                }
            ),
            encoding="utf-8",
        )
        _reliable_run_started(ctx, pid=None)
        ctx.launcher_rc = 0
        _reliable_run_finished(ctx)
        run = runs_dao.get_run(conn, "run-1")
        assert run is not None and run.status == "succeeded"
        assert run.review_verdict == "LGTM"
        assert run.result_json["reviewed_sha"] == "sha-a"
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
        assert run.failure_category == "dispatcher_failure"
        assert run.failure_detail == "invalid structured result"
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
        assert run.failure_category == "dispatcher_failure"
    finally:
        conn.close()

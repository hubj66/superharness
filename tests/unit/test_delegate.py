from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tests.helpers import REPO_ROOT


_REQUIRES_POSIX_FIXTURE = pytest.mark.skipif(
    os.name != "posix",
    reason="Pi delegate integration fixture uses a POSIX shebang executable",
)


def _run_delegate_py(cwd, args: list[str] | None = None, env: dict | None = None):
    """Run delegate Python module."""
    merged = os.environ.copy()
    merged["PYTHONPATH"] = str(REPO_ROOT / "src")
    if env:
        for k, v in env.items():
            if v is None:
                merged.pop(k, None)
            else:
                merged[k] = v
    cmd = [sys.executable, "-m", "superharness.commands.delegate"] + (args or [])
    return subprocess.run(
        cmd, cwd=str(cwd), text=True, capture_output=True, env=merged, check=False
    )


def _setup_project(tmp_path: Path, extra_task_fields: str = "") -> Path:
    project = tmp_path / "proj"
    project.mkdir()
    harness = project / ".superharness"
    (harness / "handoffs").mkdir(parents=True, exist_ok=True)
    task_block = "\n".join(
        [
            "id: test-contract",
            "tasks:",
            "  - id: mcp-docs",
            "    owner: codex-cli",
            "    status: plan_approved",
            f"    project_path: '{project.as_posix()}'",
        ]
    )
    if extra_task_fields:
        task_block += "\n" + extra_task_fields
    (harness / "contract.yaml").write_text(task_block + "\n")
    return project


def _fake_bin(tmp_path: Path, *names: str) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for name in names:
        binary = bin_dir / name
        binary.write_text(f"#!/bin/bash\necho fake-{name}\n")
        binary.chmod(0o755)
    return bin_dir


def test_delegate_shorthand_preserves_pi_owner(monkeypatch, tmp_path: Path) -> None:
    """A SQLite-owned Pi task reaches the Pi delegate lane in print-only mode."""
    from superharness import cli
    from superharness.engine.db import get_connection, init_db

    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, created_at) VALUES (?, ?, ?, ?, ?)",
        ("pi-task", "Pi task", "pi", "plan_approved", "2026-08-26T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    received: list[tuple[str, tuple[str, ...]]] = []
    monkeypatch.setattr(
        cli, "_run_module", lambda module, args: received.append((module, args))
    )

    result = CliRunner().invoke(
        cli.main, ["delegate", "pi-task", "--project", str(project), "--print-only"]
    )

    assert result.exit_code == 0, result.output
    assert received == [
        (
            "superharness.commands.delegate",
            (
                "--to",
                "pi",
                "--task",
                "pi-task",
                "--project",
                str(project),
                "--print-only",
            ),
        )
    ]


@_REQUIRES_POSIX_FIXTURE
def test_delegate_shorthand_runs_fake_pi_with_target_correct_prompt(
    monkeypatch, tmp_path: Path
) -> None:
    """The real shorthand path reaches Pi through its fixture-only launcher."""
    isolated_home = tmp_path / "isolated-home"
    isolated_config = isolated_home / ".config"
    isolated_state = isolated_home / ".local" / "state"
    isolated_home.mkdir()
    monkeypatch.setenv("HOME", str(isolated_home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(isolated_config))
    monkeypatch.setenv("XDG_STATE_HOME", str(isolated_state))
    monkeypatch.setenv("SUPERHARNESS_TEST_OFFLINE", "1")

    from superharness.engine.db import get_connection, init_db

    project = tmp_path / "project"
    (project / ".superharness" / "handoffs").mkdir(parents=True)
    (project / ".git").mkdir()
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, context, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            "pi-task",
            "Pi task",
            "pi",
            "plan_approved",
            "fixture-only task context",
            "2026-08-26T00:00:00Z",
        ),
    )
    conn.commit()
    conn.close()

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    record = tmp_path / "pi-record.json"
    fake_pi = fake_bin / "pi"
    fake_pi.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        f"with open({str(record)!r}, 'w', encoding='utf-8') as stream:\n"
        "    json.dump({'argv': sys.argv[1:], 'cwd': os.getcwd()}, stream)\n"
        'sys.stdout.write(\'{"type":"session","version":3,"id":"fixture-session"}\\n\')\n'
        'sys.stdout.write(\'{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"fixture result"}],"provider":"provider-a","model":"model-a","usage":{},"cost":{},"stopReason":"stop"}}\\n\')\n'
        'sys.stdout.write(\'{"type":"agent_end","messages":[]}\\n\')\n',
        encoding="utf-8",
    )
    fake_pi.chmod(0o755)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env["HOME"] = str(isolated_home)
    env["XDG_CONFIG_HOME"] = str(isolated_config)
    env["XDG_STATE_HOME"] = str(isolated_state)
    env["SUPERHARNESS_TEST_OFFLINE"] = "1"
    env["SUPERHARNESS_CONFIRM_NON_INTERACTIVE"] = "YES"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import superharness.engine.osm as osm; "
            "osm.vault_search = lambda *_args, **_kwargs: []; "
            "from superharness.cli import main; main()",
            "delegate",
            "pi-task",
            "--project",
            str(project),
            "--non-interactive",
            "--no-auto-model",
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert record.exists(), result.stdout
    invocation = json.loads(record.read_text(encoding="utf-8"))
    prompt = invocation["argv"][invocation["argv"].index("-p") + 1]
    assert invocation["cwd"] == str(project)
    assert invocation["argv"].count(prompt) == 1
    assert "you are pi" in prompt
    assert "codex-cli" not in prompt
    assert str(isolated_home) not in prompt


def test_pi_prompt_names_pi_not_codex() -> None:
    """The common prompt identifies Pi by its actual target name."""
    from superharness.commands.delegate import _build_task_execution_prompt

    prompt = _build_task_execution_prompt(
        target="pi",
        task_id="pi-task",
        contract_id="contract",
        latest_handoff=False,
        acceptance_criteria="",
        context_hint="",
        user_instructions="",
        auto_directive="",
    )

    assert "you are pi" in prompt
    assert "codex-cli" not in prompt


@pytest.mark.parametrize(
    "target", ["claude-code", "codex-cli", "gemini-cli", "opencode"]
)
def test_task_prompt_names_each_existing_target(target: str) -> None:
    """Existing harnesses retain target-correct task prompt addressing."""
    from superharness.commands.delegate import _build_task_execution_prompt

    prompt = _build_task_execution_prompt(
        target=target,
        task_id="existing-task",
        contract_id="contract",
        latest_handoff=True,
        acceptance_criteria="",
        context_hint="",
        user_instructions="",
        auto_directive="",
    )

    assert f"addressed to {target}" in prompt


def test_inbox_watch_accepts_pi_target(monkeypatch, tmp_path: Path) -> None:
    """The watcher CLI accepts Pi without launching it."""
    from superharness.commands import inbox_watch

    watch_kwargs: dict[str, object] = {}
    monkeypatch.setattr(
        inbox_watch, "watch", lambda **kwargs: watch_kwargs.update(kwargs) or 0
    )

    monkeypatch.setattr(
        sys, "argv", ["inbox_watch", "--project", str(tmp_path), "--to", "pi"]
    )
    with pytest.raises(SystemExit) as exc_info:
        inbox_watch.main()

    assert exc_info.value.code == 0
    assert watch_kwargs["target"] == "pi"


def test_inbox_watch_both_targets_known_harnesses_once() -> None:
    """The ordinary polling expansion follows the harness registry exactly once."""
    from superharness.commands.inbox_watch import _watcher_targets
    from superharness.harnesses import KNOWN_HARNESSES

    assert _watcher_targets("both") == KNOWN_HARNESSES
    assert _watcher_targets("both").count("pi") == 1


def test_inbox_watch_both_dispatches_each_known_harness_once(
    monkeypatch, tmp_path: Path
) -> None:
    """A watcher cycle dispatches the registry lanes once, without a launcher."""
    from superharness.commands import inbox_watch
    from superharness.engine import agent_memory, behavioral
    from superharness.harnesses import KNOWN_HARNESSES

    project = tmp_path / "project"
    (project / ".superharness").mkdir(parents=True)
    dispatched: list[str] = []

    # Counter-driven maintenance may write user-global behavioral and memory
    # files. Keep this fixture cycle out of those branches and stub the
    # imported call targets as a second fence against future control-flow edits.
    monkeypatch.setattr(inbox_watch, "_watcher_cycle_count", [1])
    monkeypatch.setattr(behavioral, "refresh_behavioral_profile", lambda *_: False)
    monkeypatch.setattr(behavioral, "evaluate_all_open_trials", lambda *_: 0)
    monkeypatch.setattr(agent_memory, "promote_all_project_memory", lambda *_: 0)

    for name in (
        "_self_diagnosis",
        "_rotate_launcher_logs_if_needed",
        "_sqlite_tick",
        "_poll_operator_commands",
        "_run_scripts_heartbeat",
        "_auto_advance_orphaned_rounds",
        "_auto_close_consensus_discussions",
        "_auto_archive_stale_tasks",
        "_reconcile_zombies",
        "_analyze_task_logs",
        "_run_transcript_tail_if_enabled",
        "_run_gc_if_due",
        "_auto_delete_stale_inbox",
        "_comprehensive_gc",
        "_cancel_undispatchable_agents",
    ):
        monkeypatch.setattr(inbox_watch, name, lambda *args, **kwargs: None)
    monkeypatch.setattr(inbox_watch, "_find_scripts_dir", lambda: str(tmp_path))
    monkeypatch.setattr(inbox_watch, "_should_run", lambda *args, **kwargs: False)
    monkeypatch.setattr(
        inbox_watch,
        "_run_dispatch_cmd",
        lambda **kwargs: dispatched.append(kwargs["target"]),
    )

    inbox_watch._run_scripts(
        str(project),
        target="both",
        print_only=True,
        non_interactive=True,
        codex_bypass=False,
        launcher_timeout=0,
        recover_timeout_minutes=20,
        recover_action="stale",
    )

    assert dispatched == KNOWN_HARNESSES
    assert dispatched.count("pi") == 1


def test_watcher_peer_fallback_health_and_retry_order_are_unchanged() -> None:
    """Pi does not perturb established peer, fallback, health, or retry policy."""
    from superharness.commands.inbox_watch import (
        _AGENT_CLI_BINARY,
        _AGENT_FALLBACK,
        _FALLBACK_ORDER,
        _PEER_AGENTS,
    )

    assert _PEER_AGENTS == {
        "claude-code": "gemini-cli",
        "gemini-cli": "codex-cli",
        "codex-cli": "claude-code",
    }
    assert _FALLBACK_ORDER == ["claude-code", "codex-cli", "gemini-cli", "opencode"]
    assert _AGENT_FALLBACK["codex-cli"] == ["claude-code", "gemini-cli", "opencode"]
    assert _AGENT_CLI_BINARY == {
        "claude-code": "claude",
        "codex-cli": "codex",
        "gemini-cli": "gemini",
    }












# ---------------------------------------------------------------------------
# Model routing tests
# ---------------------------------------------------------------------------














# ---------------------------------------------------------------------------
# Scheduling gate tests
# ---------------------------------------------------------------------------












def test_delegate_scheduled_after_idempotent(repo_root, tmp_path) -> None:
    """Running delegate twice on a future-scheduled task returns same error both times."""
    project = _setup_project(
        tmp_path, extra_task_fields="    scheduled_after: '2099-12-31'"
    )

    r1 = _run_delegate_py(
        repo_root,
        args=[
            "--to",
            "codex-cli",
            "--project",
            str(project),
            "--task",
            "mcp-docs",
            "--print-only",
        ],
        env={"PATH": "/usr/bin:/bin"},
    )
    r2 = _run_delegate_py(
        repo_root,
        args=[
            "--to",
            "codex-cli",
            "--project",
            str(project),
            "--task",
            "mcp-docs",
            "--print-only",
        ],
        env={"PATH": "/usr/bin:/bin"},
    )

    assert r1.returncode == 1
    assert r2.returncode == 1
    assert r1.stderr == r2.stderr


# ---------------------------------------------------------------------------
# SDK delegation tests (--via sdk)
# ---------------------------------------------------------------------------








# ── gate 4 exit code + --plan-only ───────────────────────────────────────────


def _setup_project_todo(tmp_path: Path) -> Path:
    """Project with a single `todo` + `implementation` task."""
    project = tmp_path / "proj_todo"
    project.mkdir()
    harness = project / ".superharness"
    (harness / "handoffs").mkdir(parents=True, exist_ok=True)
    (harness / "contract.yaml").write_text(
        "id: test-contract\n"
        "tasks:\n"
        "  - id: feat.wip\n"
        "    owner: claude-code\n"
        "    status: todo\n"
        "    workflow: implementation\n"
        f"    project_path: '{project.as_posix()}'\n"
    )
    return project


def _setup_reliable_run_project(
    tmp_path: Path,
    *,
    status: str,
    run_kind: str,
    agent: str = "claude-code",
    model: str | None = None,
) -> tuple[Path, str]:
    project = tmp_path / f"proj_reliable_{run_kind}_{status}"
    project.mkdir()
    harness = project / ".superharness"
    (harness / "handoffs").mkdir(parents=True, exist_ok=True)

    from superharness.engine import runs_dao
    from superharness.engine.db import get_connection, init_db

    conn = get_connection(str(project))
    try:
        init_db(conn)
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, owner, status, effort, project_path, context,
                workflow, version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "gh-1-r2",
                "Add multiply function",
                agent,
                status,
                "low",
                str(project),
                "Add multiply(a, b) and tests for positives, negatives, and zero.",
                "reliable-orchestrator",
                1,
                "2026-01-01T00:00:00Z",
            ),
        )
        run_id = f"run-{run_kind}-{status.replace('_', '-')}"
        runs_dao.create_run(
            conn,
            id=run_id,
            task_id="gh-1-r2",
            kind=run_kind,
            agent=agent,
            model=model
            if model is not None
            else ("claude-sonnet-4-6" if agent == "claude-code" else "gpt-5.5"),
            dedupe_key=f"{run_kind}:gh-1-r2:test",
            review_target_sha="sha-review" if run_kind == "review" else None,
            now="2026-01-01T00:00:00Z",
        )
        conn.commit()
    finally:
        conn.close()
    return project, run_id


def _run_reliable_delegate(
    project: Path,
    run_id: str | None,
    *,
    agent: str = "claude-code",
    extra_args: list[str] | None = None,
):
    env = (
        {"SUPERHARNESS_RUN_ID": run_id, "SUPERHARNESS_RUN_RESULT_PATH": str(project / "artifact.json")}
        if run_id
        else None
    )
    return _run_delegate_py(
        project,
        args=[
            "--to",
            agent,
            "--project",
            str(project),
            "--task",
            "gh-1-r2",
            "--print-only",
            "--no-auto-model",
            *(extra_args or []),
        ],
        env=env,
    )


def test_delegate_public_cli_allows_reliable_plan_run_at_todo(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    r = _run_reliable_delegate(project, run_id, extra_args=["--plan-only"])
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)
    assert "Generated prompt:" in r.stdout


def test_delegate_allows_reliable_plan_retry_at_plan_proposed(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="plan_proposed", run_kind="plan"
    )
    r = _run_reliable_delegate(project, run_id, extra_args=["--plan-only"])
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)


def test_delegate_allows_reliable_implementation_run_at_in_progress(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="in_progress", run_kind="implement"
    )
    r = _run_reliable_delegate(project, run_id)
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)


def test_delegate_allows_reliable_review_run_at_review_requested(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="review_requested", run_kind="review", agent="codex-cli"
    )
    r = _run_reliable_delegate(
        project, run_id, agent="codex-cli", extra_args=["--for-review"]
    )
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)


def test_delegate_allows_reliable_repair_run_at_in_progress(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="in_progress", run_kind="repair"
    )
    r = _run_reliable_delegate(project, run_id)
    assert r.returncode == 0, (r.returncode, r.stdout, r.stderr)


def test_delegate_blocks_manual_reliable_dispatch_without_run_id(tmp_path):
    project, _run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    r = _run_reliable_delegate(project, None, extra_args=["--plan-only"])
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert "requires SUPERHARNESS_RUN_ID" in r.stderr


def test_delegate_blocks_reliable_run_kind_launch_mode_mismatch(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    r = _run_reliable_delegate(project, run_id)
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert "must dispatch with --plan-only" in r.stderr


def test_delegate_blocks_reliable_run_task_mismatch(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    from superharness.engine.db import managed_connection

    with managed_connection(str(project)) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, title, owner, status, effort, project_path, context,
                workflow, version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "gh-other",
                "Other",
                "claude-code",
                "todo",
                "low",
                str(project),
                "Other reliable task.",
                "reliable-orchestrator",
                1,
                "2026-01-01T00:00:00Z",
            ),
        )

    r = _run_delegate_py(
        project,
        args=[
            "--to",
            "claude-code",
            "--project",
            str(project),
            "--task",
            "gh-other",
            "--print-only",
            "--no-auto-model",
            "--plan-only",
        ],
        env={"SUPERHARNESS_RUN_ID": run_id},
    )
    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert "belongs to task 'gh-1-r2'" in r.stderr


@pytest.mark.parametrize(
    ("run_kind", "status", "agent", "model", "extra_kwargs"),
    [
        (
            "plan",
            "todo",
            "claude-code",
            "claude-sonnet-4-6",
            {"plan_only": True},
        ),
        ("implement", "in_progress", "claude-code", "claude-sonnet-4-6", {}),
        ("fallback", "in_progress", "codex-cli", "gpt-5.5", {}),
        ("repair", "in_progress", "claude-code", "claude-sonnet-4-6", {}),
        (
            "review",
            "review_requested",
            "codex-cli",
            "gpt-5.5",
            {"for_review": True},
        ),
    ],
)
def test_delegate_reliable_run_assignment_is_authoritative(
    tmp_path, run_kind, status, agent, model, extra_kwargs
):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status=status, run_kind=run_kind, agent=agent, model=model
    )
    from superharness.commands.delegate import delegate

    with (
        patch.dict(os.environ, {"SUPERHARNESS_RUN_ID": run_id}),
        patch("superharness.engine.orchestrator.Orchestrator") as orchestrator,
        patch("superharness.commands.delegate._launch_agent") as launch,
    ):
        rc = delegate(
            project_dir=str(project),
            target=agent,
            task_id="gh-1-r2",
            print_only=True,
            non_interactive=False,
            codex_bypass=False,
            orchestrate=True,
            no_auto_model=False,
            **extra_kwargs,
        )

    assert rc == 0
    orchestrator.assert_not_called()
    launch.assert_called_once()
    assert launch.call_args.args[0] == agent
    assert launch.call_args.kwargs["model"] == model


def test_delegate_reliable_run_model_mismatch_fails_closed(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path,
        status="todo",
        run_kind="plan",
        agent="claude-code",
        model="claude-sonnet-4-6",
    )

    r = _run_delegate_py(
        project,
        args=[
            "--to",
            "claude-code",
            "--project",
            str(project),
            "--task",
            "gh-1-r2",
            "--print-only",
            "--plan-only",
            "--model",
            "gpt-5-codex",
        ],
        env={"SUPERHARNESS_RUN_ID": run_id},
    )

    assert r.returncode == 2, (r.returncode, r.stdout, r.stderr)
    assert "uses model 'claude-sonnet-4-6', not 'gpt-5-codex'" in r.stderr


@pytest.mark.parametrize(
    ("run_kind", "status", "agent", "extra_args"),
    [
        ("plan", "todo", "claude-code", ["--plan-only"]),
        ("implement", "in_progress", "claude-code", []),
        ("fallback", "in_progress", "codex-cli", []),
        ("repair", "in_progress", "claude-code", []),
        ("review", "review_requested", "codex-cli", ["--for-review"]),
    ],
)
def test_reliable_prompt_does_not_delegate_lifecycle_or_shipping(
    tmp_path, run_kind, status, agent, extra_args
):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status=status, run_kind=run_kind, agent=agent
    )
    result = _run_reliable_delegate(project, run_id, agent=agent, extra_args=extra_args)
    assert result.returncode == 0, result.stderr
    assert "continue reliable-orchestrator Run" in result.stdout
    assert "Do not run `shux task status`" in result.stdout
    assert "Do not commit, push" in result.stdout
    assert "shux contract` to update task status" not in result.stdout
    assert "Run `shux task status" not in result.stdout
    assert "ALLOW_PUSH=1 /ship commit" not in result.stdout
    for field in ("schema_version", "run_id", "task_id", "kind", "agent", "exit_code", "completion_status"):
        assert field in result.stdout
    assert f"Set kind exactly to {run_kind}" in result.stdout
    assert f"Set agent exactly to {agent}" in result.stdout
    assert "completion_status must be one of" in result.stdout
    assert "Do not use custom fields such as status" in result.stdout
    if run_kind != "review":
        for field in ("worktree_path", "branch_name", "base_sha", "head_sha", "dirty"):
            assert field in result.stdout


def test_reliable_review_prompt_is_read_only_and_sha_bound(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="review_requested", run_kind="review", agent="codex-cli"
    )
    result = _run_reliable_delegate(
        project, run_id, agent="codex-cli", extra_args=["--for-review"]
    )
    assert result.returncode == 0, result.stderr
    assert "Review only" in result.stdout
    assert "sha-review" in result.stdout
    assert "Do not modify any file or task" in result.stdout
    assert "Structured result contract:" in result.stdout
    assert "Write exactly one JSON object to" in result.stdout
    assert "reviewed_sha" in result.stdout
    assert "review_verdict must be LGTM or REJECTED" in result.stdout
    assert "Set reviewed_sha exactly to sha-review" in result.stdout
    assert "findings" in result.stdout
    assert str(project / "artifact.json") in result.stdout
    assert "BLOCKED" not in result.stdout


@pytest.mark.parametrize("requested_status", ["plan_proposed", "done"])
def test_reliable_run_cannot_mutate_task_status_from_agent_process(
    tmp_path, requested_status
):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    from superharness.commands.task import status_update
    from superharness.engine.db import managed_connection

    with (
        patch.dict(os.environ, {"SUPERHARNESS_RUN_ID": run_id}),
        pytest.raises(SystemExit) as exc_info,
    ):
        status_update(
            str(project),
            "gh-1-r2",
            requested_status,
            "claude-code",
            summary="agent attempted lifecycle mutation",
        )
    assert exc_info.value.code == 2
    with managed_connection(str(project)) as conn:
        task = conn.execute(
            "SELECT status FROM tasks WHERE id=?", ("gh-1-r2",)
        ).fetchone()
    assert task[0] == "todo"


def test_reliable_run_task_status_cli_is_lifecycle_guarded(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="todo", run_kind="plan"
    )
    env = os.environ.copy()
    env["SUPERHARNESS_RUN_ID"] = run_id
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "superharness.commands.task",
            "status",
            "--project",
            str(project),
            "--id",
            "gh-1-r2",
            "--status",
            "done",
            "--actor",
            "claude-code",
            "--summary",
            "agent attempted close",
        ],
        cwd=str(project),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2, result.stderr
    assert "LifecycleOrchestrator owns task lifecycle" in result.stderr


def test_reliable_run_cannot_close_task_from_agent_process(tmp_path):
    project, run_id = _setup_reliable_run_project(
        tmp_path, status="review_passed", run_kind="review", agent="codex-cli"
    )
    from superharness.commands.close import close_task

    with patch.dict(os.environ, {"SUPERHARNESS_RUN_ID": run_id}):
        result = close_task(
            str(project), "gh-1-r2", "codex-cli", "agent attempted close", force=True
        )
    assert result == 2







# ── ship_on_complete directive ────────────────────────────────────────────────


def _setup_project_ship_on_complete(tmp_path: Path) -> Path:
    """Project with a plan_approved + ship_on_complete task."""
    project = tmp_path / "proj_ship"
    project.mkdir()
    harness = project / ".superharness"
    (harness / "handoffs").mkdir(parents=True, exist_ok=True)
    (harness / "contract.yaml").write_text(
        "id: test-contract\n"
        "tasks:\n"
        "  - id: feat.ship-me\n"
        "    owner: claude-code\n"
        "    status: plan_approved\n"
        "    workflow: implementation\n"
        "    ship_on_complete: true\n"
        f"    project_path: '{project.as_posix()}'\n"
    )
    return project








class TestSaveContextSnapshotTaskUsage:
    """_save_context_snapshot must persist SDK dispatch cost/tokens to task_usage
    (source='sdk') in addition to the existing YAML sidecar cache."""

    def _setup(self, tmp_path: Path) -> Path:
        project = tmp_path / "proj"
        project.mkdir()
        (project / ".superharness").mkdir()
        from superharness.engine.db import get_connection, init_db

        conn = get_connection(str(project))
        init_db(conn)
        conn.execute(
            "INSERT INTO tasks (id, title, status, version, created_at) "
            "VALUES ('t1', 'T', 'in_progress', 1, '2026-01-01T00:00:00Z')"
        )
        conn.commit()
        conn.close()
        return project

    def test_save_context_snapshot_writes_task_usage_row(self, tmp_path: Path) -> None:
        from superharness.commands.delegate import _save_context_snapshot
        from superharness.engine import usage_dao
        from superharness.engine.db import get_connection, init_db

        project = self._setup(tmp_path)
        result = {
            "output": "done",
            "input_tokens": 200,
            "output_tokens": 80,
            "cost_usd": 0.02,
        }
        _save_context_snapshot(str(project), "t1", result, model="claude-sonnet-5")

        conn = get_connection(str(project))
        init_db(conn)
        rows = usage_dao.list_for_task(conn, "t1")
        conn.close()

        assert len(rows) == 1
        assert rows[0].source == "sdk"
        assert rows[0].agent == "claude-code"
        assert rows[0].model == "claude-sonnet-5"
        assert rows[0].input_tokens == 200
        assert rows[0].output_tokens == 80
        assert rows[0].cost_usd == 0.02

    def test_save_context_snapshot_still_writes_yaml_cache(
        self, tmp_path: Path
    ) -> None:
        from superharness.commands.delegate import _save_context_snapshot
        import yaml

        project = self._setup(tmp_path)
        result = {
            "output": "done",
            "input_tokens": 200,
            "output_tokens": 80,
            "cost_usd": 0.02,
        }
        _save_context_snapshot(str(project), "t1", result, model="claude-sonnet-5")

        cache_file = project / ".superharness" / "context-cache" / "t1.yaml"
        assert cache_file.exists()
        snapshot = yaml.safe_load(cache_file.read_text())
        assert snapshot["task_id"] == "t1"
        assert snapshot["input_tokens"] == 200
        assert snapshot["output_tokens"] == 80
        assert snapshot["cost_usd"] == 0.02

    def test_save_context_snapshot_handles_missing_cost_data_gracefully(
        self, tmp_path: Path
    ) -> None:
        from superharness.commands.delegate import _save_context_snapshot
        from superharness.engine import usage_dao
        from superharness.engine.db import get_connection, init_db

        project = self._setup(tmp_path)
        result = {
            "output": "done",
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": None,
        }
        _save_context_snapshot(str(project), "t1", result, model="unknown-model")

        conn = get_connection(str(project))
        init_db(conn)
        rows = usage_dao.list_for_task(conn, "t1")
        conn.close()

        assert len(rows) == 1
        assert rows[0].cost_usd is None

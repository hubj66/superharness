"""Iteration 3: `shux task create --from-issue` smoke + create-path regression."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


def _make_project(tmp_path: Path) -> Path:
    project = tmp_path / "proj"
    project.mkdir()
    (project / ".superharness").mkdir()
    from superharness.engine.db import get_connection, init_db

    conn = get_connection(str(project))
    init_db(conn)
    conn.close()
    return project


def test_from_issue_prefills_task(tmp_path: Path, monkeypatch) -> None:
    from superharness.commands import task as task_mod
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db

    project = _make_project(tmp_path)

    fixture_issue = {
        "title": "Fix the thing",
        "body": "Context.\n- [ ] step one",
        "labels": [{"name": "bug"}],
    }
    monkeypatch.setattr(
        "superharness.commands.issue_import._fetch_issue",
        lambda url: fixture_issue,
    )

    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--from-issue",
                "https://github.com/o/r/issues/5",
                "--owner",
                "claude-code",
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    tasks = tasks_dao.get_all(conn)
    conn.close()
    assert len(tasks) == 1
    task = tasks[0]
    assert task.title == "Fix the thing"
    assert task.context == "Context.\n- [ ] step one"
    assert task.acceptance_criteria == ["step one"]
    assert task.issue_url == "https://github.com/o/r/issues/5"


def test_from_issue_explicit_flags_override(tmp_path: Path, monkeypatch) -> None:
    from superharness.commands import task as task_mod
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db

    project = _make_project(tmp_path)

    fixture_issue = {"title": "Imported title", "body": "imported body", "labels": []}
    monkeypatch.setattr(
        "superharness.commands.issue_import._fetch_issue",
        lambda url: fixture_issue,
    )

    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--from-issue",
                "https://github.com/o/r/issues/5",
                "--owner",
                "claude-code",
                "--title",
                "Explicit title",
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    tasks = tasks_dao.get_all(conn)
    conn.close()
    assert tasks[0].title == "Explicit title"


def test_create_without_from_issue_unchanged(tmp_path: Path) -> None:
    """Regression: normal create path (no --from-issue) is unaffected."""
    from superharness.commands import task as task_mod
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db

    project = _make_project(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--title",
                "Normal task",
                "--owner",
                "claude-code",
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    tasks = tasks_dao.get_all(conn)
    conn.close()
    assert len(tasks) == 1
    assert tasks[0].title == "Normal task"
    assert tasks[0].issue_url is None


def test_create_without_title_or_from_issue_errors(tmp_path: Path) -> None:
    """Regression: --title is still required when --from-issue is absent."""
    from superharness.commands import task as task_mod

    project = _make_project(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--owner",
                "claude-code",
            ]
        )
    assert exc_info.value.code != 0


def test_create_accepts_reliable_orchestrator_workflow(tmp_path: Path) -> None:
    from superharness.commands import task as task_mod
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db
    from superharness.engine.reliable_orchestrator_gate import (
        is_reliable_orchestrated_task,
    )

    project = _make_project(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--id",
                "reliable-task",
                "--title",
                "Reliable task",
                "--owner",
                "claude-code",
                "--workflow",
                "reliable-orchestrator",
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    try:
        row = tasks_dao.get(conn, "reliable-task")
    finally:
        conn.close()
    assert row is not None
    assert row.workflow == "reliable-orchestrator"
    assert is_reliable_orchestrated_task(row)


def test_task_create_cli_accepts_reliable_orchestrator_workflow(
    tmp_path: Path,
) -> None:
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db
    from superharness.engine.reliable_orchestrator_gate import (
        is_reliable_orchestrated_task,
    )

    project = _make_project(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "superharness.commands.task",
            "create",
            "--project",
            str(project),
            "--id",
            "cli-reliable",
            "--title",
            "CLI reliable task",
            "--owner",
            "claude-code",
            "--workflow",
            "reliable-orchestrator",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "workflow must be one of" not in result.stderr

    conn = get_connection(str(project))
    init_db(conn)
    try:
        row = tasks_dao.get(conn, "cli-reliable")
    finally:
        conn.close()
    assert row is not None
    assert row.workflow == "reliable-orchestrator"
    assert is_reliable_orchestrated_task(row)


@pytest.mark.parametrize(
    "workflow",
    ["implementation", "quick", "discussion", "review", "approval", "note"],
)
def test_create_existing_valid_workflows_still_work(
    tmp_path: Path, workflow: str
) -> None:
    from superharness.commands import task as task_mod
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db

    project = _make_project(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--id",
                f"{workflow}-task",
                "--title",
                f"{workflow} task",
                "--owner",
                "claude-code",
                "--workflow",
                workflow,
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    try:
        row = tasks_dao.get(conn, f"{workflow}-task")
    finally:
        conn.close()
    assert row is not None
    assert row.workflow == workflow


def test_task_create_rejects_invalid_workflow(tmp_path: Path) -> None:
    from superharness.commands import task as task_mod

    project = _make_project(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--id",
                "bad-workflow",
                "--title",
                "Bad workflow",
                "--owner",
                "claude-code",
                "--workflow",
                "definitely-not-real",
            ]
        )
    assert exc_info.value.code != 0


def test_reliable_task_status_update_bypasses_legacy_auto_approval(
    tmp_path: Path,
) -> None:
    from superharness.commands import task as task_mod
    from superharness.engine import inbox_dao, tasks_dao
    from superharness.engine.db import get_connection, init_db

    project = _make_project(tmp_path)
    (project / ".superharness" / "profile.yaml").write_text(
        "auto_approve_plans: true\nauto_dispatch: true\nautonomy: ai_driven\n",
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "create",
                "--project",
                str(project),
                "--id",
                "reliable-plan",
                "--title",
                "Reliable plan",
                "--owner",
                "claude-code",
                "--workflow",
                "reliable-orchestrator",
            ]
        )
    assert exc_info.value.code == 0

    with pytest.raises(SystemExit) as exc_info:
        task_mod.main(
            [
                "status",
                "--project",
                str(project),
                "--id",
                "reliable-plan",
                "--status",
                "plan_proposed",
                "--actor",
                "claude-code",
            ]
        )
    assert exc_info.value.code == 0

    conn = get_connection(str(project))
    init_db(conn)
    try:
        row = tasks_dao.get(conn, "reliable-plan")
        inbox = inbox_dao.get_all(conn)
    finally:
        conn.close()
    assert row is not None
    assert row.status == "plan_proposed"
    assert inbox == []

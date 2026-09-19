"""Tests for the operator-authorized status override path.

The --operator flag on `shux task status` allows a human operator (who is not
the task owner) to apply specific recovery transitions. Currently:
    blocked -> pr_open

Requirements verified:
- Non-owner without --operator remains forbidden.
- --operator allows blocked -> pr_open.
- Illegal transitions remain rejected even with --operator.
- Ordinary owner-only writes are unchanged.
- Audit actor is the real human actor, not the task owner.
- --operator does not silently allow unrelated unsafe transitions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from superharness.engine import tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.tasks_dao import TaskRow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _project(tmp_path: Path) -> tuple[Path, object]:
    sh = tmp_path / ".superharness"
    sh.mkdir()
    conn = get_connection(str(tmp_path))
    init_db(conn)
    return tmp_path, conn


def _make_task(conn, project_dir: Path, task_id: str, status: str, owner: str = "claude-code") -> None:
    now = _now()
    tasks_dao.upsert(
        conn,
        TaskRow(
            id=task_id,
            title=f"Task {task_id}",
            status=status,
            owner=owner,
            project_path=str(project_dir),
            created_at=now,
            updated_at=now,
            version=1,
            effort=None,
            development_method=None,
            acceptance_criteria=[],
            test_types=[],
            out_of_scope=[],
            definition_of_done=[],
            context=None,
            blocked_by=[],
            parent_id=None,
            tdd=None,
            contract_locked_at=None,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Non-owner without --operator remains forbidden
# ---------------------------------------------------------------------------


def test_non_owner_without_operator_flag_is_forbidden(tmp_path: Path) -> None:
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-auth-1", "blocked")
    conn.close()

    from superharness.commands.task import status_update

    with pytest.raises(SystemExit) as exc:
        status_update(
            str(project),
            task_id="t-auth-1",
            status="pr_open",
            actor="joel",
        )
    assert exc.value.code != 0, "Expected non-zero exit for unauthorized actor"


# ---------------------------------------------------------------------------
# --operator allows blocked -> pr_open
# ---------------------------------------------------------------------------


def test_operator_flag_allows_blocked_to_pr_open(tmp_path: Path) -> None:
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-op-1", "blocked")
    conn.close()

    from superharness.commands.task import status_update

    rc = status_update(
        str(project),
        task_id="t-op-1",
        status="pr_open",
        actor="joel",
        operator=True,
    )
    assert rc == 0

    conn = get_connection(str(project))
    try:
        task = tasks_dao.get(conn, "t-op-1")
        assert task is not None
        assert task.status == "pr_open"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Audit trail: operator_command row is written with the real human actor
# ---------------------------------------------------------------------------


def test_operator_override_writes_audit_row_with_real_actor(tmp_path: Path) -> None:
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-audit-1", "blocked")
    conn.close()

    from superharness.commands.task import status_update

    status_update(
        str(project),
        task_id="t-audit-1",
        status="pr_open",
        actor="joel",
        operator=True,
    )

    conn = get_connection(str(project))
    try:
        rows = conn.execute(
            "SELECT command, task_id, sender_id, status FROM operator_commands "
            "WHERE task_id = ? ORDER BY id DESC LIMIT 1",
            ("t-audit-1",),
        ).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["command"] == "operator-recover:pr_open"
        assert row["task_id"] == "t-audit-1"
        assert row["sender_id"] == "joel"  # real human actor, not task owner
        assert row["status"] == "executed"
    finally:
        conn.close()


def test_operator_override_audit_actor_is_not_task_owner(tmp_path: Path) -> None:
    """Audit row sender_id must be the operator (joel), not the task owner (claude-code)."""
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-audit-2", "blocked", owner="claude-code")
    conn.close()

    from superharness.commands.task import status_update

    status_update(
        str(project),
        task_id="t-audit-2",
        status="pr_open",
        actor="joel",
        operator=True,
    )

    conn = get_connection(str(project))
    try:
        row = conn.execute(
            "SELECT sender_id FROM operator_commands WHERE task_id = ? LIMIT 1",
            ("t-audit-2",),
        ).fetchone()
        assert row is not None
        assert row["sender_id"] == "joel"
        assert row["sender_id"] != "claude-code"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Illegal transitions remain rejected even with --operator
# ---------------------------------------------------------------------------


def test_operator_flag_does_not_allow_arbitrary_transitions(tmp_path: Path) -> None:
    """--operator only permits transitions in OPERATOR_RECOVERY_TRANSITIONS."""
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-illegal-1", "blocked")
    conn.close()

    from superharness.commands.task import status_update

    # blocked -> done is not in OPERATOR_RECOVERY_TRANSITIONS
    with pytest.raises(SystemExit) as exc:
        status_update(
            str(project),
            task_id="t-illegal-1",
            status="done",
            actor="joel",
            operator=True,
            summary="force done",
        )
    assert exc.value.code != 0


def test_operator_flag_does_not_allow_todo_to_done(tmp_path: Path) -> None:
    """--operator flag cannot override the transition graph for non-recovery moves."""
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-illegal-2", "todo")
    conn.close()

    from superharness.commands.task import status_update

    with pytest.raises(SystemExit) as exc:
        status_update(
            str(project),
            task_id="t-illegal-2",
            status="done",
            actor="joel",
            operator=True,
            summary="force done",
        )
    assert exc.value.code != 0


# ---------------------------------------------------------------------------
# Ordinary owner behavior unchanged
# ---------------------------------------------------------------------------


def test_owner_can_still_update_without_operator_flag(tmp_path: Path) -> None:
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-owner-1", "todo")
    conn.close()

    from superharness.commands.task import status_update

    rc = status_update(
        str(project),
        task_id="t-owner-1",
        status="plan_proposed",
        actor="claude-code",
        summary="plan ready",
    )
    assert rc == 0

    conn = get_connection(str(project))
    try:
        task = tasks_dao.get(conn, "t-owner-1")
        assert task is not None
        assert task.status == "plan_proposed"
    finally:
        conn.close()


def test_owner_blocked_to_pr_open_rejected_without_operator_flag(tmp_path: Path) -> None:
    """blocked -> pr_open is not in the global graph; even the task owner needs --operator."""
    project, conn = _project(tmp_path)
    _make_task(conn, project, "t-owner-2", "blocked")
    conn.close()

    from superharness.commands.task import status_update

    with pytest.raises(SystemExit) as exc:
        status_update(
            str(project),
            task_id="t-owner-2",
            status="pr_open",
            actor="claude-code",
        )
    assert exc.value.code != 0


# ---------------------------------------------------------------------------
# OPERATOR_RECOVERY_TRANSITIONS constant is correctly defined
# ---------------------------------------------------------------------------


def test_operator_recovery_transitions_contains_blocked_to_pr_open() -> None:
    from superharness.commands.task import OPERATOR_RECOVERY_TRANSITIONS

    assert ("blocked", "pr_open") in OPERATOR_RECOVERY_TRANSITIONS


def test_operator_recovery_transitions_does_not_contain_arbitrary_moves() -> None:
    from superharness.commands.task import OPERATOR_RECOVERY_TRANSITIONS

    assert ("blocked", "done") not in OPERATOR_RECOVERY_TRANSITIONS
    assert ("todo", "done") not in OPERATOR_RECOVERY_TRANSITIONS
    assert ("blocked", "todo") not in OPERATOR_RECOVERY_TRANSITIONS

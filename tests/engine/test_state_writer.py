"""Tests for engine.state_writer — RED tests for the unified write API (iter 3a).

iter 3a (skeleton): introduces a unified write API for tasks, inbox items, and
discussion state. Writes go to SQLite first; YAML is queued for export.

The full migration (3b-3e: routing all writers through this module, switching
default backend to sqlite_only) is deferred.

These tests pin down the public API contract.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def state_writer():
    from superharness.engine import state_writer

    return state_writer


def test_module_exposes_set_task_status(state_writer) -> None:
    assert callable(state_writer.set_task_status)


def test_module_exposes_set_inbox_status(state_writer) -> None:
    assert callable(state_writer.set_inbox_status)


def test_module_exposes_upsert_handoff(state_writer) -> None:
    assert callable(state_writer.upsert_handoff)


def test_set_task_status_writes_through_to_sqlite(
    state_writer, clean_harness: Path
) -> None:
    # Seed a task in YAML so SQLite migration on first read picks it up
    contract = clean_harness / ".superharness" / "contract.yaml"
    import yaml

    contract.write_text(yaml.dump({"tasks": [{"id": "feat.foo", "status": "todo"}]}))

    ok = state_writer.set_task_status(str(clean_harness), "feat.foo", "plan_proposed")
    assert ok is True

    # Verify by reading back through state_reader (the public read path)
    from superharness.engine.state_reader import get_tasks

    tasks = get_tasks(str(clean_harness))
    foo = next((t for t in tasks if t.get("id") == "feat.foo"), None)
    assert foo is not None
    assert foo.get("status") == "plan_proposed"



def test_mirror_locked_task_skips_equivalent_empty_contract_shapes(clean_harness: Path, caplog) -> None:
    import logging
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db
    from superharness.engine.tasks_dao import TaskRow

    conn = get_connection(str(clean_harness))
    init_db(conn)
    tasks_dao.upsert(
        conn,
        TaskRow(
            id="gh-1-r6",
            title="Locked review task",
            owner="claude-code",
            status="review_requested",
            effort=None,
            project_path=str(clean_harness),
            development_method=None,
            acceptance_criteria=[],
            test_types=[],
            out_of_scope=[],
            definition_of_done=[],
            context=None,
            tdd=None,
            version=1,
            created_at="2026-09-13T11:52:16Z",
            locked_contract='{"acceptance_criteria": [], "tdd": null}',
            contract_locked_at="2026-09-13T11:52:16Z",
        ),
    )
    conn.commit()
    conn.close()

    from superharness.engine import state_writer

    with caplog.at_level(logging.WARNING, logger="superharness.engine.state_writer"):
        state_writer.mirror_task_dict(
            str(clean_harness),
            {
                "id": "gh-1-r6",
                "status": "review_requested",
                "acceptance_criteria": [],
                "tdd": False,
            },
        )

    assert "Cannot modify" not in caplog.text
    assert "NameError" not in caplog.text
    assert "_CONTRACT_LOCKED_FIELDS" not in caplog.text


def test_mirror_locked_task_still_rejects_real_contract_change(clean_harness: Path) -> None:
    from superharness.engine import tasks_dao
    from superharness.engine.db import get_connection, init_db
    from superharness.engine.state_errors import ContractLockError
    from superharness.engine.tasks_dao import TaskRow

    conn = get_connection(str(clean_harness))
    init_db(conn)
    tasks_dao.upsert(
        conn,
        TaskRow(
            id="locked-change",
            title="Locked task",
            owner="claude-code",
            status="review_requested",
            effort=None,
            project_path=str(clean_harness),
            development_method=None,
            acceptance_criteria=[],
            test_types=[],
            out_of_scope=[],
            definition_of_done=[],
            context=None,
            tdd=None,
            version=1,
            created_at="2026-09-13T11:52:16Z",
            locked_contract='{"acceptance_criteria": [], "tdd": null}',
            contract_locked_at="2026-09-13T11:52:16Z",
        ),
    )
    conn.commit()
    task = tasks_dao.get(conn, "locked-change")
    assert task is not None
    try:
        with pytest.raises(ContractLockError):
            tasks_dao.update(
                conn,
                task.id,
                task.version,
                {"acceptance_criteria": ["real change"]},
            )
    finally:
        conn.close()


def test_set_inbox_status_writes_through(state_writer, clean_harness: Path) -> None:
    from superharness.engine.db import get_connection, init_db, transaction
    from superharness.engine import inbox_dao, tasks_dao
    from superharness.engine.tasks_dao import TaskRow

    conn = get_connection(str(clean_harness))
    init_db(conn)
    with transaction(conn):
        tasks_dao.upsert(
            conn,
            TaskRow(
                id="feat.foo",
                title="feat.foo",
                owner="claude-code",
                status="todo",
                effort=None,
                project_path=None,
                development_method=None,
                acceptance_criteria=[],
                test_types=[],
                out_of_scope=[],
                definition_of_done=[],
                context=None,
                tdd=None,
                version=1,
                created_at="2026-04-27T12:00:00Z",
            ),
        )
        inbox_dao.enqueue(
            conn,
            id="test-1",
            task_id="feat.foo",
            target_agent="claude-code",
            project_path=str(clean_harness),
            now="2026-04-27T12:00:00Z",
        )
    conn.close()

    ok = state_writer.set_inbox_status(str(clean_harness), "test-1", "paused")
    assert ok is True

    # Verify by reading back through the public read path
    from superharness.engine.state_reader import get_inbox_items

    items = get_inbox_items(str(clean_harness))
    item = next((i for i in items if i.get("id") == "test-1"), None)
    assert item is not None
    assert item["status"] == "paused"


def test_set_task_status_handles_unknown_task_gracefully(
    state_writer, clean_harness: Path
) -> None:
    """Unknown task id returns False, does not raise."""
    ok = state_writer.set_task_status(str(clean_harness), "nonexistent.task", "done")
    assert ok is False


def test_set_inbox_status_handles_unknown_id_gracefully(
    state_writer, clean_harness: Path
) -> None:
    ok = state_writer.set_inbox_status(str(clean_harness), "nonexistent", "paused")
    assert ok is False


def test_write_handoff_to_db_forwards_usage_to_dao(
    state_writer, clean_harness: Path
) -> None:
    """write_handoff_to_db calls usage_dao.record() when usage data is present
    in the handoff content, and does NOT call it when absent."""
    from superharness.engine import usage_dao
    from superharness.engine.db import get_connection, init_db

    content_with_usage = {
        "task": "feat.foo",
        "phase": "report",
        "status": "report_ready",
        "from": "codex-cli",
        "to": "owner",
        "date": "2026-04-27T12:00:00Z",
        "outcome": "done",
        "input_tokens": 400,
        "output_tokens": 150,
        "cost_usd": 0.03,
        "model": "codex-cli",
    }
    ok = state_writer.write_handoff_to_db(
        str(clean_harness), content_with_usage, task_id="feat.foo", phase="report"
    )
    assert ok is True

    conn = get_connection(str(clean_harness))
    init_db(conn)
    rows = usage_dao.list_for_task(conn, "feat.foo")
    conn.close()
    assert len(rows) == 1
    assert rows[0].source == "handoff"
    assert rows[0].agent == "codex-cli"
    assert rows[0].cost_usd == 0.03

    content_without_usage = {
        "task": "feat.bar",
        "phase": "report",
        "status": "report_ready",
        "from": "codex-cli",
        "to": "owner",
        "date": "2026-04-27T12:00:00Z",
        "outcome": "done",
    }
    ok = state_writer.write_handoff_to_db(
        str(clean_harness), content_without_usage, task_id="feat.bar", phase="report"
    )
    assert ok is True

    conn = get_connection(str(clean_harness))
    init_db(conn)
    rows = usage_dao.list_for_task(conn, "feat.bar")
    conn.close()
    assert rows == []

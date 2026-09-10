from __future__ import annotations

import sqlite3

from superharness.engine import orchestrator_lease
from superharness.engine.db import init_db


T0 = "2026-01-01T00:00:00Z"
T1 = "2026-01-01T00:00:30Z"
T2 = "2026-01-01T00:01:01Z"


def test_lease_has_one_active_owner(db_conn):
    assert orchestrator_lease.acquire(db_conn, owner_id="a", now=T0)
    assert not orchestrator_lease.acquire(db_conn, owner_id="b", now=T1)
    assert orchestrator_lease.get(db_conn).owner_id == "a"


def test_owner_can_renew_and_non_owner_cannot_release(db_conn):
    assert orchestrator_lease.acquire(db_conn, owner_id="a", now=T0)
    assert orchestrator_lease.renew(db_conn, owner_id="a", now=T1)
    assert not orchestrator_lease.release(db_conn, owner_id="b")
    assert orchestrator_lease.get(db_conn).owner_id == "a"


def test_expired_lease_can_be_reclaimed(db_conn):
    assert orchestrator_lease.acquire(db_conn, owner_id="a", now=T0)
    assert orchestrator_lease.acquire(db_conn, owner_id="b", now=T2)
    assert orchestrator_lease.get(db_conn).owner_id == "b"


def test_owner_release_is_scoped(db_conn):
    assert orchestrator_lease.acquire(db_conn, owner_id="a", now=T0)
    assert orchestrator_lease.release(db_conn, owner_id="a")
    assert orchestrator_lease.get(db_conn) is None


def test_v40_to_current_migration_is_additive_and_idempotent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
    )
    conn.execute("PRAGMA user_version=40")
    init_db(conn)
    init_db(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 42
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name='orchestrator_lease'"
    ).fetchone()
    assert conn.execute(
        "SELECT name FROM sqlite_master WHERE name='agent_availability'"
    ).fetchone()
    conn.close()

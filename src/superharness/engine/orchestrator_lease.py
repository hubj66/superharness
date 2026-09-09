"""SQLite lease for the reliable-orchestrator watcher owner."""

from __future__ import annotations

import socket
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

LEASE_NAME = "reliable-orchestrator"
DEFAULT_LEASE_SECONDS = 60


@dataclass(frozen=True)
class LeaseRow:
    name: str
    owner_id: str
    host: str | None
    pid: int | None
    pid_starttime: str | None
    acquired_at: str
    heartbeat_at: str
    expires_at: str


def _expires_at(now: str, lease_seconds: int) -> str:
    current = datetime.fromisoformat(now)
    return (
        (current + timedelta(seconds=lease_seconds))
        .astimezone(UTC)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def _row(row: sqlite3.Row | None) -> LeaseRow | None:
    if row is None:
        return None
    return LeaseRow(
        name=row["name"],
        owner_id=row["owner_id"],
        host=row["host"],
        pid=row["pid"],
        pid_starttime=row["pid_starttime"],
        acquired_at=row["acquired_at"],
        heartbeat_at=row["heartbeat_at"],
        expires_at=row["expires_at"],
    )


@contextmanager
def _immediate_transaction(conn: sqlite3.Connection):
    """Serialize lease acquisition while remaining composable with callers."""
    if conn.in_transaction:
        yield
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def get(conn: sqlite3.Connection, name: str = LEASE_NAME) -> LeaseRow | None:
    return _row(
        conn.execute(
            "SELECT * FROM orchestrator_lease WHERE name=?", (name,)
        ).fetchone()
    )


def acquire(
    conn: sqlite3.Connection,
    *,
    owner_id: str,
    now: str,
    host: str | None = None,
    pid: int | None = None,
    pid_starttime: str | None = None,
    name: str = LEASE_NAME,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Acquire, renew, or reclaim one lease in the caller's transaction."""
    if not owner_id:
        raise ValueError("owner_id is required")
    expires = _expires_at(now, lease_seconds)
    with _immediate_transaction(conn):
        current = get(conn, name)
        if current is None:
            conn.execute(
                """
                INSERT INTO orchestrator_lease
                    (name, owner_id, host, pid, pid_starttime, acquired_at,
                     heartbeat_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (name, owner_id, host, pid, pid_starttime, now, now, expires),
            )
            return True
        if current.owner_id != owner_id and current.expires_at > now:
            return False
        conn.execute(
            """
            UPDATE orchestrator_lease
               SET owner_id=?, host=?, pid=?, pid_starttime=?,
                   acquired_at=CASE WHEN owner_id=? THEN acquired_at ELSE ? END,
                   heartbeat_at=?, expires_at=?
             WHERE name=? AND (owner_id=? OR expires_at <= ?)
            """,
            (
                owner_id,
                host,
                pid,
                pid_starttime,
                owner_id,
                now,
                now,
                expires,
                name,
                owner_id,
                now,
            ),
        )
        return bool(
            conn.execute(
                "SELECT 1 FROM orchestrator_lease WHERE name=? AND owner_id=?",
                (name, owner_id),
            ).fetchone()
        )


def renew(
    conn: sqlite3.Connection,
    *,
    owner_id: str,
    now: str,
    name: str = LEASE_NAME,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> bool:
    """Renew only the caller's lease; never take ownership while renewing."""
    with _immediate_transaction(conn):
        cursor = conn.execute(
            """
            UPDATE orchestrator_lease
               SET heartbeat_at=?, expires_at=?
             WHERE name=? AND owner_id=?
            """,
            (now, _expires_at(now, lease_seconds), name, owner_id),
        )
        return cursor.rowcount > 0


def release(conn: sqlite3.Connection, *, owner_id: str, name: str = LEASE_NAME) -> bool:
    """Release only the caller's lease."""
    with _immediate_transaction(conn):
        cursor = conn.execute(
            "DELETE FROM orchestrator_lease WHERE name=? AND owner_id=?",
            (name, owner_id),
        )
        return cursor.rowcount > 0


def current_process_identity() -> tuple[str, int, str | None, str]:
    """Return host, pid, Linux start time, and a stable owner id."""
    host = socket.gethostname()
    pid = __import__("os").getpid()
    starttime: str | None = None
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
            fields = stat_file.read().split()
        if len(fields) > 21:
            starttime = fields[21]
    except OSError:
        pass
    owner_id = f"{host}:{pid}:{starttime or 'unknown'}"
    return host, pid, starttime, owner_id

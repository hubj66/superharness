from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from superharness.engine.state_errors import StateError

AVAILABILITY_STATES = frozenset(
    {"available", "temporarily_blocked", "auth_blocked", "unknown"}
)
TEMPORARY_BLOCK_CATEGORIES = frozenset({"quota", "session_limit"})
AUTH_BLOCK_CATEGORIES = frozenset({"auth"})
DEFAULT_BLOCK_MINUTES = 60
AUTH_RETRY_COOLDOWN_MINUTES = 60


@dataclass(frozen=True)
class AgentAvailability:
    agent: str
    state: str
    reason: str | None
    blocked_until: str | None
    retry_after_at: str | None
    last_success_at: str | None
    last_failure_at: str | None
    last_failure_category: str | None
    source_run_id: str | None
    updated_at: str


def get(conn: sqlite3.Connection, agent: str) -> AgentAvailability | None:
    row = conn.execute(
        "SELECT * FROM agent_availability WHERE agent = ?", (agent,)
    ).fetchone()
    return _row_to_availability(row) if row else None


def list_all(conn: sqlite3.Connection) -> list[AgentAvailability]:
    rows = conn.execute("SELECT * FROM agent_availability ORDER BY agent").fetchall()
    return [_row_to_availability(row) for row in rows]


def is_selectable(conn: sqlite3.Connection, agent: str, *, now: str) -> bool:
    record = get(conn, agent)
    if record is None:
        return True
    if record.state in {"available", "unknown"}:
        return True
    if record.state == "auth_blocked":
        return _eligible_after(record.retry_after_at, now) and _eligible_after(
            record.blocked_until, now
        )
    if record.state == "temporarily_blocked":
        return _eligible_after(record.retry_after_at, now) and _eligible_after(
            record.blocked_until, now
        )
    return False


def mark_success(
    conn: sqlite3.Connection,
    agent: str,
    *,
    now: str,
    source_run_id: str | None = None,
) -> AgentAvailability:
    _validate_agent(agent)
    conn.execute(
        """
        INSERT INTO agent_availability (
            agent, state, reason, blocked_until, retry_after_at,
            last_success_at, last_failure_at, last_failure_category,
            source_run_id, updated_at
        ) VALUES (?, 'available', NULL, NULL, NULL, ?, NULL, NULL, ?, ?)
        ON CONFLICT(agent) DO UPDATE SET
            state='available',
            reason=NULL,
            blocked_until=NULL,
            retry_after_at=NULL,
            last_success_at=excluded.last_success_at,
            source_run_id=excluded.source_run_id,
            updated_at=excluded.updated_at
        """,
        (agent, now, source_run_id, now),
    )
    record = get(conn, agent)
    if record is None:
        raise StateError(f"Failed to record availability for agent '{agent}'")
    return record


def mark_failure(
    conn: sqlite3.Connection,
    agent: str,
    *,
    category: str | None,
    detail: str | None,
    now: str,
    source_run_id: str | None = None,
    default_block_minutes: int = DEFAULT_BLOCK_MINUTES,
) -> AgentAvailability:
    _validate_agent(agent)
    category = category or "unknown"
    state = "unknown"
    blocked_until = None
    retry_after_at = None
    reason = detail or category
    if category in TEMPORARY_BLOCK_CATEGORIES:
        state = "temporarily_blocked"
        retry_after_at = parse_retry_after_at(detail or "", now=now)
        blocked_until = retry_after_at or _add_minutes(now, default_block_minutes)
    elif category in AUTH_BLOCK_CATEGORIES:
        state = "auth_blocked"
        retry_after_at = parse_retry_after_at(detail or "", now=now)
        blocked_until = retry_after_at or _add_minutes(now, AUTH_RETRY_COOLDOWN_MINUTES)

    conn.execute(
        """
        INSERT INTO agent_availability (
            agent, state, reason, blocked_until, retry_after_at,
            last_success_at, last_failure_at, last_failure_category,
            source_run_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
        ON CONFLICT(agent) DO UPDATE SET
            state=excluded.state,
            reason=excluded.reason,
            blocked_until=excluded.blocked_until,
            retry_after_at=excluded.retry_after_at,
            last_failure_at=excluded.last_failure_at,
            last_failure_category=excluded.last_failure_category,
            source_run_id=excluded.source_run_id,
            updated_at=excluded.updated_at
        """,
        (
            agent,
            state,
            reason,
            blocked_until,
            retry_after_at,
            now,
            category,
            source_run_id,
            now,
        ),
    )
    record = get(conn, agent)
    if record is None:
        raise StateError(f"Failed to record availability for agent '{agent}'")
    return record


def parse_retry_after_at(detail: str, *, now: str) -> str | None:
    text = detail or ""
    iso = re.search(
        r"(20\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ|20\d\d-\d\d-\d\d \d\d:\d\d:\d\d)",
        text,
    )
    if iso:
        return iso.group(1).replace(" ", "T").removesuffix("+00:00") + (
            "" if iso.group(1).endswith("Z") else "Z"
        )
    rel = re.search(
        r"(?:retry|reset|available|try again)[^\d]{0,20}(\d{1,4})\s*(second|seconds|minute|minutes|hour|hours)",
        text,
        flags=re.IGNORECASE,
    )
    if not rel:
        return None
    amount = int(rel.group(1))
    unit = rel.group(2).lower()
    if unit.startswith("second"):
        return _add_seconds(now, amount)
    if unit.startswith("minute"):
        return _add_minutes(now, amount)
    return _add_minutes(now, amount * 60)


def _eligible_after(value: str | None, now: str) -> bool:
    if not value:
        return True
    return value <= now


def _add_seconds(now: str, seconds: int) -> str:
    return _format(_parse(now) + timedelta(seconds=seconds))


def _add_minutes(now: str, minutes: int) -> str:
    return _format(_parse(now) + timedelta(minutes=minutes))


def _parse(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(UTC)


def _format(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _validate_agent(agent: str) -> None:
    if not agent:
        raise StateError("Agent availability requires an agent")


def _row_to_availability(row: sqlite3.Row) -> AgentAvailability:
    state = row["state"]
    if state not in AVAILABILITY_STATES:
        raise StateError(f"Invalid availability state '{state}'")
    return AgentAvailability(
        agent=row["agent"],
        state=state,
        reason=row["reason"],
        blocked_until=row["blocked_until"],
        retry_after_at=row["retry_after_at"],
        last_success_at=row["last_success_at"],
        last_failure_at=row["last_failure_at"],
        last_failure_category=row["last_failure_category"],
        source_run_id=row["source_run_id"],
        updated_at=row["updated_at"],
    )

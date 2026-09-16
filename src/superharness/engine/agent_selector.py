from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from superharness.engine import agent_availability

CLAUDE_AGENT = "claude-code"
CODEX_AGENT = "codex-cli"
DEFAULT_CODEX_MODEL = "gpt-5.5"
DEFAULT_CLAUDE_MODEL = "claude-sonnet-4-6"


@dataclass(frozen=True)
class AgentAssignment:
    agent: str
    model: str | None


class AgentSelector:
    """Availability-aware role assignment for reliable orchestrator runs."""

    def __init__(self, profile: dict[str, Any] | None = None) -> None:
        self.profile = profile or {}

    def select_mutator(
        self,
        conn: sqlite3.Connection,
        *,
        now: str,
        preferred_agent: str | None = None,
        exclude_agents: set[str] | None = None,
    ) -> AgentAssignment | None:
        order = self._mutator_order(preferred_agent=preferred_agent)
        return self._first_available(
            conn, order, now=now, exclude_agents=exclude_agents
        )

    def select_reviewer(
        self,
        conn: sqlite3.Connection,
        *,
        now: str,
        source_agent: str | None,
        preferred_agent: str | None = None,
    ) -> AgentAssignment | None:
        exclude = {source_agent} if source_agent else set()
        order = self._review_order(preferred_agent=preferred_agent)
        return self._first_available(
            conn, order, now=now, exclude_agents=exclude, role="review"
        )

    def assignment_for_agent(self, agent: str) -> AgentAssignment:
        return AgentAssignment(
            agent=agent, model=self._model_for(agent, role="mutator")
        )

    def _first_available(
        self,
        conn: sqlite3.Connection,
        order: list[str],
        *,
        now: str,
        exclude_agents: set[str] | None = None,
        role: str = "mutator",
    ) -> AgentAssignment | None:
        excluded = {agent for agent in (exclude_agents or set()) if agent}
        for agent in order:
            if agent in excluded:
                continue
            if agent_availability.is_selectable(conn, agent, now=now):
                return AgentAssignment(
                    agent=agent, model=self._model_for(agent, role=role)
                )
        return None

    def _mutator_order(self, *, preferred_agent: str | None = None) -> list[str]:
        order = self._profile_order("reliable_mutating_agents") or [
            CLAUDE_AGENT,
            CODEX_AGENT,
        ]
        return _prioritize(order, preferred_agent)

    def _review_order(self, *, preferred_agent: str | None = None) -> list[str]:
        order = self._profile_order("reliable_review_agents") or [
            CODEX_AGENT,
            CLAUDE_AGENT,
        ]
        return _prioritize(order, preferred_agent)

    def _profile_order(self, key: str) -> list[str]:
        raw = self.profile.get(key)
        if isinstance(raw, str):
            values = [part.strip() for part in raw.split(",")]
        elif isinstance(raw, list):
            values = [str(part).strip() for part in raw]
        else:
            return []
        return [value for value in values if value]

    def _model_for(self, agent: str, *, role: str) -> str | None:
        if agent == CODEX_AGENT:
            if role == "review":
                return str(
                    self.profile.get("codex_review_model")
                    or self.profile.get("review_model")
                    or DEFAULT_CODEX_MODEL
                )
            return str(
                self.profile.get("codex_implementation_model")
                or self.profile.get("codex_model")
                or DEFAULT_CODEX_MODEL
            )
        if agent == CLAUDE_AGENT:
            return str(
                self.profile.get("claude_model")
                or self.profile.get("claude_code_model")
                or DEFAULT_CLAUDE_MODEL
            )
        return None


def reviewer_assignment(
    agent: str, profile: dict[str, Any] | None = None
) -> AgentAssignment:
    selector = AgentSelector(profile)
    assignment = selector.assignment_for_agent(agent)
    if agent == CODEX_AGENT:
        return AgentAssignment(
            agent=assignment.agent,
            model=str(
                (profile or {}).get("codex_review_model")
                or (profile or {}).get("review_model")
                or DEFAULT_CODEX_MODEL
            ),
        )
    return assignment


def _prioritize(order: list[str], preferred: str | None) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    candidates = [preferred, *order] if preferred else order
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            values.append(candidate)
    return values

"""Pydantic v2 models for SuperHarness protocol YAML types.

Provides runtime schema validation for the 5 protocol YAML types:
Contract, Handoff, Heartbeat, Profile, and Inbox.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator
from superharness.engine.taxonomy import VALID_EFFORTS


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TaskStatus(str, Enum):
    todo = "todo"
    plan_proposed = "plan_proposed"
    plan_approved = "plan_approved"
    in_progress = "in_progress"
    report_ready = "report_ready"
    # Review lifecycle — treated as canonical by engine/next_action.py.
    # Kept in sync so validators don't reject legitimate in-flight states.
    pending_user_approval = "pending_user_approval"
    review_requested = "review_requested"
    review_passed = "review_passed"
    review_failed = "review_failed"
    pr_open = "pr_open"
    done = "done"
    failed = "failed"
    blocked = "blocked"
    stopped = "stopped"
    # Phase 2: agent liveness signals
    waiting_input = "waiting_input"  # agent paused, needs human response
    paused = "paused"  # agent suspended (e.g. budget gate)
    # Operator-initiated soft-delete: done tasks moved out of the active view
    # while remaining in contract.yaml for history. Renderers hide archived by
    # default; consumers (e.g. Morpheme) can still surface them behind a toggle.
    archived = "archived"


class InboxStatus(str, Enum):
    pending = "pending"
    launched = "launched"
    running = "running"
    done = "done"
    failed = "failed"
    stale = "stale"
    paused = "paused"


class ModelTier(str, Enum):
    mini = "mini"
    standard = "standard"
    max = "max"


class SubtaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


class Subtask(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    title: str
    model_tier: Optional[ModelTier] = None
    owner: Optional[str] = None
    estimated_tokens: Optional[int] = None
    estimated_cost_usd: Optional[float] = None
    status: SubtaskStatus = SubtaskStatus.pending
    actual_tokens: Optional[int] = None
    actual_cost_usd: Optional[float] = None
    model_used: Optional[str] = None


class ContractTask(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    title: Optional[str] = None
    owner: Optional[str] = None
    status: TaskStatus
    project_path: Optional[str] = None
    workflow: Optional[str] = None
    development_method: Optional[str] = None
    acceptance_criteria: Optional[list[str]] = None
    test_types: Optional[list[str]] = None
    plan: Optional[dict] = Field(None, alias="tdd")
    blocked_by: Optional[Union[str, list[str]]] = None
    dependency: Optional[str] = None
    summary: Optional[str] = None
    verified: Optional[bool] = None
    verified_at: Optional[str] = None
    verified_by: Optional[str] = None
    deadline_minutes: Optional[int] = None
    review_requested_at: Optional[str] = None
    subtasks: list[Subtask] = []
    estimated_cost_usd: Optional[float] = None
    budget_usd: Optional[float] = None

    model: Optional[str] = None

    # Phase 1: new task fields
    effort: Optional[str] = "medium"
    out_of_scope: Optional[list[str]] = None
    definition_of_done: Optional[list[str]] = None
    context: Optional[str] = None
    timeout_minutes: Optional[int] = None
    progress_timeout_minutes: Optional[int] = (
        10  # reserved: agent liveness monitor (not yet wired)
    )
    # Phase 3: 1M context opt-in; auto-promoted when effort=max AND tokens>200K
    context_1m: Optional[bool] = None
    # Ship step: agent runs /ship commit before report_ready; watcher validates PR URL
    ship_on_complete: bool = False

    # Linked GitHub/GitLab issue URL — one-way snapshot pointer, never
    # written back to by shux (see shux task link / --from-issue).
    issue_url: Optional[str] = None


class Contract(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: Optional[str] = None
    created: Optional[Union[str, date, datetime]] = None
    created_by: Optional[str] = None
    status: Optional[Literal["draft", "active", "closed", "archived"]] = None
    goal: Optional[str] = None
    default_definition_of_done: Optional[list[str]] = None
    tasks: list[ContractTask] = []
    decisions: list[Any] = []
    failures: list[Any] = []


# ---------------------------------------------------------------------------
# Handoff
# ---------------------------------------------------------------------------


class Handoff(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    contract_id: Optional[str] = None
    task: str
    from_: str = Field(alias="from")
    to: str
    status: str
    summary: Optional[str] = None
    scope: list[str] = []
    commands: list[str] = []
    acceptance: list[str] = []
    risks: list[str] = []
    artifacts: list[str] = []


# ---------------------------------------------------------------------------
# Agent Pulse (Phase 2 — agent liveness signaling)
# ---------------------------------------------------------------------------


class AgentPulse(BaseModel):
    """Written by a running agent to .superharness/agent-pulse.yaml.

    Allows the operator and morpheme to detect stale/zombie tasks and
    surface "last seen X min ago" on in-progress nodes.
    """

    model_config = ConfigDict(extra="allow")

    task_id: str
    agent: str  # claude-code | codex-cli | …
    status: str = "running"  # running | waiting_input | paused
    last_seen: str  # ISO-8601 UTC timestamp
    message: Optional[str] = (
        None  # human-readable note (e.g. "waiting for approval on X")
    )
    pid: Optional[int] = None  # agent process PID if known


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class HeartbeatCheck(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    description: str
    interval_minutes: int
    enabled: bool = True


class Heartbeat(BaseModel):
    model_config = ConfigDict(extra="allow")

    checks: list[HeartbeatCheck] = []


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------


class Profile(BaseModel):
    model_config = ConfigDict(extra="allow")

    project_name: str
    created: Union[str, date]
    autonomy: Literal["autonomous", "supervised", "approval-gated"] = "approval-gated"
    primary_agent: Literal["claude-code", "codex-cli"]
    stack: str
    repo: Optional[Literal["github", "gitlab", "bitbucket", "other"]] = None
    ci: Optional[
        Literal["github-actions", "gitlab-ci", "jenkins", "circleci", "none"]
    ] = None
    team_size: Optional[Literal["solo", "small", "team"]] = None
    existing_harness: list[str] = []
    watcher: bool = False
    vault_path: Optional[str] = None
    default_model: Optional[Literal["mini", "standard", "max"]] = None
    default_effort: Optional[str] = None

    @field_validator("default_effort")
    @classmethod
    def _validate_default_effort(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in VALID_EFFORTS:
            raise ValueError(f"default_effort must be one of {VALID_EFFORTS}")
        return v


# ---------------------------------------------------------------------------
# Inbox
# ---------------------------------------------------------------------------


class InboxItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    to: str
    task: str
    project: str
    status: InboxStatus
    priority: int = 2
    retry_count: int = 0
    max_retries: int = 3
    created_at: str
    launched_at: Optional[str] = None
    done_at: Optional[str] = None
    failed_at: Optional[str] = None
    stale_at: Optional[str] = None
    failed_reason: Optional[str] = None
    stale_reason: Optional[str] = None
    pid: Optional[int] = None
    running_at: Optional[str] = None
    stopped_at: Optional[str] = None
    run_id: Optional[str] = None


class InboxDoc(BaseModel):
    """Wrapper for inbox.yaml which is a bare list at the top level."""

    model_config = ConfigDict(extra="allow")

    items: list[InboxItem]

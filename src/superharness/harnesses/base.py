"""Harness protocol + Invocation value object.

A Harness turns a task into the exact subprocess argv/env/cwd superharness
will spawn to run that agent. See docs/PLAN-adopt-omnigent.md iterations 5-6.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from superharness.engine.model_discovery import DiscoveredModel


@dataclass(frozen=True)
class Invocation:
    """An immutable, ready-to-spawn subprocess description.

    argv is stored as a tuple (not list) so that both attribute
    reassignment (blocked by frozen=True) and in-place item mutation
    (blocked by tuple's immutability) raise.
    """

    argv: tuple[str, ...]
    env: dict[str, str]
    cwd: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "argv", tuple(self.argv))


@runtime_checkable
class Harness(Protocol):
    name: str

    def build_invocation(
        self, task: dict, project_dir: str, non_interactive: bool
    ) -> Invocation: ...

    def discover_models(self, auth_mode: str = "unknown") -> list[DiscoveredModel]:
        """Return models available on this host for the given auth mode.

        Iteration 1 of PLAN-dynamic-model-selection.md: the default is a
        no-op (``[]``).  Adapters that expose a native model-list command
        (opencode) or a probe (codex/claude/gemini) override this in
        iterations 2-3.
        """
        return []


def _base_env(overrides: dict[str, str] | None = None) -> dict[str, str]:
    """Shared env-assembly helper for adapters (iteration 6 refactor target).

    Returns an overlay dict (not a full environ snapshot) — callers apply it
    on top of the inherited process environment. Empty by default, matching
    every current adapter's legacy behavior of not building a bespoke env.
    """
    return dict(overrides or {})


def discover_via_probe(
    agent: str, auth_mode: str = "unknown", budget_seconds: float = 5.0
) -> list[DiscoveredModel]:
    """Probe-based discovery for agents without a native model-list command.

    Iteration 6 of PLAN-dynamic-model-selection.md: builds the accept chain
    from the agent's adapter manifest (auth-compat-aware) and runs one
    ProbeDiscovery pass over it.  Returns [] when the manifest can't be
    loaded or the chain is empty — never raises.
    """
    from superharness.engine.adapter_registry import (
        AdapterValidationError,
        load_manifest,
    )
    from superharness.engine.probe_discovery import ProbeDiscovery

    try:
        manifest = load_manifest(agent)
    except (AdapterValidationError, KeyError, TypeError, ValueError, OSError):
        return []
    chain = manifest.resolve_accept_chain("mini", auth_mode)
    if not chain:
        return []
    probe = ProbeDiscovery(
        agent=agent,
        accept_chain=chain,
        auth_mode=auth_mode,
        budget_seconds=budget_seconds,
    )
    return probe.run()


def build_generic_invocation(
    name: str,
    task: dict,
    project_dir: str,
    non_interactive: bool,
    *,
    prefix_model: bool = True,
) -> Invocation:
    """Shared argv assembly for adapters that wrap a bash launcher with
    --project/--prompt/--non-interactive/--yolo/--codex-bypass/--model/
    --effort flags. By default, provider/model prefixing is applied
    (codex-cli, gemini-cli, opencode).

    claude-code is deliberately NOT built via this helper — Claude CLI
    rejects the anthropic/ prefix, so ClaudeHarness never prefixes its model.
    Codex CLI with ChatGPT auth also rejects provider-prefixed model names,
    so CodexHarness opts out with prefix_model=False.
    """
    from pathlib import Path

    from superharness.engine.adapter_registry import resolve_launcher
    from superharness.utils.model_routing import apply_model_prefix

    scripts_dir = str(Path(__file__).parent.parent / "scripts")
    launcher = resolve_launcher(name, scripts_dir)

    prompt = str(task.get("prompt", ""))
    model = str(task.get("model") or "")
    if model and prefix_model:
        model = apply_model_prefix(model)
    effort = str(task.get("effort") or "")
    yolo = bool(task.get("yolo", False))
    codex_bypass = bool(task.get("codex_bypass", False))

    argv: list[str] = ["bash", launcher, "--project", project_dir, "--prompt", prompt]
    if non_interactive:
        argv.append("--non-interactive")
    if yolo:
        argv.append("--yolo")
    if codex_bypass:
        argv.append("--codex-bypass")
    if model:
        argv += ["--model", model]
    if effort:
        argv += ["--effort", effort]

    return Invocation(argv=tuple(argv), env=_base_env(), cwd=project_dir)

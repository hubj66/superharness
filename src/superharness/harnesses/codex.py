"""Codex CLI harness adapter. See docs/PLAN-adopt-omnigent.md iteration 6."""

from __future__ import annotations

from superharness.harnesses.base import (
    Invocation,
    build_generic_invocation,
    discover_via_probe,
)


class CodexHarness:
    name = "codex-cli"

    def discover_models(self, auth_mode: str = "unknown") -> list:
        """Iteration 6: probe-based discovery via the manifest accept chain."""
        return discover_via_probe(self.name, auth_mode)

    def build_invocation(
        self, task: dict, project_dir: str, non_interactive: bool
    ) -> Invocation:
        return build_generic_invocation(
            self.name,
            task,
            project_dir,
            non_interactive,
            prefix_model=False,
        )

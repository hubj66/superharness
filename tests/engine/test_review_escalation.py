"""Tests for review_escalation — RED tests for iter 7 of auto-mode-gap-plan.

Replaces the simple revert-after-timeout behavior from iter 2 with an
escalation chain: if reviewer A doesn't respond, advance to reviewer B,
then to operator. Today the lifecycle rule for review_requested just
reverts to report_ready, which loses the escalation context.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests.conftest import past_iso
from tests.helpers import seed_sqlite_from_yaml


def _write_contract(project: Path, tasks: list[dict]) -> None:
    (project / ".superharness" / "contract.yaml").write_text(
        yaml.dump({"tasks": tasks})
    )
    seed_sqlite_from_yaml(project)


def _read_contract(project: Path) -> dict:
    from superharness.engine import state_reader

    return {"tasks": state_reader.get_tasks(str(project))}


def test_review_with_chain_advances_to_next_reviewer_on_timeout(
    clean_harness: Path,
) -> None:
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(
        clean_harness,
        [
            {
                "id": "feat.foo",
                "owner": "claude-code",
                "status": "review_requested",
                "review_requested_at": past_iso(121),
                "review_chain": ["codex-cli", "gemini-cli"],
                "review_chain_index": 0,
                "review_target": "codex-cli",
            }
        ],
    )
    n = escalate_stale_reviews(str(clean_harness))
    assert n == 1
    doc = _read_contract(clean_harness)
    t = doc["tasks"][0]
    assert t["status"] == "review_requested"  # still in review
    assert t["review_chain_index"] == 1
    assert t["review_target"] == "gemini-cli"


def test_review_with_chain_exhausted_escalates_to_operator(clean_harness: Path) -> None:
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(
        clean_harness,
        [
            {
                "id": "feat.foo",
                "owner": "claude-code",
                "status": "review_requested",
                "review_requested_at": past_iso(121),
                "review_chain": ["codex-cli", "gemini-cli"],
                "review_chain_index": 1,
                "review_target": "gemini-cli",
            }
        ],
    )
    n = escalate_stale_reviews(str(clean_harness))
    assert n == 1
    doc = _read_contract(clean_harness)
    t = doc["tasks"][0]
    assert t["status"] == "review_requested"
    assert t.get("escalated_to") == "operator"


def test_review_within_timeout_is_unchanged(clean_harness: Path) -> None:
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(
        clean_harness,
        [
            {
                "id": "feat.foo",
                "owner": "claude-code",
                "status": "review_requested",
                "review_requested_at": past_iso(60),
                "review_chain": ["codex-cli", "gemini-cli"],
                "review_chain_index": 0,
                "review_target": "codex-cli",
            }
        ],
    )
    n = escalate_stale_reviews(str(clean_harness))
    assert n == 0
    doc = _read_contract(clean_harness)
    t = doc["tasks"][0]
    assert t["review_chain_index"] == 0


def test_review_without_chain_falls_back_to_operator(clean_harness: Path) -> None:
    """No review_chain field means immediate operator escalation on timeout."""
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(
        clean_harness,
        [
            {
                "id": "feat.foo",
                "owner": "claude-code",
                "status": "review_requested",
                "review_requested_at": past_iso(121),
                # no review_chain
            }
        ],
    )
    n = escalate_stale_reviews(str(clean_harness))
    assert n == 1
    doc = _read_contract(clean_harness)
    assert doc["tasks"][0].get("escalated_to") == "operator"


@pytest.mark.regression
def test_review_escalation_dual_mode_writes_contract_yaml(
    clean_harness: Path, monkeypatch
) -> None:
    """STATE_BACKEND=dual: escalation must mirror to contract.yaml, not just SQLite.

    Regression: the else-branch of escalate_stale_reviews() referenced
    `contract_file` and `doc` without ever defining them — a NameError,
    silently swallowed by a broad except, that lost the ledger write and
    the YAML mirror on every dual-mode escalation. `STATE_BACKEND=dual` is
    a documented emergency-rollback mode (docs/yaml-inventory.md), still
    reachable in production, not dead code. Found by the 2026-07-21
    portfolio-ready audit's ruff pass (F821 undefined-name).
    """
    monkeypatch.setenv("STATE_BACKEND", "dual")

    _write_contract(
        clean_harness,
        [
            {
                "id": "feat.foo",
                "owner": "claude-code",
                "status": "review_requested",
                "review_requested_at": past_iso(121),
                "review_chain": ["codex-cli", "gemini-cli"],
                "review_chain_index": 0,
                "review_target": "codex-cli",
            }
        ],
    )

    from superharness.engine.review_escalation import escalate_stale_reviews

    n = escalate_stale_reviews(str(clean_harness))
    assert n == 1  # must not silently no-op via the swallowed NameError

    contract_file = clean_harness / ".superharness" / "contract.yaml"
    on_disk = yaml.safe_load(contract_file.read_text())
    t = on_disk["tasks"][0]
    assert t["review_chain_index"] == 1
    assert t["review_target"] == "gemini-cli"


@pytest.mark.regression
def test_review_escalation_skips_reliable_tasks_but_handles_legacy_tasks(clean_harness: Path) -> None:
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(clean_harness, [
        {
            "id": "reliable.task",
            "workflow": "reliable-orchestrator",
            "owner": "claude-code",
            "status": "review_requested",
            "review_requested_at": past_iso(121),
            "review_chain": ["codex-cli"],
            "review_chain_index": 0,
            "review_target": "codex-cli",
        },
        {
            "id": "legacy.task",
            "owner": "claude-code",
            "status": "review_requested",
            "review_requested_at": past_iso(121),
            "review_chain": ["codex-cli"],
            "review_chain_index": 0,
            "review_target": "codex-cli",
        },
    ])

    assert escalate_stale_reviews(str(clean_harness)) == 1
    tasks = {task["id"]: task for task in _read_contract(clean_harness)["tasks"]}
    reliable = tasks["reliable.task"]
    assert "escalated_to" not in reliable
    assert "escalated_at" not in reliable
    assert reliable["review_chain_index"] == 0
    assert tasks["legacy.task"]["escalated_to"] == "operator"


@pytest.mark.regression
def test_exhausted_reliable_review_has_no_legacy_action(clean_harness: Path) -> None:
    from superharness.engine.review_escalation import escalate_stale_reviews

    _write_contract(clean_harness, [{
        "id": "reliable.exhausted",
        "workflow": "reliable-orchestrator",
        "owner": "claude-code",
        "status": "review_requested",
        "review_requested_at": past_iso(121),
    }])

    assert escalate_stale_reviews(str(clean_harness)) == 0
    task = _read_contract(clean_harness)["tasks"][0]
    assert task["status"] == "review_requested"
    assert "escalated_to" not in task
    assert "escalated_at" not in task

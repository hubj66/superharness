"""Failure classifier — turn a (launcher_rc, error_text, log_tail) into a category.

Replaces the silent `failed_reason: "launcher exited with code 1"` pattern with
an actionable classification that drives:
  - auto_retry decisions (skip permanent blocks and quota)
  - dashboard error surface (group by class, show explain)
  - failure_patterns recording (already exists, this layer is structural)

Categories:
  permanent_block  - retrying produces same failure (bash crash, missing task,
                     syntax error, bad model name). Surface to operator.
  auth_mismatch    - agent model not authorized on current auth account (e.g.
                     codex ChatGPT account switched). Retryable after cache reset.
  transient        - timeout, network blip. Retry with backoff.
  quota            - rate limit, budget exhausted. Surface to operator + pause.
  agent_crash      - agent process died (Python traceback, segfault). Retry once.
  no_op            - agent ran but produced no artifact. Surface (likely prompt bug).
  unknown          - anything we cannot classify. Default: retry once.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

Category = Literal[
    "permanent_block",
    "auth_mismatch",
    "transient",
    "quota",
    "agent_crash",
    "no_op",
    "unknown",
]


@dataclass(frozen=True)
class FailureClassification:
    category: Category
    retryable: bool
    explain: str  # human-readable, surfaces in dashboard and failed_reason


ReliableFailureCategory = Literal[
    "quota",
    "session_limit",
    "agent_crash",
    "timeout",
    "hang",
    "auth",
    "network",
    "ship_failure",
    "invalid_result",
    "lost_process",
    "unknown",
]


@dataclass(frozen=True)
class ReliableFailureClassification:
    """Durable semantic classification for a reliable-orchestrator Run."""

    category: ReliableFailureCategory
    explain: str


# Order matters: more specific patterns first.
_PATTERNS: list[tuple[str, Category, bool, str]] = [
    # permanent_block — config / environment errors
    (
        r"unbound variable",
        "permanent_block",
        False,
        "bash unbound variable (likely empty array with set -u)",
    ),
    (
        r"command not found|No such file or directory",
        "permanent_block",
        False,
        "command or file not found",
    ),
    (
        r"task not found in contract|task does not exist",
        "permanent_block",
        False,
        "task not found in contract",
    ),
    (
        r"syntax error near unexpected token|line \d+: syntax error",
        "permanent_block",
        False,
        "shell syntax error",
    ),
    (
        r"permission denied",
        "permanent_block",
        False,
        "permission denied (chmod or owner issue)",
    ),
    # permanent_block — model name doesn't exist in the API at all (404)
    (
        r"ModelNotFoundError|Requested entity was not found",
        "permanent_block",
        False,
        "agent model not found — update models.yaml with a valid model ID",
    ),
    # auth_mismatch — model exists but is rejected by current auth account (codex/ChatGPT)
    # Retryable: cache is reset on detection so next dispatch re-evaluates auth.
    (
        r"model is not supported when using Codex with a ChatGPT account",
        "auth_mismatch",
        True,
        "codex model not supported on current ChatGPT account — auth account may have changed; superharness will reset auth cache and retry with override model",
    ),
    # auth_mismatch — API key invalidated, revoked, or account switched (any agent)
    # Gemini: API_KEY_INVALID / API key not valid; OpenCode: Invalid API key
    # Retryable: once credentials are refreshed the next dispatch should succeed.
    (
        r"API_KEY_INVALID|api.key.not.valid|invalid.api.key|incorrect.api.key"
        r"|authentication.failed|authentication_error"
        r"|caller.does.not.have.permission",
        "auth_mismatch",
        True,
        "API key invalid or account switched — update credentials and retry",
    ),
    # quota — all providers (rate limit, billing, budget)
    (
        r"rate.?limit|quota.?exceeded|budget.?exhausted|429.Too.Many"
        r"|insufficient.quota|rate_limit_exceeded",
        "quota",
        False,
        "agent quota or rate limit exceeded",
    ),
    # quota — Gemini / Google API usage limits
    (
        r"RESOURCE_EXHAUSTED|usage.?limit|you.ve reached your.*(usage|free|daily|monthly)|quota has been exceeded|free tier.*limit",
        "quota",
        False,
        "Gemini usage limit reached — resets on a daily/monthly schedule",
    ),
    # agent_crash
    (
        r"Traceback \(most recent call last\)|Segmentation fault|panic:",
        "agent_crash",
        True,
        "agent crashed (Python traceback or panic)",
    ),
]


def classify(
    *, launcher_rc: int, error_text: str = "", log_tail: str = ""
) -> FailureClassification:
    """Classify a dispatch failure into a structured category.

    Args:
      launcher_rc: exit code of the launcher subprocess.
      error_text:  failed_reason string from upstream, often empty.
      log_tail:    last N lines of the launcher log file. May be empty.

    Returns:
      FailureClassification with .category, .retryable, .explain set.
    """
    haystack = "\n".join(s for s in (error_text or "", log_tail or "") if s)

    # Special case: rc=124 from coreutils timeout(1)
    if launcher_rc == 124:
        return FailureClassification(
            category="transient",
            retryable=True,
            explain="launcher timed out",
        )

    # Special case: lifecycle gate permanent block
    if launcher_rc == 2:
        return FailureClassification(
            category="permanent_block",
            retryable=False,
            explain="lifecycle gate rejected (permanent block)",
        )

    # Pattern scan
    for pat, cat, retryable, explain in _PATTERNS:
        if re.search(pat, haystack, re.IGNORECASE):
            return FailureClassification(
                category=cat, retryable=retryable, explain=explain
            )

    # rc=0 with nothing in logs means agent ran but did nothing useful
    if launcher_rc == 0 and not haystack.strip():
        return FailureClassification(
            category="no_op",
            retryable=False,
            explain="agent ran but produced no artifact",
        )

    # Fallback
    return FailureClassification(
        category="unknown",
        retryable=True,
        explain=f"unclassified failure (exit code {launcher_rc})",
    )


_RELIABLE_SESSION_LIMIT = (
    r"session\s+limit|you(?:'|ve)\s+hit\s+your\s+session|session\s+has\s+been\s+limited"
)
_RELIABLE_QUOTA = (
    r"quota|rate[ -]?limit|too many requests|usage limit|capacity exhausted|"
    r"resource_exhausted|billing limit|credits? exhausted"
)
_RELIABLE_AUTH = (
    r"authentication|unauthori[sz]ed|invalid api key|api key not valid|"
    r"permission denied|login required|not logged in|forbidden"
)
_RELIABLE_NETWORK = (
    r"network|connection (?:reset|refused|timed out)|timed out connecting|"
    r"temporary failure in name resolution|dns|could not resolve host|"
    r"tls handshake|service unavailable|502|503|504"
)


def classify_reliable(
    *,
    launcher_rc: int | None = None,
    error_text: str = "",
    log_tail: str = "",
    timed_out: bool = False,
    hung: bool = False,
    invalid_result: bool = False,
    lost_process: bool = False,
    ship_failure: bool = False,
) -> ReliableFailureClassification:
    """Classify a reliable Run using explicit evidence and narrow signals."""
    haystack = "\n".join(value for value in (error_text, log_tail) if value)
    if ship_failure:
        return ReliableFailureClassification("ship_failure", "system shipping failed")
    if invalid_result:
        return ReliableFailureClassification(
            "invalid_result", "structured Run result was missing or invalid"
        )
    if lost_process:
        return ReliableFailureClassification(
            "lost_process", "Run process could not be verified after watcher recovery"
        )
    if timed_out:
        return ReliableFailureClassification("timeout", "configured Run timeout expired")
    if hung:
        return ReliableFailureClassification("hang", "Run heartbeat/liveness policy expired")
    if launcher_rc in {139, -11}:
        return ReliableFailureClassification(
            "agent_crash", "agent terminated with SIGSEGV (exit 139)"
        )
    if launcher_rc is not None and launcher_rc < 0:
        return ReliableFailureClassification(
            "agent_crash", f"agent terminated by signal {-launcher_rc}"
        )
    if re.search(r"segmentation fault|sigsegv|panic:|traceback", haystack, re.I):
        return ReliableFailureClassification("agent_crash", "agent crash evidence in output")
    if re.search(_RELIABLE_SESSION_LIMIT, haystack, re.I):
        return ReliableFailureClassification("session_limit", "agent session limit reached")
    if re.search(_RELIABLE_QUOTA, haystack, re.I):
        return ReliableFailureClassification("quota", "agent quota or rate limit reached")
    if re.search(_RELIABLE_AUTH, haystack, re.I):
        return ReliableFailureClassification("auth", "agent authentication failed")
    if re.search(_RELIABLE_NETWORK, haystack, re.I):
        return ReliableFailureClassification("network", "network or service failure")
    return ReliableFailureClassification(
        "unknown", f"unclassified reliable failure (exit code {launcher_rc})"
    )

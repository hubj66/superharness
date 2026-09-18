"""Tests for the Codex CLI launcher modernization (PR follow-up to #108).

The Codex CLI deprecated ``--full-auto``. The modern replacement is
``--sandbox workspace-write``. These tests pin that contract so a future
Codex CLI bump can't silently regress to the deprecated flag, and pin
the diagnostic-persistence contract for failed launches.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from superharness.harnesses.codex import CodexHarness


# The launcher script is bash-only (POSIX ``set -u`` + arrays + ``exec``);
# the fake-codex shim in the diagnostic tests also relies on bash. Windows
# CI doesn't have a bash interpreter in the unit-test job, so skip the
# whole module rather than half-run it.
pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="requires bash"
)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
LAUNCHER = REPO_ROOT / "src" / "superharness" / "scripts" / "delegate-to-codex.sh"


def _fake_codex(tmp_path: Path, script_body: str = "#!/bin/bash\necho \"$@\"\n") -> Path:
    """Write a fake ``codex`` binary that records its argv.

    Returns the bin_dir so the caller can prepend it to PATH.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    codex = bin_dir / "codex"
    codex.write_text(script_body)
    codex.chmod(0o755)
    return bin_dir


def _invoke_launcher(
    tmp_path: Path,
    *args: str,
    env_extra: dict | None = None,
    script_body: str = '#!/bin/bash\necho "$@"\n',
) -> subprocess.CompletedProcess:
    """Run the Codex launcher against a fake ``codex`` binary."""
    bin_dir = _fake_codex(tmp_path, script_body)
    project = tmp_path / "proj"
    project.mkdir()
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:/usr/bin:/bin"
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        ["bash", str(LAUNCHER), "--project", str(project), "--prompt", "test", *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


@pytest.mark.regression
@pytest.mark.parametrize(
    ("verdict", "findings"),
    [("LGTM", []), ("REJECTED", ["Ruff violations remain"])],
)
def test_codex_launcher_captures_structured_review_final_message(
    tmp_path: Path, verdict: str, findings: list[str]
) -> None:
    """Reliable Codex reviews must not depend on a model-initiated file write."""
    result_dir = tmp_path / "result dir"
    result_dir.mkdir()
    result_path = result_dir / "run-review.json"
    payload = {
        "schema_version": 1,
        "run_id": "run-review",
        "task_id": "gh-253",
        "kind": "review",
        "agent": "codex-cli",
        "exit_code": 0,
        "completion_status": "completed",
        "review_verdict": verdict,
        "reviewed_sha": "sha-a",
        "findings": findings,
    }
    fake_codex = """#!/bin/bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  if [[ "$1" == "--output-last-message" ]]; then
    printf '%s' "$FAKE_CODEX_RESULT" > "$2"
    exit 0
  fi
  shift
done
exit 9
"""

    result = _invoke_launcher(
        tmp_path,
        "--non-interactive",
        "--model",
        "gpt-5.5",
        env_extra={
            "SUPERHARNESS_RUN_RESULT_PATH": str(result_path),
            "FAKE_CODEX_RESULT": json.dumps(payload),
        },
        script_body=fake_codex,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result_path.read_text(encoding="utf-8")) == payload


# ---------------------------------------------------------------------------
# Flag-replacement contract
# ---------------------------------------------------------------------------


def test_codex_launcher_uses_sandbox_workspace_write_for_non_interactive(
    tmp_path: Path,
) -> None:
    """Non-interactive Codex dispatch must use ``--sandbox workspace-write``.

    Regression pin for the deprecated ``--full-auto`` flag, which Codex CLI
    removed.  See HANDOFF 2026-08-07 next-session move #2.
    """
    result = _invoke_launcher(tmp_path, "--non-interactive")
    assert result.returncode == 0, result.stderr
    # Modern replacement
    assert "--sandbox" in result.stdout
    assert "workspace-write" in result.stdout
    assert "--output-last-message" not in result.stdout
    # Deprecated flag must NOT appear
    assert "--full-auto" not in result.stdout


def test_codex_launcher_omits_sandbox_in_interactive_mode(tmp_path: Path) -> None:
    """Interactive Codex dispatch (no ``--non-interactive``) has no automation flag.

    The legacy script applies ``--sandbox workspace-write`` only in the
    non-interactive branch.  Interactive mode is a plain ``codex -C DIR ...``.
    """
    result = _invoke_launcher(tmp_path)
    assert result.returncode == 0, result.stderr
    # No automation flag in interactive mode
    assert "--sandbox" not in result.stdout
    assert "--full-auto" not in result.stdout
    assert "--dangerously-bypass-approvals-and-sandbox" not in result.stdout


def test_codex_launcher_codex_bypass_still_uses_dangerously_bypass_flag(
    tmp_path: Path,
) -> None:
    """``--codex-bypass`` keeps the explicit dangerous-mode flag (unchanged)."""
    result = _invoke_launcher(tmp_path, "--non-interactive", "--codex-bypass")
    assert result.returncode == 0, result.stderr
    assert "--dangerously-bypass-approvals-and-sandbox" in result.stdout
    # Even with bypass, must NOT use the deprecated --full-auto
    assert "--full-auto" not in result.stdout


def test_codex_harness_argv_for_non_interactive_omits_full_auto() -> None:
    """Direct harness check: the constructed argv must not bake ``--full-auto``.

    The launcher shell script is what builds the final codex invocation; this
    test catches regressions where someone changes the harness to pre-bake
    flags into the argv before the shell script sees them.
    """
    invocation = CodexHarness().build_invocation(
        task={"prompt": "x", "model": "", "effort": "", "yolo": False, "codex_bypass": False},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    argv_str = " ".join(invocation.argv)
    assert "--full-auto" not in argv_str


# ---------------------------------------------------------------------------
# Diagnostic-persistence contract for failed launches
# ---------------------------------------------------------------------------


def test_launch_agent_persists_redacted_stderr_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing agent launch must persist a redacted stderr diagnostic.

    Regression pin for the HANDOFF 2026-08-07 finding: a real Codex run died
    after ~23 s and the only stored output was startup text plus the
    ``--full-auto`` deprecation warning — the root-cause error never reached
    the audit log.  ``launch_agent`` now captures stderr and writes a
    redacted excerpt to the audit channel on non-zero exit.
    """
    audit_log = tmp_path / "audit.log"
    monkeypatch.setenv("SUPERHARNESS_AUDIT_LOG_FILE", str(audit_log))
    monkeypatch.setenv("SUPERHARNESS_LOG_LEVEL", "DEBUG")

    # Drop any cached handlers from earlier tests so this test sees a fresh
    # audit file rather than the global default.
    for name in ("superharness", "superharness.audit"):
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)

    # Fake agent: emits a fake API key on stderr, exits non-zero.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "failing-agent"
    fake.write_text(
        "#!/bin/bash\n"
        "echo 'token=sk-abcdef1234567890abcdef1234567890abcdef' >&2\n"
        "echo 'real failure reason: workspace missing' >&2\n"
        "exit 7\n"
    )
    fake.chmod(0o755)

    from superharness.engine.platform_runtime import launch_agent

    rc = launch_agent([str(fake)], cwd=str(tmp_path))
    assert rc == 7

    # Allow the RotatingFileHandler to flush before reading.
    for handler in logging.getLogger("superharness.audit").handlers:
        handler.flush()

    content = audit_log.read_text()
    # The diagnostic was persisted with the exit code
    assert "exit=7" in content or "exit 7" in content
    # The real failure reason survives redaction (no secret in the message)
    assert "real failure reason" in content
    # The fake API key was redacted before the audit write
    assert "sk-abcdef1234567890abcdef1234567890abcdef" not in content
    assert "sk-***" in content


def test_launch_agent_does_not_log_on_success(tmp_path, monkeypatch) -> None:
    """A successful launch must NOT write a diagnostic line.

    Logging on every launch would drown the audit channel in noise; the
    diagnostic-persistence contract only fires on non-zero exit.
    """
    audit_log = tmp_path / "audit.log"
    monkeypatch.setenv("SUPERHARNESS_AUDIT_LOG_FILE", str(audit_log))
    monkeypatch.setenv("SUPERHARNESS_LOG_LEVEL", "DEBUG")
    for name in ("superharness", "superharness.audit"):
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ok = bin_dir / "ok-agent"
    ok.write_text("#!/bin/bash\nexit 0\n")
    ok.chmod(0o755)

    from superharness.engine.platform_runtime import launch_agent

    rc = launch_agent([str(ok)], cwd=str(tmp_path))
    assert rc == 0

    for handler in logging.getLogger("superharness.audit").handlers:
        handler.flush()

    # The audit file exists (the handler wrote nothing) but contains no
    # launch_agent diagnostic line.
    if audit_log.exists():
        content = audit_log.read_text()
        assert "launch_agent" not in content or "exit=0" not in content

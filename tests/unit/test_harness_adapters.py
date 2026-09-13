"""Tests for the codex/gemini/opencode harness adapters + full dispatch
switchover through the registry.

See docs/PLAN-adopt-omnigent.md iteration 6.

Golden values captured from the LIVE legacy code path (delegate.py::
_launch_agent with platform_runtime.launch_agent mocked to record argv/cwd
instead of exec'ing) before these adapters existed, per the plan's
"capture first, hardcode, then extract" instruction. See the iteration 5
test file (test_harness_registry.py) for the same pattern applied to
claude-code.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

import superharness
from superharness.engine.adapter_registry import resolve_launcher
from superharness.harnesses import KNOWN_HARNESSES, get_harness

_REQUIRES_POSIX_FIXTURE = pytest.mark.skipif(
    os.name != "posix",
    reason="Pi launcher assertion executes the POSIX delegate-to-pi.sh script",
)


def _scripts_dir() -> str:
    return str(Path(superharness.__file__).parent / "scripts")


def test_codex_invocation_parity():
    launcher = resolve_launcher("codex-cli", _scripts_dir())
    invocation = get_harness("codex-cli").build_invocation(
        task={"prompt": "do the thing", "model": "gpt-5.5", "effort": "high"},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert invocation.argv == (
        "bash",
        launcher,
        "--project",
        "/tmp/proj",
        "--prompt",
        "do the thing",
        "--non-interactive",
        "--model",
        "gpt-5.5",
        "--effort",
        "high",
    )
    assert invocation.cwd == "/tmp/proj"
    assert "openai/gpt-5.5" not in invocation.argv


def test_codex_invocation_keeps_yolo_bypass_and_non_interactive_flags():
    invocation = get_harness("codex-cli").build_invocation(
        task={
            "prompt": "do the thing",
            "model": "gpt-5.5",
            "yolo": True,
            "codex_bypass": True,
        },
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert "--non-interactive" in invocation.argv
    assert "--yolo" in invocation.argv
    assert "--codex-bypass" in invocation.argv
    assert invocation.argv[invocation.argv.index("--model") + 1] == "gpt-5.5"


def test_gemini_invocation_parity():
    launcher = resolve_launcher("gemini-cli", _scripts_dir())
    invocation = get_harness("gemini-cli").build_invocation(
        task={"prompt": "do the thing", "model": "gemini-3-pro"},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert invocation.argv == (
        "bash",
        launcher,
        "--project",
        "/tmp/proj",
        "--prompt",
        "do the thing",
        "--non-interactive",
        "--model",
        "google/gemini-3-pro",
    )
    assert invocation.cwd == "/tmp/proj"


def test_opencode_invocation_parity():
    launcher = resolve_launcher("opencode", _scripts_dir())
    invocation = get_harness("opencode").build_invocation(
        task={"prompt": "do the thing", "model": "claude-sonnet-4-6"},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert invocation.argv == (
        "bash",
        launcher,
        "--project",
        "/tmp/proj",
        "--prompt",
        "do the thing",
        "--non-interactive",
        "--model",
        "anthropic/claude-sonnet-4-6",
    )
    assert invocation.cwd == "/tmp/proj"


def test_opencode_still_prefixes_openai_model():
    invocation = get_harness("opencode").build_invocation(
        task={"prompt": "do the thing", "model": "gpt-5.5"},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert "--model" in invocation.argv
    assert invocation.argv[invocation.argv.index("--model") + 1] == "openai/gpt-5.5"


def test_pi_invocation_parity():
    launcher = resolve_launcher("pi", _scripts_dir())
    invocation = get_harness("pi").build_invocation(
        task={"prompt": "do the thing", "model": "deepseek-v4-flash", "effort": "high"},
        project_dir="/tmp/proj",
        non_interactive=True,
    )
    assert invocation.argv == (
        "bash",
        launcher,
        "--project",
        "/tmp/proj",
        "--prompt",
        "do the thing",
        "--non-interactive",
        "--model",
        "deepseek/deepseek-v4-flash",
        "--effort",
        "high",
    )
    assert invocation.argv.count("do the thing") == 1
    assert invocation.cwd == "/tmp/proj"


def test_pi_model_discovery_parses_list_output(monkeypatch: pytest.MonkeyPatch):
    from superharness.harnesses.pi import PiHarness

    class _FakeResult:
        returncode = 0
        stdout = ""
        stderr = (
            "provider model context max-out thinking images\n"
            "provider-b model-b 1M 384K yes no\n"
            "provider-a model-a 1M 384K yes no\n"
        )

    calls: list[tuple[tuple[str, ...], dict]] = []

    def _fake_run(argv, **kwargs):
        calls.append((tuple(argv), kwargs))
        return _FakeResult()

    monkeypatch.setattr(subprocess, "run", _fake_run)

    models = PiHarness().discover_models(auth_mode="apikey")

    assert [model.id for model in models] == [
        "provider-a/model-a",
        "provider-b/model-b",
    ]
    assert all(model.source == "native" for model in models)
    assert all(model.auth_mode == "apikey" for model in models)
    assert calls == [
        (
            (
                "pi",
                "--offline",
                "--no-extensions",
                "--no-skills",
                "--no-prompt-templates",
                "--list-models",
            ),
            {"capture_output": True, "text": True, "timeout": 10, "check": False},
        )
    ]


@pytest.mark.parametrize(
    "result_or_error",
    [
        FileNotFoundError("pi not found"),
        subprocess.TimeoutExpired(cmd="pi", timeout=10),
        type(
            "Nonzero",
            (),
            {
                "returncode": 1,
                "stdout": "provider model context max-out thinking images\nprovider-a model-a 1M 384K yes no\n",
                "stderr": "",
            },
        )(),
        type(
            "Junk", (), {"returncode": 0, "stdout": "not a model table", "stderr": ""}
        )(),
        type(
            "MissingHeader",
            (),
            {
                "returncode": 0,
                "stdout": "provider-a model-a 1M 384K yes no",
                "stderr": "",
            },
        )(),
        type(
            "ZeroRows",
            (),
            {
                "returncode": 0,
                "stdout": "provider model context max-out thinking images\n",
                "stderr": "",
            },
        )(),
        type(
            "SplitStreams",
            (),
            {
                "returncode": 0,
                "stdout": "provider model context max-out thinking images\n",
                "stderr": "provider-a model-a 1M 384K yes no\n",
            },
        )(),
    ],
)
def test_pi_model_discovery_never_raises(
    monkeypatch: pytest.MonkeyPatch, result_or_error: object
):
    from superharness.harnesses.pi import PiHarness

    def _fake_run(*args, **kwargs):
        if isinstance(result_or_error, BaseException):
            raise result_or_error
        return result_or_error

    monkeypatch.setattr(subprocess, "run", _fake_run)
    assert PiHarness().discover_models() == []


@pytest.mark.parametrize(
    "task",
    [
        {"prompt": "--help"},
        {"prompt": "do the thing", "model": "--help"},
        {"prompt": "do the thing", "effort": "--help"},
    ],
)
@_REQUIRES_POSIX_FIXTURE
def test_pi_launcher_rejects_help_like_task_values_without_invoking_pi(
    tmp_path: Path, task: dict[str, str]
):
    """Regression: task data must not be mistaken for a launcher help request."""
    fake_pi = tmp_path / "pi"
    sentinel = tmp_path / "pi-was-invoked"
    fake_pi.write_text('#!/bin/sh\n: > "$PI_SENTINEL"\nexit 99\n')
    fake_pi.chmod(0o755)
    invocation = get_harness("pi").build_invocation(
        task=task,
        project_dir=str(tmp_path),
        non_interactive=True,
    )

    result = subprocess.run(
        invocation.argv,
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "PI_SENTINEL": str(sentinel),
        },
    )

    assert result.returncode != 0
    assert "Usage: delegate-to-pi.sh" in result.stderr
    assert "error:" in result.stderr
    assert not sentinel.exists()


def test_all_known_harnesses_resolve():
    assert set(KNOWN_HARNESSES) == {
        "claude-code",
        "codex-cli",
        "gemini-cli",
        "opencode",
        "pi",
    }
    for name in KNOWN_HARNESSES:
        invocation = get_harness(name).build_invocation(
            task={"prompt": "do the thing"},
            project_dir="/tmp/proj",
            non_interactive=True,
        )
        assert len(invocation.argv) > 0


def test_prompt_injection_safety_all_adapters():
    dangerous = 'do the thing; rm -rf / && echo "pwned"'
    for name in KNOWN_HARNESSES:
        invocation = get_harness(name).build_invocation(
            task={"prompt": dangerous},
            project_dir="/tmp/proj",
            non_interactive=True,
        )
        assert dangerous in invocation.argv
        assert invocation.argv.count(dangerous) == 1


def test_unknown_owner_fails_dispatch_cleanly(monkeypatch):
    """Chaos: an unknown owner string fails cleanly via the registry's
    KeyError-with-known-list, not a stuck/hung dispatch."""
    from superharness.commands import delegate

    with pytest.raises(SystemExit):
        delegate._launch_agent(
            target="not-a-real-agent",
            prompt="do the thing",
            project_dir="/tmp/proj",
            non_interactive=True,
            codex_bypass=False,
            task_id="t1",
        )

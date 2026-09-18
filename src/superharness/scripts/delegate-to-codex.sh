#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
export PYTHONPATH="${SRC_ROOT}${PYTHONPATH:+:${PYTHONPATH}}"

# Ensure agent binaries are findable under launchd's stripped PATH.
# Inherited PATH goes first so a caller-provided override (tests, CI, custom
# installs) always wins; these are fallback dirs for when PATH is empty/minimal.
export PATH="${PATH:-}:/Applications/cmux.app/Contents/Resources/bin:${HOME}/.local/bin:${HOME}/.nvm/versions/node/v25.2.1/bin:${HOME}/.pyenv/shims:${HOME}/.pyenv/bin:/usr/local/bin:/usr/bin:/bin"

# Build Codex CLI command
MODEL_ARGS=()
PROMPT=""
PROJECT_DIR="."
NON_INTERACTIVE=0
BYPASS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --prompt)
      PROMPT="$2"
      shift 2
      ;;
    --model)
      MODEL_ARGS+=("--model" "$2")
      shift 2
      ;;
    --effort)
      # Map superharness effort to codex config override
      _eff="$2"
      if [[ "$_eff" == "max" ]]; then
        _eff="xhigh"
      fi
      MODEL_ARGS+=("-c" "model_reasoning_effort=\"$_eff\"")
      shift 2
      ;;
    --non-interactive)
      NON_INTERACTIVE=1
      shift
      ;;
    --codex-bypass)
      BYPASS=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

if [[ -z "$PROMPT" ]]; then
  echo "Error: No prompt provided to delegate-to-codex.sh" >&2
  exit 1
fi

if [[ $NON_INTERACTIVE -eq 1 ]]; then
  # Build execution command with automation flags
  CODEX_ARGS=("exec" "--skip-git-repo-check" "-C" "$PROJECT_DIR")
  # Bash 3.2 (Apple's /bin/bash) errors on `set -u` + empty array expansion;
  # guard so a no-model dispatch doesn't trip the nounset check.
  if [[ ${#MODEL_ARGS[@]} -gt 0 ]]; then
    CODEX_ARGS+=("${MODEL_ARGS[@]}")
  fi

  if [[ $BYPASS -eq 1 ]]; then
    CODEX_ARGS+=("--dangerously-bypass-approvals-and-sandbox")
  else
    CODEX_ARGS+=("--sandbox" "workspace-write")
  fi

  # Reliable Runs consume a structured artifact. Capture Codex's final message
  # directly instead of relying on the model to perform an extra file write.
  if [[ -n "${SUPERHARNESS_RUN_RESULT_PATH:-}" ]]; then
    CODEX_ARGS+=("--output-last-message" "$SUPERHARNESS_RUN_RESULT_PATH")
  fi

  exec codex "${CODEX_ARGS[@]}" "$PROMPT"
else
  # Regular interactive session
  if [[ ${#MODEL_ARGS[@]} -gt 0 ]]; then
    exec codex -C "$PROJECT_DIR" "${MODEL_ARGS[@]}" "$PROMPT"
  else
    exec codex -C "$PROJECT_DIR" "$PROMPT"
  fi
fi

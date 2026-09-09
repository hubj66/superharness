"""Managed worktree helpers for reliable-orchestrator tasks."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from superharness.engine.state_errors import StateError
from superharness.engine.worktree_ops import sanitize_task_id

DEFAULT_BASE_BRANCH = "main"
DEFAULT_REMOTE = "origin"
_SAFE_BRANCH_RE = re.compile(r"[^A-Za-z0-9._/-]+")


@dataclass(frozen=True)
class ManagedWorktree:
    path: str
    branch_name: str | None
    base_sha: str


def reliable_task_branch(task_id: str) -> str:
    """Return the stable branch used for a reliable-orchestrator task."""
    safe = sanitize_task_id(task_id).strip(".-/") or "task"
    safe = _SAFE_BRANCH_RE.sub("-", safe).replace("//", "/")
    return f"shux/reliable/{safe[:80]}"


def managed_worktree_root(project_dir: str) -> str:
    override = os.environ.get("SUPERHARNESS_WORKTREE_ROOT")
    if override:
        return os.path.realpath(override)
    project_real = os.path.realpath(project_dir).strip(os.sep).replace(os.sep, "-")
    root = Path(tempfile.gettempdir()) / "superharness-worktrees" / "reliable"
    return str(root / project_real)


def is_managed_worktree_path(project_dir: str, worktree_path: str) -> bool:
    path = os.path.realpath(worktree_path)
    roots = [
        os.path.realpath(managed_worktree_root(project_dir)),
        os.path.realpath(os.path.join(tempfile.gettempdir(), "superharness-worktrees")),
        os.path.realpath(os.path.join(project_dir, ".superharness", "worktrees")),
    ]
    return any(path == root or path.startswith(root + os.sep) for root in roots)


def create_managed_worktree(
    project_dir: str,
    task_id: str,
    *,
    base_branch: str = DEFAULT_BASE_BRANCH,
    remote: str = DEFAULT_REMOTE,
) -> ManagedWorktree:
    """Create or reuse a managed worktree from an explicit remote branch SHA."""
    branch = reliable_task_branch(task_id)
    base_sha = resolve_remote_branch_sha(project_dir, remote=remote, branch=base_branch)
    root = managed_worktree_root(project_dir)
    path = os.path.join(root, branch.replace("/", "-"))
    os.makedirs(root, exist_ok=True)

    if os.path.isdir(path):
        current_branch = current_branch_name(path)
        if current_branch != branch:
            raise StateError(
                f"Managed worktree {path!r} is on {current_branch!r}, not {branch!r}"
            )
        return ManagedWorktree(path=path, branch_name=branch, base_sha=base_sha)

    if not ref_exists(project_dir, f"refs/heads/{branch}"):
        _run_git(project_dir, "branch", branch, base_sha)
    result = _run_git(project_dir, "worktree", "add", path, branch, check=False)
    if result.returncode != 0:
        raise StateError(result.stderr.strip() or "git worktree add failed")
    _link_superharness_state(project_dir, path)
    return ManagedWorktree(path=path, branch_name=branch, base_sha=base_sha)


def create_review_worktree(
    project_dir: str,
    task_id: str,
    *,
    branch_name: str,
    review_target_sha: str,
    remote: str = DEFAULT_REMOTE,
) -> ManagedWorktree:
    """Create or reuse a detached review worktree pinned to one remote SHA."""
    remote_sha = resolve_remote_branch_sha(
        project_dir, remote=remote, branch=branch_name
    )
    if remote_sha != review_target_sha:
        raise StateError(
            f"Remote {remote}/{branch_name} is {remote_sha}, not {review_target_sha}"
        )
    root = managed_worktree_root(project_dir)
    path = os.path.join(
        root, f"review-{_worktree_name(task_id)}-{review_target_sha[:12]}"
    )
    os.makedirs(root, exist_ok=True)
    if os.path.isdir(path):
        head = rev_parse(path, "HEAD")
        if head != review_target_sha:
            raise StateError(
                f"Review worktree {path!r} is at {head}, not {review_target_sha}"
            )
        return ManagedWorktree(path=path, branch_name=None, base_sha=review_target_sha)
    result = _run_git(
        project_dir,
        "worktree",
        "add",
        "--detach",
        path,
        review_target_sha,
        check=False,
    )
    if result.returncode != 0:
        raise StateError(result.stderr.strip() or "git review worktree add failed")
    _link_superharness_state(project_dir, path)
    return ManagedWorktree(path=path, branch_name=None, base_sha=review_target_sha)


def create_repair_worktree(
    project_dir: str,
    task_id: str,
    *,
    branch_name: str,
    expected_head_sha: str,
    remote: str = DEFAULT_REMOTE,
) -> ManagedWorktree:
    """Create or reset the managed task branch worktree from the current PR head."""
    remote_sha = resolve_remote_branch_sha(
        project_dir, remote=remote, branch=branch_name
    )
    if remote_sha != expected_head_sha:
        raise StateError(
            f"Remote {remote}/{branch_name} is {remote_sha}, not {expected_head_sha}"
        )
    root = managed_worktree_root(project_dir)
    path = os.path.join(root, branch_name.replace("/", "-"))
    os.makedirs(root, exist_ok=True)

    if os.path.isdir(path):
        current_branch = current_branch_name(path)
        if current_branch != branch_name:
            raise StateError(
                f"Managed worktree {path!r} is on {current_branch!r}, not {branch_name!r}"
            )
        if not _is_clean(path):
            raise StateError(f"Repair worktree {path!r} has uncommitted changes")
        _run_git(path, "reset", "--hard", remote_sha)
        return ManagedWorktree(path=path, branch_name=branch_name, base_sha=remote_sha)

    if ref_exists(project_dir, f"refs/heads/{branch_name}"):
        _run_git(project_dir, "branch", "--force", branch_name, remote_sha)
    else:
        _run_git(project_dir, "branch", branch_name, remote_sha)
    result = _run_git(project_dir, "worktree", "add", path, branch_name, check=False)
    if result.returncode != 0:
        raise StateError(result.stderr.strip() or "git repair worktree add failed")
    _link_superharness_state(project_dir, path)
    return ManagedWorktree(path=path, branch_name=branch_name, base_sha=remote_sha)


def resolve_remote_branch_sha(project_dir: str, *, remote: str, branch: str) -> str:
    _run_git(project_dir, "fetch", remote, branch)
    return rev_parse(project_dir, f"refs/remotes/{remote}/{branch}^{{commit}}")


def rev_parse(project_dir: str, ref: str) -> str:
    result = _run_git(project_dir, "rev-parse", ref)
    return result.stdout.strip()


def current_branch_name(project_dir: str) -> str:
    result = _run_git(project_dir, "symbolic-ref", "--short", "HEAD")
    return result.stdout.strip()


def ref_exists(project_dir: str, ref: str) -> bool:
    result = _run_git(project_dir, "show-ref", "--verify", "--quiet", ref, check=False)
    return result.returncode == 0


def _worktree_name(task_id: str) -> str:
    safe = sanitize_task_id(task_id).strip(".-/") or "task"
    return _SAFE_BRANCH_RE.sub("-", safe).replace("/", "-")[:80]


def _is_clean(project_dir: str) -> bool:
    result = _run_git(
        project_dir,
        "status",
        "--porcelain=v1",
        "--untracked-files=normal",
        check=False,
    )
    if result.returncode != 0:
        return False
    entries = [entry for entry in result.stdout.splitlines() if entry.strip()]
    return not any(not _is_control_plane_entry(entry) for entry in entries)


def _is_control_plane_entry(entry: str) -> bool:
    path = entry[3:] if len(entry) > 3 else entry
    normalized = path.replace("\\", "/").lstrip("/")
    return normalized == ".superharness" or normalized.startswith(".superharness/")


def _link_superharness_state(project_dir: str, worktree_path: str) -> None:
    src = os.path.join(project_dir, ".superharness")
    dst = os.path.join(worktree_path, ".superharness")
    if os.path.isdir(src) and not os.path.lexists(dst):
        os.symlink(src, dst)


def _run_git(
    project_dir: str, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", "-C", project_dir, *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "git command failed"
        raise StateError(detail)
    return result

"""System-owned shipping for reliable-orchestrator tasks."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from superharness.engine import runs_dao, tasks_dao
from superharness.engine.reliable_orchestrator_gate import is_reliable_orchestrated_task
from superharness.engine.reliable_worktree import (
    DEFAULT_BASE_BRANCH,
    DEFAULT_REMOTE,
    current_branch_name,
    is_managed_worktree_path,
    rev_parse,
)
from superharness.engine.state_errors import StateError

SYSTEM_AGENT = "system"
SHIP_FAILURES = frozenset(
    {
        "no_changes",
        "invalid_worktree",
        "wrong_branch",
        "git_identity_missing",
        "commit_failed",
        "push_failed",
        "non_fast_forward",
        "github_unavailable",
        "pr_create_failed",
        "remote_head_mismatch",
        "invalid_remote",
        "unsafe_agent_commit",
    }
)


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class ShipOutcome:
    ok: bool
    failure_category: str | None = None
    failure_detail: str | None = None
    branch_name: str | None = None
    base_sha: str | None = None
    head_sha: str | None = None
    remote_head_sha: str | None = None
    pr_number: int | None = None
    pr_url: str | None = None
    worktree_path: str | None = None


Runner = Callable[[str, Sequence[str]], CommandResult]


class SystemShipper:
    """Commit, push, and open/update a PR for one successful mutating Run."""

    def __init__(
        self,
        project_dir: str,
        *,
        base_branch: str = DEFAULT_BASE_BRANCH,
        remote: str = DEFAULT_REMOTE,
        runner: Runner | None = None,
    ) -> None:
        self.project_dir = os.path.realpath(project_dir)
        self.base_branch = base_branch
        self.remote = remote
        self._runner = runner or _subprocess_runner

    def ship(
        self,
        *,
        ship_run: runs_dao.RunRow,
        source_run: runs_dao.RunRow,
        task: tasks_dao.TaskRow,
    ) -> ShipOutcome:
        failure = self._preflight(ship_run=ship_run, source_run=source_run, task=task)
        if failure is not None:
            return failure

        worktree = os.path.realpath(source_run.worktree_path or "")
        branch = source_run.branch_name or ""
        pre_ship_head = (
            source_run.base_sha or source_run.head_sha or rev_parse(worktree, "HEAD")
        )
        normalization = self._normalize_agent_commit(
            worktree, branch, expected_base=pre_ship_head, source_run=source_run
        )
        if normalization is not None:
            return normalization
        commit_sha = self._existing_commit_sha(worktree, source_run.id, pre_ship_head)
        if commit_sha is None:
            stage_result = self._stage_intended_diff(worktree)
            if not stage_result.ok:
                return stage_result
            if self._git_identity(worktree) is None:
                return self._fail(
                    "git_identity_missing", "git user.name/user.email missing"
                )
            commit_sha = self._ensure_commit(worktree, task, source_run, pre_ship_head)
            if commit_sha is None:
                return self._fail("commit_failed", "git commit failed")

        pushed = self._push(worktree, branch)
        if not pushed.ok:
            return pushed

        pr = self._create_or_reuse_pr(worktree, branch, task)
        if not pr.ok:
            return pr

        remote_sha = self._remote_branch_sha(worktree, branch)
        if remote_sha is None:
            return self._fail("github_unavailable", "could not resolve remote head")
        if remote_sha != commit_sha:
            return self._fail(
                "remote_head_mismatch",
                f"remote head {remote_sha} != local shipped commit {commit_sha}",
            )

        return ShipOutcome(
            ok=True,
            branch_name=branch,
            base_sha=pre_ship_head,
            head_sha=commit_sha,
            remote_head_sha=remote_sha,
            pr_number=pr.pr_number,
            pr_url=pr.pr_url,
            worktree_path=worktree,
        )

    def _normalize_agent_commit(
        self,
        worktree: str,
        branch: str,
        *,
        expected_base: str,
        source_run: runs_dao.RunRow,
    ) -> ShipOutcome | None:
        """Recover an unpushed agent commit into the shipper's staging area.

        Reliable mutators are instructed to leave edits uncommitted, but a
        client may ignore that contract. On a managed task branch, an
        unpushed descendant of the recorded base can be safely made into
        working-tree changes with a mixed reset; the system then creates the
        authoritative trailer-bearing commit. Anything whose ownership or
        remote ancestry is uncertain fails closed without touching the files.
        """
        current = rev_parse(worktree, "HEAD")
        if current == expected_base or self._commit_has_trailer(
            worktree, source_run.id
        ):
            return None
        if not branch or branch in {self.base_branch, "main", "master"}:
            return self._fail(
                "unsafe_agent_commit", "agent advanced an unmanaged/default branch"
            )

        ancestor = self._git(
            worktree, "merge-base", "--is-ancestor", expected_base, current
        )
        if ancestor.returncode != 0:
            return self._fail(
                "unsafe_agent_commit",
                "agent HEAD is not a descendant of the recorded Run base",
            )

        remote = self._git(worktree, "ls-remote", self.remote, f"refs/heads/{branch}")
        if remote.returncode != 0:
            return self._fail(
                "unsafe_agent_commit",
                "could not verify whether agent commits were pushed",
            )
        remote_sha = (remote.stdout.split() or [""])[0]
        if remote_sha and remote_sha != expected_base:
            return self._fail(
                "unsafe_agent_commit",
                f"remote branch {branch} is {remote_sha}, not recorded base "
                f"{expected_base}",
            )

        reset = self._git(worktree, "reset", "--mixed", expected_base)
        if reset.returncode != 0:
            return self._fail(
                "unsafe_agent_commit",
                reset.stderr.strip() or "could not normalize agent commit",
            )
        return None

    def _preflight(
        self,
        *,
        ship_run: runs_dao.RunRow,
        source_run: runs_dao.RunRow,
        task: tasks_dao.TaskRow,
    ) -> ShipOutcome | None:
        if ship_run.kind != "ship" or ship_run.agent != SYSTEM_AGENT:
            return self._fail(
                "invalid_worktree", "ship Run must be kind=ship agent=system"
            )
        if source_run.status != "succeeded":
            return self._fail("invalid_worktree", "source Run did not succeed")
        if source_run.kind not in {"implement", "repair", "fallback"}:
            return self._fail("invalid_worktree", "source Run is not mutating code")
        if not is_reliable_orchestrated_task(task):
            return self._fail(
                "invalid_worktree", "task is not reliable-orchestrator gated"
            )
        if self.remote != DEFAULT_REMOTE:
            return self._fail("invalid_remote", "shipping may only push to origin")
        worktree = source_run.worktree_path
        if not worktree or not os.path.isdir(worktree):
            return self._fail("invalid_worktree", "source worktree does not exist")
        if not is_managed_worktree_path(self.project_dir, worktree):
            return self._fail("invalid_worktree", "source worktree is not managed")
        if not self._same_repository(worktree):
            return self._fail(
                "invalid_worktree", "source worktree is not this repository"
            )
        try:
            branch = current_branch_name(worktree)
        except StateError as exc:
            return self._fail("wrong_branch", str(exc))
        if not source_run.branch_name or branch != source_run.branch_name:
            return self._fail(
                "wrong_branch",
                f"worktree branch {branch!r} != recorded {source_run.branch_name!r}",
            )
        if branch in {self.base_branch, "main", "master"}:
            return self._fail("wrong_branch", "refusing to ship default/main branch")
        return None

    def _same_repository(self, worktree: str) -> bool:
        project_common = self._git(self.project_dir, "rev-parse", "--git-common-dir")
        worktree_common = self._git(worktree, "rev-parse", "--git-common-dir")
        if project_common.returncode != 0 or worktree_common.returncode != 0:
            return False
        project_path = _abs_git_path(self.project_dir, project_common.stdout.strip())
        worktree_path = _abs_git_path(worktree, worktree_common.stdout.strip())
        return os.path.realpath(project_path) == os.path.realpath(worktree_path)

    def _stage_intended_diff(self, worktree: str) -> ShipOutcome:
        self._git(worktree, "reset", "--quiet")
        status = self._git(
            worktree,
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=normal",
        )
        if status.returncode != 0:
            return self._fail("commit_failed", status.stderr.strip())
        allowed = [
            path for path in _intended_paths(status.stdout) if not _excluded_path(path)
        ]
        if not allowed:
            return self._fail("no_changes", "no intended changes to ship")
        add = self._git(worktree, "add", "--", *allowed)
        if add.returncode != 0:
            return self._fail("commit_failed", add.stderr.strip())
        diff = self._git(worktree, "diff", "--cached", "--quiet")
        if diff.returncode == 0:
            return self._fail("no_changes", "no intended changes to ship")
        if diff.returncode not in {0, 1}:
            return self._fail("commit_failed", diff.stderr.strip())
        return ShipOutcome(ok=True)

    def _git_identity(self, worktree: str) -> tuple[str, str] | None:
        name = self._git(worktree, "config", "--get", "user.name")
        email = self._git(worktree, "config", "--get", "user.email")
        if name.returncode != 0 or email.returncode != 0:
            return None
        name_value = name.stdout.strip()
        email_value = email.stdout.strip()
        return (name_value, email_value) if name_value and email_value else None

    def _ensure_commit(
        self,
        worktree: str,
        task: tasks_dao.TaskRow,
        source_run: runs_dao.RunRow,
        pre_ship_head: str,
    ) -> str | None:
        if self._commit_has_trailer(worktree, source_run.id):
            return rev_parse(worktree, "HEAD")
        message = "\n\n".join(
            [
                task.title.strip() or task.id,
                "\n".join(
                    [
                        f"Superharness-Task: {task.id}",
                        f"Superharness-Run: {source_run.id}",
                        f"Agent: {source_run.agent}",
                    ]
                ),
            ]
        )
        commit = self._git(worktree, "commit", "-m", message)
        if commit.returncode != 0:
            try:
                head = rev_parse(worktree, "HEAD")
            except StateError:
                return None
            return head if head != pre_ship_head else None
        return rev_parse(worktree, "HEAD")

    def _existing_commit_sha(
        self, worktree: str, source_run_id: str, pre_ship_head: str
    ) -> str | None:
        try:
            head = rev_parse(worktree, "HEAD")
        except StateError:
            return None
        if head == pre_ship_head:
            return None
        return head if self._commit_has_trailer(worktree, source_run_id) else None

    def _commit_has_trailer(self, worktree: str, source_run_id: str) -> bool:
        log = self._git(worktree, "log", "-1", "--pretty=%B")
        return (
            log.returncode == 0 and f"Superharness-Run: {source_run_id}" in log.stdout
        )

    def _push(self, worktree: str, branch: str) -> ShipOutcome:
        result = self._git(worktree, "push", self.remote, f"{branch}:{branch}")
        if result.returncode == 0:
            return ShipOutcome(ok=True)
        detail = result.stderr.strip() or result.stdout.strip()
        if (
            "non-fast-forward" in detail
            or "fetch first" in detail
            or "rejected" in detail
        ):
            return self._fail("non_fast_forward", detail)
        return self._fail("push_failed", detail)

    def _create_or_reuse_pr(
        self, worktree: str, branch: str, task: tasks_dao.TaskRow
    ) -> ShipOutcome:
        existing = self._gh_json(
            worktree,
            "pr",
            "list",
            "--head",
            branch,
            "--base",
            self.base_branch,
            "--json",
            "number,url",
            "--limit",
            "1",
        )
        if existing is None:
            return self._fail("github_unavailable", "gh pr list failed")
        if isinstance(existing, list) and existing:
            return _pr_outcome(existing[0])

        body = f"Superharness task: {task.id}\n\nSystem-owned Phase 3 shipping."
        created = self._git(
            worktree,
            "gh",
            "pr",
            "create",
            "--head",
            branch,
            "--base",
            self.base_branch,
            "--title",
            task.title.strip() or task.id,
            "--body",
            body,
        )
        if created.returncode != 0:
            return self._fail(
                "pr_create_failed", created.stderr.strip() or created.stdout.strip()
            )
        url = _first_url(created.stdout)
        if not url:
            return self._fail("pr_create_failed", "gh pr create did not return a URL")
        viewed = self._gh_json(worktree, "pr", "view", url, "--json", "number,url")
        if viewed is None:
            return self._fail("github_unavailable", "gh pr view failed")
        return _pr_outcome(viewed)

    def _remote_branch_sha(self, worktree: str, branch: str) -> str | None:
        fetch = self._git(worktree, "fetch", self.remote, branch)
        if fetch.returncode != 0:
            return None
        result = self._git(
            worktree, "rev-parse", f"refs/remotes/{self.remote}/{branch}^{{commit}}"
        )
        return result.stdout.strip() if result.returncode == 0 else None

    def _gh_json(self, cwd: str, *args: str) -> Any:
        result = self._git(cwd, "gh", *args)
        if result.returncode != 0:
            return None
        try:
            return json.loads(result.stdout or "null")
        except json.JSONDecodeError:
            return None

    def _git(self, cwd: str, *args: str) -> CommandResult:
        if args and args[0] == "gh":
            return self._runner(cwd, args)
        return self._runner(cwd, ("git", "-C", cwd, *args))

    @staticmethod
    def _fail(category: str, detail: str) -> ShipOutcome:
        if category not in SHIP_FAILURES:
            category = "commit_failed"
        return ShipOutcome(ok=False, failure_category=category, failure_detail=detail)


def _intended_paths(raw_status: str) -> list[str]:
    parts = [part for part in raw_status.split("\0") if part]
    paths: list[str] = []
    index = 0
    while index < len(parts):
        entry = parts[index]
        status = entry[:2]
        path = entry[3:] if len(entry) > 3 else ""
        if status.startswith("R") or status.endswith("R"):
            index += 1
        if path:
            paths.append(path)
        index += 1
    return paths


def _excluded_path(path: str) -> bool:
    normalized = path.replace("\\", "/").lstrip("/")
    name = os.path.basename(normalized)
    return (
        normalized in {".superharness", ".codex"}
        or normalized.startswith((".superharness/", ".codex/", "launcher-logs/"))
        or "/launcher-logs/" in normalized
        or name == "state.sqlite3"
        or name.endswith((".sqlite", ".sqlite3", ".db", ".log"))
        or name == ".env"
        or name.startswith(".env.")
    )


def _abs_git_path(cwd: str, path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(cwd, path)


def _pr_outcome(payload: Any) -> ShipOutcome:
    if not isinstance(payload, dict):
        return ShipOutcome(
            ok=False,
            failure_category="pr_create_failed",
            failure_detail="invalid PR JSON",
        )
    number = payload.get("number")
    url = payload.get("url")
    if not isinstance(number, int) or not isinstance(url, str) or not url:
        return ShipOutcome(
            ok=False,
            failure_category="pr_create_failed",
            failure_detail="missing PR number/url",
        )
    return ShipOutcome(ok=True, pr_number=number, pr_url=url)


def _first_url(output: str) -> str | None:
    for token in output.split():
        if token.startswith("https://"):
            return token
    return None


def _subprocess_runner(cwd: str, args: Sequence[str]) -> CommandResult:
    if args and args[0] == "gh":
        final_args = ["gh", *args[1:]]
    else:
        final_args = list(args)
    result = subprocess.run(
        final_args,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(result.returncode, result.stdout, result.stderr)

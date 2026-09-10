from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path

from superharness.engine import runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db
from superharness.engine.reliable_worktree import (
    create_managed_worktree,
    create_repair_worktree,
    create_review_worktree,
    reliable_task_branch,
)
from superharness.engine.shipper import CommandResult, SystemShipper

NOW = "2026-01-01T00:00:00Z"


def _run(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _repo(tmp_path: Path, *, identity: bool = True):
    project = tmp_path / "repo"
    origin = tmp_path / "origin.git"
    upstream = tmp_path / "upstream.git"
    project.mkdir(parents=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=project, check=True)
    if identity:
        _run(project, "config", "user.name", "Super Harness")
        _run(project, "config", "user.email", "super@example.test")
    (project / "README.md").write_text("base\n", encoding="utf-8")
    (project / ".gitignore").write_text("ignored.tmp\n", encoding="utf-8")
    _run(project, "add", "README.md", ".gitignore")
    _run(project, "commit", "-m", "initial")
    subprocess.run(["git", "init", "--bare", str(origin)], check=True)
    subprocess.run(["git", "init", "--bare", str(upstream)], check=True)
    _run(project, "remote", "add", "origin", str(origin))
    _run(project, "remote", "add", "upstream", str(upstream))
    _run(project, "push", "-u", "origin", "main")
    (project / ".superharness").mkdir()
    return project, origin, upstream


def _db(project: Path, worktree: Path, branch: str, head: str):
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES ('t1', 'Implement thing', 'claude-code', 'in_progress', 1, ?, "
        "'reliable-orchestrator')",
        (NOW,),
    )
    source = runs_dao.create_run(
        conn,
        id="impl-1",
        task_id="t1",
        kind="implement",
        agent="claude-code",
        dedupe_key="implement:t1:plan",
        worktree_path=str(worktree),
        branch_name=branch,
        base_sha=head,
        head_sha=head,
        now=NOW,
    )
    runs_dao.transition_run(conn, source.id, to_status="claimed", now=NOW)
    runs_dao.transition_run(conn, source.id, to_status="running", now=NOW)
    runs_dao.transition_run(conn, source.id, to_status="succeeded", now=NOW)
    source = runs_dao.get_run(conn, source.id)
    assert source is not None
    ship = runs_dao.create_run(
        conn,
        id="ship-1",
        task_id="t1",
        kind="ship",
        agent="system",
        dedupe_key=f"ship:t1:{source.id}:{head}",
        parent_run_id=source.id,
        now=NOW,
    )
    conn.commit()
    task = tasks_dao.get(conn, "t1")
    assert task is not None
    return conn, task, source, ship


def _db_source(
    project: Path,
    worktree: Path,
    branch: str,
    head: str,
    *,
    kind: str,
):
    conn, task, source, ship = _db(project, worktree, branch, head)
    conn.execute(
        "UPDATE runs SET kind=?, dedupe_key=? WHERE id=?",
        (kind, f"{kind}:t1", source.id),
    )
    conn.commit()
    source = runs_dao.get_run(conn, source.id)
    assert source is not None
    return conn, task, source, ship


class FakeGh:
    def __init__(
        self,
        *,
        existing: bool = False,
        fail_push: str | None = None,
        remote_mismatch: bool = False,
        fail_gh: bool = False,
        missing_identity: bool = False,
    ) -> None:
        self.existing = existing
        self.fail_push = fail_push
        self.remote_mismatch = remote_mismatch
        self.fail_gh = fail_gh
        self.missing_identity = missing_identity
        self.pr_creates = 0
        self.pushes: list[Sequence[str]] = []

    def __call__(self, cwd: str, args: Sequence[str]) -> CommandResult:
        if args and args[0] == "gh":
            if self.fail_gh:
                return CommandResult(1, stderr="network unavailable")
            if args[1:3] == ("pr", "list"):
                payload = (
                    [{"number": 7, "url": "https://github.com/o/r/pull/7"}]
                    if self.existing
                    else []
                )
                return CommandResult(0, stdout=json.dumps(payload))
            if args[1:3] == ("pr", "create"):
                self.pr_creates += 1
                return CommandResult(0, stdout="https://github.com/o/r/pull/8\n")
            if args[1:3] == ("pr", "view"):
                return CommandResult(
                    0,
                    stdout=json.dumps(
                        {"number": 8, "url": "https://github.com/o/r/pull/8"}
                    ),
                )
        if len(args) >= 5 and args[:4] == ("git", "-C", cwd, "push"):
            self.pushes.append(args)
            if self.fail_push:
                return CommandResult(1, stderr=self.fail_push)
        if (
            self.missing_identity
            and len(args) >= 6
            and args[:5] == ("git", "-C", cwd, "config", "--get")
        ):
            return CommandResult(1)
        result = subprocess.run(
            list(args), cwd=cwd, capture_output=True, text=True, check=False
        )
        if (
            self.remote_mismatch
            and len(args) >= 5
            and args[:4] == ("git", "-C", cwd, "rev-parse")
            and "refs/remotes/origin/" in args[4]
        ):
            return CommandResult(0, stdout="deadbeef\n")
        return CommandResult(result.returncode, result.stdout, result.stderr)


def _shipping_fixture(tmp_path: Path, *, identity: bool = True):
    os.environ["SUPERHARNESS_WORKTREE_ROOT"] = str(tmp_path / "managed-worktrees")
    project, _origin, _upstream = _repo(tmp_path, identity=identity)
    wt = create_managed_worktree(str(project), "t1")
    worktree = Path(wt.path)
    return project, worktree, wt.branch_name, wt.base_sha


def test_managed_worktree_uses_deterministic_branch_and_explicit_origin_base(tmp_path):
    _project, worktree, branch, base_sha = _shipping_fixture(tmp_path)

    assert branch == reliable_task_branch("t1")
    assert worktree.exists()
    assert _run(worktree, "rev-parse", "HEAD") == base_sha
    assert _run(worktree, "symbolic-ref", "--short", "HEAD") == branch


def test_managed_worktree_replaces_tracked_superharness_with_live_state(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("SUPERHARNESS_WORKTREE_ROOT", str(tmp_path / "managed"))
    project, _origin, _upstream = _repo(tmp_path)
    source_state = project / ".superharness"
    (source_state / "profile.yaml").write_text("workflow: tracked\n", encoding="utf-8")
    _run(project, "add", ".superharness/profile.yaml")
    _run(project, "commit", "-m", "track superharness config")
    _run(project, "push", "origin", "main")

    handoffs = source_state / "handoffs"
    handoffs.mkdir()
    (handoffs / "runtime.yaml").write_text("live: true\n", encoding="utf-8")

    wt = create_managed_worktree(str(project), "t1")
    worktree = Path(wt.path)
    dst_state = worktree / ".superharness"

    assert dst_state.is_symlink()
    assert dst_state.resolve() == source_state.resolve()
    assert (dst_state / "handoffs" / "runtime.yaml").read_text(
        encoding="utf-8"
    ) == "live: true\n"
    assert source_state.is_dir()
    assert (source_state / "handoffs" / "runtime.yaml").exists()
    assert (worktree / "README.md").is_file()
    assert not (worktree / "README.md").is_symlink()

    reused = create_managed_worktree(str(project), "t1")
    assert reused.path == wt.path
    assert dst_state.is_symlink()
    assert dst_state.resolve() == source_state.resolve()

    wrong_state = tmp_path / "wrong-state"
    wrong_state.mkdir()
    dst_state.unlink()
    dst_state.symlink_to(wrong_state)
    reused_again = create_managed_worktree(str(project), "t1")
    assert reused_again.path == wt.path
    assert dst_state.is_symlink()
    assert dst_state.resolve() == source_state.resolve()

    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    (dst_state / "handoffs" / "new-runtime.yaml").write_text(
        "runtime\n", encoding="utf-8"
    )
    shipper = SystemShipper(str(project), runner=FakeGh())
    staged = shipper._stage_intended_diff(str(worktree))
    assert staged.ok
    cached = _run(worktree, "diff", "--cached", "--name-only")
    assert "README.md" in cached.splitlines()
    assert not any(path.startswith(".superharness") for path in cached.splitlines())


def test_review_worktree_is_detached_at_exact_remote_pr_sha(tmp_path):
    project, worktree, branch, base_sha = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    _run(worktree, "add", "README.md")
    _run(worktree, "commit", "-m", "change")
    pr_head = _run(worktree, "rev-parse", "HEAD")
    _run(worktree, "push", "origin", f"{branch}:{branch}")

    review = create_review_worktree(
        str(project), "t1", branch_name=branch, review_target_sha=pr_head
    )
    review_path = Path(review.path)

    assert review.base_sha == pr_head
    assert review.branch_name is None
    assert _run(review_path, "rev-parse", "HEAD") == pr_head
    assert _run(review_path, "branch", "--show-current") == ""
    assert base_sha != pr_head


def test_repair_worktree_resets_same_task_branch_to_current_remote_head(tmp_path):
    project, worktree, branch, _base_sha = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    _run(worktree, "add", "README.md")
    _run(worktree, "commit", "-m", "change")
    pr_head = _run(worktree, "rev-parse", "HEAD")
    _run(worktree, "push", "origin", f"{branch}:{branch}")
    _run(worktree, "reset", "--hard", "HEAD~1")

    repair = create_repair_worktree(
        str(project), "t1", branch_name=branch, expected_head_sha=pr_head
    )

    assert Path(repair.path) == worktree
    assert repair.branch_name == branch
    assert repair.base_sha == pr_head
    assert _run(worktree, "rev-parse", "HEAD") == pr_head


def test_shipper_commits_intended_files_excludes_control_plane_and_pushes_origin(
    tmp_path,
):
    project, worktree, branch, head = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    (worktree / "src.py").write_text("print('ok')\n", encoding="utf-8")
    (worktree / ".codex").mkdir()
    (worktree / ".codex" / "state.json").write_text("{}", encoding="utf-8")
    (worktree / "state.sqlite3").write_text("db", encoding="utf-8")
    (worktree / "debug.log").write_text("log", encoding="utf-8")
    (worktree / "ignored.tmp").write_text("ignored\n", encoding="utf-8")
    fake = FakeGh()
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        outcome = SystemShipper(str(project), runner=fake).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert outcome.ok
        assert outcome.pr_number == 8
        assert outcome.remote_head_sha == outcome.head_sha
        assert all("upstream" not in " ".join(push) for push in fake.pushes)
        committed = _run(worktree, "show", "--name-only", "--pretty=format:", "HEAD")
        assert "README.md" in committed
        assert "src.py" in committed
        assert ".codex/state.json" not in committed
        assert "state.sqlite3" not in committed
        assert "debug.log" not in committed
        assert "ignored.tmp" not in committed
    finally:
        conn.close()


def test_existing_pr_is_reused_without_duplicate_create(tmp_path):
    project, worktree, branch, head = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    fake = FakeGh(existing=True)
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        outcome = SystemShipper(str(project), runner=fake).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert outcome.ok
        assert outcome.pr_number == 7
        assert fake.pr_creates == 0
    finally:
        conn.close()


def test_shipper_accepts_repair_and_fallback_in_phase_5(tmp_path):
    project, worktree, branch, head = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("repair\n", encoding="utf-8")
    conn, task, source, ship = _db_source(
        project, worktree, branch, head, kind="repair"
    )
    try:
        repair = SystemShipper(str(project), runner=FakeGh()).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert repair.ok
    finally:
        conn.close()

    project, worktree, branch, head = _shipping_fixture(tmp_path / "fallback")
    (worktree / "README.md").write_text("fallback\n", encoding="utf-8")
    conn, task, source, ship = _db_source(
        project, worktree, branch, head, kind="fallback"
    )
    try:
        fallback = SystemShipper(str(project), runner=FakeGh()).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert fallback.ok
    finally:
        conn.close()


def test_empty_diff_missing_identity_push_and_sha_failures_block_shipping(tmp_path):
    project, worktree, branch, head = _shipping_fixture(tmp_path)
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        no_changes = SystemShipper(str(project), runner=FakeGh()).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert no_changes.failure_category == "no_changes"
    finally:
        conn.close()

    project, worktree, branch, head = _shipping_fixture(tmp_path / "missing-id")
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        missing_identity = SystemShipper(
            str(project), runner=FakeGh(missing_identity=True)
        ).ship(ship_run=ship, source_run=source, task=task)
        assert missing_identity.failure_category == "git_identity_missing"
    finally:
        conn.close()

    project, worktree, branch, head = _shipping_fixture(tmp_path / "push-fail")
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        rejected = SystemShipper(
            str(project), runner=FakeGh(fail_push="rejected non-fast-forward")
        ).ship(ship_run=ship, source_run=source, task=task)
        assert rejected.failure_category == "non_fast_forward"
    finally:
        conn.close()

    project, worktree, branch, head = _shipping_fixture(tmp_path / "sha-mismatch")
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        mismatch = SystemShipper(
            str(project), runner=FakeGh(remote_mismatch=True)
        ).ship(ship_run=ship, source_run=source, task=task)
        assert mismatch.failure_category == "remote_head_mismatch"
    finally:
        conn.close()


def test_unmanaged_or_default_branch_worktree_is_rejected(tmp_path):
    project, _origin, _upstream = _repo(tmp_path)
    worktree = project
    head = _run(project, "rev-parse", "HEAD")
    conn, task, source, ship = _db(project, worktree, "main", head)
    try:
        outcome = SystemShipper(str(project), runner=FakeGh()).ship(
            ship_run=ship, source_run=source, task=task
        )
        assert outcome.failure_category == "invalid_worktree"
    finally:
        conn.close()


def test_gh_network_failure_is_safe_and_retry_reuses_existing_commit(tmp_path):
    project, worktree, branch, head = _shipping_fixture(tmp_path)
    (worktree / "README.md").write_text("changed\n", encoding="utf-8")
    conn, task, source, ship = _db(project, worktree, branch, head)
    try:
        first = SystemShipper(str(project), runner=FakeGh(fail_gh=True)).ship(
            ship_run=ship, source_run=source, task=task
        )
        first_commit = _run(worktree, "rev-parse", "HEAD")
        assert first.failure_category == "github_unavailable"

        retry_fake = FakeGh()
        second = SystemShipper(str(project), runner=retry_fake).ship(
            ship_run=ship, source_run=source, task=task
        )
        second_commit = _run(worktree, "rev-parse", "HEAD")
        assert second.ok
        assert second_commit == first_commit
        assert retry_fake.pr_creates == 1
    finally:
        conn.close()

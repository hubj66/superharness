from __future__ import annotations

import json
from pathlib import Path

from superharness.commands.inbox_watch import (
    _auto_close_report_ready,
    _auto_close_review_passed,
    _auto_fallback_owner_reassign,
    auto_enqueue_approved,
    auto_enqueue_todo,
)
from superharness.engine import inbox_dao, reliable_watcher, runs_dao, tasks_dao
from superharness.engine.db import get_connection, init_db


def _project(tmp_path: Path, status: str):
    project = tmp_path / "project"
    harness = project / ".superharness"
    harness.mkdir(parents=True)
    (harness / "profile.yaml").write_text(
        "auto_dispatch: true\nautonomy: ai_driven\n", encoding="utf-8"
    )
    (harness / "ledger.md").write_text("# Ledger\n", encoding="utf-8")
    conn = get_connection(str(project))
    init_db(conn)
    conn.execute(
        "INSERT INTO tasks (id, title, owner, status, version, created_at, workflow) "
        "VALUES ('t1', 't1', 'claude-code', ?, 1, '2026-01-01T00:00:00Z', 'reliable-orchestrator')",
        (status,),
    )
    conn.commit()
    return project, conn


def _set_reliable_review_metadata(
    conn,
    *,
    task_id: str = "t1",
    verdict: str = "LGTM",
    run_id: str = "review-1",
    sha: str = "sha-a",
) -> None:
    task = tasks_dao.get(conn, task_id)
    assert task is not None
    extras = json.loads(task.extras_json or "{}")
    metadata = extras.setdefault("reliable_orchestrator", {})
    metadata.update(
        {
            "branch_name": "shux/reliable/t1",
            "pr_head_sha": sha,
            "pr_number": 42,
            "pr_url": "https://github.com/o/r/pull/42",
            "last_review_run_id": run_id,
            "last_review_verdict": verdict,
            "last_reviewed_head_sha": sha,
        }
    )
    tasks_dao.update(conn, task.id, task.version, {"extras_json": json.dumps(extras)})
    conn.commit()


def _create_review_run(
    conn,
    *,
    task_id: str = "t1",
    verdict: str = "LGTM",
    run_id: str = "review-1",
    sha: str = "sha-a",
    status: str = "succeeded",
    consumed: bool = True,
    malformed: bool = False,
) -> runs_dao.RunRow:
    run = runs_dao.create_run(
        conn,
        id=run_id,
        task_id=task_id,
        kind="review",
        agent="codex-cli",
        model="gpt-5",
        dedupe_key=f"review:{task_id}:{sha}:{run_id}",
        review_target_sha=sha,
        now="2026-01-01T00:00:00Z",
    )
    runs_dao.transition_run(
        conn, run.id, to_status="claimed", now="2026-01-01T00:00:01Z"
    )
    runs_dao.transition_run(
        conn, run.id, to_status="running", now="2026-01-01T00:00:02Z"
    )
    if status == "succeeded":
        runs_dao.record_run_result(
            conn,
            run.id,
            {
                "schema_version": 1,
                "run_id": run.id,
                "task_id": task_id,
                "kind": "review",
                "agent": "codex-cli",
                "exit_code": 0,
                "completion_status": "completed",
                "review_verdict": verdict,
                "reviewed_sha": sha,
                "findings": [] if verdict == "LGTM" else ["needs changes"],
            },
            now="2026-01-01T00:00:03Z",
        )
        runs_dao.transition_run(
            conn, run.id, to_status="succeeded", now="2026-01-01T00:00:04Z"
        )
        if malformed:
            conn.execute(
                "UPDATE runs SET result_json=? WHERE id=?",
                (json.dumps({"not": "a valid review result"}), run.id),
            )
    else:
        runs_dao.transition_run(
            conn,
            run.id,
            to_status=status,
            now="2026-01-01T00:00:04Z",
            failure_category="review_failed",
            failure_detail="review failed",
        )
    if consumed:
        runs_dao.mark_run_consumed(conn, run.id, now="2026-01-01T00:00:05Z")
    conn.commit()
    resolved = runs_dao.get_run(conn, run.id)
    assert resolved is not None
    return resolved


def _pr_payload(*, merged: bool = True, sha: str = "sha-a", number: int = 42) -> dict:
    return {
        "number": number,
        "html_url": f"https://github.com/o/r/pull/{number}",
        "merged": merged,
        "state": "closed" if merged else "open",
        "head": {"sha": sha},
    }


def test_legacy_auto_enqueue_paths_ignore_reliable_tasks(tmp_path):
    project, conn = _project(tmp_path, "todo")
    try:
        assert auto_enqueue_todo(str(project)) == 0
        assert inbox_dao.get_all(conn) == []
    finally:
        conn.close()


def test_watcher_restart_and_repeated_ticks_do_not_duplicate_runs(tmp_path):
    project, conn = _project(tmp_path, "todo")
    try:
        report_errors = lambda *_args: None
        assert reliable_watcher.tick(
            str(project), "2026-01-01T00:00:00Z", report_errors
        )
        reliable_watcher.release(str(project))
        assert reliable_watcher.tick(
            str(project), "2026-01-01T00:01:00Z", report_errors
        )
        runs = runs_dao.list_runs_for_task(conn, "t1", kind="plan")
        assert len(runs) == 1
        assert len(inbox_dao.get_all(conn, status="pending")) == 1
    finally:
        reliable_watcher.release(str(project))
        conn.close()


def test_reliable_tasks_bypass_legacy_close_and_fallback_paths(tmp_path):
    project, conn = _project(tmp_path, "review_requested")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )
        _auto_close_review_passed(str(project))
        assert _auto_close_report_ready(str(project)) is None
        inbox = inbox_dao.enqueue(
            conn,
            id="failed-1",
            task_id="t1",
            target_agent="claude-code",
            now="2026-01-01T00:00:00Z",
        )
        inbox_dao.update_status(
            conn,
            inbox.id,
            from_status="pending",
            to_status="failed",
            now="2026-01-01T00:01:00Z",
            reason="test",
        )
        conn.commit()
        _auto_fallback_owner_reassign(str(project))
        assert inbox_dao.get(conn, inbox.id).status == "failed"
    finally:
        conn.close()

    project, conn = _project(tmp_path / "approved", "plan_approved")
    try:
        assert auto_enqueue_approved(str(project)) == 0
        assert inbox_dao.get_all(conn) == []
    finally:
        conn.close()


def test_reliable_review_passed_with_authoritative_lgtm_but_open_pr_does_not_close(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path, "review_passed")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )
        _create_review_run(conn)
        _set_reliable_review_metadata(conn)
        monkeypatch.setattr(
            "superharness.engine.reliable_review_autoclose.fetch_github_pr",
            lambda _project_dir, _pr_url: _pr_payload(merged=False),
        )

        _auto_close_review_passed(str(project))
        assert tasks_dao.get(conn, "t1").status == "review_passed"
        ledger = (project / ".superharness" / "ledger.md").read_text(encoding="utf-8")
        assert " — CLOSE: t1 — " not in ledger
    finally:
        conn.close()


def test_reliable_review_passed_with_authoritative_lgtm_and_merged_pr_closes_once(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path, "review_passed")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )
        _create_review_run(conn)
        _set_reliable_review_metadata(conn)
        monkeypatch.setattr(
            "superharness.engine.reliable_review_autoclose.fetch_github_pr",
            lambda _project_dir, _pr_url: _pr_payload(),
        )

        _auto_close_review_passed(str(project))
        assert tasks_dao.get(conn, "t1").status == "done"
        ledger = (project / ".superharness" / "ledger.md").read_text(encoding="utf-8")
        assert ledger.count(" — CLOSE: t1 — ") == 1

        _auto_close_review_passed(str(project))
        assert tasks_dao.get(conn, "t1").status == "done"
        ledger = (project / ".superharness" / "ledger.md").read_text(encoding="utf-8")
        assert ledger.count(" — CLOSE: t1 — ") == 1
    finally:
        conn.close()


def test_reliable_review_passed_without_authoritative_lgtm_does_not_close(tmp_path):
    project, conn = _project(tmp_path, "review_passed")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )

        _auto_close_review_passed(str(project))

        assert tasks_dao.get(conn, "t1").status == "review_passed"
        ledger = (project / ".superharness" / "ledger.md").read_text(encoding="utf-8")
        assert " — CLOSE: t1 — " not in ledger
    finally:
        conn.close()


def test_reliable_review_passed_merged_pr_wrong_or_unprovable_head_does_not_close(
    tmp_path, monkeypatch
):
    cases = [
        _pr_payload(sha="wrong-sha"),
        {
            "number": 42,
            "html_url": "https://github.com/o/r/pull/42",
            "merged": True,
            "head": {},
        },
    ]
    for index, payload in enumerate(cases):
        project, conn = _project(tmp_path / str(index), "review_passed")
        try:
            (project / ".superharness" / "profile.yaml").write_text(
                "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
                encoding="utf-8",
            )
            _create_review_run(conn)
            _set_reliable_review_metadata(conn)
            monkeypatch.setattr(
                "superharness.engine.reliable_review_autoclose.fetch_github_pr",
                lambda _project_dir, _pr_url, payload=payload: payload,
            )

            _auto_close_review_passed(str(project))

            assert tasks_dao.get(conn, "t1").status == "review_passed"
            ledger = (project / ".superharness" / "ledger.md").read_text(
                encoding="utf-8"
            )
            assert " — CLOSE: t1 — " not in ledger
        finally:
            conn.close()


def test_reliable_review_passed_github_lookup_error_does_not_close(
    tmp_path, monkeypatch
):
    project, conn = _project(tmp_path, "review_passed")
    try:
        (project / ".superharness" / "profile.yaml").write_text(
            "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
            encoding="utf-8",
        )
        _create_review_run(conn)
        _set_reliable_review_metadata(conn)
        monkeypatch.setattr(
            "superharness.engine.reliable_review_autoclose.fetch_github_pr",
            lambda _project_dir, _pr_url: None,
        )

        _auto_close_review_passed(str(project))

        assert tasks_dao.get(conn, "t1").status == "review_passed"
        ledger = (project / ".superharness" / "ledger.md").read_text(encoding="utf-8")
        assert " — CLOSE: t1 — " not in ledger
    finally:
        conn.close()


def test_reliable_review_passed_rejected_failed_or_malformed_review_does_not_close(
    tmp_path, monkeypatch
):
    cases = [
        {"verdict": "REJECTED"},
        {"status": "failed"},
        {"malformed": True},
    ]
    for index, kwargs in enumerate(cases):
        project, conn = _project(tmp_path / str(index), "review_passed")
        try:
            (project / ".superharness" / "profile.yaml").write_text(
                "auto_dispatch: true\nauto_close: true\nautonomy: ai_driven\n",
                encoding="utf-8",
            )
            verdict = kwargs.get("verdict", "LGTM")
            _create_review_run(
                conn,
                verdict=verdict,
                status=kwargs.get("status", "succeeded"),
                malformed=bool(kwargs.get("malformed", False)),
            )
            _set_reliable_review_metadata(conn, verdict=verdict)
            monkeypatch.setattr(
                "superharness.engine.reliable_review_autoclose.fetch_github_pr",
                lambda _project_dir, _pr_url: _pr_payload(),
            )

            _auto_close_review_passed(str(project))

            assert tasks_dao.get(conn, "t1").status == "review_passed"
            ledger = (project / ".superharness" / "ledger.md").read_text(
                encoding="utf-8"
            )
            assert " — CLOSE: t1 — " not in ledger
        finally:
            conn.close()

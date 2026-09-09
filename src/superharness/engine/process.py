"""Single seam for process liveness, signalling, and termination.

`os.kill(pid, 0)` is a correct liveness probe on POSIX, but on Windows it does
not query the process at all: CPython special-cases signal `0`
(`CTRL_C_EVENT`) and `1` (`CTRL_BREAK_EVENT`) to `GenerateConsoleCtrlEvent`,
which sends a console control event to the target's process group rather
than probing it. A detached/adopted process that is not in the caller's
console process group raises `OSError` from that call, which a naive
`except OSError: return False` then reports as "dead" even though the
process is alive. `pid_alive` is the only place in this codebase allowed to
contain this Windows-vs-POSIX branching; every other call site must import
it from here rather than reimplementing it.

`terminate`, `signal_process_group`, and `terminate_group` are the analogous
seam for signalling and killing. `os.killpg`/`os.getpgid` are Unix-only
(`hasattr`-guarded everywhere they were previously inlined) and this module
is now the only place that guard may live.

Note on scope: `terminate_group` is for callers that only have a `pid` read
back from disk or a database — not a `subprocess.Popen` handle they own. A
caller that owns the `Popen` (spawned the process itself) must still reap it
with `proc.wait()`; polling `pid_alive` cannot substitute for that, because a
reaped-pending zombie still answers a liveness probe successfully until its
real parent calls `wait()` on it. `engine/operator.py` is the example: it
uses `signal_process_group` for the signal-sending step (eliminating the
duplicated `hasattr`/`getpgid`/`killpg` boilerplate this module exists to
kill) but keeps its own `proc.wait()`-based escalation loop, because it is
the real parent of the processes it manages.

Windows tree-kill note: `signal_process_group`/`terminate_group` degrade to a
single-pid signal on Windows (there is no POSIX process-group concept there).
A caller that needs to kill an entire child tree on Windows (not just one
pid) needs `taskkill /T`, which this module does not attempt to replicate —
see `engine/discussion.py`'s `_terminate_process_tree` for that case.
"""

from __future__ import annotations

import os
import signal
import time
from typing import Literal

ProcessProbe = Literal["live", "dead", "reused", "unknown"]


def process_starttime(pid: int) -> str | None:
    """Read Linux proc start time, robust to spaces in the process name."""
    if pid <= 0:
        return None
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
            contents = stat_file.read()
        closing = contents.rfind(")")
        if closing < 0:
            return None
        fields_after_comm = contents[closing + 2 :].split()
        return fields_after_comm[19] if len(fields_after_comm) > 19 else None
    except (OSError, ValueError):
        return None


def probe_process(pid: int | None, expected_starttime: str | None) -> ProcessProbe:
    """Classify a persisted process identity without trusting PID alone."""
    if pid is None or pid <= 0:
        return "unknown"
    if os.name != "nt":
        actual = process_starttime(pid)
        if actual is None:
            return "dead"
        if expected_starttime is None:
            return "unknown"
        return "live" if actual == expected_starttime else "reused"
    return "live" if pid_alive(pid) else "dead"


def pid_alive(pid: int) -> bool:
    """Return True iff `pid` refers to a live process.

    Never raises. `pid <= 0` is rejected up front: on POSIX, signalling pid 0
    targets the caller's own process group and signalling pid -1 targets
    every process the caller has permission to signal, neither of which is a
    meaningful liveness answer for a specific pid.
    """
    if pid <= 0:
        return False

    if os.name == "nt":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        handle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, pid
        )
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong(STILL_ACTIVE)
            ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            return exit_code.value == STILL_ACTIVE
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False  # no such process
    except PermissionError:
        return True  # process exists; we just lack permission to signal it
    except OSError:
        return False
    return True


def terminate(pid: int) -> None:
    """Send a single graceful terminate signal to `pid`.

    Idempotent and never raises: an already-dead, inaccessible, or invalid
    pid is silently ignored — there is nothing more a caller can do about
    any of those from here.
    """
    if pid <= 0:
        return

    if os.name == "nt":
        import ctypes

        PROCESS_TERMINATE = 0x0001
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, False, pid)
        if not handle:
            return
        try:
            ctypes.windll.kernel32.TerminateProcess(handle, 1)
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def signal_process_group(pid: int, sig: int) -> None:
    """Best-effort: send `sig` to `pid`'s entire POSIX process group.

    Falls back to signalling just `pid` if this platform has no
    `killpg`/`getpgid`, or if the group cannot be determined (already dead,
    or `pid` is not a process-group leader). Never raises. No-op on an
    invalid pid.

    This does not attempt a Windows tree-kill — see the module docstring.
    """
    if pid <= 0:
        return

    if hasattr(os, "killpg") and hasattr(os, "getpgid"):
        try:
            pgid = os.getpgid(pid)
        except OSError:
            pgid = None
        if pgid is not None:
            try:
                os.killpg(pgid, sig)
                return
            except OSError:
                pass

    try:
        os.kill(pid, sig)
    except OSError:
        pass


def terminate_group(
    pid: int,
    *,
    escalate_after: float | None = 5.0,
    poll_interval: float = 0.2,
    sleep=time.sleep,
    now=time.monotonic,
) -> None:
    """Full terminate policy for a `pid` read back from disk/DB (see the
    module docstring for why this is not for a caller that owns the
    `subprocess.Popen`).

    Sends SIGTERM to `pid`'s process group, then polls `pid_alive` and
    escalates to SIGKILL after `escalate_after` seconds if it is still
    alive. `escalate_after=None` sends SIGTERM only and returns immediately
    without polling — the shape needed inside a signal handler, where
    blocking is not safe. Degrades to a single-pid `terminate`-equivalent
    signal on Windows, or whenever the process group cannot be determined.
    Never raises.
    """
    if pid <= 0:
        return

    signal_process_group(pid, signal.SIGTERM)

    if escalate_after is None:
        return

    deadline = now() + escalate_after
    while now() < deadline:
        if not pid_alive(pid):
            return
        sleep(poll_interval)

    if pid_alive(pid):
        if os.name == "nt":
            # Windows has no SIGKILL and no process groups; terminate() issues
            # a TerminateProcess, which is the forcible-kill equivalent.
            terminate(pid)
        else:
            signal_process_group(pid, signal.SIGKILL)

"""Fixed-budget, model-free memory subprocess supervision.

Only the trusted host calls this module. Namespace and registered ingest roots
are host decisions, never permissions granted by a memory tool argument.
RSS limits are sampled tripwires, not kernel allocation ceilings.
"""

from __future__ import annotations

import importlib.util
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

from .brain_reader_guardian import (
    _group_rss,
    _pressure_ok,
    _pressure_reason,
    _pressure_diag,
    _record_pressure_failure,
    _write_ready,
)

WORKER_RSS = 128 * 1024**2
TOTAL_RSS = 192 * 1024**2
HISTORY_RSS = 512 * 1024**2
HISTORY_TOTAL_RSS = 640 * 1024**2
HISTORY_DEADLINE = 10
MAX_FRAME = 512 * 1024
DEADLINES = dict(
    remember=3,
    recall=2,
    read_sources=2,
    page=2,
    status=2,
    ingest_slice=3,
    queue_source=3,
    capture_one=3,
    coverage=2,
    source_page=2,
    discover_sources=3,
    discovery_status=2,
    restore_context=2,
    update_context=3,
)
_FIELDS = {
    "remember": ({"request_id", "text"}, {"provenance", "namespace"}),
    "recall": (
        {"query"},
        {
            "limit",
            "match",
            "workspace",
            "wing",
            "room",
            "allowed_source_hashes",
            "policy_epoch",
            "cursor",
        },
    ),
    "read_sources": ({"ids"}, set()),
    "page": ({"source_id"}, {"offset", "limit", "expected_hash", "field"}),
    "status": (set(), {"after", "limit"}),
    "ingest_slice": ({"path", "allowed_root", "format"}, set()),
    "queue_source": ({"path", "allowed_root", "format"}, {"workspace"}),
    "capture_one": ({"key"}, set()),
    "coverage": (set(), set()),
    "source_page": (set(), {"after", "limit", "pending_only"}),
    "discover_sources": ({"allowed_root", "format"}, {"max_entries"}),
    "discovery_status": (set(), set()),
    "restore_context": ({"workspace", "namespace"}, {"limit"}),
    "update_context": (
        {"workspace", "namespace", "request_id", "expected_revision", "status", "source_ids"},
        set(),
    ),
}


def _pressure_attribution(value):
    """Retain the bounded record only on its matching pressure refusal."""
    detail = _pressure_diag(value.pop("pressure_diag", None))
    if (
        value.get("reason") in {"system_pressure_unavailable", "worker_deadline"}
        and detail is not None
    ):
        value["pressure_diag"] = detail
    return value


def _unavailable(reason, pressure_diag=None):
    return _pressure_attribution(
        {"status": "unavailable", "reason": reason, "pressure_diag": pressure_diag}
    )


def _admission_directory():
    base = (
        Path("/private/tmp" if sys.platform == "darwin" else "/tmp")
        / f"castle-memory-{os.getuid()}"
    )
    try:
        base.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = base.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise OSError("unsafe admission directory")
    return base


def _admission():
    """One inherited admission inode for all palaces and HOME overrides."""
    import fcntl

    base = _admission_directory()
    fd = os.open(base / "admission.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
        ):
            raise OSError("unsafe admission file")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _priority_descriptor(*, create=False):
    base = _admission_directory()
    flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
    if create:
        flags |= os.O_CREAT
    fd = os.open(base / "interactive.waiting", flags, 0o600)
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
            or info.st_size != 0
        ):
            raise OSError("unsafe priority marker")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _waiting_marker():
    """Hold shared scheduling intent; a dead caller cannot leave active intent."""
    import fcntl

    fd = _priority_descriptor(create=True)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise


def interactive_waiting():
    """Check live bounded waiters. Unsafe markers make lower-priority work yield.

    Only an advisory lock counts as waiting; stale files, process IDs or caller
    text cannot grant admission. The file never contains a command or identity.
    """
    import fcntl

    try:
        fd = _priority_descriptor()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return False
        except OSError:
            return True
    finally:
        os.close(fd)


def run_memory(
    palace: Path, operation: str, arguments: dict, *, max_seconds=None, background=False
) -> dict:
    """Execute one bounded operation with trusted host scheduling and paths."""
    if type(background) is not bool:
        raise ValueError("invalid memory scheduling policy")
    if operation not in _FIELDS or not isinstance(arguments, dict):
        raise ValueError("invalid memory operation")
    deadline = DEADLINES[operation]
    if max_seconds is not None:
        if (
            type(max_seconds) not in (int, float)
            or not math.isfinite(max_seconds)
            or max_seconds <= 0
        ):
            raise ValueError("invalid memory deadline")
        deadline = min(deadline, max_seconds)
    required, optional = _FIELDS[operation]
    if not required <= arguments.keys() or arguments.keys() - required - optional:
        raise ValueError("invalid memory arguments")
    request = {
        "palace": str(Path(palace).absolute()),
        "operation": operation,
        "arguments": arguments,
    }
    try:
        frame = json.dumps(request, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValueError("invalid memory frame") from exc
    if len(frame) > MAX_FRAME:
        raise ValueError("memory request limit")
    if sys.platform != "darwin" or importlib.util.find_spec("psutil") is None:
        return _unavailable("platform_supervision_unavailable")
    path = Path(request["palace"])
    if not path.is_dir() or path.resolve() != path:
        return _unavailable("unsafe_palace_path")
    started = time.monotonic()
    fd = waiter = None
    try:
        if background and interactive_waiting():
            return _unavailable("memory_busy")
        try:
            fd = _admission()
        except BlockingIOError:
            if background:
                return _unavailable("memory_busy")
            waiter = _waiting_marker()
            until = started + min(0.5, deadline)
            while time.monotonic() < until:
                try:
                    fd = _admission()
                    break
                except BlockingIOError:
                    time.sleep(min(0.025, max(0, until - time.monotonic())))
            if fd is None:
                return _unavailable("memory_busy")
        if background and interactive_waiting():
            return _unavailable("memory_busy")
        elapsed = time.monotonic() - started
        if elapsed >= deadline:
            return _unavailable("memory_busy")
        return _supervise(request, fd, deadline=deadline - elapsed, background_preempt=background)
    except OSError:
        return _unavailable("admission_unavailable")
    finally:
        # Never unlock explicitly: descendants retain admission until they exit.
        if fd is not None:
            os.close(fd)
        if waiter is not None:
            os.close(waiter)


def run_history(palace: Path, operation: str, arguments: dict) -> dict:
    """Operator-only history work; limits and executable are never request fields.

    It shares admission with fast memory so capture/read/native jobs cannot pile
    up. Each call yields admission after one short slice, including failures.
    """
    fields = {
        "prepare": {"source"},
        "index": set(),
        "index-scopes": set(),
        "status": set(),
        "repair": {"store_id", "cursor"},
    }
    if (
        operation not in fields
        or not isinstance(arguments, dict)
        or arguments.keys() != fields[operation]
        or (operation == "prepare" and not isinstance(arguments["source"], str))
        or (
            operation == "repair"
            and (
                not isinstance(arguments["store_id"], str)
                or re.fullmatch(r"[a-f0-9]{64}", arguments["store_id"]) is None
                or not isinstance(arguments["cursor"], dict)
            )
        )
    ):
        raise ValueError("invalid history operation or arguments")
    request = {
        "palace": str(Path(palace).absolute()),
        "operation": operation,
        "arguments": arguments,
    }
    try:
        frame = json.dumps(request, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValueError("invalid history frame") from exc
    if len(frame) > MAX_FRAME:
        raise ValueError("history request limit")
    if sys.platform != "darwin" or importlib.util.find_spec("psutil") is None:
        return _unavailable("platform_supervision_unavailable")
    path = Path(request["palace"])
    if not path.is_dir() or path.resolve() != path:
        return _unavailable("unsafe_palace_path")
    if interactive_waiting():
        return _unavailable("memory_busy")
    try:
        fd = _admission()
    except BlockingIOError:
        return _unavailable("memory_busy")
    except OSError:
        return _unavailable("admission_unavailable")
    try:
        # A waiter may have arrived between the first check and acquisition.
        if interactive_waiting():
            return _unavailable("memory_busy")
        return _supervise(
            request,
            fd,
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("memory_history_worker.py")),
            ],
            deadline=HISTORY_DEADLINE - 2,
            rss_limit=HISTORY_RSS,
            total_rss_limit=HISTORY_TOTAL_RSS,
        )
    finally:
        os.close(fd)


def _background_waiting(preempt):
    return preempt and interactive_waiting()


def _close_pressure_probe(child, selector):
    """Close/reap this one fixed child; an unconfirmed reap must propagate."""
    try:
        if selector is not None:
            selector.close()
    finally:
        if child is not None:
            try:
                if child.poll() is None:
                    try:
                        child.kill()
                    except ProcessLookupError:
                        pass
            finally:
                try:
                    child.stdout.close()
                finally:
                    child.wait(timeout=0.05)


def _background_pressure_refusal(until, diagnostic, site, sample_resources=None):
    """One cancellable fixed probe within both original deadlines."""
    started = time.monotonic()
    cutoff = min(until, started + 0.5)
    diagnostic["site"] = site
    if interactive_waiting():
        return "background_preempted"
    if started >= until:
        _record_pressure_failure(diagnostic, started, cutoff, 0, "deadline_exhausted")
        return "worker_deadline"
    child = selector = None
    output = bytearray()
    normal, pressure, reason, outcome = False, None, None, "os_error"
    try:
        selector = selectors.DefaultSelector()
        child = subprocess.Popen(
            ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        os.set_blocking(child.stdout.fileno(), False)
        selector.register(child.stdout, selectors.EVENT_READ)
        output_done = False
        while True:
            if interactive_waiting():
                reason = "background_preempted"
                break
            now = time.monotonic()
            if now >= cutoff:
                reason = "worker_deadline" if now >= until else "system_pressure_unavailable"
                outcome = "deadline_exhausted" if now >= until else "timeout"
                break
            if sample_resources is not None:
                reason = sample_resources(child.pid if child.returncode is None else None)
                if reason:
                    break
            for key, _ in selector.select(max(0, min(0.01, cutoff - time.monotonic()))):
                chunk = os.read(key.fd, 65 - len(output))
                if not chunk:
                    output_done = True
                    selector.unregister(child.stdout)
                else:
                    output.extend(chunk)
                    if len(output) > 64:
                        reason, outcome = "system_pressure_unavailable", "invalid_output"
                        break
            if reason:
                break
            status = child.poll()
            if output_done and status is not None:
                value = bytes(output).strip()
                normal = status == 0 and value in (b"1", b"2")
                pressure = (
                    int(value) if status == 0 and len(value) <= 10 and value.isdigit() else None
                )
                pressure = pressure if pressure is not None and pressure < 2**31 else None
                reason = None if normal else "system_pressure_unavailable"
                outcome = (
                    "non_normal"
                    if pressure is not None
                    else "invalid_output"
                    if status == 0
                    else "os_error"
                )
                break
    except (OSError, ValueError, subprocess.SubprocessError):
        reason, outcome = "system_pressure_unavailable", "os_error"
    finally:
        _close_pressure_probe(child, selector)
    if reason == "background_preempted" or interactive_waiting():
        return "background_preempted"
    ended = time.monotonic()
    if ended >= until and reason in (None, "system_pressure_unavailable", "worker_deadline"):
        reason = "worker_deadline"
        outcome = "late_normal" if normal else "deadline_exhausted"
    elif normal and ended >= cutoff:
        reason, outcome = "system_pressure_unavailable", "late_normal"
    if reason in {"system_pressure_unavailable", "worker_deadline"}:
        _record_pressure_failure(diagnostic, started, cutoff, cutoff - started, outcome, pressure)
    return reason


def _supervisor_resource_refusal(
    parent, guardian_pid, peaks, rss_limit, total_rss_limit, pressure_pid=None
):
    """Original samples plus the one unreaped host pressure child when present."""
    import psutil

    task_rss = _group_rss(guardian_pid)
    if pressure_pid is not None:
        try:
            task_rss += psutil.Process(pressure_pid).memory_info().rss
        except psutil.NoSuchProcess:
            pass
    total_rss = task_rss + parent.memory_info().rss
    peaks[0] = max(peaks[0], task_rss)
    peaks[1] = max(peaks[1], total_rss)
    if task_rss > rss_limit:
        return "worker_memory_limit"
    if total_rss > total_rss_limit:
        return "total_memory_limit"
    return None


def _supervised_result(value, peaks, started, background_preempt, until):
    if not isinstance(value, dict) or not isinstance(value.get("status"), str):
        return _unavailable("worker_invalid_frame")
    _pressure_attribution(value)
    value["resources"] = {
        "peak_task_rss_bytes": peaks[0],
        "peak_total_rss_bytes": peaks[1],
        "elapsed_seconds": round(time.monotonic() - started, 4),
        "memory_guarantee": "sampled RSS tripwire; transient peaks are possible",
    }
    if background_preempt and value["status"] == "ok" and time.monotonic() >= until:
        return {**_unavailable("worker_deadline"), "resources": value["resources"]}
    return value


def _stage_pressure_reason(
    preempt, until, *, diagnostic=None, site="host_initial", sample_resources=None
):
    if not preempt:
        return _pressure_reason(until, _pressure_ok)
    if interactive_waiting():
        return "background_preempted"
    reason = _background_pressure_refusal(
        until, diagnostic if diagnostic is not None else {}, site, sample_resources
    )
    if reason == "background_preempted" or interactive_waiting():
        return "background_preempted"
    if time.monotonic() >= until:
        return "worker_deadline"
    return reason


def _supervise(
    request,
    admission,
    *,
    worker_command=None,
    deadline=2,
    rss_limit=None,
    total_rss_limit=None,
    absolute_deadline=None,
    background_preempt=False,
):
    """Keep the guardian unreaped; include all preflight in one absolute budget."""
    started = time.monotonic()
    until = started + deadline
    if absolute_deadline is not None:
        until = min(until, absolute_deadline)
    import psutil

    rss_limit = WORKER_RSS if rss_limit is None else rss_limit
    total_rss_limit = TOTAL_RSS if total_rss_limit is None else total_rss_limit
    pressure_diag = {}
    pressure_reason = _stage_pressure_reason(
        background_preempt, until, diagnostic=pressure_diag, site="host_initial"
    )
    if pressure_reason:
        return _unavailable(pressure_reason, pressure_diag)
    parent = psutil.Process()
    try:
        parent_rss = parent.memory_info().rss
    except psutil.Error:
        return _unavailable("resource_measurement_unavailable")
    if parent_rss >= total_rss_limit:
        return _unavailable("parent_memory_limit")
    worker = Path(__file__).with_name("memory_worker.py")
    config = {
        "request": {**request, "_host_pid": os.getpid()},
        "command": worker_command or [sys.executable, "-I", str(worker)],
        "admission": admission,
        "deadline": until - started,
        "absolute_deadline": until,
        "rss_limit": rss_limit,
        "max_frame": MAX_FRAME,
        "parent_pid": os.getpid(),
        "total_rss_limit": total_rss_limit,
    }
    proc = None
    selector = selectors.DefaultSelector()
    output = bytearray()
    peaks = [0, parent_rss]
    next_pressure = started + 0.25

    def finish(value):
        return _supervised_result(value, peaks, started, background_preempt, until)

    def resource_refusal(pressure_pid=None):
        return _supervisor_resource_refusal(
            parent, proc.pid, peaks, rss_limit, total_rss_limit, pressure_pid
        )

    try:
        # Serialization and resource measurement also spend the same budget.
        pending = memoryview(json.dumps(config).encode() + b"\n")
        if time.monotonic() >= until:
            return finish(_unavailable("worker_deadline"))
        proc = subprocess.Popen(
            [sys.executable, "-I", str(worker.with_name("brain_reader_guardian.py"))],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            pass_fds=(admission,),
            start_new_session=True,
        )
        os.set_blocking(proc.stdin.fileno(), False)
        selector.register(proc.stdin, selectors.EVENT_WRITE, "config")
        selector.register(proc.stdout, selectors.EVENT_READ, "response")
        leader = psutil.Process(proc.pid)
        while time.monotonic() < until:
            refusal = resource_refusal()
            if refusal:
                return finish(_unavailable(refusal))
            if time.monotonic() >= next_pressure or _background_waiting(background_preempt):
                pressure_reason = _stage_pressure_reason(
                    background_preempt,
                    until,
                    diagnostic=pressure_diag,
                    site="host_periodic",
                    sample_resources=resource_refusal,
                )
                if pressure_reason:
                    return finish(_unavailable(pressure_reason, pressure_diag))
                next_pressure = time.monotonic() + 0.25
            remaining = until - time.monotonic()
            if remaining <= 0:
                return finish(_unavailable("worker_deadline"))
            for key, _ in selector.select(min(0.01, remaining)):
                if key.data == "config":
                    # Keep the pipe open after transmission as host-death channel.
                    pending = _write_ready(selector, proc.stdin, pending)
                    continue
                chunk = os.read(key.fd, 65536)
                if not chunk:
                    try:
                        return finish(json.loads(output))
                    except (ValueError, UnicodeError, RecursionError):
                        return finish(_unavailable("worker_invalid_frame"))
                output.extend(chunk)
                if len(output) > MAX_FRAME:
                    return finish(_unavailable("worker_output_limit"))
            if leader.status() == psutil.STATUS_ZOMBIE:
                # Pipe EOF is observed on the next selector iteration; no reap.
                continue
        return finish(_unavailable("worker_deadline"))
    except (OSError, ValueError, psutil.Error):
        return finish(_unavailable("worker_failed"))
    finally:
        selector.close()
        if proc is not None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                # macOS may report EPERM for groups containing only zombies.
                from .brain_reader_guardian import _owned_group_members

                if any(p.status() != psutil.STATUS_ZOMBIE for p in _owned_group_members(proc.pid)):
                    raise
            proc.wait(timeout=2)
            proc.stdin.close()
            proc.stdout.close()

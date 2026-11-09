"""Stdlib/psutil-only guardian; native packages are imported by its child only."""

from __future__ import annotations

import json
import os
import selectors
import shutil
import signal
import subprocess
import sys
import time


def _pressure_diag(value):
    """Accept only this diagnostic's fixed, body-free record (at most512 bytes)."""
    required = {"site", "outcome", "elapsed_us", "timeout_us", "remaining_us"}
    if (
        not isinstance(value, dict)
        or len(value) not in (5, 6)
        or set(value) not in (required, required | {"pressure"})
    ):
        return None
    if type(value["site"]) is not str or type(value["outcome"]) is not str:
        return None
    if value["site"] not in (
        "host_initial",
        "host_periodic",
        "guardian_initial",
        "guardian_periodic",
    ) or value["outcome"] not in (
        "late_normal",
        "non_normal",
        "timeout",
        "os_error",
        "invalid_output",
        "deadline_exhausted",
        "unsupported_platform",
    ):
        return None
    if any(type(value[k]) is not int for k in ("elapsed_us", "timeout_us", "remaining_us")):
        return None
    if not (
        0 <= value["elapsed_us"] < 2**63
        and 0 <= value["timeout_us"] <= 500000
        and -(2**63) < value["remaining_us"] < 2**63
    ):
        return None
    if "pressure" in value and not (
        type(value["pressure"]) is int and 0 <= value["pressure"] < 2**31
    ):
        return None
    if len(json.dumps(value, separators=(",", ":")).encode("ascii")) > 512:
        return None
    return dict(value)


def _record_pressure_failure(diagnostic, started, deadline, timeout, outcome, pressure=None):
    if isinstance(diagnostic, dict) and "outcome" not in diagnostic:
        ended = time.monotonic()
        diagnostic.update(
            outcome=outcome,
            elapsed_us=max(0, min(2**63 - 1, int((ended - started) * 1000000))),
            timeout_us=max(0, min(500000, int(timeout * 1000000))),
            remaining_us=(
                -1
                if deadline is None
                else max(-(2**63) + 1, min(2**63 - 1, int((deadline - ended) * 1000000)))
            ),
        )
        if pressure is not None:
            diagnostic["pressure"] = pressure
    return False


def _pressure_ok(*, deadline=None, diagnostic=None):
    """Fail closed unless normal or warning pressure is reported before the deadline."""
    started = time.monotonic() if diagnostic is not None else 0.0
    if sys.platform != "darwin":
        return _record_pressure_failure(diagnostic, started, deadline, 0, "unsupported_platform")
    timeout = 0.5 if deadline is None else min(0.5, deadline - time.monotonic())
    if timeout <= 0:
        return _record_pressure_failure(
            diagnostic, started, deadline, timeout, "deadline_exhausted"
        )
    try:
        value = subprocess.check_output(
            ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"],
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
        on_time = deadline is None or time.monotonic() < deadline
        value = value.strip()
        if on_time and value in (b"1", b"2"):
            return True
        pressure = (
            int(value)
            if isinstance(value, bytes) and len(value) <= 10 and value.isdigit()
            else None
        )
        pressure = pressure if pressure is not None and pressure < 2**31 else None
        outcome = (
            "late_normal"
            if value in (b"1", b"2")
            else "non_normal"
            if pressure is not None
            else "invalid_output"
        )
        return _record_pressure_failure(diagnostic, started, deadline, timeout, outcome, pressure)
    except (OSError, subprocess.SubprocessError) as exc:
        outcome = "timeout" if isinstance(exc, subprocess.TimeoutExpired) else "os_error"
        return _record_pressure_failure(diagnostic, started, deadline, timeout, outcome)


def _pressure_reason(deadline, check, *, diagnostic=None):
    """Do not let a blocking pressure probe reset or overrun work admission."""
    if time.monotonic() >= deadline:
        _record_pressure_failure(diagnostic, time.monotonic(), deadline, 0, "deadline_exhausted")
        return "worker_deadline"
    pressure_ok = (
        check(deadline=deadline)
        if diagnostic is None
        else check(deadline=deadline, diagnostic=diagnostic)
    )
    if time.monotonic() >= deadline:
        return "worker_deadline"
    if not pressure_ok:
        return "system_pressure_unavailable"
    return None


def _owned_group_members(group_id):
    """Enumerate the retained process group, including reparented descendants.

    Its session leader stays alive or unreaped throughout supervision, so the
    numeric group identity cannot be reused by an unrelated new process group.
    Process objects also retain start identity for psutil's checked signalling.
    """
    import psutil

    for pid in psutil.pids():
        # macOS exposes kernel PID0; getpgid(0) means *our* group, not PID0.
        if pid == 0:
            continue
        try:
            if os.getpgid(pid) != group_id:
                continue
            member = psutil.Process(pid)
            # A PID can be reused between filtering and identity capture.
            # Check the captured process still belongs to our retained group.
            if os.getpgid(pid) == group_id:
                yield member
        except (ProcessLookupError, psutil.NoSuchProcess):
            pass


def _group_rss(group_id):
    import psutil

    rss = 0
    for member in _owned_group_members(group_id):
        try:
            rss += member.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return rss


def _write_ready(selector, stream, pending, *, close=False):
    """Advance one nonblocking IPC write without leaving supervision."""
    try:
        written = os.write(stream.fileno(), pending[:65536])
    except BlockingIOError:
        return pending
    pending = pending[written:]
    if not pending:
        selector.unregister(stream)
        if close:
            stream.close()
    return pending


def _resource_reason(config):
    """Account guardian and owned descendants, plus an optional trusted host."""
    import psutil

    group_rss = _group_rss(os.getpgrp())
    if group_rss > config["rss_limit"]:
        return "worker_memory_limit"
    if "total_rss_limit" in config:
        parent_rss = psutil.Process(config["parent_pid"]).memory_info().rss
        if group_rss + parent_rss > config["total_rss_limit"]:
            return "total_memory_limit"
    return None


def _read_config(selector, started):
    """Receive a bounded first frame while retaining its one-second deadline."""
    frame = bytearray()
    while b"\n" not in frame:
        if time.monotonic() - started > 1:
            return None
        for key, _ in selector.select(0.025):
            chunk = os.read(key.fd, 16384)
            if not chunk or len(frame) + len(chunk) > 512 * 1024:
                return None
            frame.extend(chunk)
    return json.loads(frame)


def main():
    import psutil

    selector = selectors.DefaultSelector()
    selector.register(sys.stdin.buffer, selectors.EVENT_READ, "host")
    started = time.monotonic()
    child = None
    config = None
    pressure_diag = {}

    def unavailable(reason):
        if config and ("total_rss_limit" in config or reason == "system_pressure_unavailable"):
            value = {"status": "unavailable", "reason": reason}
            detail = _pressure_diag(pressure_diag)
            if reason in {"system_pressure_unavailable", "worker_deadline"} and detail is not None:
                value["pressure_diag"] = detail
            sys.stdout.write(json.dumps(value))
            sys.stdout.flush()

    try:
        config = _read_config(selector, started)
        if config is None:
            return
        until = min(started + config["deadline"], config.get("absolute_deadline", float("inf")))
        pressure_diag["site"] = "guardian_initial"
        pressure_reason = _pressure_reason(until, _pressure_ok, diagnostic=pressure_diag)
        if pressure_reason:
            unavailable(pressure_reason)
            return
        next_pressure_check = time.monotonic() + 0.25
        child = subprocess.Popen(
            config["command"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            pass_fds=(config["admission"],),
        )
        pending = memoryview(json.dumps(config["request"]).encode())
        os.set_blocking(child.stdin.fileno(), False)
        selector.register(child.stdin, selectors.EVENT_WRITE, "request")
        selector.register(child.stdout, selectors.EVENT_READ, "reader")
        output = bytearray()
        output_done = False
        while time.monotonic() < until:
            if time.monotonic() >= next_pressure_check:
                pressure_diag["site"] = "guardian_periodic"
                pressure_reason = _pressure_reason(until, _pressure_ok, diagnostic=pressure_diag)
                if pressure_reason:
                    unavailable(pressure_reason)
                    return
                next_pressure_check = time.monotonic() + 0.25
            remaining = until - time.monotonic()
            if remaining <= 0:
                unavailable("worker_deadline")
                return
            for key, _ in selector.select(min(0.025, remaining)):
                if key.data == "request":
                    pending = _write_ready(selector, child.stdin, pending, close=True)
                    continue
                chunk = os.read(key.fd, 65536)
                if key.data == "host":
                    if not chunk:  # Host died; no orphaned native reader.
                        return
                elif not chunk:
                    output_done = True
                    selector.unregister(child.stdout)
                else:
                    output.extend(chunk)
                    if len(output) > config["max_frame"]:
                        unavailable("worker_output_limit")
                        return
            reason = _resource_reason(config)
            if reason:
                unavailable(reason)
                return
            # Reaping the child cannot recycle the guardian's process-group ID.
            if output_done and child.poll() is not None:
                if child.returncode == 0:
                    sys.stdout.buffer.write(output)
                    sys.stdout.buffer.flush()
                return
        unavailable("worker_deadline")
    except Exception as exc:
        sys.stdout.write(
            json.dumps({"status": "unavailable", "reason": "guardian_" + type(exc).__name__})
        )
        sys.stdout.flush()
    finally:
        selector.close()
        try:
            # psutil checks start identity before signalling reused PIDs.
            for member in _owned_group_members(os.getpgrp()):
                if member.pid == os.getpid():
                    continue
                try:
                    member.kill()
                except psutil.NoSuchProcess:
                    pass
            if config is not None:
                shutil.rmtree(config["request"].get("snapshot_dir", ""), ignore_errors=True)
        finally:
            # Kill every member of our owned group even if cleanup failed.
            # The kernel closes admission only when its last owner exits.
            os.killpg(os.getpgrp(), signal.SIGKILL)


if __name__ == "__main__":
    main()

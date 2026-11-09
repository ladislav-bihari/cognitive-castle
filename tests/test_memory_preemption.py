"""Live admission while real bounded pressure subprocesses are already running."""

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
@pytest.mark.parametrize("phase", ["preflight", "periodic"])
def test_live_waiter_interrupts_delayed_pressure_probe(monkeypatch, tmp_path, phase):
    import psutil
    from cognitive_castle import memory_runtime as runtime

    monkeypatch.setattr(runtime, "_admission_directory", lambda: tmp_path)
    entry, waiter_ready, outcome = (
        tmp_path / name for name in ("probe-entry", "waiter-ready", "outcome")
    )
    probe_pid, worker_pid = (tmp_path / name for name in ("probe-pid", "worker-pid"))
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os,time\nfrom pathlib import Path\nPath("
        + repr(str(probe_pid))
        + ").write_text(str(os.getpid()))\ntime.sleep(1)\nprint('1')\n"
    )
    worker = tmp_path / "worker.py"
    worker.write_text(
        "import os,time\nfrom pathlib import Path\nPath("
        + repr(str(worker_pid))
        + ").write_text(str(os.getpid()))\ntime.sleep(10)\n"
    )
    waiter_code = """
import json,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from cognitive_castle import memory_runtime
from cognitive_castle.semantic_runtime import SemanticSession
base=Path(sys.argv[2])
memory_runtime._admission_directory=lambda: base
(base/'waiter-ready').write_text('ready')
until=time.monotonic()+3
while not (base/'probe-entry').exists() and time.monotonic()<until:
    time.sleep(.001)
started=time.monotonic()
with SemanticSession(base/'unused') as session:
    (base/'outcome').write_text(json.dumps({'status':session._refusal or 'acquired','elapsed':time.monotonic()-started}))
"""
    original_popen = runtime.subprocess.Popen
    checks = []
    result, cleanup = {}, {}

    def delayed_probe(command, **kwargs):
        if command != ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"]:
            return original_popen(command, **kwargs)
        checks.append(command)
        if phase == "periodic" and not worker_pid.exists():
            return original_popen([sys.executable, "-I", "-c", "print(1)"], **kwargs)
        entry.write_text("in pressure helper")
        until = time.monotonic() + 0.3
        while not runtime.interactive_waiting() and time.monotonic() < until:
            time.sleep(0.001)
        assert runtime.interactive_waiting(), "live waiter did not reach pressure gap"
        child = original_popen([sys.executable, "-I", str(probe)], **kwargs)
        probe_pid.write_text(str(child.pid))
        return child

    monkeypatch.setattr(runtime.subprocess, "Popen", delayed_probe)
    fd = runtime._admission()

    def background():
        try:
            result.update(
                runtime._supervise(
                    {},
                    fd,
                    worker_command=[sys.executable, "-I", str(worker)],
                    deadline=3,
                    background_preempt=True,
                )
            )
        finally:
            for name, path in (("probe", probe_pid), ("worker", worker_pid)):
                cleanup[name] = not path.exists() or not psutil.pid_exists(int(path.read_text()))
            os.close(fd)

    waiter = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            waiter_code,
            str(Path(__file__).resolve().parents[1]),
            str(tmp_path),
        ]
    )
    thread = threading.Thread(target=background)
    try:
        until = time.monotonic() + 2
        while not waiter_ready.exists() and time.monotonic() < until:
            time.sleep(0.001)
        assert waiter_ready.exists()
        thread.start()
        waiter.wait(timeout=4)
        thread.join(timeout=4)
        assert not thread.is_alive()
        measured = json.loads(outcome.read_text())
        print(
            json.dumps(
                {
                    "phase": phase,
                    "interactive": measured,
                    "background": result,
                    "pressure_probe_starts": len(checks),
                    "reaped_before_admission_release": cleanup,
                }
            ),
            flush=True,
        )
        assert measured["status"] == "acquired" and measured["elapsed"] < 0.5
        assert result["reason"] == "background_preempted"
        assert probe_pid.exists() and cleanup == {"probe": True, "worker": True}
        assert worker_pid.exists() == (phase == "periodic")
    finally:
        if waiter.poll() is None:
            waiter.kill()
        waiter.wait(timeout=2)
        if thread.ident is not None:
            thread.join(timeout=4)
        else:
            os.close(fd)


def test_background_pressure_probe_accepts_measured_slow_normal_within_stage_budget(
    monkeypatch,
):
    from cognitive_castle import memory_runtime as runtime

    clock, deadlines = [100.0], []
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(runtime, "interactive_waiting", lambda: False)

    def pressure(*, deadline):
        deadlines.append(deadline)
        clock[0] = 100.07845
        return True

    monkeypatch.setattr(runtime, "_pressure_ok", pressure)
    monkeypatch.setattr(
        runtime,
        "_background_pressure_refusal",
        lambda until, *a: runtime._pressure_reason(min(until, clock[0] + 0.5), pressure),
        raising=False,
    )
    assert runtime._stage_pressure_reason(True, 105.0) is None
    assert deadlines == pytest.approx([100.5])
    assert clock[0] < 105.0


def test_background_pressure_probe_respects_shorter_absolute_deadline(monkeypatch):
    from cognitive_castle import memory_runtime as runtime

    clock, deadlines = [100.0], []
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(runtime, "interactive_waiting", lambda: False)

    def pressure(*, deadline):
        deadlines.append(deadline)
        clock[0] = 100.03
        return True

    monkeypatch.setattr(runtime, "_pressure_ok", pressure)
    monkeypatch.setattr(
        runtime,
        "_background_pressure_refusal",
        lambda until, *a: runtime._pressure_reason(min(until, clock[0] + 0.5), pressure),
    )
    assert runtime._stage_pressure_reason(True, 100.02) == "worker_deadline"
    assert deadlines == pytest.approx([100.02])

"""Engine-free scheduling regressions; native process proofs require a slot."""

import json
import os
import pytest

from cognitive_castle import memory_runtime as runtime


@pytest.fixture
def admission(tmp_path, monkeypatch):
    if os.name != "posix":
        pytest.skip("POSIX flock admission only")
    directory = tmp_path / "admission"
    directory.mkdir(mode=0o700)
    monkeypatch.setattr(runtime, "_admission_directory", lambda: directory)
    monkeypatch.setattr(runtime.sys, "platform", "darwin")
    return directory


def test_background_capture_busy_never_signals_or_waits(tmp_path, admission, monkeypatch):
    held = runtime._admission()
    monkeypatch.setattr(runtime.time, "sleep", lambda *_: pytest.fail("background waited"))
    monkeypatch.setattr(runtime, "_supervise", lambda *a, **k: pytest.fail("spawned worker"))
    try:
        assert runtime.run_memory(tmp_path, "status", {}, background=True) == {
            "status": "unavailable",
            "reason": "memory_busy",
        }
        assert not (admission / "interactive.waiting").exists()
        with pytest.raises(BlockingIOError):
            runtime._admission()
    finally:
        os.close(held)


def test_background_capture_yields_to_live_waiter_before_admission(
    tmp_path, admission, monkeypatch
):
    waiter = runtime._waiting_marker()
    monkeypatch.setattr(runtime, "_admission", lambda: pytest.fail("acquired ahead of waiter"))
    try:
        assert (
            runtime.run_memory(tmp_path, "status", {}, background=True)["reason"] == "memory_busy"
        )
    finally:
        os.close(waiter)


def test_background_capture_rechecks_waiter_after_acquiring(tmp_path, admission, monkeypatch):
    acquire, waiting = runtime._admission, []

    def raced_acquire():
        fd = acquire()
        waiting.append(runtime._waiting_marker())
        return fd

    monkeypatch.setattr(runtime, "_admission", raced_acquire)
    monkeypatch.setattr(
        runtime, "_supervise", lambda *a, **k: pytest.fail("spawned ahead of waiter")
    )
    try:
        assert (
            runtime.run_memory(tmp_path, "status", {}, background=True)["reason"] == "memory_busy"
        )
        probe = acquire()  # Refusal closed the acquired admission descriptor.
        os.close(probe)
    finally:
        for waiter in waiting:
            os.close(waiter)


def test_foreground_default_still_signals_and_waits_for_background(
    tmp_path, admission, monkeypatch
):
    held = [runtime._admission()]

    def yield_background(_seconds):
        assert runtime.interactive_waiting()
        os.close(held.pop())

    def supervise(request, fd, **kwargs):
        assert not kwargs.get("background_preempt", False)
        assert request["operation"] == "status"
        with pytest.raises(BlockingIOError):
            runtime._admission()
        return {"status": "ok"}

    monkeypatch.setattr(runtime.time, "sleep", yield_background)
    monkeypatch.setattr(runtime, "_supervise", supervise)
    try:
        assert runtime.run_memory(tmp_path, "status", {}) == {"status": "ok"}
        assert not runtime.interactive_waiting()
    finally:
        for fd in held:
            os.close(fd)


def test_background_capture_enables_supervisor_priority_checks(tmp_path, admission, monkeypatch):
    def supervise(request, fd, **kwargs):
        assert kwargs["background_preempt"] is True
        assert 0 < kwargs["deadline"] <= 2
        return {"status": "ok"}

    monkeypatch.setattr(runtime, "_supervise", supervise)
    assert runtime.run_memory(tmp_path, "status", {}, background=True) == {"status": "ok"}


def test_background_capture_supervisor_refuses_waiter_before_spawn(admission, monkeypatch):
    waiter = runtime._waiting_marker()
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: pytest.fail("spawned"))
    try:
        assert (
            runtime._supervise({}, -1, background_preempt=True)["reason"] == "background_preempted"
        )
    finally:
        os.close(waiter)


def test_background_capture_coordinator_preserves_spool_when_admission_busy(
    tmp_path, admission, monkeypatch
):
    from cognitive_castle import memory_coordinator as coordinator
    from cognitive_castle.brain_api import _root_registry

    palace, originals = tmp_path / "palace", tmp_path / "originals"
    palace.mkdir()
    originals.mkdir()
    _root_registry(palace, ("test", str(originals), "claude"))
    coordinator.configure_capture(palace, enabled=True)
    source = originals / "capture.jsonl"
    source.write_text(
        json.dumps({"type": "user", "message": {"content": "preserve exactly"}}) + "\n"
    )
    lock = coordinator._coordinator_lock(palace)
    try:
        assert coordinator.handle_capture_event(palace, source, str(tmp_path), "claude")["queued"]
    finally:
        os.close(lock)
    spool = palace / ".memory/capture-spool"
    before = {p.name: p.read_bytes() for p in spool.iterdir()}
    held = runtime._admission()
    monkeypatch.setattr(runtime.time, "sleep", lambda *_: pytest.fail("capture waited"))
    monkeypatch.setattr(runtime, "_supervise", lambda *a, **k: pytest.fail("spawned worker"))
    try:
        assert coordinator.capture_tick(palace, discover=False)["reason"] == "memory_busy"
        assert not runtime.interactive_waiting()
        # The durable claim rename is allowed; its exact event remains pending.
        assert sorted(p.read_bytes() for p in spool.iterdir()) == sorted(before.values())
        assert not list((palace / ".memory").glob("*.sqlite*"))
    finally:
        os.close(held)


@pytest.mark.parametrize("background", [None, 1, "true"])
def test_background_capture_policy_is_trusted_boolean(tmp_path, background):
    with pytest.raises(ValueError):
        runtime.run_memory(tmp_path, "status", {}, background=background)


@pytest.mark.parametrize("late", [False, True])
def test_bounded_probe_preserves_feature_bool_and_rejects_late_stage(monkeypatch, late):
    from cognitive_castle import brain_reader_guardian as guardian

    now, timeouts = [100.0], []
    monkeypatch.setattr(guardian.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(guardian.sys, "platform", "darwin")

    def probe(*args, **kwargs):
        timeouts.append(kwargs["timeout"])
        if late:
            now[0] += 1
        return b"1\n"

    monkeypatch.setattr(guardian.subprocess, "check_output", probe)
    assert guardian._pressure_ok() is True
    assert timeouts == [0.5]
    until = now[0] + 0.02
    assert guardian._pressure_reason(until, guardian._pressure_ok) == (
        "worker_deadline" if late else None
    )
    assert 0 < timeouts[-1] <= 0.021


def test_background_probe_yields_to_waiter_even_when_probe_times_out(admission, monkeypatch):
    waiting = []

    def slow_probe(until, diagnostic, site, sample_resources=None):
        assert 0 < until - runtime.time.monotonic() <= 2.001
        waiting.append(runtime._waiting_marker())
        return "system_pressure_unavailable"

    monkeypatch.setattr(runtime, "_background_pressure_refusal", slow_probe)
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **k: pytest.fail("spawned"))
    try:
        assert (
            runtime._supervise({}, -1, background_preempt=True)["reason"] == "background_preempted"
        )
    finally:
        for fd in waiting:
            os.close(fd)


@pytest.mark.parametrize("late", [False, True])
def test_background_bounded_probe_preserves_default_and_rejects_late_normal(monkeypatch, late):
    from cognitive_castle import brain_reader_guardian as guardian

    now, timeouts = [100.0], []
    monkeypatch.setattr(guardian.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(guardian.sys, "platform", "darwin")

    def probe(*args, **kwargs):
        timeouts.append(kwargs["timeout"])
        if late:
            now[0] += 1
        return b"1\n"

    monkeypatch.setattr(guardian.subprocess, "check_output", probe)
    assert guardian._pressure_ok() is True
    assert timeouts == [0.5]
    until = now[0] + 0.02
    assert guardian._pressure_ok(deadline=until) is (not late)
    assert 0 < timeouts[-1] <= 0.021


@pytest.mark.skipif(os.sys.platform != "darwin", reason="macOS process supervision only")
def test_native_background_capture_yields_only_to_foreground_and_cleans_up(tmp_path):
    from test_memory_runtime import parent

    result = parent(
        "import os,time,json,concurrent.futures,psutil;from pathlib import Path;"
        "from cognitive_castle import memory_runtime as r;"
        "p=Path(sys.argv[1]);"
        "initialized=r.run_memory(p,'remember',dict(request_id='priority-fixture',"
        "text='synthetic priority baseline'));assert initialized['status']=='ok',initialized;"
        "ready=p/'ready';fd=r._admission();"
        "code='import os,time;from pathlib import Path;Path('+repr(str(ready))+')"
        ".write_text(str(os.getpid()));time.sleep(10)';"
        "\ndef background():\n"
        " try:return r._supervise({},fd,worker_command=[sys.executable,'-I','-c',code],"
        "deadline=3,background_preempt=True)\n"
        " finally:os.close(fd)\n"
        "with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:\n"
        " future=pool.submit(background);until=time.monotonic()+1\n"
        " while not ready.exists() and time.monotonic()<until:time.sleep(.01)\n"
        " assert ready.exists(),'background worker did not start'\n"
        " pid=int(ready.read_text());started=time.monotonic()\n"
        " busy=r.run_memory(p,'status',{},background=True)\n"
        " immediate=time.monotonic()-started;alive=psutil.pid_exists(pid) and not future.done()\n"
        " signalled=r.interactive_waiting()\n"
        " foreground=r.run_memory(p,'status',{})\n"
        " background_result=future.result(timeout=4)\n"
        "probe=r._admission();os.close(probe);"
        "\ntry:alive_after=psutil.Process(pid).status()!=psutil.STATUS_ZOMBIE\n"
        "except psutil.NoSuchProcess:alive_after=False\n"
        "print(json.dumps(dict(initialized=initialized,busy=busy,immediate=immediate,alive=alive,signalled=signalled,"
        "foreground=foreground,background=background_result,alive_after=alive_after,"
        "waiting_after=r.interactive_waiting())))",
        str(tmp_path),
    )
    print("CAPTURE_PRIORITY_MEASUREMENTS", json.dumps(result, sort_keys=True), flush=True)
    assert result["busy"] == {"status": "unavailable", "reason": "memory_busy"}
    assert result["immediate"] < 0.25
    assert result["alive"] and not result["signalled"]
    assert result["foreground"]["status"] == "ok", result
    assert result["background"]["reason"] == "background_preempted", result
    assert not result["alive_after"] and not result["waiting_after"]


@pytest.mark.skipif(os.sys.platform != "darwin", reason="macOS process supervision only")
def test_native_background_capture_preserves_queue_and_spool_then_resumes(tmp_path):
    from test_memory_runtime import parent

    result = parent(
        "import os,json;from pathlib import Path;"
        "from cognitive_castle import memory_runtime as r,memory_coordinator as c;"
        "from cognitive_castle.brain_api import _root_registry;"
        "p=Path(sys.argv[1]);sources=p/'originals';sources.mkdir();"
        "_root_registry(p,('fixture',str(sources),'claude'));c.configure_capture(p,enabled=True);"
        "old=sources/'old.jsonl';new=sources/'new.jsonl';"
        "old.write_text(json.dumps(dict(type='user',message=dict(content='old pending exact')))+'\\n');"
        "new.write_text(json.dumps(dict(type='user',message=dict(content='new spooled exact')))+'\\n');"
        "queued=r.run_memory(p,'queue_source',dict(path=str(old),allowed_root=str(sources),format='claude'));"
        "assert queued['status']=='ok',queued;"
        "before=r.run_memory(p,'source_page',dict(pending_only=True));"
        "lock=c._coordinator_lock(p);"
        "\ntry:spooled=c.handle_capture_event(p,new,str(p),'claude')\n"
        "finally:os.close(lock)\n"
        "assert spooled['queued'];"
        "spool=p/'.memory/capture-spool';original=[f.read_bytes() for f in spool.iterdir()];"
        "fd=r._admission();"
        "\ntry:\n"
        " busy=c.capture_tick(p,discover=False)\n"
        " retained=sorted(f.read_bytes() for f in spool.iterdir())==sorted(original)\n"
        " signalled=r.interactive_waiting()\n"
        "finally:os.close(fd)\n"
        "after=r.run_memory(p,'source_page',dict(pending_only=True));"
        "resumed=c.capture_tick(p,discover=False);"
        "recall=r.run_memory(p,'recall',dict(query='exact'));"
        "print(json.dumps(dict(busy=busy,retained=retained,signalled=signalled,"
        "queue_unchanged=before['sources']==after['sources'],resumed=resumed,"
        "spool_remaining=len(list(spool.iterdir())),texts=sorted(x['text'] for x in recall['sources']))))",
        str(tmp_path),
    )
    print("CAPTURE_RECOVERY_MEASUREMENTS", json.dumps(result, sort_keys=True), flush=True)
    assert result["busy"]["reason"] == "memory_busy", result
    assert result["retained"] and result["queue_unchanged"] and not result["signalled"]
    assert result["resumed"]["status"] == "ok", result
    assert result["resumed"]["successful_slices"] == 2
    assert result["spool_remaining"] == 0
    assert result["texts"] == ["new spooled exact", "old pending exact"]

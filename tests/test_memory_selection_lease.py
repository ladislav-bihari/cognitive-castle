"""Read-only cooperative selection fencing; no activation or native storage."""

import copy

import pytest

from test_memory_selection import fixture, select


@pytest.fixture(autouse=True)
def isolated_admission(monkeypatch, tmp_path):
    from cognitive_castle import memory_runtime

    monkeypatch.setattr(memory_runtime, "_admission_directory", lambda: tmp_path)


def api():
    from cognitive_castle import memory_selection

    return memory_selection


def test_read_operations_share_lease_but_operator_switch_must_wait(tmp_path):
    palace, _, selected = fixture(tmp_path)
    module = api()
    with module.selection_lease(palace, selected):
        with module.selection_lease(palace, selected):
            with pytest.raises(module.SelectionBusy):
                with module.selection_lease(palace, selected, exclusive=True):
                    pytest.fail("operator switched during an active canonical operation")
    with module.selection_lease(palace, selected, exclusive=True):
        with pytest.raises(module.SelectionBusy):
            with module.selection_lease(palace, selected):
                pytest.fail("read entered while the selector was being changed")


def test_replacement_happens_only_after_old_operation_releases_lease(tmp_path):
    palace, marker, selected = fixture(tmp_path)
    module = api()
    changed = copy.deepcopy(selected)
    changed["room"] = "new-selection"
    with module.selection_lease(palace, selected, exclusive=True):
        select(marker, changed)
    with pytest.raises(module.SelectionUnavailable):
        with module.selection_lease(palace, selected):
            pytest.fail("stale selected snapshot authorized an operation")
    with module.selection_lease(palace, changed):
        assert module.load_selection(palace) == changed


def test_leases_create_no_files_and_exception_releases_lock(tmp_path):
    palace, _, selected = fixture(tmp_path)
    before = sorted(p.relative_to(palace) for p in palace.rglob("*"))
    with pytest.raises(RuntimeError, match="interrupted"):
        with api().selection_lease(palace, selected):
            raise RuntimeError("interrupted")
    with api().selection_lease(palace, selected, exclusive=True):
        pass
    assert sorted(p.relative_to(palace) for p in palace.rglob("*")) == before


def test_missing_metadata_directory_is_not_created(tmp_path):
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    with pytest.raises(api().SelectionUnavailable):
        with api().selection_lease(palace, None):
            pytest.fail("missing selection directory was silently initialized")
    assert list(palace.iterdir()) == []


@pytest.mark.parametrize("kind", ["symlink", "public"])
def test_unsafe_lock_directory_never_grants_a_lease(tmp_path, kind):
    palace, _, selected = fixture(tmp_path)
    directory = palace / ".memory"
    if kind == "public":
        directory.chmod(0o755)
    else:
        moved = palace / "other"
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
    with pytest.raises(api().SelectionUnavailable):
        with api().selection_lease(palace, selected):
            pytest.fail("unsafe directory granted a selection lease")


def test_interrupted_acquisition_releases_directory_lock(monkeypatch, tmp_path):
    palace, _, selected = fixture(tmp_path)
    module = api()
    real_load = module.load_selection

    def interrupted(path):
        raise KeyboardInterrupt("operator interrupted")

    monkeypatch.setattr(module, "load_selection", interrupted)
    with pytest.raises(KeyboardInterrupt):
        with module.selection_lease(palace, selected):
            pytest.fail("interrupted validation entered operation")
    monkeypatch.setattr(module, "load_selection", real_load)
    with module.selection_lease(palace, selected, exclusive=True):
        pass


def test_selected_native_operation_fences_cooperating_switch_until_exit(monkeypatch, tmp_path):
    import time
    from cognitive_castle import memory_selected_runtime, semantic_runtime
    from test_memory_selected_runtime import session

    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)

    def exact(self, *args):
        with pytest.raises(api().SelectionBusy):
            with api().selection_lease(palace, selected, exclusive=True):
                pytest.fail("selector entered while native operation was active")
        return dict(status="ok", sources=[])

    monkeypatch.setattr(semantic_runtime.SemanticSession, "exact", exact)
    assert memory_selected_runtime.run_selected(
        palace,
        selected,
        "read_sources",
        dict(ids=["memory:" + "a" * 64]),
        until=time.monotonic() + 2,
        background=False,
    ) == dict(status="ok", sources=[])
    assert made[0].closed
    with api().selection_lease(palace, selected, exclusive=True):
        pass


def test_directory_lease_excludes_a_separate_selector_process(tmp_path):
    import subprocess
    import sys

    palace, _, selected = fixture(tmp_path)
    script = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
try:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("busy")
    else:
        print("available")
finally:
    os.close(fd)
"""

    def attempt():
        return subprocess.check_output(
            [sys.executable, "-I", "-c", script, str(palace / ".memory")],
            text=True,
            timeout=2,
        ).strip()

    with api().selection_lease(palace, selected):
        assert attempt() == "busy"
    assert attempt() == "available"


@pytest.mark.parametrize("operation", ["read", "index"])
def test_active_selector_refuses_operations_before_admission(monkeypatch, tmp_path, operation):
    import time
    from cognitive_castle import memory_runtime, memory_selected_runtime, semantic_coordinator

    palace, _, selected = fixture(tmp_path)

    def forbidden():
        pytest.fail("operation attempted worker admission while selector owned the lease")

    with api().selection_lease(palace, selected, exclusive=True):
        monkeypatch.setattr(memory_runtime, "_admission", forbidden)
        if operation == "read":
            result = memory_selected_runtime.run_selected(
                palace,
                selected,
                "read_sources",
                dict(ids=["memory:" + "a" * 64]),
                until=time.monotonic() + 2,
                background=False,
            )
        else:
            result = semantic_coordinator.index_tick(palace, diagnostic=True)
    assert result == dict(status="unavailable", reason="canonical_selection_busy")


def test_selector_waits_for_descendant_admission_after_host_fd_closes(tmp_path):
    import os
    import subprocess
    import sys
    from cognitive_castle import memory_runtime

    palace, _, selected = fixture(tmp_path)
    fd = memory_runtime._admission()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys; print('ready', flush=True); sys.stdin.readline()",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        pass_fds=(fd,),
    )
    os.close(fd)
    try:
        assert proc.stdout.readline().strip() == "ready"
        with pytest.raises(api().SelectionBusy):
            with api().selection_lease(palace, selected, exclusive=True):
                pytest.fail("selector switched while an orphaned worker retained admission")
    finally:
        proc.communicate("exit\n", timeout=2)
    assert proc.returncode == 0
    with api().selection_lease(palace, selected, exclusive=True):
        pass

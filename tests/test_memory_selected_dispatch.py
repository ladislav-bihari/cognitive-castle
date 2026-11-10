"""Ordinary dispatcher must not fall back after a selected-Lance refusal."""

import os
import time
from cognitive_castle import memory_runtime, memory_selected_runtime
from test_memory_selection import fixture


def guard_old_route(monkeypatch):
    monkeypatch.setattr(memory_runtime.sys, "platform", "darwin")

    def forbidden(*args, **kwargs):
        raise AssertionError("selected operation reached legacy SQLite worker")

    monkeypatch.setattr(memory_runtime, "_admission", forbidden)
    monkeypatch.setattr(memory_runtime, "_supervise", forbidden)


def test_ordinary_remember_dispatch_preserves_selection_refusal_and_total_deadline(
    tmp_path, monkeypatch
):
    palace, _, selected = fixture(tmp_path)
    guard_old_route(monkeypatch)

    def run(path, value, operation, arguments, *, until, background):
        assert path == palace and value == selected
        assert operation == "remember" and arguments == dict(request_id="request", text="original")
        assert 0 < until - time.monotonic() <= 0.25 and background is False
        return dict(status="in_progress", reason="actual_receipt_pending")

    monkeypatch.setattr(memory_selected_runtime, "run_selected", run)
    assert memory_runtime.run_memory(
        palace, "remember", dict(request_id="request", text="original"), max_seconds=0.25
    ) == dict(status="in_progress", reason="actual_receipt_pending")


def test_invalid_selected_marker_refuses_instead_of_using_old_storage(tmp_path, monkeypatch):
    palace, marker, _ = fixture(tmp_path)
    guard_old_route(monkeypatch)
    marker.chmod(0o600)
    marker.write_text("invalid")
    marker.chmod(0o400)
    assert memory_runtime.run_memory(palace, "coverage", {}) == dict(
        status="unavailable", reason="canonical_selection_unavailable"
    )


def test_unselected_palace_retains_existing_worker_route_without_creating_marker(
    tmp_path, monkeypatch
):
    palace = (tmp_path / "palace").resolve()
    palace.mkdir(mode=0o700)
    monkeypatch.setattr(memory_runtime.sys, "platform", "darwin")
    monkeypatch.setattr(memory_runtime, "_admission", lambda: os.open(os.devnull, os.O_RDONLY))

    def supervise(request, fd, **kwargs):
        assert request["operation"] == "coverage" and request["palace"] == str(palace)
        return dict(status="unavailable", reason="legacy_fixture_not_run")

    monkeypatch.setattr(memory_runtime, "_supervise", supervise)
    assert memory_runtime.run_memory(palace, "coverage", {}) == dict(
        status="unavailable", reason="legacy_fixture_not_run"
    )
    assert list(palace.iterdir()) == []

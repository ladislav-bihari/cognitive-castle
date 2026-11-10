"""Host scheduling admission/CLI contracts; no SQL, native engine or model."""

import argparse
import json
from pathlib import Path
import time

import pytest

from test_memory_selection import fixture


def test_selected_index_always_requests_fair_schedule(monkeypatch, tmp_path):
    from cognitive_castle import semantic_coordinator as module

    palace, _, selected = fixture(tmp_path)
    session = module.SelectedIndexSession(palace, selected)
    requests = []

    def stage(request, **limits):
        requests.append((request, limits))
        return dict(status="unavailable", reason="fixture_no_native_execution")

    monkeypatch.setattr(session, "_run_stage", stage)
    assert session.prepare_index(session.generation)["reason"] == "fixture_no_native_execution"
    assert requests[0][0]["arguments"] == dict(
        envelope_hash=module.layout_hash(selected), require_fairness=True
    )
    assert requests[0][1]["rss"] == 128 * 1024**2
    assert requests[0][1]["total"] == 192 * 1024**2


def test_operator_initialization_binds_trusted_pin_and_control_budget(monkeypatch, tmp_path):
    from cognitive_castle import semantic_coordinator as module

    palace, _, selected = fixture(tmp_path)
    session = module.SelectedIndexSession(palace, selected)
    requests = []

    def stage(request, **limits):
        requests.append((request, limits))
        return dict(status="unavailable", reason="fixture_no_native_execution")

    monkeypatch.setattr(session, "_run_stage", stage)
    session.store(
        session.generation, "queue_initialize_index_schedule", dict(envelope_hash="f" * 64)
    )
    request, limits = requests[0]
    assert request["arguments"] == dict(envelope_hash=module.layout_hash(selected))
    assert (
        limits["rss"] == 128 * 1024**2 and limits["total"] == 192 * 1024**2 and limits["cap"] == 3
    )


def test_production_prepare_stays_unqualified_before_storage_access(tmp_path):
    from cognitive_castle import semantic_coordinator as module

    palace = tmp_path / "absent"
    assert module.prepare_index_schedule(palace) == dict(
        status="unavailable", reason="production_semantic_profile_unqualified"
    )
    assert not palace.exists()


def test_prepare_without_selected_authority_creates_nothing(tmp_path):
    from cognitive_castle import semantic_coordinator as module

    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    assert module.prepare_index_schedule(palace, diagnostic=True) == dict(
        status="unavailable", reason="canonical_selection_required"
    )
    assert list(palace.iterdir()) == []


@pytest.mark.parametrize("diagnostic", [False, True])
def test_prepare_cli_never_implicitly_activates_or_creates_selection(
    monkeypatch, tmp_path, capsys, diagnostic
):
    from cognitive_castle.brain_api import configure_memory_parser, cmd_memory

    monkeypatch.delenv("CASTLE_BRAIN_HOST", raising=False)
    parser = argparse.ArgumentParser()
    parser.add_argument("--palace")
    configure_memory_parser(parser.add_subparsers(dest="command", required=True))
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    words = ["--palace", str(palace), "memory", "semantic", "prepare"]
    if diagnostic:
        words.append("--diagnostic")
    cmd_memory(parser.parse_args(words))
    expected = (
        "canonical_selection_required" if diagnostic else "production_semantic_profile_unqualified"
    )
    assert json.loads(capsys.readouterr().out) == dict(status="unavailable", reason=expected)
    assert list(palace.iterdir()) == []


def test_partial_preparation_returns_without_indexing_or_extending_deadline(monkeypatch, tmp_path):
    from cognitive_castle import semantic_coordinator as module

    palace, _, selected = fixture(tmp_path)
    made = []
    partial = dict(status="in_progress", ready=False, processed=8)

    class Session:
        def __init__(self, selected_palace, snapshot):
            assert selected_palace == palace and snapshot == selected
            self.generation = Path(snapshot["envelope"]["generation"]["path"])
            self._until = time.monotonic() + 5
            self.closed = False
            made.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

        def _ready(self):
            return None

        def store(self, directory, operation, arguments):
            assert directory == self.generation and operation == "queue_initialize_index_schedule"
            return partial.copy()

    def forbidden(*args):
        pytest.fail("partial preparation started indexing")

    monkeypatch.setattr(module, "SelectedIndexSession", Session)
    monkeypatch.setattr(module, "index_one", forbidden)
    until = time.monotonic() + 2
    assert module.prepare_index_schedule(palace, diagnostic=True, max_seconds=2) == partial
    assert len(made) == 1 and made[0].closed and made[0]._until <= until + 0.01

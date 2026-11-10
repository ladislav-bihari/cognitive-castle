"""Operator background scheduling contracts; engines stay in supervised workers."""

import copy
import math
from pathlib import Path

import pytest

from test_memory_selection import fixture, select


def api():
    from cognitive_castle import semantic_coordinator

    return semantic_coordinator


@pytest.mark.parametrize("diagnostic", [False, None, 1, "yes"])
def test_production_or_untyped_opt_in_cannot_start_model_work(tmp_path, diagnostic):
    palace = tmp_path / "absent"
    if type(diagnostic) is not bool:
        with pytest.raises(ValueError):
            api().index_tick(palace, diagnostic=diagnostic)
    else:
        assert api().index_tick(palace, diagnostic=diagnostic) == {
            "status": "unavailable",
            "reason": "production_semantic_profile_unqualified",
        }
    assert not palace.exists()


@pytest.mark.parametrize("seconds", [0, -1, 5.01, math.inf, math.nan, True, "5"])
def test_invalid_budget_is_rejected_before_metadata_io(tmp_path, seconds):
    palace = tmp_path / "absent"
    with pytest.raises(ValueError):
        api().index_tick(palace, diagnostic=True, max_seconds=seconds)
    assert not palace.exists()


def test_absent_selection_creates_nothing_and_never_falls_back(tmp_path):
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    assert api().index_tick(palace, diagnostic=True) == {
        "status": "unavailable",
        "reason": "canonical_selection_required",
    }
    assert list(palace.iterdir()) == []


def test_corrupt_selection_cannot_start_or_use_sqlite(tmp_path):
    palace, marker, _ = fixture(tmp_path)
    marker.chmod(0o600)
    marker.write_text("{}")
    assert api().index_tick(palace, diagnostic=True) == {
        "status": "unavailable",
        "reason": "canonical_selection_unavailable",
    }


def test_tick_runs_once_in_background_preserves_pending_and_closes(monkeypatch, tmp_path):
    palace, _, selected = fixture(tmp_path)
    module = api()
    events = []
    clock = [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])

    class Session:
        def __init__(self, selected_palace, snapshot):
            assert selected_palace == palace and snapshot == selected
            self._until = 105.0
            self.generation = Path(snapshot["envelope"]["generation"]["path"])
            self.closed = False
            events.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

        def _ready(self):
            return None

    monkeypatch.setattr(module, "SelectedIndexSession", Session)
    pending = dict(status="in_progress", state="verification_pending", position=0)

    def index_one(session, generation):
        assert session._until == 102.0
        assert generation == Path(selected["envelope"]["generation"]["path"])
        events.append("index")
        return copy.deepcopy(pending)

    monkeypatch.setattr(module, "index_one", index_one)
    assert module.index_tick(palace, diagnostic=True, max_seconds=2) == pending
    assert len(events) == 2 and events[0].closed


def test_selection_change_invalidates_later_native_and_ack_stages(monkeypatch, tmp_path):
    palace, marker, selected = fixture(tmp_path)
    module = api()
    from cognitive_castle import semantic_runtime

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_ready", lambda self: None)
    session = module.SelectedIndexSession(palace, selected)
    assert session._interactive is False
    assert session.envelope_hash == module.layout_hash(selected)
    assert session._ready() is None
    changed = copy.deepcopy(selected)
    changed["room"] = "different-generation-selection"
    select(marker, changed)
    assert session._ready() == {"status": "unavailable", "reason": "canonical_selection_changed"}
    assert session.store(session.generation, "queue_acknowledge", {}) == {
        "status": "unavailable",
        "reason": "canonical_selection_changed",
    }


@pytest.mark.parametrize(
    "diagnostic,reason",
    [
        (False, "production_semantic_profile_unqualified"),
        (True, "canonical_selection_required"),
    ],
)
def test_operator_cli_respects_opt_in_and_never_initializes_storage(
    monkeypatch, tmp_path, capsys, diagnostic, reason
):
    import argparse
    import json
    from cognitive_castle.brain_api import configure_memory_parser, cmd_memory

    monkeypatch.delenv("CASTLE_BRAIN_HOST", raising=False)
    parser = argparse.ArgumentParser()
    parser.add_argument("--palace")
    configure_memory_parser(parser.add_subparsers(dest="command", required=True))
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    words = ["--palace", str(palace), "memory", "semantic", "tick", "--max-seconds", "1"]
    if diagnostic:
        words.append("--diagnostic")
    cmd_memory(parser.parse_args(words))
    assert json.loads(capsys.readouterr().out) == {"status": "unavailable", "reason": reason}
    assert list(palace.iterdir()) == []


def test_deadline_spent_loading_selection_does_not_open_admission(monkeypatch, tmp_path):
    palace, _, _ = fixture(tmp_path)
    module = api()
    real_load = module.load_selection
    clock = [1.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])

    def slow_load(path):
        value = real_load(path)
        clock[0] = 6.0
        return value

    monkeypatch.setattr(module, "load_selection", slow_load)
    assert module.index_tick(palace, diagnostic=True) == {
        "status": "unavailable",
        "reason": "semantic_deadline",
    }


def test_interactive_preemption_is_preserved_before_metadata_reads(monkeypatch, tmp_path):
    palace, _, selected = fixture(tmp_path)
    module = api()
    from cognitive_castle import semantic_runtime

    monkeypatch.setattr(
        semantic_runtime.SemanticSession,
        "_ready",
        lambda self: {"status": "unavailable", "reason": "background_preempted"},
    )
    session = module.SelectedIndexSession(palace, selected)
    # Even if the selection disappears, the admission/preemption refusal wins.
    monkeypatch.setattr(module, "load_selection", lambda path: None)
    assert session._ready() == {"status": "unavailable", "reason": "background_preempted"}


@pytest.mark.skipif(__import__("sys").platform != "darwin", reason="real macOS admission")
@pytest.mark.parametrize("fault", [None, "publish_refusal", "changed_before_ack"])
def test_real_admission_and_index_driver_resume_without_reinference(monkeypatch, tmp_path, fault):
    """Only fixed worker responses are synthetic; scheduling/driver/admission are real."""
    from cognitive_castle import memory_runtime
    from cognitive_castle.semantic_profile import PROFILE_ID, digest
    from test_semantic_indexer import _publication
    from test_semantic_queue import receipt

    palace, marker, selected = fixture(tmp_path)
    module = api()
    monkeypatch.setattr(memory_runtime, "_admission_directory", lambda: tmp_path)
    publication, source = _publication()
    group = dict(
        batch_key="fixture",
        references=publication["references"],
        profile=PROFILE_ID,
        input_binding=publication["page"]["input_binding"],
        position=0,
        total_units=None,
        lane="new",
        state="inflight",
    )
    proof = receipt(
        0,
        1,
        batch_key="fixture",
        input_binding=group["input_binding"],
        reference_binding=digest(group["references"]),
    )
    events = []
    state = {"written": False, "acknowledged": False}

    def supervise(request, admission, **limits):
        assert limits["background_preempt"] is True
        assert 0 < limits["deadline"] <= 5
        operation = request["operation"]
        events.append(operation)
        if operation.startswith("queue_"):
            assert limits["rss_limit"] == 128 * 1024**2
            assert limits["total_rss_limit"] == 192 * 1024**2
        elif operation.startswith("vector_"):
            assert limits["rss_limit"] == 192 * 1024**2
            assert limits["total_rss_limit"] == 256 * 1024**2
        else:
            assert limits["rss_limit"] == 512 * 1024**2
            assert limits["total_rss_limit"] == 640 * 1024**2
        if operation == "queue_prepare_index":
            return dict(status="ok", intents=[], selection=dict(status="ok", group=group))
        if operation == "vector_layout_prepare":
            if fault == "changed_before_ack" and state["written"]:
                changed = copy.deepcopy(selected)
                changed["room"] = "operator-replacement"
                select(marker, changed)
            return dict(
                status="ok",
                sources=[source],
                receipt=proof if state["written"] else {"status": "not_found"},
            )
        if operation == "tokenize":
            assert not state["written"], "a committed vector must not be re-tokenized"
            return copy.deepcopy(publication["page"])
        if operation == "infer":
            assert not state["written"], "a committed vector must not be re-inferred"
            return copy.deepcopy(publication["inference"])
        if operation == "vector_layout_publish":
            if fault == "publish_refusal":
                return dict(status="unavailable", reason="worker_deadline")
            state["written"] = True
            return dict(status="ok")
        assert operation == "queue_acknowledge"
        assert request["arguments"]["receipt"] == proof
        state["acknowledged"] = True
        return dict(status="ok", state="complete")

    monkeypatch.setattr(memory_runtime, "_supervise", supervise)
    first = module.index_tick(palace, diagnostic=True)
    if fault == "publish_refusal":
        assert first["status"] == "unavailable" and first["reason"] == "worker_deadline"
        assert first["phase"] == "publish"
        assert not state["written"] and not state["acknowledged"]
    else:
        assert first["status"] == "in_progress" and first["state"] == "verification_pending"
        assert first["position"] == 0 and state["written"] and not state["acknowledged"]
        second = module.index_tick(palace, diagnostic=True)
        if fault == "changed_before_ack":
            assert second == dict(status="unavailable", reason="canonical_selection_changed")
            assert not state["acknowledged"]
        else:
            assert second["status"] == "ok" and second["state"] == "complete"
            assert state["acknowledged"]
        assert events.count("tokenize") == events.count("infer") == 1
    # Both the success and refusal paths release the real global admission.
    import os

    fd = memory_runtime._admission()
    os.close(fd)

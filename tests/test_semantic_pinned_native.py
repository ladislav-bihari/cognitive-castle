"""Opt-in actual registered semantic storage proof, with fixed fixture vectors.

No model is invoked. These tests prove outbox/authority/storage contracts only.
"""

import json
import sys

from test_memory_selected_native import _handoff, _write, pytestmark  # noqa: F401


def _observe(monkeypatch):
    from cognitive_castle import semantic_runtime
    from test_layout_native_integration import _start, _record

    original = semantic_runtime.SemanticSession._run_stage

    def observe(self, request, **limits):
        operation = request["operation"]
        queue = operation.startswith("queue_")
        label = ("queue_pinned_" if queue else "pinned_native_") + operation
        assert (limits["rss"], limits["total"]) == (
            (128 * 1024**2, 192 * 1024**2) if queue else (192 * 1024**2, 256 * 1024**2)
        )
        before = len(self.resources)
        call = _start(label)
        result = original(self, request, **limits)
        wrong = request["arguments"].get("envelope_hash") == "f" * 64
        _record(
            self,
            label,
            result,
            before,
            call,
            expected_reason="selected_layout_authority"
            if wrong and operation == "layout_read"
            else None,
        )
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)


def _selection():
    from cognitive_castle.memory_selection import load_selection
    from cognitive_castle.migration_codec import hashed

    root, evidence = _handoff()
    selection = load_selection(root / "palace")
    return root, evidence["record"], selection, hashed(selection["envelope"])


def _session(root, selected, pin):
    from cognitive_castle.semantic_runtime import LayoutSemanticSession
    from pathlib import Path

    return LayoutSemanticSession(
        Path(selected["profile"]["path"]), root / "generation", interactive=False, envelope_hash=pin
    )


def test_pinned_outbox_recovers_only_after_actual_registered_receipt(monkeypatch):
    from cognitive_castle.semantic_indexer import recover_intents, _hydrate

    root, source, selected, pin = _selection()
    _observe(monkeypatch)
    generation = root / "generation"
    with _session(root, selected, "f" * 64) as session:
        wrong = recover_intents(session, generation)
        assert wrong["status"] == "ok" and wrong["recovered"] == 0
        assert (
            len(wrong["pending"]) == 1
            and wrong["pending"][0]["reason"] == "selected_layout_authority"
        )
        assert session.store(generation, "queue_status", {})["counts"]["pending"] == 0
    with _session(root, selected, pin) as session:
        result = recover_intents(session, generation)
        assert result == dict(status="ok", recovered=1, pending=[])
        assert session.store(generation, "queue_intents", {})["intents"] == []
        chosen = session.store(generation, "queue_select", {})
        assert chosen["status"] == "ok"
    group = chosen["group"]
    assert group["references"][0]["canonical_id"] == source["id"] and group["position"] == 0
    with _session(root, selected, pin) as session:
        assert _hydrate(session, generation, group) == dict(
            status="ok",
            sources=[dict(source=source, expected_hash=source["provenance"]["text_hash"])],
        )
    _write(root / "semantic-handoff.json", dict(group=group, pin=pin))
    assert not (generation / "exact/records.lance").exists()
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )


def test_pinned_vectors_publish_recover_and_hydrate_registered_original(monkeypatch):
    from cognitive_castle.semantic_indexer import index_one, _hydrate
    from test_layout_semantic_native import _arguments

    root, source, selected, pin = _selection()
    _observe(monkeypatch)
    handoff = json.loads((root / "semantic-handoff.json").read_text())
    group = handoff["group"]
    assert handoff["pin"] == pin
    arguments = _arguments(source)
    arguments.update(batch_key=group["batch_key"], references=group["references"])
    generation = root / "generation"
    with _session(root, selected, "f" * 64) as session:
        assert session.store(generation, "vector_publish", arguments) == dict(status="conflict")
    assert not (generation / "vectors").exists()
    with _session(root, selected, pin) as session:
        committed = session.store(generation, "vector_publish", arguments)
        assert committed["status"] == "ok", committed
    receipt_args = dict(batch_key=group["batch_key"], start_window=0)
    with _session(root, selected, "f" * 64) as session:
        assert session.store(generation, "vector_receipt", receipt_args) == dict(status="conflict")
    with _session(root, selected, pin) as session:
        assert session.store(generation, "vector_receipt", receipt_args) == committed
    with _session(root, selected, pin) as session:
        result = index_one(session, generation)
        assert result["status"] == "ok" and result["recovered_vector"] is True
        assert result["state"] == "complete", result
    with _session(root, selected, pin) as session:
        nearest = session.store(
            generation,
            "vector_nearest",
            dict(
                vector=arguments["inference"]["vectors"][0]["vector"],
                profile=group["profile"],
                generation=None,
                scope=group["references"][0]["scope"],
                allowed_source_hashes=None,
                allowed_exposure_hashes=[group["references"][0]["exposure_hash"]],
                limit=1,
            ),
        )
        assert nearest["status"] == "ok" and len(nearest["candidates"]) == 1, nearest
        assert nearest["candidates"][0]["canonical_id"] == source["id"]
        assert _hydrate(session, generation, group)["sources"][0]["source"] == source
    assert not (generation / "exact/records.lance").exists()
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )

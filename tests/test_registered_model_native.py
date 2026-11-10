"""Opt-in pinned real-model diagnostic over a NEW registered synthetic corpus.

Same eight facts/three multilingual top-one goldens as the existing semantic
fixture. No fixed vectors, model downloads, real memories or production cutover.
"""

import hashlib
import json
import os
from pathlib import Path
import sys
import time

import pytest

from test_memory_selected_native import _root, _write, pytestmark  # noqa: F401
from test_semantic_worker import DOCUMENTS, QUERIES

SCOPE = dict(workspace="/registered-model-project", wing="project", room="conversation")


def _pure_host():
    assert not {
        "sqlite3",
        "lance",
        "lancedb",
        "pyarrow",
        "numpy",
        "torch",
        "tokenizers",
        "onnxruntime",
    }.intersection(sys.modules)


def _fact(number):
    from cognitive_castle.canonical_records import prepare_remember

    return prepare_remember(str(number), DOCUMENTS[number], namespace="registered-model-fixture")


def _case():
    root = _root()
    value = json.loads((root / "model-handoff.json").read_text())
    assert value["source"] == os.environ["CASTLE_SELECTED_NATIVE_SOURCE"]
    assert value["profile"] == os.environ["CASTLE_TEST_SEMANTIC_PROFILE"]
    assert hashlib.sha256(Path(value["profile"]).read_bytes()).hexdigest() == value["profile_hash"]
    return root, value


def _session(value):
    from cognitive_castle.semantic_runtime import LayoutSemanticSession

    return LayoutSemanticSession(
        Path(value["profile"]),
        Path(value["generation"]),
        envelope_hash=value["pin"],
        interactive=False,
    )


def _observe(monkeypatch):
    from cognitive_castle import semantic_runtime
    from test_layout_native_integration import _start, _record

    original = semantic_runtime.SemanticSession._run_stage

    def observe(self, request, **limits):
        operation = request["operation"]
        worker = Path(limits["worker_command"][2]).name
        if worker == "semantic_worker.py":
            label = "model_stage_" + operation
            assert limits.get("rss", semantic_runtime.TASK_RSS) == 512 * 1024**2
            assert limits.get("total", semantic_runtime.TOTAL_RSS) == 640 * 1024**2
        elif operation.startswith("queue_"):
            label = "queue_model_" + operation
            assert (limits["rss"], limits["total"]) == (128 * 1024**2, 192 * 1024**2)
        else:
            label = "native_model_" + operation
            assert (limits["rss"], limits["total"]) == (192 * 1024**2, 256 * 1024**2)
        if operation == "layout_read" or operation in {
            "vector_layout_publish",
            "vector_layout_receipt",
        }:
            assert request["arguments"]["envelope_hash"] == self.envelope_hash
        previous = len(self.resources)
        call = _start(label)
        result = original(self, request, **limits)
        _record(self, label, result, previous, call)
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)


@pytest.mark.parametrize("number", range(8))
def test_registered_model_publish_fact(number):
    from cognitive_castle.live_publication_codec import prepare_publication
    from test_lance_live_native import _setup_live, _attempt

    root = _root()
    if number == 0:
        assert not root.exists()
        root.mkdir(mode=0o700)
        generation, _, pin = _setup_live(root)
        profile = Path(os.environ["CASTLE_TEST_SEMANTIC_PROFILE"])
        assert (
            profile.is_absolute() and not profile.is_symlink() and profile.stat().st_size <= 16384
        )
        _write(
            root / "model-handoff.json",
            dict(
                source=os.environ["CASTLE_SELECTED_NATIVE_SOURCE"],
                generation=str(generation),
                pin=pin,
                profile=str(profile),
                profile_hash=hashlib.sha256(profile.read_bytes()).hexdigest(),
            ),
        )
    root, value = _case()
    assert all((root / ("published-" + str(n) + ".json")).is_file() for n in range(number))
    prepared = _fact(number)
    publication = prepare_publication(prepared, [SCOPE for _ in prepared.records])
    for status in ("in_progress", "ok", "in_progress", "ok", "ok"):
        result, _ = _attempt(Path(value["generation"]), publication, value["pin"])
        assert result["status"] == status, result
    assert result["state"] == "snapshot_export_exhausted"
    receipt, _ = _attempt(Path(value["generation"]), publication, value["pin"], complete=True)
    assert receipt == prepared.receipt
    _write(root / ("published-" + str(number) + ".json"), receipt)
    assert not (root / "generation/exact/records.lance").exists()
    _pure_host()


@pytest.mark.parametrize("page", range(4))
def test_registered_model_recover_outbox(monkeypatch, page):
    from cognitive_castle.semantic_indexer import recover_intents

    root, value = _case()
    assert all((root / ("published-" + str(n) + ".json")).is_file() for n in range(8))
    assert all((root / ("recovered-" + str(n) + ".json")).is_file() for n in range(page))
    _observe(monkeypatch)
    started = time.monotonic()
    with _session(value) as session:
        result = recover_intents(session, root / "generation")
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION " + json.dumps(dict(operation="recovery", seconds=elapsed)),
        flush=True,
    )
    assert result == dict(status="ok", recovered=2, pending=[]) and elapsed <= 5, result
    _write(root / ("recovered-" + str(page) + ".json"), result)
    _pure_host()


@pytest.mark.parametrize("number", range(8))
def test_registered_model_indexes_original_with_actual_pinned_model(monkeypatch, number):
    from cognitive_castle.semantic_indexer import index_one

    root, value = _case()
    assert all((root / ("recovered-" + str(n) + ".json")).is_file() for n in range(4))
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(number))
    _observe(monkeypatch)
    started = time.monotonic()
    with _session(value) as session:
        result = index_one(session, root / "generation")
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION " + json.dumps(dict(operation="index", seconds=elapsed)),
        flush=True,
    )
    assert (
        result["status"] == "ok"
        and result["state"] == "complete"
        and result["recovered_vector"] is False
        and elapsed <= 5
    ), result
    with _session(value) as session:
        status = session.store(
            root / "generation", "queue_status", dict(envelope_hash=value["pin"])
        )
    assert status["status"] == "ok" and status["counts"]["complete"] == number + 1
    assert status["registered"]["confirmed_records"] == 8
    _write(
        root / ("indexed-" + str(number) + ".json"),
        dict(batch_key=result["batch_key"], receipt_id=result["receipt_id"]),
    )
    _pure_host()


@pytest.mark.parametrize("number", range(3))
def test_registered_model_multilingual_query_returns_expected_exact_original(monkeypatch, number):
    from cognitive_castle.semantic_indexer import query_exact

    root, value = _case()
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(8))
    _observe(monkeypatch)
    expected = _fact([0, 1, 3][number]).records[0].source()
    started = time.monotonic()
    with _session(value) as session:
        result = query_exact(session, root / "generation", QUERIES[number], SCOPE)
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION " + json.dumps(dict(operation="query", seconds=elapsed)),
        flush=True,
    )
    assert result["status"] == "ok" and elapsed <= 5, result
    assert (
        result["candidates"][0]["canonical_id"] == expected["id"]
        and result["sources"][0] == expected
    )
    _pure_host()

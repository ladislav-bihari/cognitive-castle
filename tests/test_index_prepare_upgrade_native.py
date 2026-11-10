"""Opt-in execution-source upgrade of the preserved synthetic real-model corpus.

Data origin and execution source are explicitly distinct; neither original
provenance nor the old failed qualification is rewritten.
"""

import hashlib
import json
import os
from pathlib import Path
import time

import pytest

from test_memory_selected_native import _root, _write, pytestmark  # noqa: F401
from test_registered_model_native import _fact, _pure_host, _session, SCOPE
from test_semantic_worker import QUERIES

DATA_SOURCE = "ac40671daf96735d5341117abeac1563dee63540"
SNAPSHOT = "14100896ab7d53b21957049a356f7dc3248799315822ad47dee5bdff0dacab41"


def _case():
    root = _root()
    value = json.loads((root / "model-handoff.json").read_text())
    assert value["source"] == os.environ["CASTLE_INDEX_DATA_SOURCE"] == DATA_SOURCE
    assert os.environ["CASTLE_SELECTED_NATIVE_SOURCE"] != DATA_SOURCE
    assert value["generation"] == str(root / "generation")
    assert value["profile"] == os.environ["CASTLE_TEST_SEMANTIC_PROFILE"]
    assert hashlib.sha256(Path(value["profile"]).read_bytes()).hexdigest() == value["profile_hash"]
    snapshot = Path(os.environ["CASTLE_INDEX_DATA_SNAPSHOT"])
    assert (
        snapshot.is_file() and not snapshot.is_symlink() and snapshot.stat().st_size < 2 * 1024**2
    )
    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == SNAPSHOT
    assert all((root / ("published-" + str(n) + ".json")).is_file() for n in range(8))
    return root, value


def _observe(monkeypatch):
    from cognitive_castle import semantic_runtime
    from test_layout_native_integration import _start, _record

    original = semantic_runtime.SemanticSession._run_stage

    def observe(self, request, **limits):
        operation = request["operation"]
        model = Path(limits["worker_command"][2]).name == "semantic_worker.py"
        label = (
            "model_stage_"
            if model
            else "queue_upgrade_"
            if operation.startswith("queue_")
            else "native_upgrade_"
        ) + operation
        if model:
            assert limits.get("rss", semantic_runtime.TASK_RSS) == 512 * 1024**2
            assert limits.get("total", semantic_runtime.TOTAL_RSS) == 640 * 1024**2
        else:
            assert (limits["rss"], limits["total"]) == (
                (128 * 1024**2, 192 * 1024**2)
                if operation.startswith("queue_")
                else (192 * 1024**2, 256 * 1024**2)
            )
        if operation in {
            "queue_prepare_index",
            "layout_read",
            "vector_layout_prepare",
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


def _index(monkeypatch, number, *, recovered):
    from cognitive_castle.semantic_indexer import index_one
    from cognitive_castle.semantic_intents import prepare_intent

    root, value = _case()
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(number))
    assert not (root / ("indexed-" + str(number) + ".json")).exists()
    _observe(monkeypatch)
    started = time.monotonic()
    with _session(value) as session:
        result = index_one(session, root / "generation")
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(dict(operation="upgrade_index", seconds=elapsed)),
        flush=True,
    )
    prepared = _fact(number)
    expected = prepare_intent(prepared, [SCOPE for _ in prepared.records], "new")["groups"][0]
    assert result["status"] == "ok" and result["state"] == "complete" and elapsed <= 5, result
    assert result["batch_key"] == expected["batch_key"] and result["recovered_vector"] is recovered
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


def test_upgraded_reader_recovers_existing_real_vector_without_inference(monkeypatch):
    from cognitive_castle import semantic_runtime

    def forbidden(*args, **kwargs):
        pytest.fail("existing committed vector must not repeat model work")

    monkeypatch.setattr(semantic_runtime.SemanticSession, "tokenize", forbidden)
    monkeypatch.setattr(semantic_runtime.SemanticSession, "infer", forbidden)
    _index(monkeypatch, 1, recovered=True)


@pytest.mark.parametrize("number", range(2, 8))
def test_upgraded_fresh_page_keeps_original_five_second_gate(monkeypatch, number):
    _index(monkeypatch, number, recovered=False)


@pytest.mark.parametrize("number", range(3))
def test_upgraded_multilingual_query_returns_original_top_one(monkeypatch, number):
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
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(dict(operation="upgrade_query", seconds=elapsed)),
        flush=True,
    )
    assert result["status"] == "ok" and elapsed <= 5, result
    assert (
        result["candidates"][0]["canonical_id"] == expected["id"]
        and result["sources"][0] == expected
    )
    _pure_host()

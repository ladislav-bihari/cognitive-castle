"""Opt-in bounded background ticks over the preserved real-model corpus.

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
SNAPSHOT = "9b4d08ceba2957035edf279f7ab33e38e8c9b2726cecbdf47d729ef2a2a003ce"


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


def _run(monkeypatch, *, forbid_model=False):
    from cognitive_castle import semantic_runtime
    from cognitive_castle.semantic_indexer import index_one

    root, value = _case()
    if forbid_model:

        def forbidden(*args, **kwargs):
            pytest.fail("a committed page must recover without repeated model work")

        monkeypatch.setattr(semantic_runtime.SemanticSession, "tokenize", forbidden)
        monkeypatch.setattr(semantic_runtime.SemanticSession, "infer", forbidden)
    _observe(monkeypatch)
    started = time.monotonic()
    with _session(value) as session:
        result = index_one(session, root / "generation")
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(dict(operation="background_tick", seconds=elapsed, status=result["status"])),
        flush=True,
    )
    assert elapsed <= 5, result
    return root, value, result


def _status(root, value):
    with _session(value) as session:
        result = session.store(
            root / "generation", "queue_status", dict(envelope_hash=value["pin"])
        )
    assert result["status"] == "ok" and result["registered"]["confirmed_records"] == 8
    return result


def test_background_upgrade_recovers_existing_page_without_inference(monkeypatch):
    root, value, result = _run(monkeypatch, forbid_model=True)
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(2))
    assert not (root / "indexed-2.json").exists()
    assert (
        result["status"] == "ok"
        and result["state"] == "complete"
        and result["recovered_vector"] is True
    ), result
    assert _status(root, value)["counts"]["complete"] == 3
    _write(
        root / "indexed-2.json",
        dict(batch_key=result["batch_key"], receipt_id=result["receipt_id"]),
    )
    _pure_host()


@pytest.mark.parametrize("number", range(3, 8))
def test_background_publish_tick_preserves_unacknowledged_position(monkeypatch, number):
    root, value = _case()
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(number))
    assert not (root / ("indexed-" + str(number) + ".json")).exists()
    assert not (root / ("pending-vector-" + str(number) + ".json")).exists()
    root, value, result = _run(monkeypatch)
    assert result["status"] == "in_progress" and result["state"] == "verification_pending", result
    assert (
        result["position"] == 0
        and result["recovered_vector"] is False
        and "receipt_id" not in result
    )
    assert _status(root, value)["counts"]["complete"] == number
    _write(
        root / ("pending-vector-" + str(number) + ".json"),
        dict(batch_key=result["batch_key"], position=0, observed_ns=time.monotonic_ns()),
    )
    _pure_host()


@pytest.mark.parametrize("number", range(3, 8))
def test_background_separate_cold_verification_advances_only_proven_position(monkeypatch, number):
    root, value = _case()
    pending = json.loads((root / ("pending-vector-" + str(number) + ".json")).read_text())
    assert not (root / ("indexed-" + str(number) + ".json")).exists()
    root, value, result = _run(monkeypatch, forbid_model=True)
    assert (
        result["status"] == "ok"
        and result["state"] == "complete"
        and result["recovered_vector"] is True
    ), result
    assert result["batch_key"] == pending["batch_key"] and pending["position"] == 0
    delay = (time.monotonic_ns() - pending["observed_ns"]) / 1e9
    assert 0 <= delay <= 120  # Verification delay, not exact-commit-to-index service lag.
    assert _status(root, value)["counts"]["complete"] == number + 1
    _write(
        root / ("indexed-" + str(number) + ".json"),
        dict(batch_key=result["batch_key"], receipt_id=result["receipt_id"]),
    )
    _write(
        root / ("verified-vector-" + str(number) + ".json"),
        dict(batch_key=result["batch_key"], verification_delay_seconds=delay),
    )
    _pure_host()


@pytest.mark.parametrize("number", range(3))
def test_background_completed_index_answers_original_multilingual_queries(monkeypatch, number):
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
        + json.dumps(dict(operation="interactive_query", seconds=elapsed)),
        flush=True,
    )
    assert result["status"] == "ok" and elapsed <= 5, result
    assert (
        result["candidates"][0]["canonical_id"] == expected["id"]
        and result["sources"][0] == expected
    )
    _pure_host()

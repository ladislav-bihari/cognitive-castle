"""Combined index reads; engine boundaries are synthetic, not durability proof."""

import json
from pathlib import Path

import pytest

from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.semantic_intents import prepare_intent
from cognitive_castle.semantic_profile import digest


def case(monkeypatch, text="original Ω\r\n"):
    from cognitive_castle import lance_vector_store as module
    from cognitive_castle.partitioned_exact_store import PartitionedExactStore

    prepared = prepare_remember("prepare", text)
    intent = prepare_intent(
        prepared, [dict(workspace=None, wing="wing", room="room") for _ in prepared.records], "new"
    )
    group = intent["groups"][0]
    args = dict(
        batch_key=group["batch_key"],
        references=group["references"],
        input_binding=group["input_binding"],
        start_window=0,
    )
    sources = [record.source() for record in prepared.records]
    reads = []

    def read(self, operation, arguments, **binding):
        reads.append((operation, arguments, binding))
        assert operation == "read_sources" and binding == dict(envelope_hash="a" * 64)
        assert arguments == dict(ids=[source["id"] for source in sources])
        return dict(status="ok", sources=sources)

    monkeypatch.setattr(PartitionedExactStore, "layout_read", read)
    monkeypatch.setattr(
        module.LanceExactStore, "read_sources", lambda *a: pytest.fail("flat fallback")
    )
    store = module.LanceVectorStore(
        Path("/trusted/generation"), exact_layout=True, envelope_hash="a" * 64
    )
    return store, args, sources, reads


@pytest.mark.parametrize("text", ["original Ω\r\n", "\x00" * 65536], ids=["one", "eight-escaped"])
@pytest.mark.parametrize("status", ["not_initialized", "not_found"])
def test_index_prepare_returns_actual_sources_and_actual_receipt_absence(monkeypatch, text, status):
    store, args, sources, reads = case(monkeypatch, text)
    monkeypatch.setattr(store, "receipt", lambda **kw: dict(status=status))
    result = store.prepare(**args)
    assert result == dict(status="ok", sources=sources, receipt=dict(status=status))
    assert len(reads) == 1
    assert len(json.dumps(result, ensure_ascii=True).encode()) < 512 * 1024


@pytest.mark.parametrize("fault", [None, "batch", "binding", "reference", "position", "corrupt"])
def test_index_prepare_checks_actual_existing_receipt_bindings(monkeypatch, fault):
    from test_semantic_queue import receipt

    store, args, sources, _ = case(monkeypatch)
    proof = receipt(
        0,
        1,
        batch_key=args["batch_key"],
        input_binding=args["input_binding"],
        reference_binding=digest(args["references"]),
    )
    if fault == "batch":
        proof["batch_key"] = "other"
    elif fault == "binding":
        proof["input_binding"] = "f" * 64
    elif fault == "reference":
        proof["reference_binding"] = "f" * 64
    elif fault == "position":
        proof = receipt(
            32,
            33,
            batch_key=args["batch_key"],
            input_binding=args["input_binding"],
            reference_binding=digest(args["references"]),
        )
    elif fault == "corrupt":
        proof = dict(status="corrupt")
    monkeypatch.setattr(store, "receipt", lambda **kw: proof)
    result = store.prepare(**args)
    if fault is None:
        assert result == dict(status="ok", sources=sources, receipt=proof)
    else:
        assert result["status"] in {"conflict", "corrupt"} and "sources" not in result


def test_index_prepare_never_reports_absence_when_actual_original_changed(monkeypatch):
    store, args, sources, _ = case(monkeypatch)
    sources[0] = dict(sources[0], text="changed")
    monkeypatch.setattr(
        store, "receipt", lambda **kw: pytest.fail("unverified originals reached vector lookup")
    )
    assert store.prepare(**args) == dict(status="stale")


@pytest.mark.parametrize(
    "field,value",
    [
        ("start_window", True),
        ("start_window", 1),
        ("start_window", -32),
        ("references", []),
        ("input_binding", "bad"),
    ],
)
def test_index_prepare_invalid_request_stops_before_native_read(monkeypatch, field, value):
    store, args, _, reads = case(monkeypatch)
    args[field] = value
    with pytest.raises(ValueError):
        store.prepare(**args)
    assert reads == []


def test_registered_index_prepare_keeps_read_guard_and_trusted_pin(monkeypatch):
    from cognitive_castle import semantic_runtime as runtime, semantic_store_worker as worker
    from cognitive_castle import lance_vector_store as vector

    calls = []
    result = dict(status="unavailable", reason="boundary_only")

    def stage(self, request, **limits):
        calls.append((request, limits))
        return result

    monkeypatch.setattr(runtime.SemanticSession, "_run_stage", stage)
    session = runtime.LayoutSemanticSession(
        Path("/profile"), Path("/trusted/generation"), envelope_hash="a" * 64
    )
    args = dict(batch_key="transport", references=[], input_binding="b" * 64, start_window=0)
    session.store(Path("/trusted/generation"), "vector_layout_prepare", args)
    request, limits = calls[-1]
    assert args == dict(
        batch_key="transport", references=[], input_binding="b" * 64, start_window=0
    )
    assert request["arguments"] == dict(args, envelope_hash="a" * 64)
    assert (limits["cap"], limits["rss"], limits["total"]) == (2, 192 * 1024**2, 256 * 1024**2)
    configurations = []

    def factory(directory, **configuration):
        from types import SimpleNamespace

        configurations.append((directory, configuration))
        return SimpleNamespace(
            prepare=lambda **kw: result if kw == args else pytest.fail("changed payload")
        )

    monkeypatch.setattr(vector, "LanceVectorStore", factory)
    assert worker._execute(dict(request, _host_pid=1)) == result
    assert configurations == [
        (
            Path("/trusted/generation"),
            dict(writable=False, exact_layout=True, envelope_hash="a" * 64),
        )
    ]

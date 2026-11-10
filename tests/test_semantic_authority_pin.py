"""Trusted semantic authority pins at real coordinator/worker boundaries."""

from pathlib import Path

import pytest

from cognitive_castle import semantic_runtime as runtime

GENERATION = Path("/trusted/generation")
PIN = "a" * 64


def test_selected_semantic_session_pins_exact_hydration_and_copies_vector_arguments(monkeypatch):
    calls = []

    def stage(self, request, **limits):
        calls.append((request, limits))
        return dict(status="unavailable", reason="boundary_only")

    monkeypatch.setattr(runtime.SemanticSession, "_run_stage", stage)
    session = runtime.LayoutSemanticSession(Path("/profile"), GENERATION, envelope_hash=PIN)
    for operation in ("receipt", "read_sources", "page"):
        args = dict(fixture="unchanged")
        session.exact(GENERATION / "exact", operation, args)
        request, limits = calls[-1]
        assert request["arguments"] == dict(operation=operation, arguments=args, envelope_hash=PIN)
        assert limits["cap"] == 2 and limits["rss"] == 192 * 1024**2
    for operation in ("vector_publish", "vector_receipt"):
        args = dict(batch_key="same")
        session.store(GENERATION, operation, args)
        request, limits = calls[-1]
        assert args == dict(batch_key="same")
        assert request["operation"] == operation.replace("vector_", "vector_layout_")
        assert request["arguments"] == dict(batch_key="same", envelope_hash=PIN)
        assert limits["cap"] == (3 if operation == "vector_publish" else 2)
    session.store(GENERATION, "queue_status", {})
    assert calls[-1][0]["arguments"] == {} and calls[-1][1]["rss"] == 128 * 1024**2


@pytest.mark.parametrize("pin", ["", "A" * 64, "a" * 63, 1, {}])
def test_invalid_selected_semantic_pin_refuses_before_any_stage(pin):
    with pytest.raises(ValueError):
        runtime.LayoutSemanticSession(Path("/profile"), GENERATION, envelope_hash=pin)


@pytest.mark.parametrize("operation", ["publish", "receipt"])
def test_bound_vector_worker_consumes_pin_as_configuration_without_changing_payload(
    monkeypatch, operation
):
    from cognitive_castle import lance_vector_store as vector, semantic_store_worker as worker
    from types import SimpleNamespace
    from test_vector_exposure_grants import publication

    _, args, _ = publication("original")
    if operation == "receipt":
        args = dict(batch_key="original", start_window=0)
    calls = []

    def factory(directory, **configuration):
        calls.append((directory, configuration))
        return SimpleNamespace(
            **{
                operation: lambda **values: (
                    dict(status="ok") if values == args else pytest.fail("payload changed")
                )
            }
        )

    monkeypatch.setattr(vector, "LanceVectorStore", factory)
    original = dict(args, envelope_hash=PIN)
    result = worker._execute(
        dict(
            directory=str(GENERATION),
            operation="vector_layout_" + operation,
            arguments=original,
            _host_pid=1,
        )
    )
    assert result == dict(status="ok") and original == dict(args, envelope_hash=PIN)
    assert calls == [
        (GENERATION, dict(writable=operation == "publish", exact_layout=True, envelope_hash=PIN))
    ]


@pytest.mark.parametrize("conflict", [False, True])
def test_vector_canonical_read_uses_independent_pin_and_never_flat_fallback(monkeypatch, conflict):
    from cognitive_castle import lance_vector_store as vector, partitioned_exact_store as exact
    from cognitive_castle.lance_exact_store import _StoreError
    from test_vector_exposure_grants import publication

    _, args, source = publication("original")
    seen = []

    class Authority:
        def __init__(self, directory):
            assert directory == GENERATION

        def layout_read(self, operation, arguments, *, envelope_hash):
            assert operation == "read_sources" and arguments == dict(ids=[source["id"]])
            seen.append(envelope_hash)
            return dict(status="conflict") if conflict else dict(status="ok", sources=[source])

    monkeypatch.setattr(exact, "PartitionedExactStore", Authority)
    monkeypatch.setattr(
        vector.LanceExactStore, "read_sources", lambda *a: pytest.fail("flat fallback")
    )
    store = vector.LanceVectorStore(GENERATION, exact_layout=True, envelope_hash=PIN)
    if conflict:
        with pytest.raises(_StoreError) as caught:
            store._sources(args["references"], args["page"]["input_binding"])
        assert caught.value.status == "conflict"
    else:
        assert store._sources(args["references"], args["page"]["input_binding"]) == {
            source["id"]: source
        }
    assert seen == [PIN]


def test_pin_cannot_select_flat_vector_authority():
    from cognitive_castle.lance_vector_store import LanceVectorStore

    with pytest.raises(ValueError):
        LanceVectorStore(GENERATION, envelope_hash=PIN)

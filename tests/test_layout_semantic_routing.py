"""Semantic control flow uses registered exact authority; no native IO here."""

from pathlib import Path
from types import SimpleNamespace as NS

import pytest

GENERATION = Path("/trusted/generation")


def session_case(monkeypatch):
    from cognitive_castle import semantic_runtime as runtime

    calls = []

    def stage(self, request, **profile):
        calls.append((request, profile))
        return dict(status="ok", sources=[])

    monkeypatch.setattr(runtime.SemanticSession, "_run_stage", stage)
    return runtime.LayoutSemanticSession(Path("/offline/profile"), GENERATION), calls


@pytest.mark.parametrize(
    "operation,arguments",
    [
        ("read_sources", {"ids": ["memory:" + "a" * 64]}),
        ("receipt", {"namespace": "operator", "key": "request"}),
        ("page", {"source_id": "memory:" + "a" * 64, "offset": 0, "expected_hash": "b" * 64}),
    ],
)
def test_registered_semantic_reads_use_facade_and_approved_exact_profile(
    monkeypatch, operation, arguments
):
    session, calls = session_case(monkeypatch)
    result = session.exact(GENERATION / "exact", operation, arguments)
    assert result["status"] == "ok" and len(calls) == 1
    request, profile = calls[0]
    assert request == dict(
        directory=str(GENERATION),
        operation="layout_read",
        arguments=dict(operation=operation, arguments=arguments),
    )
    assert (profile["cap"], profile["rss"], profile["total"]) == (2, 192 * 1024**2, 256 * 1024**2)


@pytest.mark.parametrize(
    "directory,operation",
    [
        (GENERATION / "exact", "remember"),
        (Path("/other/exact"), "read_sources"),
        (GENERATION / "other/../exact", "read_sources"),
    ],
)
def test_registered_semantic_session_never_uses_foreign_or_raw_mutation_paths(
    monkeypatch, directory, operation
):
    session, calls = session_case(monkeypatch)
    with pytest.raises(ValueError):
        session.exact(directory, operation, {})
    assert calls == []


def test_vector_publication_selects_registered_hydration_without_changing_queue_budget(monkeypatch):
    session, calls = session_case(monkeypatch)
    session.store(GENERATION, "vector_publish", {"proof": "transport-boundary-only"})
    request, profile = calls[-1]
    assert request["operation"] == "vector_layout_publish"
    assert request["directory"] == str(GENERATION)
    assert (profile["cap"], profile["rss"], profile["total"]) == (3, 192 * 1024**2, 256 * 1024**2)
    session.store(GENERATION, "queue_status", {})
    assert (calls[-1][1]["cap"], calls[-1][1]["rss"], calls[-1][1]["total"]) == (
        2,
        128 * 1024**2,
        192 * 1024**2,
    )
    with pytest.raises(ValueError):
        session.store(Path("/other"), "vector_publish", {})


def test_registered_vector_receipt_revalidates_the_same_canonical_authority(monkeypatch):
    session, calls = session_case(monkeypatch)
    session.store(GENERATION, "vector_receipt", dict(batch_key="original", start_window=0))
    request, profile = calls[-1]
    assert request["operation"] == "vector_layout_receipt"
    assert (profile["cap"], profile["rss"], profile["total"]) == (2, 192 * 1024**2, 256 * 1024**2)


def test_indexer_hydrates_original_refs_through_the_registered_session(monkeypatch):
    from cognitive_castle import semantic_indexer, semantic_runtime
    from cognitive_castle.semantic_profile import PROFILE_ID
    from test_vector_exposure_grants import publication

    _, args, source = publication("original")
    calls = []

    def stage(self, request, **profile):
        assert request["operation"] == "layout_read"
        calls.append(request)
        return dict(status="ok", sources=[source])

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", stage)
    session = semantic_runtime.LayoutSemanticSession(Path("/offline/profile"), GENERATION)
    group = dict(
        references=args["references"],
        profile=PROFILE_ID,
        input_binding=args["page"]["input_binding"],
    )
    hydrated = semantic_indexer._hydrate(session, GENERATION, group)
    assert hydrated == dict(
        status="ok", sources=[dict(source=source, expected_hash=source["provenance"]["text_hash"])]
    )
    assert len(calls) == 1


@pytest.mark.parametrize("fault", [None, "unavailable", "changed"])
def test_vector_source_validation_reads_only_registered_canonical_records(monkeypatch, fault):
    from cognitive_castle import lance_vector_store as vector, partitioned_exact_store as exact
    from cognitive_castle.lance_exact_store import _StoreError
    from test_vector_exposure_grants import publication

    _, args, source = publication("original")
    calls = []

    class Authority:
        def __init__(self, directory):
            assert directory == GENERATION

        def layout_read(self, operation, arguments):
            calls.append((operation, arguments))
            if fault == "unavailable":
                return dict(status="unavailable")
            return dict(status="ok", sources=[dict(source, text="changed") if fault else source])

    monkeypatch.setattr(exact, "PartitionedExactStore", Authority)
    monkeypatch.setattr(
        vector.LanceExactStore, "read_sources", lambda *args: pytest.fail("flat source bypass")
    )
    store = vector.LanceVectorStore(GENERATION, exact_layout=True)
    if fault:
        with pytest.raises(_StoreError):
            store._sources(args["references"], args["page"]["input_binding"])
    else:
        assert store._sources(args["references"], args["page"]["input_binding"]) == {
            source["id"]: source
        }
    assert calls == [("read_sources", {"ids": [source["id"]]})]


@pytest.mark.parametrize("operation", ["publish", "receipt"])
def test_registered_vector_dispatch_preserves_closed_fields_and_backend_selection(
    monkeypatch, operation
):
    from cognitive_castle import lance_vector_store as vector, semantic_store_worker as worker
    from test_vector_exposure_grants import publication

    _, arguments, _ = publication("original")
    if operation == "receipt":
        arguments = dict(batch_key="original", start_window=0)
    calls = []

    def factory(directory, **configuration):
        calls.append((directory, configuration))
        return NS(
            **{
                operation: lambda **args: (
                    dict(status="ok") if args == arguments else pytest.fail("changed inputs")
                )
            }
        )

    monkeypatch.setattr(vector, "LanceVectorStore", factory)
    request = dict(
        directory=str(GENERATION),
        operation="vector_layout_" + operation,
        arguments=arguments,
        _host_pid=1,
    )
    assert worker._execute(request) == dict(status="ok")
    assert calls == [(GENERATION, dict(writable=operation == "publish", exact_layout=True))]
    with pytest.raises(ValueError, match="arguments"):
        worker._execute(dict(request, arguments=dict(arguments, worker_command=[])))
    assert len(calls) == 1

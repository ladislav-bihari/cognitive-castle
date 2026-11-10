"""Actual index coordinator over fixed synthetic worker responses, no engines."""

import copy
from pathlib import Path

import pytest

from cognitive_castle.semantic_profile import PROFILE_ID, digest
from test_semantic_indexer import _publication
from test_semantic_queue import receipt


def case(fault=None, *, committed=False, deferred=False, publish_refused=False):
    from cognitive_castle.semantic_indexer import index_one

    args, source = _publication()
    group = dict(
        batch_key=args["batch_key"],
        references=args["references"],
        profile=PROFILE_ID,
        input_binding=args["page"]["input_binding"],
        position=0,
        total_units=None,
        lane="new",
        state="inflight",
    )
    proof = receipt(
        0,
        1,
        batch_key=group["batch_key"],
        input_binding=group["input_binding"],
        reference_binding=digest(group["references"]),
    )
    prepared = dict(status="ok", intents=[], selection=dict(status="ok", group=group))
    native = dict(
        status="ok", sources=[source], receipt=proof if committed else dict(status="not_found")
    )
    if fault == "control":
        prepared = dict(status="unavailable", reason="worker_deadline")
    elif fault == "control_shape":
        prepared["extra"] = "forbidden"
    elif fault == "unverified_selection":
        prepared["intents"] = [dict(intent_id="invalid")]
    elif fault == "selection_shape":
        prepared["selection"] = dict(status="ok")
    elif fault == "group_shape":
        del group["input_binding"]
    elif fault == "native":
        native = dict(status="unavailable", reason="worker_deadline")
    elif fault == "native_shape":
        del native["receipt"]
    elif fault == "source":
        native["sources"] = [dict(source, text="changed")]
    elif fault == "native_sources":
        native["sources"] = "not a source list"
    elif fault == "profile":
        group["profile"] = "f" * 64
    elif fault == "receipt":
        proof["input_binding"] = "f" * 64
    events = []

    class Session:
        resources = []
        _defer_vector_verification = deferred

        def prepare_index(self, directory):
            events.append("control_prepare")
            return prepared

        def prepare_group(self, directory, selected):
            events.append("native_prepare")
            assert selected == group
            return native

        def tokenize(self, sources, continuation):
            events.append("tokenize")
            assert sources == [dict(source=source, expected_hash=source["provenance"]["text_hash"])]
            assert continuation is None
            return args["page"]

        def infer(self, page):
            events.append("infer")
            assert page == args["page"]
            return args["inference"]

        def store(self, directory, operation, arguments):
            assert operation not in {"queue_intents", "queue_select"}, "duplicate control start"
            events.append(operation)
            if operation == "vector_publish":
                return (
                    dict(status="unavailable", reason="worker_deadline")
                    if publish_refused
                    else dict(status="ok")
                )
            if operation == "vector_receipt":
                assert "vector_publish" in events, "cold proof must follow actual publication"
                return proof
            assert operation == "queue_acknowledge" and arguments["receipt"] == proof
            return dict(status="ok", state="complete")

    return index_one(Session(), Path("/generation")), events


def test_background_publication_yields_before_cold_verification_without_ack():
    result, events = case(deferred=True)
    assert result["status"] == "in_progress" and result["state"] == "verification_pending"
    assert result["position"] == 0 and "receipt_id" not in result
    assert events == ["control_prepare", "native_prepare", "tokenize", "infer", "vector_publish"]


def test_background_next_tick_requires_actual_receipt_without_model_work():
    result, events = case(committed=True, deferred=True)
    assert (
        result["status"] == "ok"
        and result["state"] == "complete"
        and result["recovered_vector"] is True
    )
    assert events == ["control_prepare", "native_prepare", "queue_acknowledge"]


def test_background_unconfirmed_write_reports_refusal_without_ack_or_larger_retry():
    result, events = case(deferred=True, publish_refused=True)
    assert result["status"] == "unavailable" and result["reason"] == "worker_deadline"
    assert result["phase"] == "publish" and "queue_acknowledge" not in events
    assert events == ["control_prepare", "native_prepare", "tokenize", "infer", "vector_publish"]


@pytest.mark.parametrize("committed", [False, True])
def test_prepared_index_preserves_cold_post_write_proof_or_recovers_existing_page(committed):
    result, events = case(committed=committed)
    assert result["status"] == "ok" and result["state"] == "complete"
    assert result["recovered_vector"] is committed
    assert events == (
        ["control_prepare", "native_prepare", "queue_acknowledge"]
        if committed
        else [
            "control_prepare",
            "native_prepare",
            "tokenize",
            "infer",
            "vector_publish",
            "vector_receipt",
            "queue_acknowledge",
        ]
    )


@pytest.mark.parametrize(
    "fault",
    [
        "control",
        "control_shape",
        "unverified_selection",
        "selection_shape",
        "group_shape",
        "native",
        "native_shape",
        "source",
        "native_sources",
        "profile",
        "receipt",
    ],
)
def test_prepared_index_refusal_or_invalid_binding_never_acknowledges(fault):
    result, events = case(fault, committed=True)
    assert result["status"] != "ok"
    assert "queue_acknowledge" not in events and "tokenize" not in events and "infer" not in events


def test_registered_preparation_hooks_use_same_guarded_operations(monkeypatch):
    from cognitive_castle import semantic_runtime as runtime

    args, _ = _publication()
    group = dict(
        batch_key=args["batch_key"],
        references=args["references"],
        input_binding=args["page"]["input_binding"],
        position=0,
    )
    selected = Path("/trusted/generation")
    calls = []

    def stage(self, request, **limits):
        calls.append((request, limits))
        return dict(status="unavailable", reason="transport_boundary_only")

    monkeypatch.setattr(runtime.SemanticSession, "_run_stage", stage)
    session = runtime.LayoutSemanticSession(Path("/profile"), selected, envelope_hash="a" * 64)
    session.prepare_index(selected)
    first, limit = calls[-1]
    assert first["operation"] == "queue_prepare_index" and first["arguments"] == dict(
        envelope_hash="a" * 64
    )
    assert (limit["cap"], limit["rss"], limit["total"]) == (3, 128 * 1024**2, 192 * 1024**2)
    saved = copy.deepcopy(group)
    session.prepare_group(selected, group)
    second, limit = calls[-1]
    assert second["operation"] == "vector_layout_prepare"
    assert second["arguments"] == dict(
        batch_key=group["batch_key"],
        references=group["references"],
        input_binding=group["input_binding"],
        start_window=0,
        envelope_hash="a" * 64,
    )
    assert group == saved
    assert (limit["cap"], limit["rss"], limit["total"]) == (2, 192 * 1024**2, 256 * 1024**2)

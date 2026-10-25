"""Native single-view preparation, placement and cold recovery contracts."""

import copy
import sys

import pytest

from test_layout_source_planning import flow_case, request_case


def test_native_preparation_request_preserves_original_cursor():
    runtime, original, window = request_case("native")
    window["action"] = "prepare"
    saved = copy.deepcopy(original)
    result = runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    assert result == {**original, "layout": window}
    assert original == saved
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def test_preparation_does_not_change_sqlite_or_admitted_export_contract():
    runtime, original, window = request_case("sqlite")
    window["action"] = "prepare"
    with pytest.raises(ValueError):
        runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    runtime, original, window = request_case("native")
    window.update(action="prepare", allowed_plan_hashes=["a" * 64])
    with pytest.raises(ValueError):
        runtime.source_request(original["descriptor"], original["cursor"], layout=window)


def test_preparation_refuses_valid_mid_owner_cursor():
    runtime, original, window = request_case("native")
    window["action"] = "prepare"
    cursor = dict(original["cursor"], owner="a" * 64, entry=1, field="record")
    runtime.source_request(original["descriptor"], cursor)
    with pytest.raises(ValueError):
        runtime.source_request(original["descriptor"], cursor, layout=window)


def prepared_case(monkeypatch, **options):
    run, request, window, events, golden, reg, qualification = flow_case(
        "native", monkeypatch, **options
    )
    window["action"] = "prepare"
    result = run(request["descriptor"], request["cursor"], layout=window)
    return result, request, window, events, golden, reg, qualification


def test_preparation_keeps_exact_export_in_one_protected_view(monkeypatch):
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_codec import validate_layout_source_result

    result, request, window, events, golden, _, _ = prepared_case(monkeypatch)
    assert result["status"] == "ok"
    assert result["export"] == golden
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]
    assert events == ["open", "owner", "emit", "files", "close"]
    assert result["preparation_hash"] == migration.hashed([result["plans"], golden])
    validate_layout_source_result(result, window, request["descriptor"], request["cursor"])
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize("damage", ["plan", "body", "hash", "cursor", "unknown"])
def test_prepared_slice_refuses_changed_association(monkeypatch, damage):
    from cognitive_castle.lance_layout_codec import validate_layout_source_result

    result, request, window, _, _, _, _ = prepared_case(monkeypatch)
    assert result["status"] == "ok"
    if damage == "plan":
        result["plans"][0]["expansion"]["physical_rows"] -= 1
    elif damage == "body":
        result["export"]["entries"][0]["text"] += "changed"
    elif damage == "hash":
        result["preparation_hash"] = "f" * 64
    elif damage == "cursor":
        result["cursor_after"] = result["export"]["cursor_after"]
    else:
        result["persisted"] = True
    with pytest.raises(ValueError):
        validate_layout_source_result(result, window, request["descriptor"], request["cursor"])


def test_preparation_eof_keeps_original_terminal_evidence(monkeypatch):
    from cognitive_castle import migration_codec as migration

    result, request, _, events, _, reg, _ = prepared_case(monkeypatch, eof=True)
    assert result["status"] == "ok" and result["plans"] == []
    assert result["preparation_hash"] is None
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]
    assert migration.validate_export(result["export"], reg, request["cursor"])["complete"]
    assert events == ["open", "files", "close"]


def test_preparation_postcheck_discards_all_transient_evidence(monkeypatch):
    result, request, _, events, _, _, _ = prepared_case(monkeypatch, post_failure=True)
    assert "emit" in events
    assert result["status"] == "stale" and result["reason"] == "source_changed_after_summary"
    assert result["plans"] == [] and result["export"] is None
    assert result["preparation_hash"] is None
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]


def test_preparation_guard_refusal_clears_checksum_and_body():
    runtime, original, window = request_case("native")
    window["action"] = "prepare"
    request = runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    result = runtime.source_result(request, {"status": "unavailable", "reason": "worker_deadline"})
    assert result["reason"] == "worker_deadline" and result["preparation_hash"] is None
    assert result["plans"] == [] and result["export"] is None
    assert result["cursor_before"] == result["cursor_after"] == original["cursor"]


@pytest.mark.parametrize("failure", ["memory", "frame"])
def test_preparation_wrapper_refusal_never_keeps_a_body(monkeypatch, failure):
    from cognitive_castle.sqlite_export_codec import layout_result

    run, request, window, _, _, _, _ = flow_case("native", monkeypatch)
    window["action"] = "prepare"
    calls = []

    def fail_once(*args):
        calls.append(1)
        if len(calls) == 1:
            raise MemoryError if failure == "memory" else ValueError
        return layout_result(*args)

    run.__globals__["layout_result"] = fail_once
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "qualification_required"
    assert result["plans"] == [] and result["export"] is None and result["preparation_hash"] is None
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]


def native_session(monkeypatch, *, pending=False, admitted=False):
    from test_layout_coordinator import PendingSession
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration

    run, request, window, events, golden, reg, _ = flow_case("native", monkeypatch)
    planning = run(request["descriptor"], request["cursor"], layout=window)
    assert planning["status"] == "ok"
    session = PendingSession(new_owner=True, fresh=0 if pending else 1)
    envelope = session.state["layout"]["envelope"]
    envelope["qualification_hash"] = window["qualification_hash"]
    session.plan = planning["plans"][0]
    session.target = layout.target(envelope, 0, [session.plan])
    session.reg, session.exported = reg, golden
    session.intent = layout.prepare_layout_interval(reg, golden, session.target)
    session.state.update(
        registration=reg,
        pending=session.intent if pending else None,
        staging_cursor=request["cursor"],
        completed_frontier=request["cursor"],
    )
    session.state["layout"]["envelope_hash"] = migration.hashed(envelope)
    session.state["native_layout"].update(
        qualification_hash=window["qualification_hash"],
        plan=session.plan if pending or admitted else None,
        target=session.target if pending or admitted else None,
    )
    session.batch.update(
        receipt=layout.expected_receipt(session.intent),
        routes=[
            dict(
                route,
                format=layout.ROUTE,
                row_hash=route["row_hash"] or "c" * 64,
                partition_id=0,
                version=7,
                receipt=layout.batch_receipt_id(session.intent),
                state="staged",
            )
            for route in session.intent["routes"]
        ],
    )
    events.clear()

    def source(descriptor, cursor, actual_window):
        session.calls.append(("layout_source", copy.deepcopy(actual_window)))
        return run(descriptor, cursor, layout=actual_window)

    session.layout_source = source
    return session, events


def test_new_native_owner_binds_before_any_durable_write_with_one_source_view(monkeypatch):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session, events = native_session(monkeypatch)
    result = _layout_fixture_tick(session, session.reg)
    assert result == {"status": "ok", "intent_id": session.intent["intent_id"]}
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "queue_layout_bind",
        "queue_layout_prepare",
        "layout_import_batch",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    assert session.calls[1][1]["action"] == "prepare"
    assert events.count("open") == events.count("owner") == events.count("emit") == 1
    for op, args in session.calls:
        if op.startswith("queue_"):
            assert "preparation_hash" not in repr(args) and "BODY_SENTINEL" not in repr(args)


@pytest.mark.parametrize("fault", ["refused", "late", "target"])
def test_prepared_native_body_cannot_bypass_placement(monkeypatch, fault):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session, events = native_session(monkeypatch)
    original = session.store

    def store(directory, operation, arguments):
        result = original(directory, operation, arguments)
        if operation == "queue_layout_bind":
            if fault == "refused":
                return {"status": "unavailable", "reason": "placement_refused"}
            if fault == "late":
                session.remaining = 0
            if fault == "target":
                result = copy.deepcopy(result)
                result["targets"][0]["owner_plan_hashes"] = ["f" * 64]
        return result

    session.store = store
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] in {"unavailable", "conflict"}
    assert events.count("open") == 1
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "queue_layout_bind",
    ]


@pytest.mark.parametrize("pending", [False, True])
def test_admitted_native_owner_regenerates_original_export_on_new_tick(monkeypatch, pending):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session, events = native_session(monkeypatch, pending=pending, admitted=True)
    result = _layout_fixture_tick(session, session.reg)
    assert result == {"status": "ok", "intent_id": session.intent["intent_id"]}
    assert session.calls[1][1]["action"] == "export"
    assert events.count("open") == events.count("owner") == events.count("emit") == 1
    assert "queue_layout_bind" not in [op for op, _ in session.calls]
    if pending:
        assert "queue_layout_prepare" not in [op for op, _ in session.calls]


def test_changed_prepared_body_after_binding_cannot_be_written(monkeypatch):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session, _ = native_session(monkeypatch)
    original_source, original_store = session.layout_source, session.store
    responses = []

    def source(*args):
        result = original_source(*args)
        responses.append(result)
        return result

    def store(directory, operation, arguments):
        result = original_store(directory, operation, arguments)
        if operation == "queue_layout_bind":
            responses[0]["export"]["entries"][0]["text"] += "changed after binding"
        return result

    session.layout_source, session.store = source, store
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] == "conflict" and result["phase"] == "source"
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "queue_layout_bind",
    ]

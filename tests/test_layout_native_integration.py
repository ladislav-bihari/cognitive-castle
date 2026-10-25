"""Disposable Task3 integration fixture; execution needs a frozen root scope.

The host never imports a storage engine. Publication uses the real facade and
one real SemanticSession per tick; fault injection only hides completed replies.
These small fixture storage ceilings are not a production capacity permit.
"""

import copy
import json
import os
from pathlib import Path
import sys
import time


def _operator_worker(expected_hash):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import semantic_store_worker as worker
    from cognitive_castle.lance_layout_directory import _register_layout_authority
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_queue import SemanticQueue

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if request.keys() != {"directory", "operation", "arguments", "_host_pid"}:
        raise ValueError("fixed operator fields")
    if request["operation"] != "register" or request["arguments"].keys() != {
        "registration",
        "envelope",
        "qualification",
    }:
        raise ValueError("fixed operator operation")
    worker._install_guards("queue")
    refused = worker._admit(request, "queue")
    if refused is not None:
        return refused
    import sqlite3

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    args = request["arguments"]
    store = SemanticQueue(Path(request["directory"]), writable=True)
    opened = store.migration_open(args["registration"])
    if opened["status"] != "ok":
        return opened
    return store._run(
        True,
        lambda conn: _register_layout_authority(
            conn,
            args["registration"]["job_id"],
            args["envelope"],
            args["qualification"],
            expected_hash=expected_hash,
        ),
    )


def _route_fault_worker(expected_hash):
    """One fixed route fault or read-only absence observation in the fixture."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import bulk_exact_codec as bulk, semantic_store_worker as worker
    from cognitive_castle.lance_layout_directory import _NativeLayoutDirectory
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_queue import SemanticQueue

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if request.keys() != {"directory", "operation", "arguments", "_host_pid"}:
        raise ValueError("fixed fault fields")
    if request["operation"] not in {"lose_route", "resolve_absence"} or request[
        "arguments"
    ].keys() != {"id"}:
        raise ValueError("fixed fault operation")
    identity = bulk.identity(request["arguments"]["id"], prefix="memory")
    worker._install_guards("queue")
    refused = worker._admit(request, "queue")
    if refused is not None:
        return refused
    import sqlite3

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216

    def lose(conn):
        authority = _NativeLayoutDirectory(conn)
        if authority.envelope["qualification_hash"] != expected_hash:
            raise ValueError("independent fixture qualification")
        if request["operation"] == "resolve_absence":
            return authority.resolve([identity])
        claim = conn.execute("SELECT state FROM layout_claims WHERE id=?", (identity,)).fetchone()
        if claim is None or claim[0] != "confirmed":
            raise ValueError("confirmed fixture claim")
        changed = conn.execute("DELETE FROM layout_routes WHERE id=?", (identity,)).rowcount
        if changed != 1:
            raise ValueError("one derived route only")
        assert (
            conn.execute("SELECT state FROM layout_claims WHERE id=?", (identity,)).fetchone()[0]
            == "confirmed"
        )
        return {"status": "ok", "claim_retained": True}

    write = request["operation"] == "lose_route"
    return SemanticQueue(Path(request["directory"]), writable=write)._run(write, lose)


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3 or sys.argv[1] not in {"operator", "route_fault"}:
            raise ValueError("fixed operator invocation")
        output = (_operator_worker if sys.argv[1] == "operator" else _route_fault_worker)(
            sys.argv[2]
        )
    except Exception as exc:
        output = {"status": "unavailable", "reason": "fixture_" + type(exc).__name__}
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
    if len(raw) > 512 * 1024:
        raw = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - the supervised operator exits before test imports.

_CALLS = 0
_MAX_CALLS = 150


def _start(operation):
    global _CALLS
    _CALLS += 1
    assert _CALLS <= _MAX_CALLS
    print("LAYOUT_NATIVE_START " + json.dumps(dict(call=_CALLS, operation=operation)), flush=True)
    return _CALLS


def _record(session, operation, result, previous, call, *, expected_reason=None):
    metrics = session.resources[previous:]
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation=operation,
                status=result.get("status"),
                reason=result.get("reason"),
                resources=metrics,
            ),
            separators=(",", ":"),
        ),
        flush=True,
    )
    # A pressure/admission refusal is not evidence for a negative protocol case.
    assert len(metrics) == 1, result
    assert result.get("reason") == expected_reason, result


class ObservedLayoutSession:
    def __init__(self, session, *, loss=None):
        self.session = session
        self.loss = loss
        self.calls = []
        self.arguments = {}
        self.results = {}
        self.counts = {}

    @property
    def remaining(self):
        return self.session.remaining

    def _ready(self):
        return self.session._ready()

    def call(self, name, method, *args):
        self.counts[name] = self.counts.get(name, 0) + 1
        point = name + ":" + str(self.counts[name])
        arguments = copy.deepcopy(args)
        previous = len(self.session.resources)
        call = _start(point)
        result = method(*args)
        _record(self.session, point, result, previous, call)
        self.calls.append(point)
        self.arguments[point] = arguments
        self.results[point] = copy.deepcopy(result)
        if self.loss == point:
            assert result["status"] == "ok", result
            return {"status": "unavailable", "reason": "fixture_lost_response"}
        return result

    def store(self, generation, operation, arguments):
        return self.call(operation, self.session.store, generation, operation, arguments)

    def exact(self, generation, operation, arguments):
        return self.call(operation, self.session.exact, generation, operation, arguments)

    def layout_source(self, descriptor, cursor, window):
        return self.call("layout_source", self.session.layout_source, descriptor, cursor, window)


def _qualification(generation):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration

    info = generation.stat()
    identity = dict(path=str(generation), device=info.st_dev, inode=info.st_ino)
    mib = 1024**2
    qualification = dict(
        format=layout.QUALIFICATION,
        generation=identity,
        schema="castle-exact-six-string-v1",
        storage_mode="2.0",
        partition=dict(
            max_physical_rows=4096,
            max_fragments=64,
            max_versions=128,
            max_artifact_entries=512,
            max_artifact_depth=6,
            max_logical_bytes=32 * mib,
            max_allocated_bytes=32 * mib,
            max_file_bytes=4 * mib,
            max_batch_receipts=64,
        ),
        reservations={
            operation: dict(
                physical_rows=513 if operation == "append" else 0,
                fragments=1 if operation == "append" else 0,
                versions=1,
                artifact_entries=16,
                logical_bytes=2 * mib,
                allocated_bytes=2 * mib,
                file_bytes=4 * mib,
            )
            for operation in ("append", "maintenance")
        },
        max_remote_opens=2,
        working_temp_bytes=4 * mib,
        source_cost=dict(maintenance_every_batches=2, frame_overhead_bytes=32768),
    )
    envelope = dict(
        format=layout.ENVELOPE,
        generation=identity,
        qualification_hash=migration.hashed(qualification),
        max_partitions=2,
        limits=dict(
            physical_rows=4096,
            encoded_bytes=32 * mib,
            retained_bytes=32 * mib,
            batches=64,
            fragments=64,
            versions=128,
            artifacts=512,
            index_outputs=64,
            index_bytes=32 * mib,
            # Retained spill and directory artifacts remain charged next tick.
            temp_bytes=(4 + 4 + 1) * mib,
        ),
    )
    layout.validate_qualification(
        qualification, envelope, expected_hash=migration.hashed(qualification)
    )
    return qualification, envelope


def _session(generation):
    from cognitive_castle.semantic_runtime import SemanticSession

    return SemanticSession(generation / "unused-profile", interactive=False)


def _setup(tmp_path, *, large=10, native=False):
    from cognitive_castle import migration_codec as migration

    operation = "source_fixture_native" if native else "source_fixture"
    call = _start(operation)
    if native:
        from test_native_source_export import make_fixture

        # Existing separately supervised setup:512/640MiB and at most5s.
        source = make_fixture(tmp_path / "source", "single")
    else:
        from test_sqlite_source_export import fixture

        source = fixture(tmp_path, large=large)
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation=operation,
                status=source["status"],
                reason=source.get("reason"),
                resources=[source.get("resources")],
            )
        ),
        flush=True,
    )
    assert isinstance(source.get("resources"), dict), source
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    (generation / "exact/partitions").mkdir(mode=0o700)
    (generation / "exact/tmp").mkdir(mode=0o700)
    qualification, envelope = _qualification(generation)
    for partition in range(envelope["max_partitions"]):
        (generation / "exact/partitions" / str(partition)).mkdir(mode=0o700)
    registration = migration.registration(source["descriptor"], str(generation))
    # Independent trusted operator argv is outside the ordinary request schema.
    expected_hash = migration.hashed(qualification)
    with _session(generation) as session:
        call = _start("operator_register")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="register",
                arguments=dict(
                    registration=registration,
                    envelope=envelope,
                    qualification=qualification,
                ),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                "operator",
                expected_hash,
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        _record(session, "operator_register", result, 0, call)
    assert result["status"] == "ok", result
    return source, generation, registration, envelope


def _tick(registration, *, loss=None):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    generation = Path(registration["destination"])
    started = time.monotonic()
    with _session(generation) as session:
        observed = ObservedLayoutSession(session, loss=loss)
        result = _layout_fixture_tick(observed, registration)
        elapsed = time.monotonic() - started
        print(
            "LAYOUT_NATIVE_TICK "
            + json.dumps(
                dict(
                    status=result.get("status"),
                    reason=result.get("reason"),
                    elapsed=elapsed,
                    calls=observed.calls,
                )
            ),
            flush=True,
        )
        assert elapsed <= 5, result
    return result, observed


def _queue_state(registration):
    generation = Path(registration["destination"])
    with _session(generation) as session:
        return ObservedLayoutSession(session).store(
            generation, "queue_migration_state", dict(job_id=registration["job_id"])
        )


def _queue(generation, operation, arguments):
    with _session(generation) as session:
        return ObservedLayoutSession(session).store(generation, operation, arguments)


def _attach_shared_source(base, generation, envelope, *, original_history=False):
    from cognitive_castle import migration_codec as migration
    from test_sqlite_source_export import fixture

    base.mkdir(mode=0o700)
    call = _start("source_fixture_second")
    source = (
        fixture(base, migration_inventory="protocol", large=5000)
        if original_history
        else fixture(base)
    )
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation="source_fixture_second",
                status=source["status"],
                reason=source.get("reason"),
                resources=[source.get("resources")],
            )
        ),
        flush=True,
    )
    assert isinstance(source.get("resources"), dict), source
    qualification, same_envelope = _qualification(generation)
    assert same_envelope == envelope
    registration = migration.registration(source["descriptor"], str(generation))
    with _session(generation) as session:
        call = _start("operator_attach_source")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="register",
                arguments=dict(
                    registration=registration,
                    envelope=envelope,
                    qualification=qualification,
                ),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                "operator",
                envelope["qualification_hash"],
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        _record(session, "operator_attach_source", result, 0, call)
    assert result["status"] == "ok", result
    return source, registration


def _route_fixture(generation, envelope, identity, *, operation):
    assert operation in {"lose_route", "resolve_absence"}
    with _session(generation) as session:
        call = _start("fixture_" + operation)
        result = session._run_stage(
            dict(directory=str(generation), operation=operation, arguments=dict(id=identity)),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).resolve()),
                "route_fault",
                envelope["qualification_hash"],
            ],
            cap=3 if operation == "lose_route" else 2,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        _record(session, "fixture_" + operation, result, 0, call)
    return result


def _read(generation, operation, arguments, *, legacy_home=None):
    with _session(generation) as session:
        observed = ObservedLayoutSession(session)
        if legacy_home is not None:
            return observed.exact(legacy_home, operation, arguments)
        return observed.exact(
            generation, "layout_read", dict(operation=operation, arguments=arguments)
        )


def _assert_grouped_proof(observed, pending, original_owner, version):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration

    assert observed.arguments["layout_verify_owners:1"][2] == dict(
        intent_id=pending["intent_id"], version=version
    )
    actual = observed.results["layout_verify_owners:1"]
    proofs = layout.validate_grouped_owner_proof(
        actual, pending, {"finalizations": [original_owner]}, version
    )
    assert proofs == [
        dict(
            owner=original_owner["owner"],
            manifest_hash=migration.hashed(original_owner),
            partition_id=pending["target"]["partition_id"],
            version=version,
            live_receipt_ids=[]
            if original_owner["live"] is None
            else original_owner["live"]["receipt"]["ids"],
        )
    ]
    assert "manifest" not in actual and "BODY_SENTINEL" not in str(actual)


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_layout_first_live_owner_uses_real_source_and_proof_ack(tmp_path):
    from cognitive_castle.canonical_records import prepare_remember

    source, generation, registration, _ = _setup(tmp_path)
    original = prepare_remember(
        "nine",
        ("a" * 8191 + "€") * 7 + "b" * 8178,
        {"workspace": "original", "note": "\x00"},
    )
    assert original.receipt == source["receipt"]
    result, observed = _tick(registration)
    assert result["status"] == "ok", result
    imports = [
        value
        for point, value in observed.results.items()
        if point.startswith("layout_import_batch:")
    ]
    assert [value["outcome"] for value in imports] == ["fresh_append", "existing_verified"]
    assert imports[0]["routes"] == [] and imports[1]["routes"]
    assert observed.calls[-4:] == [
        "layout_import_batch:1",
        "layout_import_batch:2",
        "layout_verify_owners:1",
        "queue_layout_ack:1",
    ]
    assert (
        observed.arguments["layout_import_batch:1"] == observed.arguments["layout_import_batch:2"]
    )
    import_args = observed.arguments["layout_import_batch:1"][2]
    assert len(import_args["finalizations"]) == 1
    owner = import_args["finalizations"][0]
    version = imports[1]["routes"][-1]["version"]
    _assert_grouped_proof(
        observed, observed.arguments["queue_layout_prepare:1"][2]["intent"], owner, version
    )
    expected_receipt = dict(status="ok", digest=original.digest, receipt=original.receipt)
    with _session(generation) as session:
        checked = ObservedLayoutSession(session)
        state = checked.store(
            generation, "queue_migration_state", dict(job_id=registration["job_id"])
        )
    assert state["status"] == "ok" and state["pending"] is None, state
    assert state["counts"]["live_owners"] == 1
    assert state["owner_references"] == state["semantic_pending"] == 1
    live = owner["live"]
    assert live == dict(
        namespace="operator", key="nine", digest=original.digest, receipt=original.receipt
    )
    with _session(generation) as session:
        checked = ObservedLayoutSession(session)
        receipt = checked.exact(
            generation,
            "layout_read",
            dict(
                operation="receipt",
                arguments=dict(namespace=live["namespace"], key=live["key"]),
            ),
        )
    assert receipt == expected_receipt
    assert source["descriptor"] == registration["descriptor"]
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_layout_lost_append_keeps_pending_and_blocks_legacy_reads(tmp_path):
    from cognitive_castle.migration_codec import initial_source_cursor

    source, generation, registration, _ = _setup(tmp_path)
    lost, first = _tick(registration, loss="layout_import_batch:1")
    assert lost["status"] == "unavailable" and lost["reason"] == "fixture_lost_response", lost
    assert first.results["layout_import_batch:1"]["outcome"] == "fresh_append"
    assert not any(
        point.startswith(("layout_verify_owners:", "queue_layout_ack:")) for point in first.calls
    )
    before = _queue_state(registration)
    assert before["status"] == "ok" and before["pending"] is not None, before
    assert before["counts"]["live_owners"] == before["owner_references"] == 0
    assert (
        before["staging_cursor"]
        == before["completed_frontier"]
        == initial_source_cursor(registration)
    )
    assert before["last_ack"] is None and before["semantic_pending"] == 0
    pending = before["pending"]
    home = generation / "exact/partitions" / str(pending["target"]["partition_id"])
    actual = first.arguments["layout_import_batch:1"][2]
    owner = actual["finalizations"][0]
    first_id = source["receipt"]["ids"][0]
    for operation, arguments in (
        ("bulk_owner", dict(owner=owner["owner"])),
        ("read_sources", dict(ids=[first_id])),
        ("receipt", dict(namespace="operator", key="nine")),
    ):
        refused = _read(generation, operation, arguments, legacy_home=home)
        assert refused == {"status": "unavailable"}, refused
    gated = _read(generation, "bulk_owner", dict(owner=owner["owner"]))
    assert gated["status"] == "unavailable", gated
    recovered, retry = _tick(registration)
    assert recovered["status"] == "ok", recovered
    assert retry.arguments["layout_import_batch:1"] == first.arguments["layout_import_batch:1"]
    exported_sources = [
        arguments
        for point, arguments in first.arguments.items()
        if point.startswith("layout_source:") and arguments[2]["action"] == "export"
    ]
    assert len(exported_sources) == 1
    assert retry.arguments["layout_source:1"] == exported_sources[0]
    imports = [
        value for point, value in retry.results.items() if point.startswith("layout_import_batch:")
    ]
    assert len(imports) == 1 and imports[0]["outcome"] == "existing_verified"
    assert retry.calls[-3:] == [
        "layout_import_batch:1",
        "layout_verify_owners:1",
        "queue_layout_ack:1",
    ]
    assert imports[0]["receipt"] == first.results["layout_import_batch:1"]["receipt"]
    # The first empty-home append is still the only native version after retry.
    assert {route["version"] for route in imports[0]["routes"]} == {1}
    _assert_grouped_proof(retry, pending, owner, 1)
    after = _queue_state(registration)
    assert after["status"] == "ok" and after["pending"] is None, after
    assert after["counts"]["live_owners"] == after["owner_references"] == 1
    assert after["semantic_pending"] == 1
    assert after["staging_cursor"] == after["completed_frontier"] == pending["cursor_after"]
    assert after["last_ack"]["intent_id"] == pending["intent_id"]


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_layout_lost_ack_replays_committed_result_without_recount(tmp_path):
    _, generation, registration, _ = _setup(tmp_path)
    lost, observed = _tick(registration, loss="queue_layout_ack:1")
    assert lost == {"status": "unavailable", "reason": "fixture_lost_response"}, lost
    actual_result = observed.results["queue_layout_ack:1"]
    before = _queue_state(registration)
    assert before["status"] == "ok" and before["pending"] is None, before
    assert before["counts"]["live_owners"] == before["owner_references"] == 1
    with _session(generation) as session:
        replayed = ObservedLayoutSession(session).store(
            generation, "queue_layout_ack", observed.arguments["queue_layout_ack:1"][2]
        )
    assert replayed == actual_result
    after = _queue_state(registration)
    for key in (
        "pending",
        "counts",
        "completed_frontier",
        "staging_cursor",
        "last_ack",
        "owner_references",
        "semantic_pending",
    ):
        assert after[key] == before[key]


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_layout_rebuild_debt_and_cross_partition_original_version_reuse(tmp_path):
    first_call = _CALLS
    source, generation, registration, envelope = _setup(tmp_path, large=5000)
    published, first = _tick(registration)
    assert published["status"] == "ok", published
    lost, history_fresh = _tick(registration, loss="layout_import_batch:1")
    assert lost["status"] == "unavailable" and lost["reason"] == "fixture_lost_response", lost
    assert history_fresh.results["layout_import_batch:1"]["outcome"] == "fresh_append"
    # The native V2 append exists, but metadata still has V1 and a pending intent.
    reset = _queue(generation, "queue_layout_rebuild_prepare", dict(partition_id=0, version=1))
    assert reset["status"] == "ok", reset
    published, second = _tick(registration)
    assert published["status"] == "ok", published
    assert (
        second.arguments["layout_import_batch:1"]
        == history_fresh.arguments["layout_import_batch:1"]
    )
    first_routes = first.results["layout_import_batch:2"]["routes"]
    second_routes = second.results["layout_import_batch:1"]["routes"]
    assert 32 < len(first_routes) + len(second_routes) <= 64
    assert {route["partition_id"] for route in first_routes + second_routes} == {0}
    version = second_routes[-1]["version"]
    assert version == 2
    identity = source["receipt"]["ids"][0]
    original = {
        **next(route for route in first_routes if route["id"] == identity),
        "state": "published",
    }
    assert original["version"] == 1
    absent_id = "memory:" + "e" * 64
    assert absent_id not in {route["id"] for route in first_routes + second_routes}
    # A proof of the new batch must not erase an earlier reconstruction debt.
    debt = _route_fixture(generation, envelope, absent_id, operation="resolve_absence")
    assert debt == dict(
        status="unavailable", routes=[], absent=[], unavailable=[absent_id], claims=[]
    )
    before = _queue_state(registration)
    assert before["counts"]["live_owners"] == before["counts"]["historical_occurrences"] == 1
    removed = _route_fixture(generation, envelope, identity, operation="lose_route")
    assert removed == {"status": "ok", "claim_retained": True}, removed
    missing = _queue(generation, "queue_layout_routes", dict(ids=[identity]))
    assert missing["status"] == "unavailable" and missing["unavailable"] == [identity]
    assert missing["routes"] == []
    started = _queue(
        generation, "queue_layout_rebuild_prepare", dict(partition_id=0, version=version)
    )
    assert started["status"] == "ok", started
    stale_pin = _read(generation, "directory_page", dict(partition_id=0, version=1, cursor=0))
    assert stale_pin == {"status": "stale"}, stale_pin
    old = _read(generation, "directory_page", dict(partition_id=0, version=version, cursor=0))
    assert old["status"] == "ok", old
    assert len(old["page"]["rows"]) == 32 and old["page"]["complete"] is False
    restarted = _queue(
        generation, "queue_layout_rebuild_prepare", dict(partition_id=0, version=version)
    )
    assert restarted["status"] == "ok" and restarted["rebuild_token"] != started["rebuild_token"]
    stale = _queue(generation, "queue_layout_reconcile", dict(page=old["page"]))
    assert stale["status"] == "conflict", stale
    first_page = _read(
        generation, "directory_page", dict(partition_id=0, version=version, cursor=0)
    )
    assert first_page["status"] == "ok", first_page
    partial = _queue(generation, "queue_layout_reconcile", dict(page=first_page["page"]))
    assert partial == dict(status="ok", next_cursor=32, registry_ready=False)
    replayed = _queue(generation, "queue_layout_reconcile", dict(page=first_page["page"]))
    assert replayed == partial
    uncertain = _route_fixture(generation, envelope, absent_id, operation="resolve_absence")
    assert uncertain == dict(
        status="unavailable", routes=[], absent=[], unavailable=[absent_id], claims=[]
    )
    interrupted = _queue_state(registration)
    for key in (
        "pending",
        "counts",
        "staging_cursor",
        "completed_frontier",
        "last_ack",
        "owner_references",
        "semantic_pending",
    ):
        assert interrupted[key] == before[key]
    final_page = _read(
        generation, "directory_page", dict(partition_id=0, version=version, cursor=32)
    )
    assert final_page["status"] == "ok" and final_page["page"]["complete"] is True, final_page
    assert 0 < len(final_page["page"]["rows"]) <= 32
    complete = _queue(generation, "queue_layout_reconcile", dict(page=final_page["page"]))
    assert complete == dict(
        status="ok", next_cursor=final_page["page"]["cursor_after"], registry_ready=True
    )
    absent = _route_fixture(generation, envelope, absent_id, operation="resolve_absence")
    assert absent == dict(status="ok", routes=[], absent=[absent_id], unavailable=[], claims=[])
    restored = _queue(generation, "queue_layout_routes", dict(ids=[identity]))
    assert restored == dict(status="ok", routes=[original], unavailable=[])
    visible = _read(generation, "read_sources", dict(ids=[identity]))
    assert visible["status"] == "ok", visible
    assert visible["sources"] == [
        next(item for item in source["original_public"] if item["id"] == identity)
    ]
    after = _queue_state(registration)
    for key in (
        "pending",
        "counts",
        "staging_cursor",
        "completed_frontier",
        "last_ack",
        "owner_references",
        "semantic_pending",
    ):
        assert after[key] == before[key]
    # Persist a real index atV3 so later reuse must resolve an older original
    # history routeV2 against this newer registered home0 pin, remotely.
    prepared_index = _queue(
        generation, "queue_layout_maintenance_prepare", dict(partition_id=0, version=2)
    )
    assert prepared_index["status"] == "ok", prepared_index
    with _session(generation) as session:
        maintained = ObservedLayoutSession(session).exact(
            generation, "layout_maintain", dict(partition_id=0)
        )
    assert maintained["status"] == "ok", maintained
    indexed = maintained["pin"]
    assert (indexed["version"], indexed["physical_rows"], indexed["fragments"]) == (
        3,
        len(first_routes) + len(second_routes),
        2,
    )
    assert indexed["index"]["segments"] == 1 and indexed["index"]["indexed_fragments"] == 2
    assert indexed["index"]["tail_rows"] == indexed["index"]["tail_fragments"] == 0
    persisted_index = _read(generation, "directory_page", dict(partition_id=0, version=3, cursor=0))
    assert persisted_index["status"] == "ok" and persisted_index["page"]["pin"] == indexed
    assert len(persisted_index["page"]["rows"]) == 32
    assert persisted_index["page"]["complete"] is False
    confirmed_index = _queue(
        generation, "queue_layout_reconcile", dict(page=persisted_index["page"])
    )
    assert confirmed_index["status"] == "ok" and confirmed_index["registry_ready"] is True
    # Two conservative owner reservations consumed 8MiB of the fixed9MiB
    # temp allowance. A third owner rolls over without optimistic reuse savings.
    shared, new_registration = _attach_shared_source(
        tmp_path / "second", generation, envelope, original_history=True
    )
    assert new_registration["source_binding"] != registration["source_binding"]
    # The protected fixture keeps an actual original historical occurrence and
    # its own original chunk/caller relation. No LIVE owner is relabelled.
    history_owner = second.arguments["layout_import_batch:1"][2]["finalizations"][0]
    original_history = next(
        item for item in source["history"] if item["identity"] == history_owner["metadata_id"]
    )
    shared_history = next(
        item for item in shared["history"] if item["identity"] == history_owner["metadata_id"]
    )
    assert original_history == shared_history
    assert history_owner["metadata_id"] == min(item["identity"] for item in source["history"])
    assert history_owner["metadata_id"] == min(item["identity"] for item in shared["history"])
    history_id = original_history["ids"][0]
    history_original = {
        **next(route for route in second_routes if route["id"] == history_id),
        "state": "published",
    }
    assert history_original["partition_id"] == 0 and history_original["version"] == 2
    original_source = next(item for item in source["original_public"] if item["id"] == history_id)
    assert (
        next(item for item in shared["original_public"] if item["id"] == history_id)
        == original_source
    )
    reused, third = _tick(new_registration)
    assert reused["status"] == "ok", reused
    prepared = third.arguments["queue_layout_prepare:1"][2]["intent"]
    assert prepared["target"]["partition_id"] == 1
    actual_routes = third.results["layout_import_batch:2"]["routes"]
    assert next(route for route in actual_routes if route["id"] == history_id) == history_original
    assert actual_routes[-1]["partition_id"] == 1 and actual_routes[-1]["version"] == 1
    physical_home = _read(generation, "directory_page", dict(partition_id=1, version=1, cursor=0))
    assert physical_home["status"] == "ok" and physical_home["page"]["complete"] is True, (
        physical_home
    )
    assert history_id not in {row["id"] for row in physical_home["page"]["rows"]}
    stable = _queue(generation, "queue_layout_routes", dict(ids=[history_id]))
    assert stable == dict(status="ok", routes=[history_original], unavailable=[])
    visible = _read(generation, "read_sources", dict(ids=[history_id]))
    assert visible["status"] == "ok", visible
    assert visible["sources"] == [original_source]
    new_state = _queue_state(new_registration)
    assert new_state["counts"]["live_owners"] == 0
    assert new_state["counts"]["historical_occurrences"] == new_state["owner_references"] == 1
    original_state = _queue_state(registration)
    for key in (
        "pending",
        "counts",
        "staging_cursor",
        "completed_frontier",
        "last_ack",
        "owner_references",
        "semantic_pending",
    ):
        assert original_state[key] == before[key]
    assert _CALLS - first_call == 63


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_source_layout_wrapper_keeps_original_address_and_text(tmp_path):
    from cognitive_castle.canonical_records import _hash, _json
    from test_native_source_export import artifact_fingerprint

    first_call = _CALLS
    source, generation, registration, _ = _setup(tmp_path, native=True)
    source_before = artifact_fingerprint(source["descriptor"])
    assert len(source["rows"]) == 1
    original = source["rows"][0]
    expected_text = "a" * 8189 + "€🙂\x00e\u0301" + "z" * 8200
    assert original["text_hash"] == _hash(expected_text)
    published, observed = _tick(registration)
    assert published["status"] == "ok", published
    assert observed.calls.count("layout_source:1") == 1 and "layout_source:2" not in observed.calls
    assert observed.arguments["layout_source:1"][2]["action"] == "prepare"
    planned = observed.results["layout_source:1"]["plans"]
    assert len(planned) == 1
    assert planned[0]["source_key"] == [_json(original["following"])]
    assert planned[0]["text_hash"] == original["text_hash"]
    first = observed.arguments["layout_import_batch:1"]
    assert first == observed.arguments["layout_import_batch:2"]
    records = [row for row in first[2]["entries"] if row["kind"] == "bulk_record"]
    assert [(row["id"], row["text"], row["provenance"]) for row in records] == [
        (record["id"], record["text"], _json(record["provenance"]))
        for record in original["records"]
    ]
    owner = first[2]["finalizations"][0]
    assert owner["live"] is None and owner["text_hash"] == original["text_hash"]
    assert owner["text_chars"] == len(expected_text)
    assert owner["text_bytes"] == len(expected_text.encode())
    assert observed.calls[-4:] == [
        "layout_import_batch:1",
        "layout_import_batch:2",
        "layout_verify_owners:1",
        "queue_layout_ack:1",
    ]
    assert observed.results["layout_import_batch:1"]["outcome"] == "fresh_append"
    existing = observed.results["layout_import_batch:2"]
    assert existing["outcome"] == "existing_verified"
    version = existing["routes"][-1]["version"]
    _assert_grouped_proof(
        observed, observed.arguments["queue_layout_prepare:1"][2]["intent"], owner, version
    )
    state = _queue_state(registration)
    assert state["status"] == "ok" and state["pending"] is None, state
    assert state["counts"]["historical_occurrences"] == state["owner_references"] == 1
    assert state["counts"]["members"] == len(original["records"])
    assert state["staging_cursor"]["key"] == [_json(original["following"])]
    assert state["completed_frontier"] == state["staging_cursor"]
    lost, exhausted = _tick(registration, loss="queue_layout_ack:1")
    assert lost == {"status": "unavailable", "reason": "fixture_lost_response"}, lost
    assert exhausted.calls == ["queue_layout_resume:1", "layout_source:1", "queue_layout_ack:1"]
    assert exhausted.arguments["queue_layout_ack:1"][2]["intent_id"] is None
    replayed, terminal = _tick(registration)
    assert replayed == exhausted.results["queue_layout_ack:1"]
    assert terminal.calls == ["queue_layout_resume:1"]
    final = _queue_state(registration)
    assert final["state"] == "snapshot_export_exhausted" and final["last_ack"]["intent_id"] is None
    for key in ("counts", "completed_frontier", "owner_references", "semantic_pending"):
        assert final[key] == state[key]
    assert artifact_fingerprint(source["descriptor"]) == source_before
    assert _CALLS - first_call == 16
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)

"""Guarded real coordination SQL proves prefix bind/replay/whole rollback."""

import json
import os
from pathlib import Path
import sys


def _transaction_worker():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import semantic_store_worker as worker
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_queue import SemanticQueue
    from cognitive_castle.lance_layout_directory import LayoutDirectory, _NativeLayoutDirectory
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_exact_store import _StoreError

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if request.keys() != {"directory", "operation", "arguments", "_host_pid"}:
        raise ValueError("fixed prefix binding fields")
    if request["operation"] not in {
        "queue_prefix_fault",
        "queue_prefix_bind",
        "queue_prefix_replay",
    } or request["arguments"].keys() != {"job_id", "plans", "envelope"}:
        raise ValueError("fixed prefix binding operation")
    worker._install_guards("queue")
    refusal = worker._admit(request, "queue")
    if refusal is not None:
        return refusal
    import sqlite3

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    args = request["arguments"]
    store = SemanticQueue(Path(request["directory"]), writable=True)

    def snapshot(conn):
        directory = _NativeLayoutDirectory(conn)
        state = directory._state(args["job_id"])
        context = directory.context(args["job_id"])
        for table in ("layout_routes", "layout_claims", "migration_owners"):
            assert conn.execute("SELECT 1 FROM " + table + " LIMIT 1").fetchone() is None
        assert state["pending"] is None and state[
            "staging_cursor"
        ] == migration.initial_source_cursor(state["registration"])
        assert (
            state["completed_frontier"] == state["staging_cursor"]
            and state["counts"] == migration.empty_counts()
        )
        assert directory.routes([p["owner"] for p in args["plans"]])["status"] == "unavailable"
        return dict(
            status="ok",
            context=context,
            homes=[
                tuple(r)
                for r in conn.execute(
                    "SELECT owner,partition_id,published FROM layout_homes ORDER BY owner LIMIT 9"
                )
            ],
            spans=[
                tuple(r) for r in conn.execute("SELECT * FROM layout_spans ORDER BY owner LIMIT 9")
            ],
            reservations=[
                tuple(r)
                for r in conn.execute(
                    "SELECT partition_id,reservation FROM layout_partitions ORDER BY partition_id LIMIT 3"
                )
            ],
            pins=[directory.partition(i)["pin"] for i in range(args["envelope"]["max_partitions"])],
        )

    before = store._run(False, snapshot)
    assert before["status"] == "ok", before
    action = request["operation"]
    calls = 0
    original = LayoutDirectory.bind

    def injected(self, *values, **kwargs):
        nonlocal calls
        result = original(self, *values, **kwargs)
        calls += 1
        if calls == 2:
            raise _StoreError("unavailable")
        return result

    if action == "queue_prefix_fault":
        LayoutDirectory.bind = injected
    try:
        result = store._run(
            True,
            lambda conn: _NativeLayoutDirectory(conn).bind_prefix(
                args["job_id"], args["plans"], args["envelope"]
            ),
        )
    finally:
        LayoutDirectory.bind = original
    after = store._run(False, snapshot)
    assert after["status"] == "ok", after
    if action == "queue_prefix_fault":
        assert calls == 2 and result["status"] == "unavailable", result
        assert (
            before == after
            and not after["homes"]
            and not after["spans"]
            and not after["reservations"]
        )
    else:
        assert result["status"] == "ok", result
        hashes = [migration.hashed(p) for p in args["plans"][:2]]
        assert (
            result["target"]["owner_plan_hashes"] == hashes
            and result["target"]["partition_id"] == 0
        )
        assert {r[0] for r in after["homes"]} == {p["owner"] for p in args["plans"][:2]}
        assert len(after["homes"]) == len(after["spans"]) == 2 and len(after["reservations"]) == 1
        assert all(tuple(r[1:]) == (0, 0, 0, 0) for r in after["spans"])
        assert all(r[1:] == (0, 0) for r in after["homes"])
        assert after["context"]["format"] == "lance-layout-native-context-v2"
        assert after["context"]["first_owner"] == args["plans"][0]["owner"]
        if action == "queue_prefix_replay":
            assert before == after
    assert not {"lance", "pyarrow", "numpy", "torch", "sentence_transformers"}.intersection(
        sys.modules
    )
    return dict(status="ok", fault_calls=calls, binding=result, snapshot=after)


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["transaction"]:
            raise ValueError("fixed prefix binding invocation")
        output = _transaction_worker()
    except Exception as exc:
        trace = exc.__traceback__
        while trace.tb_next is not None:
            trace = trace.tb_next
        output = dict(
            status="unavailable",
            reason="prefix_binding_" + type(exc).__name__,
            failure_file=Path(trace.tb_frame.f_code.co_filename).name,
            failure_line=trace.tb_lineno,
        )
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False).encode()
    if len(raw) > 512 * 1024:
        raw = b'{"status":"unavailable","reason":"prefix_binding_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402
import test_layout_native_integration as native  # noqa: E402
from test_sqlite_source_export import fixture  # noqa: E402
from test_layout_native_tail import _tree  # noqa: E402

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def test_guarded_prefix_binding_rolls_back_and_replays_without_publication(tmp_path):
    from cognitive_castle import migration_codec as migration, lance_layout_codec as codec

    first_call = native._CALLS
    call = native._start("source_fixture")
    source = fixture(tmp_path, prefix_small=True)
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation="source_fixture",
                status=source["status"],
                reason=source.get("reason"),
                resources=[source["resources"]],
            )
        ),
        flush=True,
    )
    before_source = _tree(tmp_path / "source", hashes=True)
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    for path in (generation / "exact", generation / "exact/partitions", generation / "exact/tmp"):
        path.mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    for pid in range(envelope["max_partitions"]):
        (generation / "exact/partitions" / str(pid)).mkdir(mode=0o700)
    registration = migration.registration(source["descriptor"], str(generation))
    with native._session(generation) as session:
        call = native._start("operator_register")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="register",
                arguments=dict(
                    registration=registration, envelope=envelope, qualification=qualification
                ),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(native.__file__).resolve()),
                "operator",
                migration.hashed(qualification),
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        native._record(session, "operator_register", result, 0, call)
    assert result["status"] == "ok", result
    artifact_before = (generation / "exact/layout-qualification.json").read_bytes()
    window = dict(
        format=codec.PREFIX_SOURCE_REQUEST,
        action="prepare",
        generation=envelope["generation"],
        qualification_hash=migration.hashed(qualification),
        allowed_plan_hashes=[],
        packing=codec.PACKING,
    )
    with native._session(generation) as session:
        call = native._start("layout_source")
        prepared = session.layout_source(
            source["descriptor"], migration.initial_source_cursor(registration), window
        )
        native._record(session, "layout_source", prepared, 0, call)
    assert prepared["status"] == "ok" and len(prepared["prefix"]["components"]) >= 3, prepared
    plans = [p["plan"] for p in prepared["prefix"]["components"][:3]]
    for operation in ("queue_prefix_fault", "queue_prefix_bind", "queue_prefix_replay"):
        with native._session(generation) as session:
            call = native._start(operation)
            result = session._run_stage(
                dict(
                    directory=str(generation),
                    operation=operation,
                    arguments=dict(job_id=registration["job_id"], plans=plans, envelope=envelope),
                ),
                worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "transaction"],
                cap=3,
                rss=128 * 1024**2,
                total=192 * 1024**2,
            )
            native._record(session, operation, result, 0, call)
        assert result["status"] == "ok", result
        assert _tree(tmp_path / "source", hashes=True) == before_source
        assert (generation / "exact/layout-qualification.json").read_bytes() == artifact_before
    assert native._CALLS - first_call == 6

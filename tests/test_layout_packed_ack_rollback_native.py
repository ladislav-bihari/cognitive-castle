"""Actual SQL rollback after the second span write, using real Lance proofs."""

import json
import os
from pathlib import Path
import sys


def _worker():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import semantic_store_worker as worker
    from cognitive_castle.semantic_profile import decode_frame, canonical_json
    from cognitive_castle.semantic_queue import SemanticQueue
    from cognitive_castle.lance_layout_directory import _NativeLayoutDirectory
    from cognitive_castle.lance_exact_store import _StoreError

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if request.keys() != {"directory", "operation", "arguments", "_host_pid"}:
        raise ValueError("fixed rollback fields")
    if request["operation"] != "queue_ack_fault" or request["arguments"].keys() != {
        "job_id",
        "intent_id",
        "verified_summary",
    }:
        raise ValueError("fixed rollback operation")
    worker._install_guards("queue")
    refusal = worker._admit(request, "queue")
    if refusal is not None:
        return refusal
    import sqlite3

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    store = SemanticQueue(Path(request["directory"]), writable=True)
    args = request["arguments"]
    tables = (
        "groups",
        "schedule",
        "acknowledgements",
        "intents",
        "migration_jobs",
        "migration_owners",
        "layout_jobs",
        "layout_partitions",
        "layout_homes",
        "layout_routes",
        "layout_authority",
        "layout_registry",
        "layout_claims",
        "layout_spans",
    )

    def snapshot(conn):
        result = {}
        for name in tables:
            rows = conn.execute("SELECT * FROM " + name + " LIMIT 129").fetchall()
            assert len(rows) <= 128
            # Sorting is fixture evidence only, never source or owner ordering.
            result[name] = sorted([list(row) for row in rows], key=canonical_json)
        assert len(canonical_json(result).encode()) <= 192 * 1024
        return dict(status="ok", metadata=result)

    before = store._run(False, snapshot)
    assert before["status"] == "ok", before
    count = 0

    class FaultConnection:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def execute(self, statement, arguments=()):
            nonlocal count
            result = self.conn.execute(statement, arguments)
            if statement.startswith("UPDATE layout_spans SET first_row="):
                count += 1
                if count == 2:
                    raise _StoreError("unavailable")
            return result

    refused = store._run(
        True, lambda conn: _NativeLayoutDirectory(FaultConnection(conn)).ack(**args)
    )
    after = store._run(False, snapshot)
    assert count == 2 and refused == dict(status="unavailable"), (count, refused)
    assert after == before
    assert not {"lance", "pyarrow", "numpy", "torch", "tokenizers", "onnxruntime"}.intersection(
        sys.modules
    )
    return dict(status="ok", injected_after_spans=count, unchanged=True)


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["fault"]:
            raise ValueError("fixed rollback invocation")
        output = _worker()
    except Exception as exc:
        trace = exc.__traceback__
        while trace.tb_next is not None:
            trace = trace.tb_next
        output = dict(
            status="unavailable",
            reason="rollback_" + type(exc).__name__,
            failure_file=Path(trace.tb_frame.f_code.co_filename).name,
            failure_line=trace.tb_lineno,
        )
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False).encode()
    if len(raw) > 4096:
        raw = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import time  # noqa: E402
import test_layout_native_integration as native  # noqa: E402
import test_layout_packed_native as packed  # noqa: E402
from test_layout_native_tail import _tree  # noqa: E402

pytestmark = packed.pytestmark


def _fault_step(reg):
    from cognitive_castle.migration_coordinator import _layout_prefix_step

    generation = Path(reg["destination"])

    class Observed(native.ObservedLayoutSession):
        def store(self, directory, operation, arguments):
            if operation != "queue_layout_ack":
                return super().store(directory, operation, arguments)
            result = self.call(
                "queue_ack_fault",
                self.session._run_stage,
                dict(directory=str(directory), operation="queue_ack_fault", arguments=arguments),
            )
            assert result == dict(status="ok", injected_after_spans=2, unchanged=True), result
            return dict(status="unavailable", reason="fixture_fault_rollback")

        def call(self, name, method, *args):
            if name != "queue_ack_fault":
                return super().call(name, method, *args)
            previous = len(self.session.resources)
            call = native._start(name)
            result = method(
                *args,
                worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "fault"],
                cap=3,
                rss=128 * 1024**2,
                total=192 * 1024**2,
            )
            native._record(self.session, name, result, previous, call)
            self.calls.append(name + ":1")
            return result

    started = time.monotonic()
    with native._session(generation) as session:
        observed = Observed(session)
        result = _layout_prefix_step(observed, reg)
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
    return result


def test_real_second_owner_ack_failure_rolls_back_every_metadata_table_then_recovers(tmp_path):
    start = native._CALLS
    source, generation, reg, _ = packed._setup(tmp_path)
    before_source = _tree(tmp_path / "source", hashes=True)
    artifact = generation / "exact/layout-qualification.json"
    before_artifact = artifact.read_bytes()
    fresh, first = packed._step(reg)
    assert fresh["status"] == "in_progress", fresh
    intent = first.arguments["queue_layout_prepare:1"][2]["intent"]
    manifests = first.arguments["layout_import_batch:1"][2]["finalizations"]
    assert len(manifests) == 2
    failed = _fault_step(reg)
    assert failed == dict(status="unavailable", reason="fixture_fault_rollback")
    state = native._queue_state(reg)
    original = first.results["queue_layout_resume:1"]
    assert state["pending"] == intent
    for field in ("staging_cursor", "completed_frontier", "counts", "last_ack"):
        assert state[field] == original[field]
    for manifest in manifests:
        assert native._read(generation, "bulk_owner", dict(owner=manifest["owner"])) == {
            "status": "unavailable"
        }
    done, checked = packed._step(reg)
    assert done["status"] == "ok", done
    assert checked.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    state = native._queue_state(reg)
    assert state["pending"] is None and state["counts"]["live_owners"] == 2
    packed._assert_originals(source, generation, manifests)
    for manifest in manifests:
        owner = native._read(generation, "bulk_owner", dict(owner=manifest["owner"]))
        assert owner["status"] == "ok" and owner["manifest"] == manifest
        live = manifest["live"]
        assert native._read(
            generation, "receipt", dict(namespace=live["namespace"], key=live["key"])
        ) == dict(status="ok", digest=live["digest"], receipt=live["receipt"])
    packed._assert_frozen(tmp_path, before_source, artifact, before_artifact)
    assert native._CALLS - start == 26

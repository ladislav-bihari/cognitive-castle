"""Finite direct-live proof on the fixed supported native qualification host.

All data is synthetic. The host imports no storage/model engine. Only the
separately invoked trusted operator can create the registered authority.
"""

import json
import os
from pathlib import Path
import platform
import sys


def _operator(expected_hash):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import semantic_store_worker as worker
    from cognitive_castle.lance_layout_directory import _prepare_layout_authority
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_queue import SemanticQueue

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if request.keys() != {"directory", "operation", "arguments", "_host_pid"}:
        raise ValueError("fixed operator request")
    if request["operation"] != "prepare" or request["arguments"].keys() != {
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
    return store._run(
        True,
        lambda conn: _prepare_layout_authority(
            conn, args["envelope"], args["qualification"], expected_hash=expected_hash
        ),
    )


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3 or sys.argv[1] != "operator":
            raise ValueError("fixed operator invocation")
        output = _operator(sys.argv[2])
    except Exception as exc:
        output = dict(status="unavailable", reason="fixture_" + type(exc).__name__)
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False).encode()
    if len(raw) > 512 * 1024:
        raw = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - operator must exit before importing the test host.
from test_layout_native_integration import (  # noqa: E402
    ObservedLayoutSession,
    _qualification,
    _read,
    _record,
    _session,
    _start,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin"
    or platform.machine() != "arm64"
    or platform.python_version() != "3.12.10",
    reason="requires fixed macOS arm64 Python 3.12.10 qualification host",
)


def _setup_live(tmp_path):
    from cognitive_castle import live_publication_codec as codec, migration_codec as migration
    from cognitive_castle.canonical_records import prepare_remember

    generation = tmp_path.resolve() / "generation"
    generation.mkdir(mode=0o700)
    for name in ("exact", "exact/partitions", "exact/tmp"):
        (generation / name).mkdir(mode=0o700)
    qualification, envelope = _qualification(generation)
    for pid in range(envelope["max_partitions"]):
        (generation / "exact/partitions" / str(pid)).mkdir(mode=0o700)
    with _session(generation) as session:
        call = _start("direct_operator_prepare")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="prepare",
                arguments=dict(envelope=envelope, qualification=qualification),
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
        _record(session, "direct_operator_prepare", result, 0, call)
    assert result["status"] == "ok", result
    canonical = prepare_remember(
        "direct-request", "Direct Ω\x00\r\n  exact body", {"note": "original Ω"}
    )
    publication = codec.prepare_publication(
        canonical, [dict(workspace="/trusted/project", wing="project", room="conversation")]
    )
    return generation, publication, migration.hashed(envelope)


def _attempt(generation, publication, binding, *, loss=None, complete=False):
    from cognitive_castle.live_publication import publish, step

    with _session(generation) as session:
        observed = ObservedLayoutSession(session, loss=loss)
        result = (publish if complete else step)(observed, generation, publication, binding)
    return result, observed


def _assert_source(generation, publication):
    canonical = publication.canonical
    read = _read(generation, "read_sources", dict(ids=canonical.receipt["ids"]))
    assert read["status"] == "ok", read
    assert read["sources"] == [record.source() for record in canonical.records]
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )


def _case_publication_reopens_and_refuses_changed_request(tmp_path):
    from cognitive_castle import live_publication_codec as codec
    from cognitive_castle.canonical_records import prepare_remember

    generation, publication, binding = _setup_live(tmp_path)
    first, observed = _attempt(generation, publication, binding)
    assert first["status"] == "ok" and first["counts"]["members"] == 0, first
    assert "ids" not in first and "receipt" not in first
    assert [observed.results[f"layout_import_batch:{n}"]["outcome"] for n in (1, 2)] == [
        "fresh_append",
        "existing_verified",
    ]
    final, observed = _attempt(generation, publication, binding)
    assert final["status"] == "ok" and final["counts"]["members"] == 1, final
    assert observed.calls[-4:] == [
        "layout_import_batch:1",
        "layout_import_batch:2",
        "layout_verify_owners:1",
        "queue_layout_ack:1",
    ]
    receipt, _ = _attempt(generation, publication, binding, complete=True)
    assert receipt == publication.canonical.receipt
    again, observed = _attempt(generation, publication, binding, complete=True)
    assert again == receipt and not any(
        c.startswith("layout_import_batch:") for c in observed.calls
    )
    changed = codec.prepare_publication(
        prepare_remember("direct-request", "changed", {"note": "original Ω"}),
        [dict(workspace="/trusted/project", wing="project", room="conversation")],
    )
    conflict, observed = _attempt(generation, changed, binding)
    assert conflict["status"] == "conflict"
    assert observed.calls == ["queue_live_open:1"]
    _assert_source(generation, publication)


def _case_lost_append_response_resumes_exact_pending_batch(tmp_path):
    generation, publication, binding = _setup_live(tmp_path)
    lost, before = _attempt(generation, publication, binding, loss="layout_import_batch:1")
    assert lost["status"] == "unavailable" and lost["reason"] == "fixture_lost_response", lost
    assert "queue_layout_ack:1" not in before.calls
    resumed, after = _attempt(generation, publication, binding)
    assert resumed["status"] == "ok" and resumed["counts"]["members"] == 0, resumed
    assert after.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    assert before.arguments["layout_import_batch:1"] == after.arguments["layout_import_batch:1"]
    final, _ = _attempt(generation, publication, binding)
    assert final["status"] == "ok" and final["counts"]["members"] == 1, final
    receipt, _ = _attempt(generation, publication, binding, complete=True)
    assert receipt == publication.canonical.receipt
    _assert_source(generation, publication)


def test_direct_native_publication_reopens_and_refuses_changed_request(tmp_path):
    _case_publication_reopens_and_refuses_changed_request(tmp_path)


def test_direct_native_lost_append_response_resumes_exact_pending_batch(tmp_path):
    _case_lost_append_response_resumes_exact_pending_batch(tmp_path)

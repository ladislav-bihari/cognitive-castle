"""Actual SQL controller/worker contracts; native publication remains separate."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import pytest
from cognitive_castle.fast_memory import MemoryStore
from test_capture_control_store import create
from test_capture_handoff import transcript


def worker():
    from cognitive_castle import capture_control_worker

    return capture_control_worker


def request(authority, operation, arguments):
    return dict(
        authority=authority["authority"],
        authority_hash=authority["authority_hash"],
        operation=operation,
        arguments=arguments,
        _host_pid=123,
    )


def source_case(tmp_path, text="WORKER_BODY Ω\x00\r\n  "):
    store, authority = create(tmp_path)
    path = transcript(tmp_path, text)
    result = worker()._execute(
        request(
            authority,
            "source_queue",
            dict(path=str(path), allowed_root=str(tmp_path), format="codex", workspace="/project"),
        )
    )
    assert result["status"] == "ok"
    return store, authority, path, result["source_key"], text


def step(authority, key, publication=None):
    args = dict(source_key=key)
    if publication is not None:
        args["publication"] = publication
    return worker()._execute(request(authority, "source_prepare", args))


def publication(tmp_path, preparation):
    from cognitive_castle.canonical_records import prepare_remember

    prepared = prepare_remember(
        preparation["request_id"],
        preparation["text"],
        preparation["provenance"],
        namespace="transcript",
    )
    publisher = MemoryStore(tmp_path / "fixture-publisher-only")
    assert (
        publisher.remember(
            prepared.request_id,
            preparation["text"],
            preparation["provenance"],
            namespace="transcript",
        )["status"]
        == "ok"
    )
    actual = publisher.receipt(namespace="transcript", key=prepared.request_id)
    return dict(request_id=prepared.request_id, digest=actual["digest"], receipt=actual["receipt"])


def test_worker_handoff_reserves_hash_without_confirming_or_advancing(tmp_path):
    store, authority, path, key, text = source_case(tmp_path)
    first = step(authority, key)
    assert first["status"] == "prepared" and first["publication_consumed"] is False
    assert first["preparation"]["text"] == text
    assert first["scope"] == dict(workspace="/project", attribution="host_event", map=True)
    checkpoint = store.load_checkpoint(key)
    assert checkpoint["offset"] == 0
    assert checkpoint["pending"] == dict(
        hash=hashlib.sha256(path.read_bytes()).hexdigest(), cursor=[-1, 0]
    )
    again = step(authority, key)
    assert again["preparation"] == first["preparation"] and again["scope"] == first["scope"]
    status = worker()._execute(request(authority, "source_page", dict(pending_only=True)))[
        "sources"
    ][0]
    assert status["state"] == "pending" and status["last_status"] == "prepared"
    assert status["publication_pending"] == 1 and "last_error" not in status
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0


def test_worker_matching_proof_confirms_then_advances_exact_source(tmp_path):
    store, authority, path, key, text = source_case(tmp_path)
    first = step(authority, key)
    proof = publication(tmp_path, first["preparation"])
    done = step(authority, key, proof)
    assert done["status"] == "ok" and done["publication_consumed"] is True
    assert store.load_checkpoint(key)["offset"] == path.stat().st_size
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 1
    assert worker()._execute(request(authority, "coverage", {}))["pending_sources"] == 0


def test_worker_wrong_digest_never_confirms_or_advances(tmp_path):
    store, authority, _, key, _ = source_case(tmp_path)
    first = step(authority, key)
    proof = publication(tmp_path, first["preparation"])
    proof["digest"] = "f" * 64
    result = step(authority, key, proof)
    assert result["status"] == "conflict" and result["publication_consumed"] is False
    assert store.load_checkpoint(key)["offset"] == 0
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0


def test_worker_rewritten_prepared_record_gets_a_new_generation(tmp_path):
    store, authority, path, key, _ = source_case(tmp_path)
    old = step(authority, key)
    record = json.loads(path.read_bytes())
    record["payload"]["content"][0]["text"] = "changed original"
    path.write_bytes(json.dumps(record).encode() + b"\n")
    for _ in range(3):
        changed = step(authority, key)
        if changed["status"] == "prepared":
            break
    assert changed["status"] == "prepared" and changed["preparation"]["text"] == "changed original"
    assert changed["preparation"]["provenance"]["generation"] == 1
    assert old["preparation"]["provenance"]["generation"] == 0
    assert store.load_checkpoint(key)["offset"] == 0
    assert worker()._execute(request(authority, "coverage", {}))["publication_pending"] == 2


def test_worker_max_escaped_handoff_fits_reply_budget(tmp_path):
    _, authority, _, key, _ = source_case(tmp_path, text="\x00" * 65536)
    result = step(authority, key)
    assert result["status"] == "prepared" and result["preparation"]["text"] == "\x00" * 65536
    assert len(json.dumps(result, ensure_ascii=False).encode()) <= 512 * 1024 - 16384


def test_worker_unknown_operation_and_extra_fields_fail_before_storage(tmp_path, monkeypatch):
    _, authority, _, key, _ = source_case(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid request reached store")

    monkeypatch.setattr(worker(), "_store", forbidden)
    for operation, args in [
        ("initialize", {}),
        ("source_prepare", dict(source_key=key, text="injected")),
        ("coverage", dict(extra=True)),
    ]:
        with pytest.raises(ValueError):
            worker()._execute(request(authority, operation, args))


def test_actual_worker_executable_refuses_unsupervised_invocation_without_creation(tmp_path):
    from cognitive_castle.canonical_records import _hash, _json

    module = worker()
    directory = tmp_path / "absent"
    authority = dict(
        format="castle-capture-control-v1",
        directory=dict(path=str(directory), device=1, inode=2),
        database=dict(device=1, inode=3),
        max_bytes=3 * 1024**2,
    )
    req = dict(
        authority=authority,
        authority_hash=_hash(_json(authority)),
        operation="coverage",
        arguments={},
        _host_pid=123,
    )
    result = subprocess.run(
        [sys.executable, "-I", str(Path(module.__file__))],
        input=json.dumps(req).encode(),
        capture_output=True,
        timeout=3,
    )
    assert result.returncode == 0
    response = json.loads(result.stdout)
    assert response["status"] == "unavailable" and response["reason"] == "semantic_unsupervised"
    assert not directory.exists()


def test_failed_journal_reservation_never_advances_or_hands_off_body(tmp_path, monkeypatch):
    import sqlite3
    from contextlib import contextmanager
    from cognitive_castle.capture_control_store import CaptureControlStore

    store, authority, _, key, _ = source_case(tmp_path)
    connect = CaptureControlStore._connect

    class Fault:
        def __init__(self, conn):
            self.conn = conn

        def execute(self, sql, *args):
            if sql.startswith("INSERT INTO capture_publication_intents"):
                raise sqlite3.OperationalError("injected intent refusal")
            return self.conn.execute(sql, *args)

    @contextmanager
    def refusing(self, *args, **kwargs):
        with connect(self, *args, **kwargs) as conn:
            yield Fault(conn)

    monkeypatch.setattr(CaptureControlStore, "_connect", refusing)
    result = step(authority, key)
    assert result["status"] == "unavailable" and "preparation" not in result
    checkpoint = store.load_checkpoint(key)
    assert checkpoint["offset"] == 0
    assert checkpoint["pending"]["cursor"] == [-1, 0]
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_publication_intents").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0

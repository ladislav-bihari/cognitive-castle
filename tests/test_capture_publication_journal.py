"""Disposable real-SQL coordination fixtures, not Lance or resource qualification."""

import json

import pytest

from cognitive_castle import memory_capture as capture
from cognitive_castle.fast_memory import MemoryStore
from test_memory_scope import write, rows, texts


def queued(tmp_path, workspace="/a"):
    path = tmp_path / "session.jsonl"
    write(path, rows("/unused", "BODY_SENTINEL česky  ")[1:])
    store = MemoryStore(tmp_path / "memory")
    key = capture.queue_source(
        store, path, allowed_root=tmp_path, format="codex", workspace=workspace
    )["source_key"]
    return store, path, key


def drive(store, key):
    for _ in range(4):
        result = capture.capture_one(store, key)
        if result["status"] != "ok" or not result.get("pending_bytes"):
            return result
    return result


def test_intent_exists_before_actual_append_and_never_contains_body(tmp_path, monkeypatch):
    store, _, key = queued(tmp_path)
    remember = store.remember
    calls = []

    def inspect(*args, **kwargs):
        with store._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE name='capture_publication_intents'"
            ).fetchone()
            assert exists, "capture must persist publication intent before append"
            pending = conn.execute(
                "SELECT * FROM capture_publication_intents WHERE source_key=?", (key,)
            ).fetchall()
            assert len(pending) == 1 and pending[0]["state"] == "pending"
            raw = pending[0]["value"]
            assert "BODY_SENTINEL" not in raw and "česky" not in raw
            intent = json.loads(raw)
            assert intent["workspace"] == "/a" and intent["namespace"] == "transcript"
        calls.append(1)
        return remember(*args, **kwargs)

    monkeypatch.setattr(store, "remember", inspect)
    assert drive(store, key)["status"] == "ok"
    assert calls == [1]
    assert texts(store, "/a") == ["BODY_SENTINEL česky  "]
    with store._connect() as conn:
        assert (
            conn.execute("SELECT state FROM capture_publication_intents").fetchone()[0] == "applied"
        )


@pytest.mark.parametrize("committed", [False, True])
def test_crash_around_append_recovers_original_scope_after_changed_hint(
    tmp_path, monkeypatch, committed
):
    store, path, key = queued(tmp_path)
    remember = store.remember

    def interrupted(*args, **kwargs):
        if committed:
            assert remember(*args, **kwargs)["status"] == "ok"
        raise RuntimeError("host terminated at append boundary")

    monkeypatch.setattr(store, "remember", interrupted)
    with pytest.raises(RuntimeError):
        drive(store, key)
    assert store.load_checkpoint(key)["offset"] == 0
    capture.queue_source(store, path, allowed_root=tmp_path, format="codex", workspace="/b")
    restarted = MemoryStore(store.directory)
    assert drive(restarted, key)["status"] == "ok"
    assert texts(restarted, "/a") == ["BODY_SENTINEL česky  "]
    assert texts(restarted, "/b") == []
    with restarted._connect() as conn:
        assert conn.execute("SELECT count(*) FROM chunks").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM capture_publication_intents").fetchone()[0] == 1
    assert (
        "after_rowid"
        not in capture._prepare_scope(restarted, key, restarted.load_checkpoint(key))[
            "scope_attempt"
        ]
    )


def test_recovery_refusal_does_not_apply_expected_ids_or_advance_checkpoint(tmp_path, monkeypatch):
    store, _, key = queued(tmp_path)
    remember = store.remember

    def lost(*args, **kwargs):
        assert remember(*args, **kwargs)["status"] == "ok"
        raise RuntimeError("lost committed response")

    monkeypatch.setattr(store, "remember", lost)
    with pytest.raises(RuntimeError):
        drive(store, key)
    restarted = MemoryStore(store.directory)
    receipt = restarted.receipt
    monkeypatch.setattr(restarted, "receipt", lambda **kwargs: dict(status="unavailable"))
    assert capture.capture_one(restarted, key)["status"] == "unavailable"
    assert restarted.load_checkpoint(key)["offset"] == 0
    with restarted._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 0
        assert (
            conn.execute("SELECT state FROM capture_publication_intents").fetchone()[0] == "pending"
        )
    monkeypatch.setattr(restarted, "receipt", receipt)
    assert drive(restarted, key)["status"] == "ok"
    assert texts(restarted, "/a") == ["BODY_SENTINEL česky  "]


@pytest.mark.parametrize("fault", ["missing", "corrupt", "ids"])
def test_actual_receipt_read_rejects_damaged_originals(tmp_path, fault):
    store = MemoryStore(tmp_path / "memory")
    saved = store.remember("request", "first" * 2000, dict(role="user"))
    assert saved["status"] == "ok" and len(saved["ids"]) == 2
    actual = store.receipt(namespace="operator", key="request")
    assert actual["status"] == "ok" and actual["receipt"] == saved
    with store._connect(True) as conn:
        if fault == "missing":
            conn.execute("DELETE FROM chunks WHERE id=?", (saved["ids"][1],))
        elif fault == "corrupt":
            conn.execute("UPDATE chunks SET text='changed' WHERE id=?", (saved["ids"][0],))
        else:
            conn.execute("UPDATE receipts SET ids='[]'")
    assert store.receipt(namespace="operator", key="request")["status"] == "corrupt"


def test_journal_write_refusal_prevents_the_canonical_append(tmp_path, monkeypatch):
    store, _, key = queued(tmp_path)
    capture._prepare_scope(store, key, None)
    with store._connect(True) as conn:
        conn.execute(
            "CREATE TRIGGER reject_capture_intent BEFORE INSERT ON capture_publication_intents BEGIN SELECT RAISE(ABORT,'injected journal refusal'); END"
        )
    monkeypatch.setattr(
        store, "remember", lambda *args, **kwargs: pytest.fail("append after journal refusal")
    )
    assert drive(store, key)["status"] != "ok"
    assert store.load_checkpoint(key)["offset"] == 0
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM chunks").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM capture_publication_intents").fetchone()[0] == 0


def test_changed_journal_binding_cannot_authorize_scope_recovery(tmp_path, monkeypatch):
    store, _, key = queued(tmp_path)
    remember = store.remember

    def lost(*args, **kwargs):
        assert remember(*args, **kwargs)["status"] == "ok"
        raise RuntimeError("lost response")

    monkeypatch.setattr(store, "remember", lost)
    with pytest.raises(RuntimeError):
        drive(store, key)
    with store._connect(True) as conn:
        row = conn.execute("SELECT request_key,value FROM capture_publication_intents").fetchone()
        changed = dict(json.loads(row["value"]), workspace="/b")
        conn.execute(
            "UPDATE capture_publication_intents SET value=? WHERE request_key=?",
            (json.dumps(changed), row["request_key"]),
        )
    restarted = MemoryStore(store.directory)
    monkeypatch.setattr(
        restarted, "receipt", lambda **kwargs: pytest.fail("corrupt intent reached backend")
    )
    assert capture.capture_one(restarted, key)["status"] == "corrupt"
    with restarted._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 0


def test_new_capture_does_not_read_the_sqlite_body_rowid(tmp_path, monkeypatch):
    import sqlite3
    from contextlib import contextmanager

    store, _, key = queued(tmp_path)
    connect = store._connect

    def authorize(action, table, column, database, trigger):
        return (
            sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and table == "chunks" and column.lower() == "rowid"
            else sqlite3.SQLITE_OK
        )

    @contextmanager
    def protected(*args, **kwargs):
        with connect(*args, **kwargs) as conn:
            conn.set_authorizer(authorize)
            yield conn

    monkeypatch.setattr(store, "_connect", protected)
    assert drive(store, key)["status"] == "ok"
    assert texts(store, "/a") == ["BODY_SENTINEL česky  "]


def test_unwritten_old_generation_intent_keeps_coverage_incomplete(tmp_path, monkeypatch):
    store, path, key = queued(tmp_path)

    def before_append(*args, **kwargs):
        raise RuntimeError("before append")

    monkeypatch.setattr(store, "remember", before_append)
    with pytest.raises(RuntimeError):
        drive(store, key)
    path.rename(tmp_path / "protected-original.jsonl")
    write(path, rows("/b", "new generation"))
    restarted = MemoryStore(store.directory)
    capture.queue_source(restarted, path, allowed_root=tmp_path, format="codex")
    assert drive(restarted, key)["status"] == "ok"
    assert texts(restarted, "/b") == ["new generation"]
    assert texts(restarted, "/a") == []
    source = capture.source_page(restarted)["sources"][0]
    assert source["publication_pending"] == 1 and source["state"] == "pending"
    assert capture.coverage(restarted)["coverage"] == "partial"


def test_recovery_reads_at_most_two_pending_receipts_per_tick(tmp_path, monkeypatch):
    from cognitive_castle.capture_publication import CapturePublication, recover_publications
    from cognitive_castle.canonical_records import prepare_remember

    store, _, key = queued(tmp_path)
    remember = store.remember
    saved = []

    def stop(*args, **kwargs):
        saved.append((args, kwargs))
        raise RuntimeError("before append")

    monkeypatch.setattr(store, "remember", stop)
    with pytest.raises(RuntimeError):
        drive(store, key)
    args, kwargs = saved[0]
    monkeypatch.setattr(store, "remember", remember)
    remember(*args, **kwargs)
    with store._connect() as conn:
        source = json.loads(
            conn.execute("SELECT value FROM capture_sources WHERE source_key=?", (key,)).fetchone()[
                0
            ]
        )
    journal = CapturePublication(store, key, source["scope_attempt"])
    for ordinal in (1, 2):
        prepared = prepare_remember(
            "other-" + str(ordinal), "fixture " + str(ordinal), args[2], namespace="transcript"
        )
        assert journal.prepare(prepared)["status"] == "ok"
        assert (
            remember(
                prepared.request_id, prepared.records[0].text, args[2], namespace="transcript"
            )["status"]
            == "ok"
        )
    receipts = []
    actual = store.receipt

    def observe(**kwargs):
        receipts.append(kwargs)
        return actual(**kwargs)

    monkeypatch.setattr(store, "receipt", observe)
    first = recover_publications(store, key)
    assert first["status"] == "ok" and first["checked"] == 2 and first["pending"] == 1
    assert len(receipts) == 2
    with store._connect() as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM capture_publication_intents WHERE state='pending'"
            ).fetchone()[0]
            == 1
        )
    assert recover_publications(store, key)["status"] == "ok"
    assert len(receipts) == 3


def test_scope_write_failure_rolls_back_every_id_and_confirmation(tmp_path, monkeypatch):
    store, path, key = queued(tmp_path)
    write(path, rows("/a", "z" * 9000))
    capture.queue_source(store, path, allowed_root=tmp_path, format="codex")
    remember = store.remember

    def inject(*args, **kwargs):
        receipt = remember(*args, **kwargs)
        assert len(receipt["ids"]) == 2
        with store._connect(True) as conn:
            conn.execute(
                "CREATE TRIGGER reject_second_scope BEFORE INSERT ON capture_chunk_scopes "
                "WHEN NEW.chunk_id='"
                + receipt["ids"][1]
                + "' BEGIN SELECT RAISE(ABORT,'second scope'); END"
            )
        return receipt

    monkeypatch.setattr(store, "remember", inject)
    assert drive(store, key)["status"] != "ok"
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 0
        assert (
            conn.execute("SELECT state FROM capture_publication_intents").fetchone()[0] == "pending"
        )
    with store._connect(True) as conn:
        conn.execute("DROP TRIGGER reject_second_scope")
    restarted = MemoryStore(store.directory)
    assert drive(restarted, key)["status"] == "ok"
    with restarted._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 2
        assert (
            conn.execute("SELECT state FROM capture_publication_intents").fetchone()[0] == "applied"
        )
        assert conn.execute("SELECT count(*) FROM chunks").fetchone()[0] == 2


def test_legacy_pending_watermark_recovers_once_without_reassigning_old_ids(tmp_path):
    from cognitive_castle.memory_ingest import ingest_slice

    store, path, key = queued(tmp_path)
    with store._connect(True) as conn:
        row = conn.execute(
            "SELECT source_key,state,value FROM capture_sources WHERE source_key=?", (key,)
        ).fetchone()
        source = capture._decode(row)
        source["scope_attempt"] = dict(
            after_rowid=0, hint="/a", hint_observation=source["host_workspace_observation"]
        )
        capture._write(conn, source)
    for _ in range(4):
        result = ingest_slice(store, path, allowed_root=tmp_path, format="codex")
        if result.get("offset"):
            break
    assert result["status"] == "ok" and result["offset"] == path.stat().st_size
    capture.queue_source(store, path, allowed_root=tmp_path, format="codex", workspace="/b")
    assert drive(store, key)["status"] == "ok"
    assert texts(store, "/a") == ["BODY_SENTINEL česky  "]
    assert texts(store, "/b") == []


def test_original_journal_rows_remain_streamable_audit_evidence(tmp_path):
    import time
    from cognitive_castle.sqlite_source_export import Source, next_unit
    from cognitive_castle.canonical_records import _json

    store, _, key = queued(tmp_path)
    assert drive(store, key)["status"] == "ok"
    with store._connect() as conn:
        original = dict(conn.execute("SELECT * FROM capture_publication_intents").fetchone())
        # Exercise the actual keyset/field streamer with a real read connection.
        # Full descriptor admission is a separate supervised native scope.
        source = Source.__new__(Source)
        source.conn, source.tables, source.until = (
            conn,
            {"capture_publication_intents"},
            time.monotonic() + 1.65,
        )
        phase, position = next_unit(source, dict(phase="audit", key=None, owner=None))
        assert phase == "audit" and position == [
            "capture_publication_intents",
            _json([original["request_key"]]),
        ]
        copied = json.loads(
            "".join(
                source.object(
                    "capture_publication_intents", "request_key=?", (original["request_key"],)
                )
            )
        )
        assert copied == original


def test_actual_receipt_preserves_nul_and_multibyte_originals(tmp_path):
    store = MemoryStore(tmp_path / "memory")
    original = "\x00exact česky\x00 tail  "
    saved = store.remember("nul", original, dict(note="\x00caller"))
    assert saved["status"] == "ok"
    actual = store.receipt(namespace="operator", key="nul")
    assert actual["status"] == "ok" and actual["receipt"] == saved


def test_nul_suffix_cannot_hide_a_corrupted_journal_value(tmp_path, monkeypatch):
    store, _, key = queued(tmp_path)
    remember = store.remember

    def lost(*args, **kwargs):
        assert remember(*args, **kwargs)["status"] == "ok"
        raise RuntimeError("lost response")

    monkeypatch.setattr(store, "remember", lost)
    with pytest.raises(RuntimeError):
        drive(store, key)
    with store._connect(True) as conn:
        row = conn.execute("SELECT value FROM capture_publication_intents").fetchone()
        conn.execute("UPDATE capture_publication_intents SET value=?", (row[0] + "\x00hidden",))
    restarted = MemoryStore(store.directory)
    monkeypatch.setattr(
        restarted, "receipt", lambda **kwargs: pytest.fail("corrupt journal reached backend")
    )
    assert capture.capture_one(restarted, key)["status"] == "corrupt"


def test_unknown_journal_state_cannot_be_reported_as_complete(tmp_path):
    store, _, key = queued(tmp_path)
    assert drive(store, key)["status"] == "ok"
    with store._connect(True) as conn:
        conn.execute("UPDATE capture_publication_intents SET state='unknown'")
    assert capture.capture_one(store, key)["status"] == "corrupt"

    assert capture.coverage(store)["publication_pending"] == 1
    assert capture.coverage(store)["coverage"] == "partial"
    pending = capture.source_page(store, pending_only=True)["sources"]
    assert len(pending) == 1 and pending[0]["publication_pending"] == 1


def test_native_fixture_cursor_matches_actual_descriptor_entry_contract(tmp_path):
    import time
    from cognitive_castle.sqlite_source_export import Source, _export_owner
    from cognitive_castle.sqlite_export_codec import source_binding, empty_counts
    from test_migration_codec import descriptor
    from test_capture_journal_export_native import journal_cursor

    store, _, key = queued(tmp_path)
    assert drive(store, key)["status"] == "ok"
    with store._connect() as conn:
        original = dict(conn.execute("SELECT * FROM capture_publication_intents").fetchone())
        source = Source.__new__(Source)
        source.conn, source.tables, source.until = (
            conn,
            {"capture_publication_intents"},
            time.monotonic() + 1.65,
        )
        source.descriptor = descriptor()
        source.binding, source.manifest = source_binding(source.descriptor), {"original": {}}
        cursor = journal_cursor(source.descriptor, original["request_key"])
        result = dict(entries=[], finalizations=[], counts=empty_counts())
        _export_owner(source, result, cursor, "audit", cursor["key"])
        assert result["counts"]["audit_owners"] == 1
        assert len(result["finalizations"]) == 1
        copied = json.loads("".join(row["text"] for row in result["entries"]))
        assert copied["row"] == original


def test_missing_old_receipts_do_not_starve_later_confirmed_publications(tmp_path, monkeypatch):
    from cognitive_castle.capture_publication import CapturePublication, recover_publications
    from cognitive_castle.canonical_records import prepare_remember

    store, _, key = queued(tmp_path)
    remember = store.remember
    saved = []

    def interrupted(*args, **kwargs):
        saved.append((args, kwargs))
        raise RuntimeError("before append")

    monkeypatch.setattr(store, "remember", interrupted)
    with pytest.raises(RuntimeError):
        drive(store, key)
    args, kwargs = saved[0]
    monkeypatch.setattr(store, "remember", remember)
    payloads = {args[0]: (args[1], args[2])}
    with store._connect() as conn:
        source = json.loads(
            conn.execute("SELECT value FROM capture_sources WHERE source_key=?", (key,)).fetchone()[
                0
            ]
        )
    journal = CapturePublication(store, key, source["scope_attempt"])
    for ordinal in (1, 2):
        request, text = "other-" + str(ordinal), "fixture " + str(ordinal)
        prepared = prepare_remember(request, text, args[2], namespace="transcript")
        assert journal.prepare(prepared)["status"] == "ok"
        payloads[request] = (text, args[2])
    with store._connect() as conn:
        pending = [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT value FROM capture_publication_intents ORDER BY request_key"
            )
        ]
    last = pending[-1]
    text, provenance = payloads[last["key"]]
    assert remember(last["key"], text, provenance, namespace="transcript")["status"] == "ok"
    observed, receipt = [], store.receipt

    def read(**kwargs):
        observed.append(kwargs["key"])
        return receipt(**kwargs)

    monkeypatch.setattr(store, "receipt", read)
    first = recover_publications(store, key)
    assert first["status"] == "ok", "missing receipts must not block the next bounded capture slice"
    assert len(observed) == 2
    assert recover_publications(store, key)["status"] == "ok"
    assert observed == [item["key"] for item in pending]
    with store._connect() as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM capture_publication_intents WHERE state='applied'"
            ).fetchone()[0]
            == 1
        )
    assert capture.coverage(store)["publication_pending"] == 2
    assert capture.coverage(store)["coverage"] == "partial"

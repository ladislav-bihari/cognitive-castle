"""Control bookkeeping only. Disposable SQL proof fixture is not production routing."""

import pytest
from cognitive_castle import memory_capture as capture
from cognitive_castle.capture_publication import CapturePublication
from cognitive_castle.fast_memory import MemoryStore, _Corrupt
from cognitive_castle.memory_ingest import ingest_slice
from test_capture_control_store import create, checkpoint
from test_capture_handoff import transcript, stage


def staged(tmp_path, workspace="/project", body="COUNT_BODY_SENTINEL Ω" * 500):
    store, _ = create(tmp_path)
    path = transcript(tmp_path, body)
    key = capture.queue_source(
        store, path, allowed_root=tmp_path, format="codex", workspace=workspace
    )["source_key"]
    source = capture._prepare_scope(store, key, None)
    journal = CapturePublication(store, key, source["scope_attempt"])
    prepared = []

    def reserve(value):
        prepared.append(value)
        return journal.prepare(value)

    result = ingest_slice(
        store,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=stage,
        on_prepared=reserve,
        on_published=journal.confirm,
    )
    assert result["status"] == "prepared" and len(prepared) == 1
    return store, key, journal, prepared[0], source["scope_attempt"], body


def scope(store, key, attempt):
    current = store.load_checkpoint(key)
    with store._connect(True) as conn:
        return capture._scope_chunks(conn, key, current, attempt)


def actual_fixture_receipt(tmp_path, prepared, body):
    publisher = MemoryStore(tmp_path / "fixture-publisher-only")
    caller = prepared.records[0].source()["provenance"]["caller"]
    assert (
        publisher.remember(prepared.request_id, body, caller, namespace="transcript")["status"]
        == "ok"
    )
    observed = publisher.receipt(namespace="transcript", key=prepared.request_id)
    assert observed["status"] == "ok"
    return observed["receipt"]


def test_preparation_never_counts_as_verified_and_confirmed_ids_are_idempotent(tmp_path):
    store, key, journal, prepared, attempt, body = staged(tmp_path)
    before = scope(store, key, attempt)
    assert before["scoped_chunks"] == 0 and before["unscoped_chunks"] == 0
    receipt = actual_fixture_receipt(tmp_path, prepared, body)
    assert len(receipt["ids"]) == 2
    assert journal.confirm(prepared, receipt)["status"] == "ok"
    assert journal.confirm(prepared, receipt)["status"] == "ok"
    observed = scope(store, key, attempt)
    assert observed["scoped_chunks"] == 2 and observed["unscoped_chunks"] == 0
    assert observed["scope_accounting_complete"] is True
    assert store.load_checkpoint(key)["offset"] == 0
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 2
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='chunks'").fetchone() is None


def test_unattributed_confirmed_ids_stay_exactly_counted_without_inventing_scope(tmp_path):
    store, key, journal, prepared, attempt, body = staged(tmp_path, workspace=None)
    journal.confirm(prepared, actual_fixture_receipt(tmp_path, prepared, body))
    observed = scope(store, key, attempt)
    assert observed["workspace"] is None and observed["workspace_scope"] == "unattributed"
    assert observed["scoped_chunks"] == 0 and observed["unscoped_chunks"] == 2
    assert observed["scope_accounting_complete"] is True


def test_imported_checkpoint_without_verified_baseline_remains_unknown_in_coverage(tmp_path):
    store, _ = create(tmp_path)
    path = transcript(tmp_path, "original")
    key = capture.queue_source(store, path, allowed_root=tmp_path, format="codex")["source_key"]
    value = checkpoint()
    value.update(offset=1)
    store.save_checkpoint(key, value)
    source = capture._prepare_scope(store, key, None)
    attempt = source["scope_attempt"]
    observed = scope(store, key, attempt)
    assert observed["scope_accounting_complete"] is False and observed["unscoped_chunks"] is None
    assert observed["known_unscoped_chunks"] == 0
    with store._connect(True) as conn:
        source = capture._decode(
            conn.execute(
                "SELECT source_key,state,value FROM capture_sources WHERE source_key=?", (key,)
            ).fetchone()
        )
        source.update(observed, state="caught_up")
        capture._write(conn, source)
    result = capture.coverage(store)
    assert result["coverage"] == "partial" and result["unscoped_chunks"] is None
    assert (
        result["known_unscoped_chunks"] == 0 and result["scope_accounting_incomplete_sources"] == 1
    )
    assert capture.source_page(store, pending_only=True)["sources"][0]["state"] == "pending"


def test_verified_id_ownership_conflict_rolls_back_scope_and_confirmation(tmp_path):
    store, key, journal, prepared, attempt, body = staged(tmp_path)
    receipt = actual_fixture_receipt(tmp_path, prepared, body)
    with store._connect(True) as conn:
        conn.execute(
            "INSERT INTO capture_verified_chunks VALUES (?,?,?)", (receipt["ids"][0], "f" * 64, 0)
        )
    with pytest.raises(_Corrupt):
        journal.confirm(prepared, receipt)
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 0
        assert (
            conn.execute("SELECT state FROM capture_publication_intents").fetchone()[0] == "pending"
        )
    assert store.load_checkpoint(key)["offset"] == 0


def test_prepared_capture_restarts_scope_without_a_canonical_body_table(tmp_path):
    store, key, _, _, _, _ = staged(tmp_path)
    restarted = capture._prepare_scope(store, key, store.load_checkpoint(key))
    assert restarted["scoped_chunks"] == 0 and restarted["scope_accounting_complete"] is True
    assert capture.source_page(store)["sources"][0]["publication_pending"] == 1


def test_legacy_rowid_attempt_cannot_claim_a_complete_control_baseline(tmp_path):
    store, key, _, _, attempt, _ = staged(tmp_path)
    attempt = {**attempt, "after_rowid": 0}
    observed = scope(store, key, attempt)
    assert observed["scope_accounting_complete"] is False and observed["unscoped_chunks"] is None

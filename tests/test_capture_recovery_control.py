"""Real disposable SQL journal handoff; no native storage or user palace."""

import copy
import json
import pytest
from cognitive_castle import capture_publication as journal
from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.fast_memory import _Corrupt
from test_capture_publication_journal import queued, drive


def pending(tmp_path, monkeypatch, count=1):
    store, _, key = queued(tmp_path)
    remember = store.remember
    saved = []

    def lost(*args, **kwargs):
        assert remember(*args, **kwargs)["status"] == "ok"
        saved.append((args, kwargs))
        raise RuntimeError("lost actual publication reply")

    monkeypatch.setattr(store, "remember", lost)
    with pytest.raises(RuntimeError):
        drive(store, key)
    monkeypatch.setattr(store, "remember", remember)
    args, kwargs = saved[0]
    with store._connect() as conn:
        source = json.loads(
            conn.execute("SELECT value FROM capture_sources WHERE source_key=?", (key,)).fetchone()[
                0
            ]
        )
    publication = journal.CapturePublication(store, key, source["scope_attempt"])
    for i in range(1, count):
        prepared = prepare_remember(
            f"other-{i}", f"exact body {i}", args[2], namespace="transcript"
        )
        assert publication.prepare(prepared)["status"] == "ok"
        assert (
            remember(
                prepared.request_id, prepared.records[0].text, args[2], namespace="transcript"
            )["status"]
            == "ok"
        )
    return store, key


def proofs(store, page):
    return [store.receipt(namespace=r["namespace"], key=r["key"]) for r in page["references"]]


def scopes(store):
    with store._connect() as conn:
        return conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0]


def test_split_recovery_needs_only_metadata_until_external_proof_and_replays_ack(
    tmp_path, monkeypatch
):
    store, key = pending(tmp_path, monkeypatch)
    before = store.load_checkpoint(key)
    receipt = store.receipt
    monkeypatch.setattr(
        store, "receipt", lambda **kw: pytest.fail("control operation started canonical reader")
    )
    page = journal.select_recovery_page(store, key)
    assert len(page["references"]) == 1
    assert "BODY_SENTINEL" not in json.dumps(page)
    actual = [receipt(namespace=r["namespace"], key=r["key"]) for r in page["references"]]
    first = journal.apply_recovery_page(store, page, actual)
    assert first == dict(status="ok", checked=1, recovered=1, pending=0)
    assert journal.apply_recovery_page(store, page, actual) == first
    assert scopes(store) == 1 and store.load_checkpoint(key) == before


def test_split_recovery_rejects_invalid_proof_without_applying_expected_ids(tmp_path, monkeypatch):
    store, key = pending(tmp_path, monkeypatch)
    page = journal.select_recovery_page(store, key)
    actual = proofs(store, page)
    for mutation in ("digest", "receipt", "extra", "unavailable"):
        changed = copy.deepcopy(actual)
        if mutation == "digest":
            changed[0]["digest"] = "f" * 64
        elif mutation == "receipt":
            changed[0]["receipt"]["stored_text_bytes"] += 1
        elif mutation == "extra":
            changed[0]["text"] = "never accept a body in control proof"
        else:
            changed[0] = {"status": "unavailable"}
        with pytest.raises(_Corrupt):
            journal.apply_recovery_page(store, page, changed)
        assert scopes(store) == 0
    assert journal.apply_recovery_page(store, page, actual)["recovered"] == 1


def test_split_recovery_stale_cursor_cannot_overwrite_newer_progress(tmp_path, monkeypatch):
    store, key = pending(tmp_path, monkeypatch)
    page = journal.select_recovery_page(store, key)
    actual = proofs(store, page)
    with store._connect(True) as conn:
        source = journal._recovery_source(conn, key)
        source["publication_recovery_after"] = "e" * 64
        from cognitive_castle.memory_capture import _write

        _write(conn, source)
    with pytest.raises(_Corrupt):
        journal.apply_recovery_page(store, page, actual)
    assert scopes(store) == 0


def test_split_recovery_absent_proof_advances_fair_cursor_but_changed_retry_conflicts(
    tmp_path, monkeypatch
):
    store, key = pending(tmp_path, monkeypatch)
    page = journal.select_recovery_page(store, key)
    actual = proofs(store, page)
    result = journal.apply_recovery_page(store, page, [{"status": "not_found"}])
    assert result == dict(status="ok", checked=1, recovered=0, pending=1)
    assert journal.apply_recovery_page(store, page, [{"status": "not_found"}]) == result
    with pytest.raises(_Corrupt):
        journal.apply_recovery_page(store, page, actual)
    assert scopes(store) == 0
    next_page = journal.select_recovery_page(store, key)
    assert next_page["after"] == page["references"][-1]["id"]
    assert next_page["references"] == page["references"]
    assert (
        journal.apply_recovery_page(store, next_page, [{"status": "not_found"}])["recovered"] == 0
    )
    third_page = journal.select_recovery_page(store, key)
    assert third_page["references"] == next_page["references"]
    assert third_page["after"] == next_page["after"]
    assert third_page["revision"] == next_page["revision"] + 1
    assert journal.apply_recovery_page(store, third_page, actual)["recovered"] == 1


def test_split_recovery_rolls_back_first_application_when_second_proof_conflicts(
    tmp_path, monkeypatch
):
    store, key = pending(tmp_path, monkeypatch, count=2)
    page = journal.select_recovery_page(store, key)
    actual = proofs(store, page)
    assert len(actual) == 2
    changed = copy.deepcopy(actual)
    changed[1]["digest"] = "e" * 64
    with pytest.raises(_Corrupt):
        journal.apply_recovery_page(store, page, changed)
    assert scopes(store) == 0
    assert journal.apply_recovery_page(store, page, actual) == dict(
        status="ok", checked=2, recovered=2, pending=0
    )
    assert scopes(store) == 2


def test_split_recovery_corrupt_saved_ack_cannot_fabricate_a_successful_retry(
    tmp_path, monkeypatch
):
    store, key = pending(tmp_path, monkeypatch)
    page = journal.select_recovery_page(store, key)
    actual = proofs(store, page)
    journal.apply_recovery_page(store, page, actual)
    with store._connect(True) as conn:
        source = journal._recovery_source(conn, key)
        source["publication_recovery_ack"]["result"]["pending"] = 99
        from cognitive_castle.memory_capture import _write

        _write(conn, source)
    with pytest.raises(_Corrupt):
        journal.apply_recovery_page(store, page, actual)


def test_saved_ack_rejects_noninteger_source_revision():
    import hashlib

    ack = dict(
        page="a" * 64,
        proofs="b" * 64,
        result=dict(status="ok", checked=1, recovered=1, pending=0),
        revision=1,
    )
    raw = json.dumps(ack, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    ack["binding"] = hashlib.sha256(raw.encode()).hexdigest()
    for revision in (True, 1.0):
        source = dict(publication_recovery_ack=ack, publication_recovery_revision=revision)
        with pytest.raises(_Corrupt):
            journal._recovery_ack(source)

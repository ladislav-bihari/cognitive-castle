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

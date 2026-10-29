"""Receipt-derived capture identities, using disposable SQLite fixtures only."""

import json

import pytest

from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.fast_memory import MemoryStore
from cognitive_castle import memory_capture as capture
from cognitive_castle.memory_ingest import ingest_slice


def transcript(tmp_path):
    path = tmp_path / "session.jsonl"
    path.write_text(
        json.dumps(
            dict(
                type="response_item",
                payload=dict(
                    type="message",
                    role="user",
                    content=[dict(type="input_text", text="Exact original\nčeský text  ")],
                ),
            )
        )
        + "\n"
    )
    return path


def advance(store, path, **kwargs):
    """Bounded functional progress across valid 250ms no-progress slices.

    This is not a latency qualification. No production deadline is changed.
    """
    for _ in range(4):
        result = ingest_slice(store, path, allowed_root=path.parent, format="codex", **kwargs)
        if result["status"] != "ok" or result["offset"]:
            return result
    return result


@pytest.mark.parametrize("fault", ["ids", "bytes", "hash", "missing", "boolean"])
def test_malformed_success_receipt_never_advances_capture_checkpoint(tmp_path, monkeypatch, fault):
    store = MemoryStore(tmp_path / "memory")
    path = transcript(tmp_path)
    original = store.remember

    def damaged(*args, **kwargs):
        receipt = original(*args, **kwargs)
        assert receipt["status"] == "ok"
        if fault == "ids":
            receipt["ids"] = ["memory:" + "a" * 64]
        elif fault == "bytes":
            receipt["stored_text_bytes"] += 1
        elif fault == "hash":
            receipt["text_sha256"] = "b" * 64
        elif fault == "boolean":
            receipt["stored_text_bytes"] = True
        else:
            receipt.pop("ids")
        return receipt

    monkeypatch.setattr(store, "remember", damaged)
    result = advance(store, path)
    assert result["status"] == "unavailable", result
    key = capture.source_key(path, "codex")
    checkpoint = store.load_checkpoint(key)
    assert checkpoint["offset"] == 0 and "pending" not in checkpoint
    monkeypatch.setattr(store, "remember", original)
    retried = advance(store, path)
    assert retried["status"] == "ok" and retried["offset"] == path.stat().st_size
    assert store.status()["indexed_chunks"] == 1


def test_publication_observer_precedes_checkpoint_and_refusal_is_replayable(tmp_path):
    store = MemoryStore(tmp_path / "memory")
    path = transcript(tmp_path)
    key = capture.source_key(path, "codex")
    seen = []

    def refuse(prepared, receipt):
        assert prepared.receipt == receipt
        assert prepared.namespace == "transcript"
        checkpoint = store.load_checkpoint(key)
        assert checkpoint["offset"] == 0 and "pending" not in checkpoint
        seen.extend(receipt["ids"])
        return dict(status="unavailable", reason="metadata_write_refused")

    result = advance(store, path, on_published=refuse)
    assert result["status"] == "unavailable" and len(seen) == 1
    assert store.load_checkpoint(key)["offset"] == 0
    replayed = []

    def accept(prepared, receipt):
        replayed.extend(receipt["ids"])
        return dict(status="ok")

    result = advance(store, path, on_published=accept)
    assert result["status"] == "ok" and result["offset"] == path.stat().st_size
    assert replayed == seen and store.status()["indexed_chunks"] == 1


def test_receipt_validator_preserves_multichunk_originals_and_rejects_boolean_counts():
    from cognitive_castle.canonical_records import validate_remember_receipt

    prepared = prepare_remember("request", "ž" * 6000 + "\n  end", dict(origin="caller"))
    expected = prepared.receipt
    assert len(expected["ids"]) == 2
    assert validate_remember_receipt(prepared, expected) == expected
    with pytest.raises(ValueError):
        validate_remember_receipt(
            prepare_remember("single", "x"),
            dict(prepare_remember("single", "x").receipt, stored_text_bytes=True),
        )


def test_observer_cannot_mutate_a_receipt_then_advance_the_checkpoint(tmp_path):
    store = MemoryStore(tmp_path / "memory")
    path = transcript(tmp_path)

    def mutate(prepared, receipt):
        receipt["ids"].clear()
        return dict(status="ok")

    result = advance(store, path, on_published=mutate)
    assert result["status"] == "unavailable", result
    checkpoint = store.load_checkpoint(capture.source_key(path, "codex"))
    assert checkpoint["offset"] == 0 and "pending" not in checkpoint

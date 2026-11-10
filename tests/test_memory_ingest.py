import json
import os

import pytest

from cognitive_castle.fast_memory import MemoryStore
from cognitive_castle import memory_ingest as ingest


requires_posix_storage = pytest.mark.skipif(
    os.name != "posix" or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"),
    reason="transcript source opening and private MemoryStore persistence require POSIX",
)


def record(text, role="user", **extra):
    return {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": role,
            "content": [{"type": "input_text", "text": text}],
            **extra,
        },
    }


def line(value):
    return json.dumps(value, ensure_ascii=False).encode() + b"\n"


def test_extract_exact_and_exclusions():
    text = "  hé\n\t🙂  "
    assert ingest.extract_messages(record(text), "codex")[0]["text"] == text
    assert ingest.extract_messages(record(text, "assistant", channel="analysis"), "codex") == []
    assert (
        ingest.extract_messages(
            {"type": "event_msg", "payload": {"type": "user_message", "message": text}}, "codex"
        )
        == []
    )
    blocks = ingest.extract_messages(
        {
            "type": "human",
            "message": {
                "content": [
                    {"type": "tool_result", "content": "secret"},
                    {"type": "text", "text": text},
                ]
            },
        },
        "claude",
    )
    assert len(blocks) == 1 and blocks[0]["role"] == "human" and blocks[0]["block"] == 1


@requires_posix_storage
def test_append_tail_and_dedup(tmp_path):
    path = tmp_path / "session.jsonl"
    path.write_bytes(line(record("first apple")) + line(record("second pear"))[:-1])
    store = MemoryStore(tmp_path / "memory")
    first = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert first["coverage"] == "pending_tail"
    assert store.status()["indexed_chunks"] == 1
    with path.open("ab") as out:
        out.write(b"\n")
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert store.status()["indexed_chunks"] == 2


@requires_posix_storage
def test_replacement_retains_history_and_paths_refused(tmp_path):
    path = tmp_path / "session"
    path.write_bytes(line(record("oldword")))
    store = MemoryStore(tmp_path / "memory")
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    path.unlink()
    path.write_bytes(line(record("newword")))
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert store.status()["indexed_chunks"] == 2
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(ValueError):
        ingest.ingest_slice(store, link, allowed_root=tmp_path, format="codex")
    with pytest.raises(ValueError):
        ingest.ingest_slice(store, path, allowed_root=tmp_path / "other", format="codex")


@requires_posix_storage
def test_failed_piece_retry_and_large_text(tmp_path, monkeypatch):
    from types import SimpleNamespace

    path = tmp_path / "session"
    text = "🙂" * 20000
    path.write_bytes(line(record(text)))
    store = MemoryStore(tmp_path / "memory")
    original = store.remember
    calls = []

    committed = []
    clock = [0.0]
    # Force a valid slice boundary after the first durable piece. This test
    # must not depend on two SQLite publications fitting one wall-clock slice.
    monkeypatch.setattr(ingest, "time", SimpleNamespace(monotonic=lambda: clock[0]))

    def fail_second(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 2:
            return {"status": "busy"}
        receipt = original(*args, **kwargs)
        committed.append(receipt)
        clock[0] += 1
        return receipt

    monkeypatch.setattr(store, "remember", fail_second)
    partial = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert partial["status"] == "ok" and partial["offset"] == 0 and len(calls) == 1
    assert (
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")["status"] == "busy"
    )
    assert len(calls) == 2 and calls[0] != calls[1]
    receipts = []
    replayed = []

    def capture(*args, **kwargs):
        replayed.append(args[0])
        receipt = original(*args, **kwargs)
        receipts.append(receipt)
        clock[0] += 1
        return receipt

    monkeypatch.setattr(store, "remember", capture)
    result = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert result["status"] == "ok" and result["offset"] == path.stat().st_size
    assert replayed == [calls[1]]
    assert store.status()["indexed_chunks"] == 10
    restored = "".join(
        source["text"]
        for receipt in committed + receipts
        for source in store.read_sources(receipt["ids"])["sources"]
    )
    assert restored == text


@requires_posix_storage
def test_oversized_record_is_gap_without_json_decode(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(b"x" * (ingest.MAX_RECORD_BYTES + 10) + b"\n" + line(record("aftergap")))
    store = MemoryStore(tmp_path / "memory")
    decode = json.loads

    def bounded_decode(value, *args, **kwargs):
        assert len(value) <= ingest.MAX_RECORD_BYTES
        assert not isinstance(value, bytes) or not value.startswith(b"x")
        return decode(value, *args, **kwargs)

    monkeypatch.setattr(ingest.json, "loads", bounded_decode)
    result = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert result["gaps"] >= 1
    assert store.recall("aftergap")["sources"]
    assert result["coverage"] == "partial"


@requires_posix_storage
def test_checkpoint_failure_visible(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(line(record("apple")))
    store = MemoryStore(tmp_path / "memory")

    def fail(*args, **kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(store, "save_checkpoint", fail)
    with pytest.raises(OSError):
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")


def test_claude_internal_channels_excluded():
    assert (
        ingest.extract_messages(
            {"type": "assistant", "message": {"channel": "analysis", "content": "hidden"}}, "claude"
        )
        == []
    )


@requires_posix_storage
def test_segmented_oversize_and_truncation(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(b"x" * (ingest.MAX_SLICE_BYTES + 100) + b"\n" + line(record("after")))
    store = MemoryStore(tmp_path / "memory")
    first = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert first["offset"] <= ingest.MAX_SLICE_BYTES and first["gaps"] == 1
    second = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert second["gaps"] == 1 and store.recall("after")["sources"]
    path.write_bytes(line(record("short")))
    third = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert third["generation"] == 1
    assert store.recall("after")["sources"]


@requires_posix_storage
def test_load_checkpoint_failure_propagates(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(line(record("apple")))
    store = MemoryStore(tmp_path / "memory")

    def fail(*args, **kwargs):
        raise OSError("unsafe store")

    monkeypatch.setattr(store, "load_checkpoint", fail)
    with pytest.raises(OSError):
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert not store.directory.exists()


def test_parent_traversal_refused(tmp_path):
    root = tmp_path / "registered"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(line(record("forbidden")))
    with pytest.raises(ValueError):
        ingest.ingest_slice(
            MemoryStore(tmp_path / "memory"),
            root / ".." / "outside",
            allowed_root=root,
            format="codex",
        )


@pytest.mark.parametrize("shape", ["blocks", "pieces"])
@requires_posix_storage
def test_deadline_continuation_makes_progress(tmp_path, monkeypatch, shape):
    path = tmp_path / "session"
    value = record("unused")
    value["payload"]["content"] = (
        [{"type": "input_text", "text": f"block{i}"} for i in range(12)]
        if shape == "blocks"
        else [{"type": "input_text", "text": "x" * (65536 * 12)}]
    )
    path.write_bytes(line(value))
    store = MemoryStore(tmp_path / "memory")
    clock = [0.0]
    monkeypatch.setattr(ingest.time, "monotonic", lambda: clock[0])
    original = store.remember
    calls = []

    def slow(*args, **kwargs):
        calls.append(args[0])
        result = original(*args, **kwargs)
        clock[0] += 0.13
        return result

    monkeypatch.setattr(store, "remember", slow)
    for _ in range(8):
        before = len(calls)
        result = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
        assert len(calls) - before <= 2
        if result["offset"] == path.stat().st_size:
            break
    assert result["offset"] == path.stat().st_size
    assert len(calls) == len(set(calls)) == 12


@requires_posix_storage
def test_replacement_after_partial_first_record_is_new_generation(tmp_path, monkeypatch):
    path = tmp_path / "session"
    raw = line(record("🙂" * 20000))
    path.write_bytes(raw)
    store = MemoryStore(tmp_path / "memory")
    original = store.remember
    count = [0]

    def fail_second(*args, **kwargs):
        count[0] += 1
        return {"status": "busy"} if count[0] == 2 else original(*args, **kwargs)

    monkeypatch.setattr(store, "remember", fail_second)
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(raw)
    replacement.replace(path)
    monkeypatch.setattr(store, "remember", original)
    result = ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert result["generation"] == 1
    assert store.status()["indexed_chunks"] == 18


@requires_posix_storage
def test_all_source_reads_within_budget(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(b"x" * (ingest.MAX_SLICE_BYTES + 100))
    original = ingest._open_source
    returned = [0]

    class Reader:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.wrapped.close()

        def read(self, n):
            value = self.wrapped.read(n)
            returned[0] += len(value)
            return value

        def readline(self, n):
            value = self.wrapped.readline(n)
            returned[0] += len(value)
            return value

        def seek(self, *args):
            return self.wrapped.seek(*args)

        def tell(self):
            return self.wrapped.tell()

    def instrument(*args):
        source, info, name = original(*args)
        return Reader(source), info, name

    monkeypatch.setattr(ingest, "_open_source", instrument)
    store = MemoryStore(tmp_path / "memory")
    for _ in range(2):
        returned[0] = 0
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
        assert returned[0] <= ingest.MAX_SLICE_BYTES


def test_unsupported_format_rejected_before_source_open(tmp_path, monkeypatch):
    def unexpected_open(*args):
        pytest.fail("unsupported format must not open a source")

    monkeypatch.setattr(ingest, "_open_source", unexpected_open)
    with pytest.raises(ValueError, match="unsupported transcript format"):
        ingest.extract_messages({}, "unsupported")
    with pytest.raises(ValueError, match="unsupported transcript format"):
        ingest.ingest_slice(
            MemoryStore(tmp_path / "memory"),
            tmp_path / "absent",
            allowed_root=tmp_path,
            format="unsupported",
        )


def observations(store):
    result, after = [], 0
    while True:
        page = store.status(after=after, limit=2)
        assert page["status"] == "ok"
        assert len(json.dumps(page).encode()) <= 32768
        result.extend(page["observations"])
        if page["next_after"] is None:
            return result
        after = page["next_after"]


@requires_posix_storage
def test_durable_coverage_restart_history_and_retry(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(line({"type": "event_msg"}) + b'{"pending":')
    store = MemoryStore(tmp_path / "memory")
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    before = observations(MemoryStore(store.directory))
    assert {o["reason"] for o in before} >= {"unsupported", "pending_tail"}
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert observations(store) == before
    replacement = tmp_path / "replacement"
    replacement.write_bytes(line(record("new exact")))
    replacement.replace(path)
    original = store.remember
    monkeypatch.setattr(store, "remember", lambda *a, **k: {"status": "busy"})
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    failed = observations(MemoryStore(store.directory))
    assert {o["reason"] for o in failed} >= {"changed", "write_attempt", "failed"}
    assert {o["generation"] for o in failed} == {0, 1}
    assert all("text" not in o and str(path) not in json.dumps(o) for o in failed)
    monkeypatch.setattr(store, "remember", original)
    ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    assert any(o["reason"] == "indexed_record" for o in observations(store))


@requires_posix_storage
def test_oversized_segment_coverage_is_durable_and_scoped(tmp_path):
    path = tmp_path / "session"
    raw = b"x" * (ingest.MAX_SLICE_BYTES + 100) + b"\n"
    path.write_bytes(raw)
    store = MemoryStore(tmp_path / "memory")
    for _ in range(3):
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    segments = [o for o in observations(MemoryStore(store.directory)) if o["reason"] == "oversized"]
    assert len(segments) > 1
    offset = 0
    import hashlib

    for segment in segments:
        assert segment["record_start"] == 0
        assert segment["start"] == offset
        assert segment["hash_scope"] == "segment"
        assert (
            segment["sha256"] == hashlib.sha256(raw[segment["start"] : segment["end"]]).hexdigest()
        )
        offset = segment["end"]
    assert offset == len(raw)


@requires_posix_storage
def test_failed_failure_recording_leaves_honest_attempt(tmp_path, monkeypatch):
    path = tmp_path / "session"
    path.write_bytes(line(record("exact original")))
    store = MemoryStore(tmp_path / "memory")
    save = store.save_checkpoint

    def fail_observation(*args, **kwargs):
        if any(o["reason"] == "failed" for o in kwargs.get("observations", [])):
            raise OSError("synthetic disk failure")
        return save(*args, **kwargs)

    monkeypatch.setattr(store, "save_checkpoint", fail_observation)
    monkeypatch.setattr(store, "remember", lambda *a, **k: {"status": "quota"})
    with pytest.raises(OSError):
        ingest.ingest_slice(store, path, allowed_root=tmp_path, format="codex")
    rows = observations(MemoryStore(store.directory))
    assert not any(o["reason"] == "failed" for o in rows)
    attempt = next(o for o in rows if o["reason"] == "write_attempt")
    assert attempt["outcome"] == "unknown_or_interrupted"
    assert store.load_checkpoint(attempt["source_key"])["offset"] == 0
    assert path.read_bytes() == line(record("exact original"))

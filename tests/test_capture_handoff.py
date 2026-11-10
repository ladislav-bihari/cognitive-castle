"""Real parser handoffs with metadata-only state; no SQLite/native/model host."""

import copy
import hashlib
import json

import pytest

from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.memory_ingest import ingest_slice


class MetadataState:
    def __init__(self):
        self.checkpoints = {}
        self.observations = []

    def load_checkpoint(self, key):
        return copy.deepcopy(self.checkpoints.get(key))

    def save_checkpoint(self, key, value, *, observations=None):
        self.checkpoints[key] = copy.deepcopy(value)
        self.observations.extend(copy.deepcopy(observations or []))

    def remember(self, *args, **kwargs):
        raise AssertionError("external publication used canonical SQLite backend")


def transcript(tmp_path, *texts):
    path = tmp_path / "source.jsonl"
    record = {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text} for text in texts],
        },
    }
    path.write_bytes(json.dumps(record, ensure_ascii=False).encode() + b"\n")
    return path


def stage(request_id, text, provenance, *, namespace):
    return dict(
        status="prepared",
        preparation=dict(
            request_id=request_id, text=text, provenance=provenance, namespace=namespace
        ),
    )


def test_prepared_handoff_preserves_text_and_never_advances_piece_checkpoint(tmp_path):
    text = "  HANDOFF_BODY Ω\x00\r\n  "
    path = transcript(tmp_path, text)
    state = MetadataState()
    reserved = []

    def reserve(prepared):
        reserved.append(prepared)
        return dict(status="ok")

    def publish(request_id, piece, provenance, *, namespace):
        assert reserved[-1].request_id == request_id  # Journal precedes handoff.
        return stage(request_id, piece, provenance, namespace=namespace)

    def no_confirmation(*args):
        raise AssertionError("preparation was treated as a durable receipt")

    first = ingest_slice(
        state,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=publish,
        on_prepared=reserve,
        on_published=no_confirmation,
    )
    assert first["status"] == "prepared" and first["coverage"] == "partial"
    assert first["preparation"]["text"] == text
    checkpoint = state.checkpoints[first["source_key"]]
    assert checkpoint["offset"] == 0
    assert checkpoint["pending"] == dict(
        hash=hashlib.sha256(path.read_bytes()).hexdigest(), cursor=[-1, 0]
    )
    assert "HANDOFF_BODY" not in json.dumps(state.checkpoints)
    assert all(value["reason"] != "failed" for value in state.observations)
    replay = ingest_slice(
        state,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=publish,
        on_prepared=reserve,
        on_published=no_confirmation,
    )
    assert replay == first and reserved[0] == reserved[1]


def test_confirmation_advances_only_verified_piece_then_prepares_next(tmp_path):
    path = transcript(tmp_path, "first Ω", "second\x00")
    state = MetadataState()
    reserved, applied = [], []

    def reserve(prepared):
        reserved.append(prepared)
        return dict(status="ok")

    def confirm(prepared, receipt):
        applied.append((prepared, receipt))
        return dict(status="ok")

    def run(publisher):
        return ingest_slice(
            state,
            path,
            allowed_root=tmp_path,
            format="codex",
            publisher=publisher,
            on_prepared=reserve,
            on_published=confirm,
        )

    first = run(stage)
    original = reserved[-1]

    def accept_first(request_id, text, provenance, *, namespace):
        if request_id == original.request_id:
            return original.receipt  # Parser contract test; no durability claim.
        return stage(request_id, text, provenance, namespace=namespace)

    second = run(accept_first)
    assert second["status"] == "prepared" and second["preparation"]["text"] == "second\x00"
    checkpoint = state.checkpoints[first["source_key"]]
    assert checkpoint["offset"] == 0 and checkpoint["pending"]["cursor"] == [0, len("first Ω")]
    assert applied == [(original, original.receipt)]
    following = reserved[-1]

    def accept_second(request_id, text, provenance, *, namespace):
        assert request_id == following.request_id
        return following.receipt

    final = run(accept_second)
    assert final["status"] == "ok"
    checkpoint = state.checkpoints[first["source_key"]]
    assert checkpoint["offset"] == path.stat().st_size and "pending" not in checkpoint
    assert checkpoint["accounting"]["processed_records"] == 1
    assert len(applied) == 2 and all(value["reason"] != "failed" for value in state.observations)


@pytest.mark.parametrize(
    "value",
    [None, dict(status="prepared", preparation="bad"), dict(status="prepared", preparation={})],
    ids=["missing-result", "non-object", "empty-handoff"],
)
def test_invalid_publisher_result_never_confirms_or_advances(tmp_path, value):
    path = transcript(tmp_path, "body")
    state = MetadataState()

    def no_confirmation(*args):
        raise AssertionError("invalid publisher result was confirmed")

    result = ingest_slice(
        state,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=lambda *a, **k: value,
        on_prepared=lambda prepared: dict(status="ok"),
        on_published=no_confirmation,
    )
    assert result["status"] == "unavailable"
    assert state.checkpoints[result["source_key"]]["offset"] == 0


def test_mismatched_receipt_never_advances_checkpoint(tmp_path):
    path = transcript(tmp_path, "real")
    state = MetadataState()
    wrong = prepare_remember("other", "other", namespace="transcript").receipt
    result = ingest_slice(
        state,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=lambda *a, **k: wrong,
        on_prepared=lambda prepared: dict(status="ok"),
        on_published=lambda *a: pytest.fail("wrong receipt confirmed"),
    )
    assert result["status"] == "unavailable"
    assert state.checkpoints[result["source_key"]]["offset"] == 0


def test_external_publisher_requires_observers_before_any_source_io():
    with pytest.raises(ValueError):
        ingest_slice(
            MetadataState(),
            "/never-open",
            allowed_root="/never-open",
            format="codex",
            publisher=stage,
        )

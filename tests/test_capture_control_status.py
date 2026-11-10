"""Real metadata-only status pages from the independent control worker."""

import hashlib
import json
import pytest

from test_capture_control_store import create, checkpoint
from test_capture_control_worker import worker, request


def test_control_status_pages_recorded_observations_without_canonical_text(tmp_path):
    store, authority = create(tmp_path)
    for number in range(3):
        key = hashlib.sha256(str(number).encode()).hexdigest()
        store.save_checkpoint(
            key,
            checkpoint(),
            observations=[
                dict(
                    generation=0,
                    reason="malformed",
                    start=number,
                    end=number + 1,
                    record_start=number,
                    hash_scope="unknown",
                )
            ],
        )
    before = hashlib.sha256(store.db_path.read_bytes()).hexdigest()
    first = worker()._execute(request(authority, "control_status", dict(limit=2)))
    assert first["status"] == "ok" and len(first["observations"]) == 2 and first["next_after"] == 2
    assert first["observation_currentness"] == "recorded_only_source_not_reopened"
    second = worker()._execute(
        request(authority, "control_status", dict(after=first["next_after"], limit=2))
    )
    assert [row["sequence"] for row in second["observations"]] == [3] and second[
        "next_after"
    ] is None
    assert hashlib.sha256(store.db_path.read_bytes()).hexdigest() == before
    assert "indexed_chunks" not in first  # A metadata sidecar is not a canonical record count.
    with store._connect() as conn:
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='chunks'").fetchone() is None


@pytest.mark.parametrize("args", [dict(after=-1), dict(after=True), dict(limit=0), dict(limit=17)])
def test_control_status_invalid_bounds_refuse_before_store(monkeypatch, args):
    monkeypatch.setattr(worker(), "_store", lambda *a: pytest.fail("invalid status reached SQL"))
    # A valid authority is unnecessary: validate_operation is the pre-store boundary.
    from cognitive_castle.capture_control_codec import validate_operation

    with pytest.raises(ValueError):
        validate_operation("control_status", args)


def test_control_status_refuses_malformed_observation_without_writes(tmp_path):
    store, authority = create(tmp_path)
    with store._connect(True) as conn:
        conn.execute(
            "INSERT INTO coverage_observations(identity,value) VALUES (?,?)",
            ("a" * 64, json.dumps(dict(source_key="bad", kind="gap", start=0, end=1))),
        )
    before = store.db_path.read_bytes()
    assert worker()._execute(request(authority, "control_status", {}))["status"] == "corrupt"
    assert store.db_path.read_bytes() == before

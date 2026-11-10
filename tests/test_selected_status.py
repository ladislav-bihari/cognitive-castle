"""Actual registered SQL status and selected host composition boundaries."""

import time
import pytest

from test_layout_live_control import control as control, prepare
from cognitive_castle import migration_codec as migration


def test_pinned_queue_status_reports_recorded_claim_basis_without_mutation(control):
    store, _, envelope = control
    prepare(control)
    path = store.directory / "queue.sqlite3"
    before = path.read_bytes()
    result = store.status(envelope_hash=migration.hashed(envelope))
    assert result["status"] == "ok"
    assert result["registered"] == dict(
        envelope_hash=migration.hashed(envelope),
        confirmed_records=0,
        pending_claims=0,
        count_basis="registered_claims",
        native_rows_rechecked=False,
    )
    assert result["coverage_complete"] is False and path.read_bytes() == before


def test_pinned_queue_status_refuses_wrong_authority_without_writes(control):
    store, _, _ = control
    prepare(control)
    path = store.directory / "queue.sqlite3"
    before = path.read_bytes()
    assert store.status(envelope_hash="f" * 64) == dict(status="conflict")
    assert path.read_bytes() == before


@pytest.mark.parametrize("pin", [True, "invalid"])
def test_queue_status_rejects_invalid_pin_before_sql(control, monkeypatch, pin):
    store, _, _ = control
    monkeypatch.setattr(store, "_run", lambda *a: pytest.fail("invalid status pin reached SQL"))
    with pytest.raises(ValueError):
        store.status(envelope_hash=pin)


@pytest.mark.parametrize("failure", [None, "control_status", "queue_status", "coverage"])
def test_selected_status_uses_current_authority_and_original_single_lease(
    monkeypatch, tmp_path, failure
):
    from cognitive_castle import memory_selected_runtime as api, semantic_runtime
    from cognitive_castle.capture_control_runtime import CaptureControlClient
    from test_memory_selection import fixture
    from test_memory_selected_runtime import session

    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)
    calls = []
    refusal = dict(status="unavailable", reason="measured_status_refusal")

    def control_call(self, operation, args):
        assert self.session is made[0]
        calls.append(operation)
        if operation == failure:
            return refusal
        if operation == "control_status":
            assert args == dict(after=7, limit=2)
            return dict(
                status="ok",
                observations=[],
                next_after=None,
                observation_currentness="recorded_only_source_not_reopened",
            )
        assert operation == "coverage"
        return dict(status="ok", coverage="partial", pending_sources=2)

    def store(self, generation, operation, args):
        calls.append(operation)
        assert operation == "queue_status" and args == dict(envelope_hash=api.layout_hash(selected))
        if operation == failure:
            return refusal
        return dict(
            status="ok",
            counts=dict(pending=0, inflight=0, complete=0, refused=0),
            groups=[],
            positions_truncated=False,
            coverage_complete=False,
            registered=dict(
                envelope_hash=api.layout_hash(selected),
                confirmed_records=3,
                pending_claims=1,
                count_basis="registered_claims",
                native_rows_rechecked=False,
            ),
        )

    monkeypatch.setattr(CaptureControlClient, "call", control_call)
    monkeypatch.setattr(semantic_runtime.SemanticSession, "store", store, raising=False)
    until = time.monotonic() + 2
    result = api.run_selected(
        palace, selected, "status", dict(after=7, limit=2), until=until, background=False
    )
    assert len(made) == 1 and made[0].closed and made[0]._until <= until
    assert made[0].calls == []  # No native/flat read or publication.
    if failure:
        assert result == refusal and calls[-1] == failure
    else:
        assert calls == ["control_status", "queue_status", "coverage"]
        assert result["status"] == "ok" and result["indexed_chunks"] == 3
        assert result["canonical"]["native_rows_rechecked"] is False
        assert result["generation"] == api.layout_hash(selected)
        assert result["capture"]["pending_sources"] == 2
        assert result["coverage_detail"]["complete"] is False
        assert "semantic_coverage_unqualified" in result["coverage_detail"]["limitations"]

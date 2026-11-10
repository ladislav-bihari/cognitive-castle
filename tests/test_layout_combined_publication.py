"""Combined native boundary contracts; real engine qualification remains separate."""

import copy
from pathlib import Path
import pytest
from cognitive_castle.partitioned_exact_store import PartitionedExactStore
from test_layout_coordinator import GroupedSession


@pytest.mark.parametrize("outcome", ["fresh_append", "unavailable"])
def test_combined_import_never_reads_owners_without_actual_existing_replay(monkeypatch, outcome):
    store = PartitionedExactStore(Path("/private/generation"), writable=True)
    batch = (
        {"status": "unavailable", "reason": "worker_deadline"}
        if outcome == "unavailable"
        else dict(
            status="ok",
            format="lance-layout-batch-result-v1",
            outcome=outcome,
            receipt={},
            routes=[],
        )
    )
    monkeypatch.setattr(store, "layout_import_batch", lambda **kw: copy.deepcopy(batch))

    def forbidden(**kw):
        raise AssertionError("unverified append cannot produce owner evidence")

    monkeypatch.setattr(store, "layout_verify_owners", forbidden)
    result = store.layout_import_verified(
        intent_id="a" * 64, batch_id="a" * 64, source_binding="b" * 64, entries=[], finalizations=[]
    )
    assert result == (batch if outcome == "unavailable" else {**batch, "owner_proof": None})


def test_combined_import_observes_the_actual_replay_version(monkeypatch):
    store = PartitionedExactStore(Path("/private/generation"), writable=True)
    batch = dict(
        status="ok",
        format="lance-layout-batch-result-v1",
        outcome="existing_verified",
        receipt={},
        routes=[dict(version=7)],
    )
    proof = {"status": "unavailable", "reason": "worker_deadline"}
    monkeypatch.setattr(store, "layout_import_batch", lambda **kw: copy.deepcopy(batch))

    def observe(**kw):
        assert kw == dict(intent_id="a" * 64, version=7)
        return proof

    monkeypatch.setattr(store, "layout_verify_owners", observe)
    assert (
        store.layout_import_verified(
            intent_id="a" * 64,
            batch_id="a" * 64,
            source_binding="b" * 64,
            entries=[],
            finalizations=GroupedSession(live=True).exported["finalizations"],
        )
        == proof
    )


class CombinedSession(GroupedSession):
    def exact(self, directory, operation, arguments):
        assert operation == "layout_import_verified", (
            "combined path must not start a second owner worker"
        )
        assert directory == Path(self.reg["destination"])
        self.calls.append((operation, copy.deepcopy(arguments)))
        batch = copy.deepcopy(self.batch)
        manifest = self.exported["finalizations"][0]
        live = manifest["live"]
        proof = dict(
            status="ok",
            format="lance-layout-owner-verification-v1",
            intent_hash=self.migration.hashed(self.intent),
            owners=[
                dict(
                    owner=manifest["owner"],
                    manifest_hash=self.migration.hashed(manifest),
                    partition_id=0,
                    version=7,
                    live_receipt_ids=live["receipt"]["ids"],
                    live=dict(
                        namespace=live["namespace"],
                        key=live["key"],
                        digest=live["digest"],
                        receipt_hash=self.migration.hashed(live["receipt"]),
                    ),
                )
            ],
        )
        if self.mode == "missing":
            proof = None
        elif self.mode == "hash":
            proof["owners"][0]["manifest_hash"] = "f" * 64
        elif self.mode == "version":
            proof["owners"][0]["version"] = 8
        elif self.mode == "live":
            proof["owners"][0]["live"]["receipt_hash"] = "f" * 64
        elif self.mode == "intent":
            proof["intent_hash"] = "f" * 64
        elif self.mode == "late":
            self.remaining = 0
        return {**batch, "owner_proof": proof}


def test_combined_coordinator_acks_actual_grouped_observations_in_one_native_stage():
    from cognitive_castle.migration_coordinator import _layout_tick

    session = CombinedSession(live=True)
    result = _layout_tick(session, session.reg, defer_append=True, combined_owners=True)
    assert result == dict(status="ok", intent_id=session.intent["intent_id"])
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "layout_import_verified",
        "queue_layout_ack",
    ]
    summary = session.calls[-1][1]["verified_summary"]
    assert summary["owner_proofs"][0]["version"] == 7
    assert (
        summary["owner_proofs"][0]["live_receipt_ids"]
        == session.intent["owners"][0]["live_receipt_ids"]
    )


@pytest.mark.parametrize("mode", ["missing", "hash", "version", "live", "intent", "late"])
def test_combined_coordinator_never_acks_missing_changed_or_late_evidence(mode):
    from cognitive_castle.migration_coordinator import _layout_tick

    session = CombinedSession(live=True, mode=mode)
    result = _layout_tick(session, session.reg, defer_append=True, combined_owners=True)
    assert result["status"] == ("unavailable" if mode == "late" else "conflict")
    assert "queue_layout_ack" not in [op for op, _ in session.calls]

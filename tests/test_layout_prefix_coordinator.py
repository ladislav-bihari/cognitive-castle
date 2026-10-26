"""Prefix controller ordering over real metadata logic; no native IO proof."""

import copy
from pathlib import Path

import pytest


class Model:
    def __init__(self, monkeypatch, *, unbound=False, reverse=True):
        from test_layout_packed_transaction import case
        from test_layout_source_packing import prefix_case

        self.directory, self.job, _, self.store, self.state, _ = case(monkeypatch)
        self.reg = self.state["registration"]
        self.parts = prefix_case()[2]
        self.reverse = reverse
        self.calls = []
        self.durable = set()
        self.bad_source = self.bad_owner = self.late_group = False
        self.lost_bind = self.lost_ack = self.bad_binding = False
        if unbound:
            for key in ("homes", "spans", "reservations"):
                self.store[key].clear()
            for registry in self.store["registry"].values():
                registry["active_owner"] = None

    def session(self):
        return Session(self)


class Session:
    def __init__(self, model):
        self.model = model
        self.expired = False

    @property
    def remaining(self):
        return 0 if self.expired else 5

    def _ready(self):
        return dict(status="unavailable", reason="semantic_deadline") if self.expired else None

    def store(self, generation, operation, arguments):
        m = self.model
        assert Path(generation) == Path(m.reg["destination"])
        m.calls.append((operation, copy.deepcopy(arguments)))
        if operation == "queue_layout_resume":
            context = m.directory.context(m.job)
            if m.reverse and "bound_plans" in context:
                context["bound_plans"].reverse()
            return dict(copy.deepcopy(m.state), status="ok", native_layout=context)
        if operation == "queue_layout_bind_prefix":
            result = m.directory.bind_prefix(**arguments)
            if m.bad_binding:
                result["target"]["owner_plan_hashes"].reverse()
            if m.lost_bind:
                m.lost_bind = False
                return dict(status="unavailable", reason="lost_bind_reply")
            return result
        if operation == "queue_layout_prepare":
            return m.directory.prepare(**arguments)
        if operation == "queue_layout_ack":
            result = m.directory.ack(**arguments)
            if m.lost_ack:
                m.lost_ack = False
                return dict(status="unavailable", reason="lost_ack_reply")
            return result
        raise AssertionError("unexpected operation " + operation)

    def layout_source(self, descriptor, before, window):
        from cognitive_castle import lance_layout_codec as layout
        from cognitive_castle.lance_layout_packing import pack_source_prefix, _prefix_envelope

        m = self.model
        assert descriptor == m.reg["descriptor"]
        assert window["format"] == layout.PREFIX_SOURCE_REQUEST
        m.calls.append(("source_" + window["action"], copy.deepcopy(window)))
        start = next(i for i, p in enumerate(m.parts) if p["export"]["cursor_before"] == before)
        parts = m.parts[start:]
        if window["action"] == "export":
            parts = parts[: len(window["allowed_plan_hashes"])]
        packet = pack_source_prefix(m.reg, before, parts)
        response = _prefix_envelope(m.reg, before, window["action"], prefix=packet)
        if m.bad_source:
            response["prefix"]["components"][-1]["export"]["entries"][0]["text"] += (
                "changed original"
            )
        return response

    def exact(self, generation, operation, arguments):
        from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
        from test_layout_packed_transaction import observed

        m = self.model
        m.calls.append((operation, copy.deepcopy(arguments)))
        intent = m.state["pending"]
        owner_ids = {o["owner"] for o in intent["owners"]}
        parts = [p for p in m.parts if p["plan"]["owner"] in owner_ids]
        if operation == "layout_import_batch":
            assert arguments["entries"] == [r for p in parts for r in p["export"]["entries"]]
            assert arguments["finalizations"] == [p["export"]["finalizations"][0] for p in parts]
            if intent["intent_id"] not in m.durable:
                m.durable.add(intent["intent_id"])
                return dict(
                    status="ok",
                    format=layout.BATCH_RESULT,
                    outcome="fresh_append",
                    receipt=layout.expected_receipt(intent),
                    routes=[],
                )
            return dict(status="ok", **observed(intent, parts)["batch"])
        if operation == "layout_verify_owners":
            assert intent["intent_id"] in m.durable
            manifests = [p["export"]["finalizations"][0] for p in parts]
            proofs = [
                layout.owner_observation(manifest, intent["target"]["partition_id"], 1)
                for manifest in manifests
            ]
            if m.bad_owner:
                proofs[-1]["manifest_hash"] = "e" * 64
            if m.late_group:
                self.expired = True
            return dict(
                status="ok",
                format=layout.GROUPED_OWNER_PROOF,
                intent_hash=migration.hashed(intent),
                owners=proofs,
            )
        raise AssertionError("unexpected exact " + operation)


def step(model):
    from cognitive_castle import migration_coordinator as controller

    return controller._layout_prefix_step(model.session(), model.reg)


@pytest.mark.parametrize("unbound", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_prefix_fresh_yields_then_cold_existing_group_and_ack_original_order(
    monkeypatch, unbound, reverse
):
    model = Model(monkeypatch, unbound=unbound, reverse=reverse)
    before = copy.deepcopy(model.state["staging_cursor"])
    first = step(model)
    assert first["status"] == "in_progress" and first["cursor_after"] == before
    pending = copy.deepcopy(model.state["pending"])
    assert len(pending["owners"]) == 2 and not model.store["migration_owners"]
    assert [op for op, _ in model.calls] == [
        "queue_layout_resume",
        "source_prepare",
        "queue_layout_bind_prefix",
        "queue_layout_prepare",
        "layout_import_batch",
    ]
    assert not any(h["published"] for h in model.store["homes"].values())
    model.calls.clear()
    result = step(model)
    assert result["status"] == "ok" and result["completed_frontier"] == pending["cursor_after"]
    assert model.state["pending"] is None and len(model.store["migration_owners"]) == 2
    assert [op for op, _ in model.calls] == [
        "queue_layout_resume",
        "source_export",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    assert model.calls[1][1]["allowed_plan_hashes"] == pending["target"]["owner_plan_hashes"]


@pytest.mark.parametrize("fault", ["bad_source", "bad_owner", "late_group"])
def test_later_source_or_owner_failure_never_acknowledges_any_prefix_part(monkeypatch, fault):
    model = Model(monkeypatch)
    assert step(model)["status"] == "in_progress"
    before = copy.deepcopy(model.state["staging_cursor"])
    pending = copy.deepcopy(model.state["pending"])
    model.calls.clear()
    setattr(model, fault, True)
    result = step(model)
    assert result["status"] != "ok" and result["cursor_after"] == before
    assert model.state["pending"] == pending and not model.store["migration_owners"]
    assert not any(h["published"] for h in model.store["homes"].values())
    assert "queue_layout_ack" not in [op for op, _ in model.calls]
    if fault == "bad_source":
        assert "layout_import_batch" not in [op for op, _ in model.calls]


def test_lost_binding_reply_recovers_from_original_source_without_reserving_a_ghost(monkeypatch):
    model = Model(monkeypatch, unbound=True)
    model.lost_bind = True
    first = step(model)
    assert first["status"] == "unavailable" and model.state["pending"] is None
    homes = copy.deepcopy(model.store["homes"])
    assert len(homes) == 2 and not model.store["claims"]
    assert step(model)["status"] == "in_progress"
    assert model.store["homes"] == homes
    assert step(model)["status"] == "ok"


def test_bad_binding_response_cannot_authorize_prepare_or_canonical_import(monkeypatch):
    model = Model(monkeypatch)
    model.bad_binding = True
    result = step(model)
    assert result["status"] == "conflict" and result["phase"] == "bind"
    assert [op for op, _ in model.calls] == [
        "queue_layout_resume",
        "source_prepare",
        "queue_layout_bind_prefix",
    ]
    assert model.state["pending"] is None and not model.store["claims"]


def test_lost_ack_reply_resumes_after_committed_frontier_without_duplicate_owners(monkeypatch):
    model = Model(monkeypatch)
    assert step(model)["status"] == "in_progress"
    after = copy.deepcopy(model.state["pending"]["cursor_after"])
    model.lost_ack = True
    assert step(model)["status"] == "unavailable"
    assert model.state["pending"] is None and model.state["completed_frontier"] == after
    assert len(model.store["migration_owners"]) == 2
    model.calls.clear()
    assert step(model)["status"] == "in_progress"
    assert model.state["pending"]["cursor_before"] == after
    assert len(model.store["migration_owners"]) == 2
    assert all(
        p["owner"] == model.parts[2]["plan"]["owner"] for p in model.state["pending"]["owners"]
    )


def test_full_metadata_capacity_refuses_before_reserving_any_owner(monkeypatch):
    from cognitive_castle import migration_codec as migration

    model = Model(monkeypatch, unbound=True)
    before = dict(model.state["staging_cursor"], phase="history", key=["!" * 32768])
    migration.cursor(before, model.reg)
    model.state.update(staging_cursor=before, completed_frontier=copy.deepcopy(before))
    model.state["last_ack"] = dict(
        intent_id="e" * 64,
        summary_hash="f" * 64,
        result=dict(
            status="ok",
            job_id=model.job,
            intent_id="e" * 64,
            state="active",
            staging_cursor=before,
            completed_frontier=before,
            counts=model.state["counts"],
        ),
    )
    model.parts[0]["export"]["cursor_before"] = copy.deepcopy(before)
    for _ in range(3):
        exported = model.parts[0]["export"]
        exported["frame_bytes"] = len(migration.frame_bytes(exported))
    migration.bounded(
        dict(model.state, status="ok", native_layout=model.directory.context(model.job))
    )
    result = step(model)
    assert result["status"] == "conflict" and result["phase"] == "source"
    assert [op for op, _ in model.calls] == ["queue_layout_resume", "source_prepare"]
    assert (
        not model.store["homes"] and not model.store["reservations"] and not model.store["claims"]
    )
    assert model.state["pending"] is None and model.state["staging_cursor"] == before


def test_first_fallback_uses_original_single_owner_without_extra_resume_or_deadline():
    from test_layout_coordinator import PendingSession
    from cognitive_castle import migration_coordinator as controller
    from cognitive_castle.lance_layout_packing import _prefix_envelope

    class Fallback(PendingSession):
        def layout_source(self, descriptor, cursor, window):
            if window["format"] == self.layout.PREFIX_SOURCE_REQUEST:
                self.calls.append(("prefix_source", copy.deepcopy(window)))
                return _prefix_envelope(self.reg, cursor, "prepare", fallback_plan=self.plan)
            return super().layout_source(descriptor, cursor, window)

    session = Fallback(new_owner=True, fresh=1)
    result = controller._layout_prefix_step(session, session.reg)
    assert result["status"] == "in_progress" and result["intent_id"] == session.intent["intent_id"]
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "prefix_source",
        "queue_layout_bind",
        "layout_source",
        "queue_layout_prepare",
        "layout_import_batch",
    ]


def test_existing_legacy_pending_replays_with_original_contract_and_same_session():
    from test_layout_coordinator import PendingSession
    from cognitive_castle import migration_coordinator as controller

    session = PendingSession()
    result = controller._layout_prefix_step(session, session.reg)
    assert result == dict(status="ok", intent_id=session.intent["intent_id"])
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]


def test_terminal_source_keeps_original_coverage_and_completed_frontier(monkeypatch):
    from test_layout_prefix_protocol import terminal
    from cognitive_castle.lance_layout_packing import _prefix_envelope

    model = Model(monkeypatch, unbound=True)
    before = copy.deepcopy(model.state["staging_cursor"])
    session = model.session()
    evidence = terminal(model.reg, before)

    def source(descriptor, cursor, window):
        model.calls.append(("source_prepare", copy.deepcopy(window)))
        return _prefix_envelope(model.reg, cursor, "prepare", terminal=evidence)

    def ack(job_id, intent_id, verified_summary):
        assert job_id == model.job and intent_id is None
        assert verified_summary == dict(
            state="snapshot_export_exhausted",
            cursor_before=before,
            cursor_after=evidence["cursor_after"],
            coverage=evidence["coverage"],
        )
        return dict(status="ok", state="snapshot_export_exhausted", completed_frontier=before)

    session.layout_source = source
    model.directory.ack = ack
    from cognitive_castle import migration_coordinator as controller

    result = controller._layout_prefix_step(session, model.reg)
    assert result["completed_frontier"] == before
    assert [op for op, _ in model.calls] == [
        "queue_layout_resume",
        "source_prepare",
        "queue_layout_ack",
    ]

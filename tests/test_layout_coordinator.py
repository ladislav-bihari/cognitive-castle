"""Engine-free publication ordering; fake boundaries prove no native IO."""

import copy
from pathlib import Path
import sys
import pytest

from test_lance_layout_codec import fixture


def pending_case(*, live=False):
    layout, migration, reg, exported, plan, envelope, target = fixture()
    if live:
        from test_migration_codec import exported as source_export

        exported = source_export(reg, phase="live")
        reference = migration.owner_reference(
            exported["finalizations"][0], "live", exported["cursor_after"]["key"]
        )
        plan = layout.owner_plan(reg, reference, plan["expansion"])
        target = layout.target(envelope, 0, [plan])
    intent = layout.prepare_layout_interval(reg, exported, target)
    before = copy.deepcopy(exported["cursor_before"])
    state = dict(
        status="ok",
        registration=reg,
        pending=intent,
        staging_cursor=before,
        completed_frontier=before,
        counts=copy.deepcopy(exported["counts"]),
        state="active",
        last_ack=None,
        layout=dict(
            format=layout.ATTACHMENT, envelope_hash=migration.hashed(envelope), envelope=envelope
        ),
        native_layout=dict(
            format="lance-layout-native-context-v1",
            generation=envelope["generation"],
            qualification_hash=envelope["qualification_hash"],
            epoch="e" * 64,
            plan=plan,
            target=target,
        ),
    )
    routes = [
        dict(
            r,
            format=layout.ROUTE,
            row_hash=r["row_hash"] or "c" * 64,
            partition_id=target["partition_id"],
            version=7,
            receipt=layout.batch_receipt_id(intent),
            state="staged",
        )
        for r in intent["routes"]
    ]
    batch = dict(
        status="ok",
        format=layout.BATCH_RESULT,
        outcome="existing_verified",
        receipt=layout.expected_receipt(intent),
        routes=routes,
    )
    return layout, migration, reg, exported, intent, state, batch


class PendingSession:
    def __init__(
        self,
        *,
        lost_publish=False,
        late_owner=False,
        changed_source=False,
        fresh=0,
        source_refusal=False,
        live=False,
        live_refusal=False,
        new_owner=False,
    ):
        (
            self.layout,
            self.migration,
            self.reg,
            self.exported,
            self.intent,
            self.state,
            self.batch,
        ) = pending_case(live=live or live_refusal)
        self.remaining = 5
        self.calls = []
        self.lost_publish, self.late_owner, self.changed_source = (
            lost_publish,
            late_owner,
            changed_source,
        )
        self.fresh, self.source_refusal = fresh, source_refusal
        self.live_refusal = live_refusal
        self.plan = self.state["native_layout"]["plan"]
        self.target = self.state["native_layout"]["target"]
        if new_owner:
            self.state["pending"] = None
            self.state["native_layout"].update(plan=None, target=None)

    def _ready(self):
        return (
            {"status": "unavailable", "reason": "semantic_deadline"}
            if self.remaining <= 0
            else None
        )

    def store(self, directory, operation, arguments):
        assert directory == Path(self.reg["destination"])
        self.calls.append((operation, copy.deepcopy(arguments)))
        if operation == "queue_layout_resume":
            assert arguments == {"registration": self.reg}
            return copy.deepcopy(self.state)
        if operation == "queue_migration_refuse":
            return {"status": "ok"}
        if operation == "queue_layout_bind":
            assert arguments == dict(
                job_id=self.reg["job_id"],
                plans=[self.plan],
                envelope=self.state["layout"]["envelope"],
            )
            return dict(status="ok", layout=self.state["layout"], targets=[self.target])
        if operation == "queue_layout_prepare":
            assert arguments == dict(
                job_id=self.reg["job_id"],
                expected_cursor=self.intent["cursor_before"],
                intent=self.intent,
            )
            return dict(status="ok", intent_id=self.intent["intent_id"])
        assert operation == "queue_layout_ack"
        assert self._ready() is None, "late worker success cannot authorize ACK"
        assert arguments["intent_id"] == self.intent["intent_id"]
        self.layout.validate_layout_verified(arguments["verified_summary"], self.intent)
        return {"status": "ok", "intent_id": arguments["intent_id"]}

    def layout_source(self, descriptor, cursor, window):
        assert descriptor == self.reg["descriptor"]
        assert cursor == self.intent["cursor_before"]
        self.layout.validate_layout_source_window(window, descriptor, cursor)
        assert window["generation"] == self.state["layout"]["envelope"]["generation"]
        self.calls.append(("layout_source", copy.deepcopy(window)))
        if self.source_refusal:
            return {"status": "unavailable", "reason": "source_deadline"}
        if window["action"] == "plan":
            assert window["allowed_plan_hashes"] == []
            return dict(
                status="ok",
                reason=None,
                format=self.layout.SOURCE_RESULT,
                action="plan",
                source_binding=self.reg["source_binding"],
                cursor_before=cursor,
                cursor_after=cursor,
                plans=[self.plan],
                export=None,
            )
        assert window["allowed_plan_hashes"] == self.target["owner_plan_hashes"]
        result = copy.deepcopy(self.exported)
        if self.changed_source:
            result["entries"] = []
        return dict(
            status="ok",
            reason=None,
            format=self.layout.SOURCE_RESULT,
            action="export",
            source_binding=self.reg["source_binding"],
            cursor_before=cursor,
            cursor_after=result["cursor_after"],
            plans=[self.plan],
            export=result,
        )

    def exact(self, directory, operation, arguments):
        assert directory == Path(self.reg["destination"]), (
            "partition paths belong to trusted facade"
        )
        self.calls.append((operation, copy.deepcopy(arguments)))
        if operation == "layout_import_batch":
            assert arguments == dict(
                intent_id=self.intent["intent_id"],
                batch_id=self.intent["intent_id"],
                source_binding=self.reg["source_binding"],
                entries=self.exported["entries"],
                finalizations=self.exported["finalizations"],
            )
            if self.lost_publish:
                return {"status": "unavailable", "reason": "lost_publication_response"}
            if self.fresh:
                self.fresh -= 1
                return {**self.batch, "outcome": "fresh_append", "routes": []}
            return copy.deepcopy(self.batch)
        assert operation == "layout_verify_owners"
        assert arguments == dict(intent_id=self.intent["intent_id"], version=7)
        if self.live_refusal:
            return {"status": "unavailable", "reason": "semantic_deadline"}
        if self.late_owner:
            self.remaining = 0
        return grouped_observation(self)


def grouped_observation(session):
    owner = session.exported["finalizations"][0]
    live = owner["live"]
    return dict(
        status="ok",
        format="lance-layout-owner-verification-v1",
        intent_hash=session.migration.hashed(session.intent),
        owners=[
            dict(
                owner=owner["owner"],
                manifest_hash=session.migration.hashed(owner),
                partition_id=session.intent["target"]["partition_id"],
                version=7,
                live_receipt_ids=[] if live is None else live["receipt"]["ids"],
                live=None
                if live is None
                else dict(
                    namespace=live["namespace"],
                    key=live["key"],
                    digest=live["digest"],
                    receipt_hash=session.migration.hashed(live["receipt"]),
                ),
            )
        ],
    )


def test_layout_tick_keeps_fixed_partition_through_retry():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    first = PendingSession(lost_publish=True)
    saved = copy.deepcopy(first.state)
    refusal = _layout_fixture_tick(first, first.reg)
    assert refusal["status"] == "unavailable"
    assert first.state == saved
    assert not any(
        op
        in {"queue_layout_ack", "layout_verify_owners", "queue_layout_bind", "queue_layout_prepare"}
        for op, _ in first.calls
    )
    retry = PendingSession()
    assert _layout_fixture_tick(retry, retry.reg) == {
        "status": "ok",
        "intent_id": retry.intent["intent_id"],
    }
    assert [
        (op, args) for op, args in first.calls if op in {"layout_source", "layout_import_batch"}
    ] == [(op, args) for op, args in retry.calls if op in {"layout_source", "layout_import_batch"}]
    assert [op for op, _ in retry.calls] == [
        "queue_layout_resume",
        "layout_source",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def test_layout_resume_precedes_pending_original_regeneration():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession()
    saved = copy.deepcopy(session.state)
    assert _layout_fixture_tick(session, session.reg) == {
        "status": "ok",
        "intent_id": session.intent["intent_id"],
    }
    assert session.calls[0] == ("queue_layout_resume", {"registration": session.reg})
    source = next(
        arguments for operation, arguments in session.calls if operation == "layout_source"
    )
    assert source == dict(
        format=session.layout.SOURCE_REQUEST,
        action="export",
        generation=session.state["native_layout"]["generation"],
        qualification_hash=session.state["native_layout"]["qualification_hash"],
        allowed_plan_hashes=session.intent["target"]["owner_plan_hashes"],
        packing=session.layout.PACKING,
    )
    assert session.state == saved
    assert sum(operation == "queue_layout_resume" for operation, _ in session.calls) == 1
    assert not any(
        operation in {"queue_migration_state", "queue_layout_bind", "queue_layout_prepare"}
        for operation, _ in session.calls
    )


@pytest.mark.parametrize("mismatched", [False, True])
def test_layout_resume_refusal_stops_before_plan_source_or_mutation(mismatched):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(new_owner=True)
    saved = copy.deepcopy(session.state)
    refusal = {"status": "unavailable", "reason": "hot_journal_unrecoverable"}

    def resume(directory, operation, arguments):
        assert directory == Path(session.reg["destination"])
        session.calls.append((operation, copy.deepcopy(arguments)))
        assert operation == "queue_layout_resume"
        assert arguments == {"registration": session.reg}
        if not mismatched:
            return copy.deepcopy(refusal)
        state = copy.deepcopy(session.state)
        state["registration"]["job_id"] = "f" * 64
        return state

    session.store = resume
    expected = (
        {"status": "unavailable", "reason": "layout_registration", "phase": "open"}
        if mismatched
        else refusal
    )
    assert _layout_fixture_tick(session, session.reg) == expected
    assert session.calls == [("queue_layout_resume", {"registration": session.reg})]
    assert session.state == saved


def test_layout_tick_refuses_late_success():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(late_owner=True)
    saved = copy.deepcopy(session.state)
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] == "unavailable" and result["reason"] == "semantic_deadline"
    assert session.state == saved
    assert not any(op == "queue_layout_ack" for op, _ in session.calls)


def test_layout_protocol_changed_regeneration_never_publishes():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(changed_source=True)
    saved = copy.deepcopy(session.state)
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] == "conflict"
    assert session.state == saved
    assert not any(
        op in {"layout_import_batch", "layout_verify_owners", "queue_layout_ack"}
        for op, _ in session.calls
    )


def test_layout_protocol_fresh_append_requires_reopened_existing():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(fresh=1)
    result = _layout_fixture_tick(session, session.reg)
    assert result == {"status": "ok", "intent_id": session.intent["intent_id"]}
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "layout_import_batch",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    imports = [args for op, args in session.calls if op == "layout_import_batch"]
    assert imports[0] == imports[1]


def test_layout_protocol_two_fresh_results_never_publish():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(fresh=2)
    assert _layout_fixture_tick(session, session.reg)["status"] == "conflict"
    assert len([op for op, _ in session.calls if op == "layout_import_batch"]) == 2
    assert not any(op in {"layout_verify_owners", "queue_layout_ack"} for op, _ in session.calls)


def test_layout_protocol_source_refusal_preserves_reason():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(source_refusal=True)
    saved = copy.deepcopy(session.state)
    result = _layout_fixture_tick(session, session.reg)
    assert (result["status"], result["reason"], result["phase"]) == (
        "unavailable",
        "source_deadline",
        "source",
    )
    assert session.state == saved
    assert not any(
        op in {"layout_import_batch", "layout_verify_owners", "queue_layout_ack"}
        for op, _ in session.calls
    )


def test_layout_protocol_live_refusal_preserves_resource_reason():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(live_refusal=True)
    result = _layout_fixture_tick(session, session.reg)
    assert (result["status"], result["reason"], result["phase"]) == (
        "unavailable",
        "semantic_deadline",
        "owner",
    )
    assert not any(op == "queue_layout_ack" for op, _ in session.calls)


def test_layout_protocol_late_live_owner_never_starts_receipt_or_ack():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(live=True, late_owner=True)
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] == "unavailable" and result["reason"] == "semantic_deadline"
    assert not any(op in {"queue_layout_ack", "layout_read"} for op, args in session.calls)


def test_layout_protocol_pending_owner_and_live_use_same_grouped_replay_context():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(live=True)
    assert _layout_fixture_tick(session, session.reg)["status"] == "ok"
    reads = [args for op, args in session.calls if op == "layout_verify_owners"]
    assert reads == [{"intent_id": session.intent["intent_id"], "version": 7}]
    assert not any(op == "layout_read" for op, _ in session.calls)


def test_layout_protocol_first_owner_plans_binds_and_publishes_same_session():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(new_owner=True, fresh=1, live=True)
    assert _layout_fixture_tick(session, session.reg)["status"] == "ok"
    assert [operation for operation, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "queue_layout_bind",
        "layout_source",
        "queue_layout_prepare",
        "layout_import_batch",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]


def test_owner_plan_never_changes_source_frontier():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = PendingSession(new_owner=True, source_refusal=True)
    saved = copy.deepcopy(session.state)
    result = _layout_fixture_tick(session, session.reg)
    assert (result["status"], result["reason"]) == ("unavailable", "source_deadline")
    assert result["cursor_before"] == result["cursor_after"] == session.state["staging_cursor"]
    assert session.state == saved
    assert [op for op, _ in session.calls] == ["queue_layout_resume", "layout_source"]


class GroupedSession(PendingSession):
    def __init__(self, *, mode=None, **kwargs):
        super().__init__(**kwargs)
        self.mode = mode

    def exact(self, directory, operation, arguments):
        if operation == "layout_import_batch":
            return super().exact(directory, operation, arguments)
        assert operation == "layout_verify_owners", "owner/LIVE must share one worker"
        assert directory == Path(self.reg["destination"])
        assert arguments == dict(intent_id=self.intent["intent_id"], version=7)
        self.calls.append((operation, copy.deepcopy(arguments)))
        if self.mode == "refused":
            return dict(status="unavailable", reason="system_pressure_unavailable")
        owner = self.exported["finalizations"][0]
        live = owner["live"]
        result = dict(
            status="ok",
            format="lance-layout-owner-verification-v1",
            intent_hash=self.migration.hashed(self.intent),
            owners=[
                dict(
                    owner=owner["owner"],
                    manifest_hash=self.migration.hashed(owner),
                    partition_id=self.intent["target"]["partition_id"],
                    version=7,
                    live_receipt_ids=[] if live is None else live["receipt"]["ids"],
                    live=None
                    if live is None
                    else dict(
                        namespace=live["namespace"],
                        key=live["key"],
                        digest=live["digest"],
                        receipt_hash=self.migration.hashed(live["receipt"]),
                    ),
                )
            ],
        )
        if self.mode == "late":
            self.remaining = 0
        elif self.mode == "summary":
            result["owners"][0]["manifest_hash"] = "f" * 64
        return result


@pytest.mark.parametrize("live,fresh", [(False, 1), (True, 1), (True, 0)])
def test_grouped_owner_proof_coordinator_preserves_actual_existing_and_atomic_ack(live, fresh):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = GroupedSession(live=live, fresh=fresh)
    saved = copy.deepcopy(session.state)
    assert _layout_fixture_tick(session, session.reg) == dict(
        status="ok", intent_id=session.intent["intent_id"]
    )
    operations = [operation for operation, _ in session.calls]
    assert operations == [
        "queue_layout_resume",
        "layout_source",
        *(["layout_import_batch"] * (fresh + 1)),
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    assert session.state == saved
    summary = session.calls[-1][1]["verified_summary"]
    session.layout.validate_layout_verified(summary, session.intent)


@pytest.mark.parametrize("mode", ["refused", "late", "summary"])
def test_grouped_owner_proof_coordinator_refusal_or_late_summary_never_acks(mode):
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = GroupedSession(live=True, mode=mode)
    saved = copy.deepcopy(session.state)
    result = _layout_fixture_tick(session, session.reg)
    assert result["status"] == ("conflict" if mode == "summary" else "unavailable")
    if mode == "refused":
        assert result["reason"] == "system_pressure_unavailable"
    elif mode == "late":
        assert result["reason"] == "semantic_deadline"
    assert not any(operation == "queue_layout_ack" for operation, _ in session.calls)
    assert session.state == saved


def test_grouped_owner_proof_staging_interval_skips_owner_operation():
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    session = GroupedSession(fresh=1)
    stage = copy.deepcopy(session.exported)
    stage.update(finalizations=[], phase_complete=False, counts=session.migration.empty_counts())
    stage["cursor_after"].update(owner="owner", entry=len(stage["entries"]), field="finalization")
    for _ in range(3):
        stage["frame_bytes"] = len(session.migration.frame_bytes(stage))
    session.exported = stage
    session.intent = session.layout.prepare_layout_interval(session.reg, stage, session.target)
    session.state["pending"] = session.intent
    session.batch.update(
        receipt=session.layout.expected_receipt(session.intent),
        routes=[
            dict(
                candidate,
                format=session.layout.ROUTE,
                row_hash=candidate["row_hash"] or "c" * 64,
                partition_id=session.target["partition_id"],
                version=7,
                receipt=session.layout.batch_receipt_id(session.intent),
                state="staged",
            )
            for candidate in session.intent["routes"]
        ],
    )
    assert session.intent["owners"] == []
    assert _layout_fixture_tick(session, session.reg)["status"] == "ok"
    operations = [operation for operation, _ in session.calls]
    assert operations.count("layout_import_batch") == 2
    assert "layout_verify_owners" not in operations and "layout_read" not in operations
    assert operations[-1] == "queue_layout_ack"


def test_grouped_owner_proof_runtime_stays_read_only_two_seconds(monkeypatch):
    from cognitive_castle.semantic_runtime import SemanticSession
    from cognitive_castle.lance_exact_worker import _FIELDS

    assert _FIELDS["layout_verify_owners"] == ({"intent_id", "version"}, set())
    session = SemanticSession(Path("/private/profile"))
    monkeypatch.setattr(session, "_run_stage", lambda request, **limits: (request, limits))
    request, limits = session.exact(
        Path("/private/generation"), "layout_verify_owners", dict(intent_id="a" * 64, version=7)
    )
    assert request["arguments"] == dict(intent_id="a" * 64, version=7)
    assert limits["cap"] == 2 and (limits["rss"], limits["total"]) == (192 * 1024**2, 256 * 1024**2)


@pytest.mark.parametrize("native", [False, True])
def test_layout_protocol_fixed_source_profiles_are_not_request_budgets(monkeypatch, native):
    from cognitive_castle.semantic_runtime import SemanticSession
    from cognitive_castle import migration_codec as migration, lance_layout_codec as layout
    from test_migration_codec import descriptor

    if native:
        from test_native_export_codec import descriptor
    reg = migration.registration(descriptor(), "/private/generation")
    before = migration.initial_source_cursor(reg)
    window = dict(
        format=layout.SOURCE_REQUEST,
        action="plan",
        generation={"path": reg["destination"], "device": 1, "inode": 2},
        qualification_hash="a" * 64,
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    session = SemanticSession(Path("/private/profile"))
    observed = []

    def run(request, **limits):
        observed.append((request, limits))
        return {"status": "unavailable", "reason": "fixture_source_stop"}

    monkeypatch.setattr(session, "_run_stage", run)
    result = session.layout_source(reg["descriptor"], before, window)
    assert result["status"] == "unavailable" and result["reason"] == "fixture_source_stop"
    assert result["cursor_before"] == result["cursor_after"] == before and result["plans"] == []
    request, limits = observed[0]
    assert request == dict(descriptor=reg["descriptor"], cursor=before, layout=window)
    assert limits["cap"] == 2 and limits["rss"] == (512 if native else 128) * 1024**2
    assert limits["total"] == (640 if native else 192) * 1024**2
    assert Path(limits["worker_command"][-1]).name == (
        "native_source_worker.py" if native else "sqlite_source_worker.py"
    )


@pytest.mark.parametrize(
    "operation,cap", [("layout_import_batch", 3), ("layout_maintain", 3), ("layout_read", 2)]
)
def test_layout_protocol_exact_profiles_stay_fixed(monkeypatch, operation, cap):
    from cognitive_castle.semantic_runtime import SemanticSession

    session = SemanticSession(Path("/private/profile"))
    monkeypatch.setattr(session, "_run_stage", lambda request, **limits: limits)
    observed = session.exact(Path("/private/generation"), operation, {})
    assert observed["cap"] == cap
    assert (observed["rss"], observed["total"]) == (192 * 1024**2, 256 * 1024**2)
    assert Path(observed["worker_command"][-1]).name == "lance_exact_worker.py"

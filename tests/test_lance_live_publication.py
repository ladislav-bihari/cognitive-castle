"""Direct request production and coordinator refusal contracts, without engines."""

import copy
from pathlib import Path

import pytest

from cognitive_castle import lance_layout_codec as layout
from cognitive_castle import live_publication_codec as codec
from cognitive_castle import migration_codec as migration
from test_live_registration import publication
from test_partitioned_exact_store import qualification_case


def api():
    from cognitive_castle import live_publication

    return live_publication


def source_case(text="body", workspace="/project"):
    prepared = publication(text, scope=workspace)
    _, _, qualification, envelope = qualification_case()
    reg = migration.registration(
        codec.source_descriptor(prepared.intent), envelope["generation"]["path"]
    )
    window = dict(
        format=layout.SOURCE_REQUEST,
        action="plan",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    return prepared, qualification, reg, window


@pytest.mark.parametrize(
    "text,scope",
    [("body", "/project"), ("\x00" * 65536, "\x00" * 2048)],
    ids=["short", "max-escaped"],
)
def test_direct_source_plan_reserves_every_actual_frame_and_finalization(text, scope):
    prepared, qualification, reg, window = source_case(text, scope)
    before = migration.initial_source_cursor(reg)
    result = api().source_result(prepared, reg, before, window, qualification)
    layout.validate_layout_source_result(result, window, reg["descriptor"], before)
    assert result["export"] is None and result["cursor_after"] == before
    plan = result["plans"][0]
    frames = list(codec.batches(prepared))
    assert plan["expansion"]["batches"] == len(frames)
    assert plan["expansion"]["physical_rows"] == len(prepared.entries) + len(frames) + 2
    exported_rows, finalized = [], []
    export_window = {**window, "action": "export", "allowed_plan_hashes": [migration.hashed(plan)]}
    for _ in frames:
        result = api().source_result(prepared, reg, before, export_window, qualification)
        layout.validate_layout_source_result(result, export_window, reg["descriptor"], before)
        assert result["plans"] == [plan]
        inner = result["export"]
        exported_rows.extend(inner["entries"])
        finalized.extend(inner["finalizations"])
        before = inner["cursor_after"]
    assert exported_rows == list(prepared.entries) and finalized == [prepared.manifest]
    terminal = api().source_result(prepared, reg, before, window, qualification)
    assert terminal["plans"] == [] and terminal["export"]["complete"]
    assert terminal["export"]["coverage"] == "incomplete"


def test_direct_source_rejects_changed_request_or_unpinned_qualification():
    prepared, qualification, reg, window = source_case()
    before = migration.initial_source_cursor(reg)
    with pytest.raises(ValueError):
        api().source_result(publication("changed"), reg, before, window, qualification)
    changed = copy.deepcopy(qualification)
    changed["working_temp_bytes"] += 1
    with pytest.raises(ValueError):
        api().source_result(prepared, reg, before, window, changed)


@pytest.mark.parametrize("operation", ["step", "publish"])
def test_refused_session_never_opens_control_or_native_storage(operation):
    class Refused:
        def _ready(self):
            return dict(status="unavailable", reason="semantic_deadline")

        def store(self, *args, **kwargs):
            raise AssertionError("refusal reached control storage")

        def exact(self, *args, **kwargs):
            raise AssertionError("refusal reached native storage")

    result = getattr(api(), operation)(Refused(), Path("/generation"), publication(), "a" * 64)
    assert result == dict(status="unavailable", reason="semantic_deadline")


def test_modified_preparation_is_refused_before_registering_outbox():
    class Ready:
        def _ready(self):
            return None

        def store(self, *args, **kwargs):
            raise AssertionError("changed preparation reached durable storage")

    prepared = publication()
    prepared.entries[0]["text"] = "changed"
    with pytest.raises(ValueError):
        api().publish(Ready(), Path("/generation"), prepared, "a" * 64)


def test_final_receipt_read_is_bound_to_original_selected_authority(monkeypatch):
    prepared, _, reg, _ = source_case()
    terminal = {**migration.initial_source_cursor(reg), "phase": "done"}

    def finished(*args):
        return dict(
            status="ok",
            job_id=reg["job_id"],
            staging_cursor=terminal,
            completed_frontier=terminal,
            counts=migration.empty_counts(),
            state="snapshot_export_exhausted",
        )

    monkeypatch.setattr(api(), "step", finished)

    class Ready:
        def _ready(self):
            return None

        def exact(self, generation, operation, arguments):
            assert operation == "layout_read" and arguments["operation"] == "receipt"
            assert arguments["envelope_hash"] == "a" * 64
            return dict(status="unavailable", reason="native_fixture_not_run")

    assert api().publish(Ready(), Path(reg["destination"]), prepared, "a" * 64) == dict(
        status="unavailable", reason="native_fixture_not_run"
    )


def test_verified_interval_yields_before_next_interval_and_later_reads_actual_receipt(monkeypatch):
    prepared, _, reg, _ = source_case()
    before = migration.initial_source_cursor(reg)
    calls = []

    def interval(session, generation, publication, envelope_hash):
        calls.append(session)
        return dict(
            status="ok",
            job_id=reg["job_id"],
            staging_cursor=before,
            completed_frontier=before,
            counts=migration.empty_counts(),
            state="active" if len(calls) == 1 else "snapshot_export_exhausted",
        )

    monkeypatch.setattr(api(), "step", interval)

    class Lease:
        def __init__(self):
            self.reads = 0

        def _ready(self):
            return None

        def exact(self, directory, operation, arguments):
            self.reads += 1
            assert operation == "layout_read" and arguments["operation"] == "receipt"
            assert arguments["envelope_hash"] == "a" * 64
            return dict(
                status="ok", digest=prepared.canonical.digest, receipt=prepared.canonical.receipt
            )

    first = Lease()
    result = api().publish(first, Path(reg["destination"]), prepared, "a" * 64)
    assert result == dict(status="in_progress", reason="direct_publication_intervals")
    assert calls == [first] and first.reads == 0
    later = Lease()
    assert (
        api().publish(later, Path(reg["destination"]), prepared, "a" * 64)
        == prepared.canonical.receipt
    )
    assert calls == [first, later] and later.reads == 1


def test_fresh_append_yields_without_claiming_proof_or_starting_another_worker(tmp_path):
    from cognitive_castle.canonical_records import _json

    prepared, qualification, _, _ = source_case()
    generation = tmp_path.resolve() / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    info = generation.stat()
    identity = dict(path=str(generation), device=info.st_dev, inode=info.st_ino)
    qualification["generation"] = identity
    _, _, _, envelope = qualification_case()
    envelope.update(generation=identity, qualification_hash=migration.hashed(qualification))
    artifact = generation / "exact/layout-qualification.json"
    artifact.write_text(_json(qualification))
    artifact.chmod(0o400)
    reg = migration.registration(codec.source_descriptor(prepared.intent), str(generation))
    before = migration.initial_source_cursor(reg)
    window = dict(
        format=layout.SOURCE_REQUEST,
        action="plan",
        generation=identity,
        qualification_hash=envelope["qualification_hash"],
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    plan = api().source_result(prepared, reg, before, window, qualification)["plans"][0]
    target = layout.target(envelope, 0, [plan])
    exported = codec.source_slice(prepared, before)
    pending = layout.prepare_layout_interval(reg, exported, target)
    state = dict(
        status="ok",
        registration=reg,
        pending=pending,
        staging_cursor=before,
        completed_frontier=before,
        counts=migration.empty_counts(),
        state="active",
        last_ack=None,
        layout=dict(
            format=layout.ATTACHMENT, envelope_hash=migration.hashed(envelope), envelope=envelope
        ),
        native_layout=dict(
            format="lance-layout-native-context-v1",
            generation=identity,
            qualification_hash=envelope["qualification_hash"],
            epoch="e" * 64,
            plan=plan,
            target=target,
        ),
    )

    class FreshOnly:
        def __init__(self):
            self.append_seen = False

        def _ready(self):
            return None

        def store(self, directory, operation, arguments):
            assert operation == "queue_live_open", "fresh preparation must not ACK"
            return copy.deepcopy(state)

        def exact(self, directory, operation, arguments):
            assert not self.append_seen, "cold verification needs the next bounded lease"
            self.append_seen = True
            assert operation == "layout_import_verified"
            # A fresh response deliberately contains no durable routes/proofs.
            return dict(
                status="ok",
                format=layout.BATCH_RESULT,
                outcome="fresh_append",
                receipt=layout.expected_receipt(pending),
                routes=[],
                owner_proof=None,
            )

    session = FreshOnly()
    result = api().step(session, generation, prepared, migration.hashed(envelope))
    assert session.append_seen and result["status"] == "in_progress"
    assert result["phase"] == "verify_replay" and result["cursor_after"] == before
    assert "receipt" not in result and "ids" not in result

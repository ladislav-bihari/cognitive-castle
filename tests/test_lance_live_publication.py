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

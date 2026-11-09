"""Direct requests share registered placement without pretending to be archives."""

import copy

import pytest

from cognitive_castle import live_publication_codec as live
from cognitive_castle import migration_codec as migration
from cognitive_castle.canonical_records import _json, prepare_remember


def publication(text="PRIVATE_BODY Ω\x00", scope="/project"):
    prepared = prepare_remember("request", text, namespace="operator")
    return live.prepare_publication(
        prepared,
        [dict(workspace=scope, wing="project", room="conversation") for _ in prepared.records],
    )


def test_registration_is_compact_body_free_and_independent_of_archive_descriptors():
    prepared = publication()
    descriptor = live.source_descriptor(prepared.intent)
    reg = migration.registration(descriptor, "/private/generation")
    assert reg["format"] == "direct-live-registration-v1"
    assert reg["source_binding"] == prepared.intent["source_binding"]
    assert descriptor["receipt"] == prepared.canonical.receipt
    assert descriptor["manifest_hash"] == prepared.intent["manifest_hash"]
    assert "PRIVATE_BODY" not in _json(reg)
    assert not {"path", "snapshot_id", "archive_id", "identity"}.intersection(descriptor)
    assert len(_json(reg).encode()) < 4096
    assert migration.validate_registration(reg) == reg


def test_request_identity_cannot_evade_conflict_by_changing_content_or_scope():
    original = migration.registration(live.source_descriptor(publication().intent), "/generation")
    for changed in (publication("different"), publication(scope="/other")):
        reg = migration.registration(live.source_descriptor(changed.intent), "/generation")
        assert reg["job_id"] == original["job_id"]
        assert reg["source_binding"] != original["source_binding"]
        assert reg != original  # Existing queue registration equality must reject this.
    elsewhere = migration.registration(original["descriptor"], "/other-generation")
    assert elsewhere["job_id"] != original["job_id"]


@pytest.mark.parametrize("field", ["digest", "scope_hash", "owner", "source_binding"])
def test_changed_descriptor_commitments_are_rejected(field):
    descriptor = live.source_descriptor(publication().intent)
    descriptor[field] = "0" * 64
    with pytest.raises(ValueError):
        migration.registration(descriptor, "/generation")


def test_descriptor_does_not_alias_mutable_intent_receipt():
    prepared = publication()
    descriptor = live.source_descriptor(prepared.intent)
    prepared.intent["semantic"]["receipt"]["ids"].clear()
    assert descriptor["receipt"]["ids"] == prepared.canonical.receipt["ids"]


def test_direct_cursor_stays_in_its_own_source_domain():
    prepared = publication()
    reg = migration.registration(live.source_descriptor(prepared.intent), "/generation")
    start = migration.initial_source_cursor(reg)
    assert start["format"] == "direct-live-cursor-v1"
    assert start["phase"] == "live" and start["key"] is None
    assert migration.cursor(start, reg) == start
    positioned = {
        **start,
        "key": ["operator", "request"],
        "owner": prepared.manifest["owner_key"],
        "entry": 1,
    }
    assert migration.cursor(positioned, reg) == positioned
    for changes in (
        dict(format="protected-sqlite-export-v1"),
        dict(phase="history"),
        dict(key=["operator", "other"]),
        dict(owner="wrong", key=["operator", "request"]),
        dict(entry=True),
        dict(phase="done", key=["operator", "request"]),
    ):
        with pytest.raises(ValueError):
            migration.cursor({**start, **changes}, reg)
    assert migration.cursor({**start, "phase": "done"}, reg)["phase"] == "done"


def test_owner_reference_must_match_registered_manifest_and_original_receipt():
    prepared = publication()
    reg = migration.registration(live.source_descriptor(prepared.intent), "/generation")
    reference = migration.owner_reference(prepared.manifest, "live", ["operator", "request"])
    assert migration.validate_owner_reference(reference, reg) == reference
    for field, value in (
        ("manifest_hash", "0" * 64),
        ("live_receipt_ids", []),
        ("text_hash", "0" * 64),
        ("members", 0),
        ("text_bytes", 0),
        ("source_key", ["operator", "other"]),
    ):
        changed = copy.deepcopy(reference)
        changed[field] = value
        with pytest.raises(ValueError):
            migration.validate_owner_reference(changed, reg)


def test_direct_stream_replays_exact_boundaries_and_uses_registered_layout_intervals():
    from cognitive_castle import lance_layout_codec as layout

    prepared = publication("Ω\x00\r\n" * 8000)
    reg = migration.registration(live.source_descriptor(prepared.intent), "/generation")
    ref = migration.owner_reference(prepared.manifest, "live", ["operator", "request"])
    plan = layout.owner_plan(reg, ref, {name: 1000000 for name in layout.EXPANSION})
    envelope = dict(
        format=layout.ENVELOPE,
        generation=dict(path="/generation", device=1, inode=2),
        qualification_hash="a" * 64,
        max_partitions=1,
        limits={name: 10000000 for name in layout.EXPANSION},
    )
    target = layout.target(envelope, 0, [plan])
    before = migration.initial_source_cursor(reg)
    rows, finals, total = [], [], 0
    for _ in range(20):
        result = live.source_slice(prepared, before)
        assert result == live.source_slice(prepared, copy.deepcopy(before))
        migration.validate_export(result, reg, before)
        if result["complete"]:
            assert finals == [prepared.manifest]
            assert rows == list(prepared.entries)
            assert total == len(prepared.canonical.records)
            break
        assert not finals  # No entry/manifest can be emitted after finalization.
        interval = layout.prepare_layout_interval(reg, result, target)
        layout.validate_attached_layout_intent(interval, reg)
        assert "PRIVATE_BODY" not in _json(interval)
        rows.extend(result["entries"])
        finals.extend(result["finalizations"])
        total += result["counts"]["members"]
        before = result["cursor_after"]
    else:
        pytest.fail("bounded direct request did not finish")


def test_direct_stream_rejects_unissued_position_and_mutated_preparation():
    prepared = publication()
    reg = migration.registration(live.source_descriptor(prepared.intent), "/generation")
    before = migration.initial_source_cursor(reg)
    forged = {**before, "owner": prepared.manifest["owner_key"], "key": ["operator", "request"], "entry": 999}
    with pytest.raises(ValueError):
        live.source_slice(prepared, forged)
    prepared.entries[0]["text"] = "changed"
    with pytest.raises(ValueError):
        live.source_slice(prepared, before)

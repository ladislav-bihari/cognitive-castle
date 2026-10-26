"""Original owner exports remain independently bound inside a bounded prefix."""

import copy
import json
import sys

import pytest


def prefix_case(backend="sqlite", *, count=3, text_size=12, metadata_size=3):
    from cognitive_castle import bulk_exact_codec as bulk, migration_codec as migration
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.sqlite_export_codec import empty_counts, frame_bytes
    from test_lance_layout_codec import fixture

    if backend == "native":
        from test_native_export_codec import descriptor
        from cognitive_castle.native_export_codec import initial_cursor
    else:
        from test_migration_codec import descriptor
        from cognitive_castle.sqlite_export_codec import initial_cursor
    reg = migration.registration(descriptor(), "/private/generation")
    before = (
        initial_cursor(
            reg["source_binding"], reg["descriptor"]["store_id"], reg["descriptor"]["start_cursor"]
        )
        if backend == "native"
        else initial_cursor(reg["source_binding"], reg["descriptor"]["store_id"])
    )
    cursor = copy.deepcopy(before)
    components = []
    expansion = fixture()[4]["expansion"]
    for ordinal in range(count):
        if backend == "native":
            d = reg["descriptor"]
            owner_key = migration.hashed(
                [d["archive_id"], d["original_snapshot_id"], d["version"], 0, ordinal]
            )
            following = dict(fragment=0, offset=ordinal + 1, row_address=ordinal)
            key = [migration._json(following)]
            records = []  # Exact empty native occurrences are supported.
        else:
            owner_key = "owner-" + str(ordinal)
            key = [owner_key]
            records = prepare_remember("prefix", chr(97 + ordinal) * text_size).records
        entries, manifest = bulk.prepare_owner(
            owner_key,
            reg["source_binding"],
            records,
            metadata="m" * metadata_size,
            columns='{"original":"columns"}',
            scopes='{"original":"scopes"}',
            descriptor="DESCRIPTOR_SENTINEL",
            **({"metadata_id": owner_key} if backend == "native" else {}),
        )
        after = dict(cursor, phase="history", key=key)
        counts = dict(empty_counts(), historical_occurrences=1, members=len(records))
        exported = dict(
            status="ok",
            reason=None,
            source_binding=reg["source_binding"],
            cursor_before=copy.deepcopy(cursor),
            cursor_after=after,
            entries=entries,
            finalizations=[manifest],
            counts=counts,
            phase="history",
            phase_complete=True,
            complete=False,
            coverage="indexed-only",
            frame_bytes=0,
        )
        for _ in range(3):
            exported["frame_bytes"] = len(frame_bytes(exported))
        migration.validate_export(exported, reg, cursor)
        plan = layout.owner_plan(
            reg, migration.owner_reference(manifest, "history", key), copy.deepcopy(expansion)
        )
        components.append(dict(plan=plan, export=exported))
        cursor = copy.deepcopy(after)
    envelope = fixture()[5]
    return reg, before, components, envelope


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_packing_is_deterministic_across_frame_phase_and_home_boundaries(backend):
    from cognitive_castle.lance_layout_packing import (
        pack_source_prefix,
        validate_source_prefix,
        admit_source_prefix,
    )
    from cognitive_castle import lance_layout_codec as layout

    reg, before, components, envelope = prefix_case(backend)
    saved = copy.deepcopy(components)
    packet = pack_source_prefix(reg, before, iter(components))
    assert packet == pack_source_prefix(reg, before, iter(copy.deepcopy(components)))
    assert components == saved
    assert packet["components"] == components
    assert packet["cursor_before"] == before
    assert packet["cursor_after"] == components[-1]["export"]["cursor_after"]
    assert validate_source_prefix(packet, reg, before) == packet
    target = layout.target(envelope, 0, [c["plan"] for c in components[:2]])
    admitted = admit_source_prefix(packet, reg, target)
    assert admitted["components"] == components[:2]
    assert admitted["cursor_after"] == components[1]["export"]["cursor_after"]
    assert packet["components"] == saved
    assert validate_source_prefix(admitted, reg, before) == admitted
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def test_packing_preserves_each_native_before_next_cursor():
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, components, _ = prefix_case("native")
    packet = pack_source_prefix(reg, before, iter(components))
    for ordinal, component in enumerate(packet["components"]):
        original = component["export"]
        assert original == components[ordinal]["export"]
        previous = json.loads(original["cursor_before"]["key"][0])
        following = json.loads(original["cursor_after"]["key"][0])
        assert previous["offset"] == ordinal
        assert following == dict(fragment=0, offset=ordinal + 1, row_address=ordinal)
        assert component["plan"]["source_key"] == original["cursor_after"]["key"]


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_prefix_reads_no_ninth_owner(backend):
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, components, _ = prefix_case(backend, count=8)

    def candidates():
        yield from components
        raise AssertionError("A ninth owner must not be requested")

    packet = pack_source_prefix(reg, before, candidates())
    assert packet["components"] == components


@pytest.mark.parametrize("change", ["unknown", "body", "plan", "hash", "before", "after", "frame"])
def test_prefix_refuses_tampered_or_unknown_evidence(change):
    from cognitive_castle.lance_layout_packing import pack_source_prefix, validate_source_prefix

    reg, before, components, _ = prefix_case()
    packet = copy.deepcopy(pack_source_prefix(reg, before, components))
    if change == "unknown":
        packet["verified"] = True
    elif change == "body":
        packet["components"][0]["export"]["entries"][0]["text"] += "changed"
    elif change == "plan":
        packet["components"][0]["plan"]["manifest_hash"] = "0" * 64
    elif change == "hash":
        packet["prefix_hash"] = "0" * 64
    elif change == "before":
        packet["cursor_before"] = components[0]["export"]["cursor_after"]
    elif change == "after":
        packet["cursor_after"] = components[0]["export"]["cursor_after"]
    else:
        packet["frame_bytes"] += 1
    with pytest.raises(ValueError):
        validate_source_prefix(packet, reg, before)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_original_cursor_gap_is_not_a_packing_boundary(backend):
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, components, _ = prefix_case(backend)
    components[1]["export"]["cursor_before"] = copy.deepcopy(before)
    with pytest.raises(ValueError):
        pack_source_prefix(reg, before, components)


@pytest.mark.parametrize("selection", [[1], [1, 0], [0, 2]])
def test_admission_cannot_skip_or_reorder_original_prefix(selection):
    from cognitive_castle.lance_layout_packing import pack_source_prefix, admit_source_prefix
    from cognitive_castle import lance_layout_codec as layout

    reg, before, components, envelope = prefix_case()
    packet = pack_source_prefix(reg, before, components)
    target = layout.target(envelope, 0, [components[i]["plan"] for i in selection])
    with pytest.raises(ValueError):
        admit_source_prefix(packet, reg, target)


def test_frame_boundary_keeps_original_next_owner_for_later():
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, components, _ = prefix_case(count=2, text_size=60000, metadata_size=200000)
    packet = pack_source_prefix(reg, before, components)
    assert packet["components"] == components[:1]
    assert packet["cursor_after"] == components[1]["export"]["cursor_before"]


def test_malformed_later_owner_refuses_whole_attempt():
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, components, _ = prefix_case()
    components[1]["export"]["finalizations"][0]["source_binding"] = "other"
    with pytest.raises(ValueError):
        pack_source_prefix(reg, before, components)


def test_phase_boundary_preserves_original_unselected_audit_owner():
    from cognitive_castle.lance_layout_packing import pack_source_prefix
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from test_migration_codec import exported

    reg, before, components, _ = prefix_case(count=1)
    audit = exported(reg, phase="audit")
    audit["cursor_before"] = copy.deepcopy(components[0]["export"]["cursor_after"])
    for _ in range(3):
        audit["frame_bytes"] = len(migration.frame_bytes(audit))
    migration.validate_export(audit, reg, audit["cursor_before"])
    plan = layout.owner_plan(
        reg,
        migration.owner_reference(audit["finalizations"][0], "audit", audit["cursor_after"]["key"]),
        copy.deepcopy(components[0]["plan"]["expansion"]),
    )
    packet = pack_source_prefix(reg, before, [*components, dict(plan=plan, export=audit)])
    assert packet["components"] == components
    assert packet["cursor_after"] == audit["cursor_before"]


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("ordinal", [0, 1])
def test_partial_owner_uses_single_owner_fallback_without_advancing_prefix(backend, ordinal):
    from cognitive_castle.lance_layout_packing import pack_source_prefix
    from cognitive_castle import migration_codec as migration

    reg, before, components, _ = prefix_case(backend, count=2)
    selected = components[ordinal]
    stage = selected["export"]
    following = copy.deepcopy(
        stage["cursor_after"] if backend == "sqlite" else stage["cursor_before"]
    )
    following.update(
        phase="history",
        owner=selected["plan"]["owner_key"],
        entry=len(stage["entries"]),
        field="finalization",
    )
    stage.update(
        cursor_after=following,
        finalizations=[],
        phase_complete=False,
        counts=migration.empty_counts(),
    )
    for _ in range(3):
        stage["frame_bytes"] = len(migration.frame_bytes(stage))
    migration.validate_export(stage, reg, stage["cursor_before"])
    if ordinal == 0:
        with pytest.raises(ValueError, match="whole owner"):
            pack_source_prefix(reg, before, components)
    else:
        packet = pack_source_prefix(reg, before, components)
        assert packet["components"] == components[:1]
        assert packet["cursor_after"] == stage["cursor_before"]


def test_shared_canonical_id_ends_prefix_before_unpublished_reuse():
    from cognitive_castle.lance_layout_packing import pack_source_prefix
    from cognitive_castle import (
        bulk_exact_codec as bulk,
        lance_layout_codec as layout,
        migration_codec as migration,
    )
    from cognitive_castle.canonical_records import prepare_remember

    reg, before, components, _ = prefix_case(count=2)
    records = prepare_remember("prefix", "a" * 12).records
    owner_key = components[1]["plan"]["owner_key"]
    entries, manifest = bulk.prepare_owner(owner_key, reg["source_binding"], records)
    second = components[1]["export"]
    second.update(entries=entries, finalizations=[manifest])
    for _ in range(3):
        second["frame_bytes"] = len(migration.frame_bytes(second))
    migration.validate_export(second, reg, second["cursor_before"])
    components[1]["plan"] = layout.owner_plan(
        reg,
        migration.owner_reference(manifest, "history", second["cursor_after"]["key"]),
        copy.deepcopy(components[1]["plan"]["expansion"]),
    )
    packet = pack_source_prefix(reg, before, components)
    assert packet["components"] == components[:1]
    assert packet["cursor_after"] == second["cursor_before"]

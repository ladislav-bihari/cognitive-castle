"""Pure strict layout metadata; no storage engines or source access."""

import copy
import json
import sys

import pytest

from test_migration_codec import descriptor, exported


def fixture():
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle import migration_codec as migration

    reg = migration.registration(descriptor(), "/private/generation")
    result = exported(reg)
    owner = migration.owner_reference(
        result["finalizations"][0], result["phase"], result["cursor_after"]["key"]
    )
    expansion = {name: 100000 for name in layout.EXPANSION}
    plan = layout.owner_plan(reg, owner, expansion)
    envelope = {
        "format": layout.ENVELOPE,
        "generation": {"path": reg["destination"], "device": 1, "inode": 2},
        "qualification_hash": "a" * 64,
        "max_partitions": 4,
        "limits": {name: 1000000 for name in layout.EXPANSION},
    }
    target = layout.target(envelope, 0, [plan])
    return layout, migration, reg, result, plan, envelope, target


def test_direct_publication_reserves_its_separate_finalization_batch():
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle import live_publication_codec as live
    from test_live_registration import publication
    from test_partitioned_exact_store import qualification_case

    _, migration, qualification, envelope = qualification_case()
    prepared = publication("body")
    reg = migration.registration(
        live.source_descriptor(prepared.intent), envelope["generation"]["path"]
    )
    counter = layout.LayoutPlanningCounters(qualification)
    for row in prepared.entries:
        counter.add_entry(row)
    plan = counter.finish(
        reg, prepared.manifest, "live", ["operator", "request"], separate_finalization=True
    )
    assert len(list(live.batches(prepared))) == 2
    assert plan["expansion"]["batches"] == 2
    assert plan["expansion"]["fragments"] == 2
    # All input rows + owner/live-alias + one receipt for each physical batch.
    assert plan["expansion"]["physical_rows"] == len(prepared.entries) + 4


def test_layout_versions_preserve_v1_job_and_source_domains():
    layout, migration, reg, result, _, _, target = fixture()
    before = copy.deepcopy(reg)
    old = migration.prepare_interval(reg, result)
    intent = layout.prepare_layout_interval(reg, result, target)
    assert reg == before == migration.registration(descriptor(), reg["destination"])
    assert migration.validate_interval(old, reg) == old
    assert intent["registration_format"] == reg["format"]
    assert intent["source_binding"] == reg["source_binding"]
    assert intent["intent_id"] != old["intent_id"]
    with pytest.raises(ValueError):
        migration.validate_interval(intent, reg)
    from test_native_export_codec import descriptor as native_descriptor

    native = migration.registration(native_descriptor(), reg["destination"])
    assert native["format"] == "native-migration-v1" and native["job_id"] != reg["job_id"]
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def test_plan_and_routes_are_body_free_and_bounded():
    layout, _, reg, result, plan, envelope, target = fixture()
    layout.validate_owner_plan(plan, reg)
    layout.validate_envelope(envelope)
    assert layout.partition_path(target) == "/private/generation/exact/partitions/0"
    intent = layout.prepare_layout_interval(reg, result, target)
    for sentinel in ("BODY_SENTINEL", "METADATA_SENTINEL", "SCOPE_SENTINEL", "DESCRIPTOR_SENTINEL"):
        assert sentinel not in json.dumps([plan, target, intent])
    for change in ({"text": "body"}, {"manifest": result["finalizations"][0]}, {"format": "other"}):
        with pytest.raises(ValueError):
            layout.validate_owner_plan({**plan, **change}, reg)
    for bad in (True, 0, -1, 1.0, 2**63):
        mutant = copy.deepcopy(plan)
        mutant["expansion"]["physical_rows"] = bad
        with pytest.raises(ValueError):
            layout.validate_owner_plan(mutant, reg)
    with pytest.raises(ValueError):
        layout.target(envelope, 0, [plan] * 257)
    with pytest.raises(ValueError):
        layout.validate_target({**target, "partition_id": "../other"})


def test_layout_intent_binds_fixed_target_and_window():
    layout, _, reg, result, _, envelope, target = fixture()
    intent = layout.prepare_layout_interval(reg, result, target)
    assert intent == layout.prepare_layout_interval(
        reg, copy.deepcopy(result), copy.deepcopy(target)
    )
    assert layout.validate_layout_intent(intent, reg) == intent
    for field, value in (
        ("target", layout.target(envelope, 1, [fixture()[4]])),
        ("packing", "bulk-v1"),
        ("format", "sqlite-migration-v1"),
    ):
        mutant = copy.deepcopy(intent)
        mutant[field] = value
        with pytest.raises(ValueError):
            layout.validate_layout_intent(mutant, reg)
    mutant = copy.deepcopy(intent)
    mutant["window"]["cursor"] = mutant["cursor_after"]
    with pytest.raises(ValueError):
        layout.validate_layout_intent(mutant, reg)
    assert intent["routes"][-1]["id"] == layout.batch_receipt_id(intent)


def test_verified_requires_identical_replay_and_exact_observation_set():
    layout, _, reg, result, _, _, target = fixture()
    intent = layout.prepare_layout_interval(reg, result, target)
    receipt = layout.expected_receipt(intent)
    routes = [
        {
            **r,
            "format": layout.ROUTE,
            "row_hash": r["row_hash"] or "c" * 64,
            "partition_id": 0,
            "version": 7,
            "receipt": receipt["receipt_id"],
            "state": "staged",
        }
        for r in intent["routes"]
    ]
    batch = {
        "format": layout.BATCH_RESULT,
        "outcome": "existing_verified",
        "receipt": receipt,
        "routes": routes,
    }
    owner = intent["owners"][0]
    proofs = [
        {
            "owner": owner["owner"],
            "manifest_hash": owner["manifest_hash"],
            "partition_id": 0,
            "version": 7,
            "live_receipt_ids": owner["live_receipt_ids"],
        }
    ]
    verified = layout.verified_summary(intent, batch, proofs)
    assert layout.validate_layout_verified(verified, intent) == verified
    # Metadata alone cannot establish a body digest. The frozen intent hash and
    # actual unchanged bulk receipt reject a changed digest at the proof boundary.
    changed = {**intent, "digest": "b" * 64}
    assert layout.validate_layout_intent(changed, reg) == changed
    with pytest.raises(ValueError):
        layout.validate_layout_verified(verified, changed)
    for change in (
        {"outcome": "fresh_append"},
        {"routes": routes[:-1]},
        {"routes": routes + routes[:1]},
    ):
        with pytest.raises(ValueError):
            layout.verified_summary(intent, {**batch, **change}, proofs)
    mutant = copy.deepcopy(verified)
    page = next(r for r in mutant["batch"]["routes"] if r["kind"] == "bulk_page")
    page["row_hash"] = "d" * 64
    with pytest.raises(ValueError):
        layout.validate_layout_verified(mutant, intent)


def test_candidate_coverage_and_source_gate_are_exact():
    layout, _, reg, result, _, _, target = fixture()
    intent = layout.prepare_layout_interval(reg, result, target)
    mutations = [intent["routes"][-1:], intent["routes"][:-2] + intent["routes"][-1:]]
    for routes in mutations:
        with pytest.raises(ValueError):
            layout.validate_layout_intent({**intent, "routes": routes}, reg)
    changed = copy.deepcopy(intent)
    next(r for r in changed["routes"] if r["kind"] == "bulk_record")["kind"] = "bulk_page"
    with pytest.raises(ValueError):
        layout.validate_layout_intent(changed, reg)
    from cognitive_castle import migration_codec as migration

    live = exported(reg, phase="live")
    reference = migration.owner_reference(
        live["finalizations"][0], "live", live["cursor_after"]["key"]
    )
    plan = layout.owner_plan(reg, reference, {k: 100000 for k in layout.EXPANSION})
    live_intent = layout.prepare_layout_interval(reg, live, layout.target(fixture()[5], 0, [plan]))
    bad = copy.deepcopy(live_intent)
    next(r for r in bad["routes"] if r["kind"] == "receipt")["id"] = "receipt:" + "f" * 64
    with pytest.raises(ValueError):
        layout.validate_layout_intent(bad, reg)
    stage = copy.deepcopy(result)
    stage.update(finalizations=[], phase_complete=False, counts=migration.empty_counts())
    stage["cursor_after"].update(owner="owner", entry=len(stage["entries"]), field="finalization")
    for _ in range(3):
        stage["frame_bytes"] = len(migration.frame_bytes(stage))
    staged = layout.prepare_layout_interval(reg, stage, target)
    for candidate in staged["routes"]:
        candidate["gate"] = "receipt:" + "f" * 64
    with pytest.raises(ValueError):
        layout.validate_layout_intent(staged, reg)


def test_home_proof_versions_and_integer_types_are_bound():
    layout, _, reg, result, _, _, target = fixture()
    intent = layout.prepare_layout_interval(reg, result, target)
    receipt = layout.expected_receipt(intent)
    routes = [
        {
            **r,
            "format": layout.ROUTE,
            "row_hash": r["row_hash"] or "c" * 64,
            "partition_id": 0,
            "version": 7,
            "receipt": receipt["receipt_id"],
            "state": "staged",
        }
        for r in intent["routes"]
    ]
    owner = intent["owners"][0]
    proofs = [
        dict(
            owner=owner["owner"],
            manifest_hash=owner["manifest_hash"],
            partition_id=0,
            version=7,
            live_receipt_ids=owner["live_receipt_ids"],
        )
    ]
    batch = dict(
        format=layout.BATCH_RESULT, outcome="existing_verified", receipt=receipt, routes=routes
    )
    verified = layout.verified_summary(intent, batch, proofs)
    for field, bad in (("version", 8), ("partition_id", False)):
        mutant = copy.deepcopy(verified)
        mutant["owner_proofs"][0][field] = bad
        with pytest.raises(ValueError):
            layout.validate_layout_verified(mutant, intent)
    mutant = copy.deepcopy(verified)
    mutant["batch"]["routes"][0]["version"] = 8
    with pytest.raises(ValueError):
        layout.validate_layout_verified(mutant, intent)
    mutant = copy.deepcopy(verified)
    mutant["batch"]["receipt"]["published_owners"] = True
    with pytest.raises(ValueError):
        layout.validate_layout_verified(mutant, intent)

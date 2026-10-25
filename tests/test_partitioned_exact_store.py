"""Pure layout authority contracts; real engine cases require separate grants."""

import copy
import os
import sys

import pytest

from test_lance_layout_codec import fixture


def test_partition_protocol_global_candidate_resolution_uses_bounded_groups(monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle import partitioned_exact_store as module

    store = object.__new__(module._PartitionStore)
    store.pid = 0
    store.intent_id = "a" * 64
    ids = ["receipt:" + format(index, "064x") for index in range(67)]
    sizes = []

    def resolve(group, **kwargs):
        sizes.append(len(group))
        assert len(group) <= 32
        return dict(
            status="ok",
            routes=[],
            absent=[],
            unavailable=[],
            claims=[
                dict(
                    id=identity,
                    partition_id=0,
                    gate="receipt:" + "f" * 64,
                    state="reserved",
                    row_hash=None,
                    version=None,
                )
                for identity in group
            ],
        )

    store.authority = SimpleNamespace(resolve=resolve)
    monkeypatch.setattr(module, "lookup_many", lambda *a, **kw: {})
    assert store._lookup_rows(None, ids, checked=True) == {}
    assert sizes == [32, 32, 3]


def test_partition_protocol_access_revalidates_registered_artifact_under_lock(monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle import (
        partitioned_exact_store as module,
        lance_layout_directory as directory,
    )
    from cognitive_castle.lance_exact_store import _StoreError

    _, _, q, envelope = qualification_case()
    store = object.__new__(module._PartitionStore)
    store.pid = 0
    store.registry = {"pin": "same"}
    store.intent_id = "a" * 64
    store.proof_version = None
    store.maintenance = store.enumerating = False
    store.authority = SimpleNamespace(
        qualification=q, envelope=envelope, partition=lambda pid: store.registry
    )
    monkeypatch.setattr(
        directory, "load_layout_qualification", lambda *args: {**q, "working_temp_bytes": 1}
    )
    with pytest.raises(_StoreError):
        store._check_namespace_access()


def qualification_case():
    layout, migration, _, _, _, envelope, _ = fixture()
    value = dict(
        format="lance-layout-qualification-v1",
        generation=envelope["generation"],
        schema="castle-exact-six-string-v1",
        storage_mode="2.0",
        partition=dict(
            max_physical_rows=16384,
            max_fragments=256,
            max_versions=512,
            max_artifact_entries=1024,
            max_artifact_depth=6,
            max_logical_bytes=1000000,
            max_allocated_bytes=1000000,
            max_file_bytes=500000,
            max_batch_receipts=256,
        ),
        reservations={
            op: dict(
                physical_rows=513 if op == "append" else 0,
                fragments=1 if op == "append" else 0,
                versions=1,
                artifact_entries=16,
                logical_bytes=65536,
                allocated_bytes=65536,
                file_bytes=500000,
            )
            for op in ("append", "maintenance")
        },
        max_remote_opens=4,
        working_temp_bytes=65536,
        source_cost=dict(maintenance_every_batches=9, frame_overhead_bytes=32768),
    )
    envelope["qualification_hash"] = migration.hashed(value)
    return layout, migration, value, envelope


def test_partition_protocol_qualification_requires_independent_frozen_hash():
    layout, migration, value, envelope = qualification_case()
    expected = migration.hashed(value)
    assert layout.validate_qualification(value, envelope, expected_hash=expected) == value
    mutant = copy.deepcopy(value)
    mutant["max_remote_opens"] = 3
    with pytest.raises(ValueError):
        layout.validate_qualification(mutant, envelope, expected_hash=expected)
    with pytest.raises(ValueError):
        layout.validate_qualification(value, envelope, expected_hash="b" * 64)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("max_remote_opens", True),
        ("working_temp_bytes", 0),
        ("format", "lance-layout-envelope-v1"),
        ("schema", "other"),
        ("storage_mode", "1.0"),
        ("caller_budget", 1),
    ],
)
def test_partition_protocol_closed_qualification_refuses_unregistered_policy(field, bad):
    layout, migration, value, envelope = qualification_case()
    value[field] = bad
    envelope["qualification_hash"] = migration.hashed(value)
    with pytest.raises(ValueError):
        layout.validate_qualification(value, envelope, expected_hash=envelope["qualification_hash"])


def test_partition_protocol_qualification_charges_fit_registered_envelope():
    layout, migration, value, envelope = qualification_case()
    for mutate in (
        lambda q: q["partition"].update(max_physical_rows=1000001),
        lambda q: q["partition"].update(max_file_bytes=1000001),
        lambda q: q["reservations"]["append"].update(physical_rows=514),
        lambda q: q["reservations"]["maintenance"].update(fragments=1),
        lambda q: q["source_cost"].update(maintenance_every_batches=33),
        lambda q: q["generation"].update(inode=True),
    ):
        mutant = copy.deepcopy(value)
        mutate(mutant)
        bound = copy.deepcopy(envelope)
        bound["qualification_hash"] = migration.hashed(mutant)
        with pytest.raises(ValueError):
            layout.validate_qualification(mutant, bound, expected_hash=bound["qualification_hash"])


def test_partition_protocol_planning_counts_complete_expansion_without_bodies():
    layout, _, qualification, _ = qualification_case()
    _, _, reg, exported, _, _, _ = fixture()
    counters = layout.LayoutPlanningCounters(qualification)
    for row in exported["entries"]:
        counters.add_entry(row)
    plan = counters.finish(
        reg, exported["finalizations"][0], exported["phase"], exported["cursor_after"]["key"]
    )
    # Three descriptor pages + one membership/record pair + owner and batch receipts.
    assert len(exported["entries"]) == 5
    assert plan["expansion"]["physical_rows"] == 7
    assert plan["expansion"]["batches"] == plan["expansion"]["fragments"] == 1
    assert plan["expansion"]["versions"] == 2
    assert plan["expansion"]["retained_bytes"] == 131072
    assert plan["expansion"]["artifacts"] == 32
    assert plan["expansion"]["index_outputs"] == 1
    assert plan["expansion"]["index_bytes"] == plan["expansion"]["temp_bytes"] == 65536
    assert all(type(value) is int for value in vars(counters).values())
    assert "BODY_SENTINEL" not in str(plan)


def test_partition_protocol_positive_write_charges_before_native_access():
    layout, migration, value, envelope = qualification_case()
    for operation in ("append", "maintenance"):
        for field in ("artifact_entries", "logical_bytes", "allocated_bytes", "file_bytes"):
            mutant = copy.deepcopy(value)
            mutant["reservations"][operation][field] = 0
            bound = {**envelope, "qualification_hash": migration.hashed(mutant)}
            with pytest.raises(ValueError):
                layout.validate_qualification(
                    mutant, bound, expected_hash=bound["qualification_hash"]
                )


def test_partition_protocol_source_window_preserves_original_cursor_and_closed_refusal():
    layout, migration, reg, exported, plan, envelope, _ = fixture()
    window = dict(
        format="lance-layout-source-request-v1",
        action="plan",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    before = exported["cursor_before"]
    assert layout.validate_layout_source_window(window, reg["descriptor"], before) == window
    result = dict(
        status="ok",
        reason=None,
        format="lance-layout-source-result-v1",
        action="plan",
        source_binding=reg["source_binding"],
        cursor_before=before,
        cursor_after=before,
        plans=[plan],
        export=None,
    )
    assert layout.validate_layout_source_result(result, window, reg["descriptor"], before) == result
    with pytest.raises(ValueError):
        layout.validate_layout_source_result(
            {**result, "cursor_after": exported["cursor_after"]}, window, reg["descriptor"], before
        )
    refusal = {**result, "status": "unavailable", "reason": "source_deadline", "plans": []}
    assert (
        layout.validate_layout_source_result(refusal, window, reg["descriptor"], before) == refusal
    )
    with pytest.raises(ValueError):
        layout.validate_layout_source_result(
            {**refusal, "plans": [plan]}, window, reg["descriptor"], before
        )


def test_partition_protocol_private_artifact_loader_binds_identity_and_hash(tmp_path):
    if os.name != "posix":
        pytest.skip("private POSIX qualification artifact")
    from cognitive_castle.lance_layout_directory import load_layout_qualification

    layout, migration, value, _ = qualification_case()
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    info = generation.stat()
    value["generation"] = {"path": str(generation), "device": info.st_dev, "inode": info.st_ino}
    expected = migration.hashed(value)
    artifact = generation / "exact/layout-qualification.json"
    from cognitive_castle.canonical_records import _json

    artifact.write_text(_json(value))
    artifact.chmod(0o400)
    assert load_layout_qualification(value["generation"], expected) == value
    with pytest.raises(ValueError):
        load_layout_qualification({**value["generation"], "inode": info.st_ino + 1}, expected)
    with pytest.raises(ValueError):
        load_layout_qualification(value["generation"], "b" * 64)
    artifact.chmod(0o600)
    with pytest.raises(ValueError):
        load_layout_qualification(value["generation"], expected)
    assert not {"sqlite3", "lance", "pyarrow"}.intersection(sys.modules)


@pytest.mark.parametrize("live", [False, True])
def test_partition_protocol_encoded_expansion_covers_actual_receipt_rows(live):
    import json
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle.canonical_records import CanonicalRecord, _hash, _json

    layout, migration, q, _ = qualification_case()
    _, _, reg, exported, _, _, _ = fixture()
    q["source_cost"].update(maintenance_every_batches=1, frame_overhead_bytes=1)
    if live:
        from test_migration_codec import exported as source_export

        exported = source_export(reg, phase="live")
        rows, owner = exported["entries"], exported["finalizations"][0]
        phase, key = "live", exported["cursor_after"]["key"]
    else:
        original = exported["entries"][-1]
        provenance = json.loads(original["provenance"])
        records = [
            CanonicalRecord(
                "memory:" + _hash(f"sizing:{ordinal}"),
                "x",
                _json(
                    {
                        **provenance,
                        "text_hash": _hash("x"),
                        "chunk_start": ordinal,
                        "chunk_end": ordinal + 1,
                    }
                ),
            )
            for ordinal in range(128)
        ]
        rows, owner = bulk.prepare_owner(
            "owner",
            reg["source_binding"],
            records,
            metadata="",
            columns="",
            scopes="",
            descriptor="",
        )
        phase, key = "history", ["owner"]
        assert len(rows) == 256
    groups = list(bulk.entry_batches(rows))
    try:
        bulk.validate_batch("oracle", reg["source_binding"], groups[-1], [owner])
    except ValueError:
        groups.append([])
    physical = []
    for ordinal, entries in enumerate(groups):
        finals = [owner] if ordinal == len(groups) - 1 else []
        candidates = list(entries)
        if finals:
            candidates.append(bulk.receipt_row(owner, owner["owner"]))
            if owner["live"]:
                receipt = owner["live"]
                candidates.append(
                    dict(
                        id="receipt:" + _hash(_json([receipt["namespace"], receipt["key"]])),
                        kind="receipt",
                        text="",
                        provenance="",
                        digest=receipt["digest"],
                        receipt=_json(receipt["receipt"]),
                    )
                )
        batch_id = f"oracle-{ordinal}"
        batch = dict(
            format=bulk.FORMAT,
            state="committed_batch" if finals else "stage_batch",
            batch_id=batch_id,
            source_binding=reg["source_binding"],
            digest=bulk.validate_batch(batch_id, reg["source_binding"], entries, finals),
            proof=[[row["id"], _hash(_json(row))] for row in candidates],
            records=sum(row["kind"] == "bulk_record" for row in entries),
            published_owners=len(finals),
            entries=len(entries),
        )
        physical.extend([*candidates, bulk.receipt_row(batch, bulk.key("batch", batch_id))])
    counters = layout.LayoutPlanningCounters(q)
    for row in rows:
        counters.add_entry(row)
    plan = counters.finish(reg, owner, phase, key)
    actual = sum(len(json.dumps(row, allow_nan=False).encode()) for row in physical)
    assert plan["expansion"]["physical_rows"] == len(physical)
    assert (
        plan["expansion"]["encoded_bytes"]
        >= actual + len(groups) * q["source_cost"]["frame_overhead_bytes"]
    )


def test_partition_protocol_registry_pin_binds_physical_identity_and_single_index():
    layout, _, qualification, envelope = qualification_case()
    pin = dict(
        format="lance-layout-partition-pin-v1",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        partition_id=0,
        parent_identity=[1, 3],
        directory_identity=[1, 4],
        version=0,
        physical_rows=0,
        fragments=0,
        index=dict(version=None, segments=0, indexed_fragments=0, tail_fragments=0, tail_rows=0),
    )
    assert layout.validate_partition_pin(pin, qualification, envelope) == pin
    after = {
        **pin,
        "version": 1,
        "physical_rows": 8,
        "fragments": 1,
        "index": dict(version=None, segments=0, indexed_fragments=0, tail_fragments=1, tail_rows=8),
    }
    assert layout.validate_partition_pin(after, qualification, envelope) == after
    for change in (
        {"partition_id": envelope["max_partitions"]},
        {"version": True},
        {"directory_identity": [1, False]},
        {"qualification_hash": "b" * 64},
        {"physical_rows": 0},
        {"index": {**after["index"], "segments": 2, "version": 1}},
        {"index": {**after["index"], "tail_rows": 1025}},
    ):
        with pytest.raises(ValueError):
            layout.validate_partition_pin({**after, **change}, qualification, envelope)


def test_partition_protocol_facade_refuses_missing_registry_before_native_or_mkdir(
    tmp_path, monkeypatch
):
    from contextlib import contextmanager
    from cognitive_castle.partitioned_exact_store import PartitionedExactStore
    from cognitive_castle import lance_layout_directory as directory
    from cognitive_castle.lance_exact_store import _StoreError

    _, _, reg, exported, _, _, _ = fixture()
    target = tmp_path / "absent-generation"
    seen = []

    @contextmanager
    def missing(generation):
        seen.append(generation)
        raise _StoreError("unavailable")
        yield

    monkeypatch.setattr(directory, "readonly_directory", missing)
    store = PartitionedExactStore(target, writable=True)
    result = store.layout_import_batch(
        "a" * 64, "a" * 64, reg["source_binding"], exported["entries"], exported["finalizations"]
    )
    assert result == {"status": "unavailable"} and seen == [target]
    assert not target.exists()
    assert not {"sqlite3", "lance", "pyarrow"}.intersection(sys.modules)


def test_partition_protocol_facade_closed_read_whitelist_precedes_metadata(tmp_path):
    from cognitive_castle.partitioned_exact_store import PartitionedExactStore

    store = PartitionedExactStore(tmp_path / "absent")
    for operation, args, context in [
        ("remember", {}, None),
        ("unknown", {}, None),
        (
            "bulk_page",
            {"owner": "receipt:" + "a" * 64, "field": "metadata", "ordinal": 0},
            {"intent_id": "b" * 64, "version": 1},
        ),
    ]:
        with pytest.raises(ValueError):
            store.layout_read(operation, args, context=context)
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize("case", ["original", "namespace", "key", "history", "multiple"])
def test_partition_protocol_pending_live_receipt_binds_source_key_not_memory_ids(monkeypatch, case):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from cognitive_castle import partitioned_exact_store as module
    from cognitive_castle.canonical_records import _hash, _json
    from test_migration_codec import exported as source_export

    layout, migration, reg, _, base_plan, envelope, _ = fixture()
    live_export = source_export(reg, phase="live")
    live = live_export["finalizations"][0]["live"]
    exported = source_export(reg, phase="history") if case == "history" else live_export
    reference = migration.owner_reference(
        exported["finalizations"][0], exported["phase"], exported["cursor_after"]["key"]
    )
    plan = layout.owner_plan(reg, reference, base_plan["expansion"])
    target = layout.target(envelope, 0, [plan])
    pending = layout.prepare_layout_interval(reg, exported, target)
    layout.validate_layout_intent(pending, reg)
    arguments = dict(namespace=live["namespace"], key=live["key"])
    receipt_id = "receipt:" + _hash(_json([arguments["namespace"], arguments["key"]]))
    if case != "history":
        assert pending["owners"][0]["live_receipt_ids"] == live["receipt"]["ids"]
        assert all(identity.startswith("memory:") for identity in live["receipt"]["ids"])
        assert receipt_id not in pending["owners"][0]["live_receipt_ids"]
        assert any(
            row["id"] == receipt_id and row["kind"] == "receipt" for row in pending["routes"]
        )
    if case in {"namespace", "key"}:
        arguments[case] += "-foreign"
    elif case == "multiple":
        pending["owners"] = [pending["owners"][0], pending["owners"][0]]
    opened = []
    authority = SimpleNamespace(pending=lambda identity: pending)
    store = module.PartitionedExactStore(reg["destination"])

    @contextmanager
    def registered():
        yield authority

    class Home:
        def __init__(self, actual_authority, pid, owner, source_binding, **context):
            assert actual_authority is authority and pid == target["partition_id"]
            assert owner == reference["owner"] and source_binding == reg["source_binding"]
            assert context == dict(intent_id=pending["intent_id"], proof_version=7)
            opened.append("home")

        def receipt(self, **actual):
            assert actual == dict(namespace=live["namespace"], key=live["key"])
            return dict(status="ok", digest=live["digest"], receipt=live["receipt"])

        def release_remote(self):
            opened.append("released")

    monkeypatch.setattr(store, "_authority", registered)
    monkeypatch.setattr(module, "_PartitionStore", Home)
    result = store.layout_read(
        "receipt", arguments, context=dict(intent_id=pending["intent_id"], version=7)
    )
    if case == "original":
        assert result == dict(status="ok", digest=live["digest"], receipt=live["receipt"])
        assert opened == ["home", "released"]
    else:
        assert result == {"status": "conflict"}
        assert opened == [], "wrong owner/receipt must refuse before opening storage"
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def grouped_case(phase="live"):
    from test_migration_codec import exported as source_export

    layout, migration, reg, _, base_plan, envelope, _ = fixture()
    exported = source_export(reg, phase=phase)
    manifest = exported["finalizations"][0]
    reference = migration.owner_reference(manifest, phase, exported["cursor_after"]["key"])
    plan = layout.owner_plan(reg, reference, base_plan["expansion"])
    target = layout.target(envelope, 0, [plan])
    pending = layout.prepare_layout_interval(reg, exported, target)
    live = manifest["live"]
    proof = dict(
        owner=manifest["owner"],
        manifest_hash=migration.hashed(manifest),
        partition_id=target["partition_id"],
        version=7,
        live_receipt_ids=[] if live is None else live["receipt"]["ids"],
        live=None
        if live is None
        else dict(
            namespace=live["namespace"],
            key=live["key"],
            digest=live["digest"],
            receipt_hash=migration.hashed(live["receipt"]),
        ),
    )
    observed = dict(
        status="ok",
        format="lance-layout-owner-verification-v1",
        intent_hash=migration.hashed(pending),
        owners=[proof],
    )
    return layout, migration, reg, exported, pending, observed


@pytest.mark.parametrize("phase", ["live", "history", "audit"])
def test_grouped_owner_proof_uses_one_open_and_original_receipt_validator(monkeypatch, phase):
    _check_grouped_facade(monkeypatch, phase=phase)


@pytest.mark.parametrize(
    "fault", ["owner_hash", "live_digest", "missing_receipt", "multiple", "empty", "stale_version"]
)
def test_grouped_owner_proof_refuses_actual_bad_evidence(monkeypatch, fault):
    _check_grouped_facade(monkeypatch, fault=fault)


def _check_grouped_facade(monkeypatch, *, phase="live", fault=None):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from cognitive_castle import bulk_exact_codec as bulk, partitioned_exact_store as module
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.lance_exact_store import _StoreError

    _, _, reg, exported, pending, expected = grouped_case(phase)
    manifest = exported["finalizations"][0]
    rows = {row["id"]: copy.deepcopy(row) for row in exported["entries"]}
    rows[manifest["owner"]] = bulk.receipt_row(manifest, manifest["owner"])
    live = manifest["live"]
    if live is not None:
        receipt_id = "receipt:" + _hash(_json([live["namespace"], live["key"]]))
        rows[receipt_id] = dict(
            id=receipt_id,
            kind="receipt",
            text="",
            provenance="",
            digest=live["digest"],
            receipt=_json(live["receipt"]),
        )
    if fault == "owner_hash":
        changed = {**manifest, "text_hash": "e" * 64}
        rows[manifest["owner"]] = bulk.receipt_row(changed, manifest["owner"])
    elif fault == "live_digest":
        rows[receipt_id]["digest"] = "e" * 64
    elif fault == "missing_receipt":
        del rows[receipt_id]
    elif fault == "multiple":
        pending["owners"] *= 2
    elif fault == "empty":
        pending["owners"] = []
    calls = []

    def actual_pending(identity):
        assert identity == pending["intent_id"]
        return pending

    authority = SimpleNamespace(pending=actual_pending)
    dataset = SimpleNamespace(version=7)
    original_home = module._PartitionStore

    class Home(original_home):
        def __init__(self, actual_authority, pid, owner, source_binding, **context):
            assert actual_authority is authority and pid == pending["target"]["partition_id"]
            assert owner == manifest["owner"] and source_binding == reg["source_binding"]
            assert context == dict(intent_id=pending["intent_id"], proof_version=7)
            calls.append("home")

        def _read(self, *, version=None):
            calls.append("open")
            assert version == 7
            if fault == "stale_version":
                raise _StoreError("stale")
            return dataset, {}

        def _lookup_rows(self, actual_dataset, ids, *, checked=False):
            assert actual_dataset is dataset and len(ids) <= 32
            calls.append("lookup")
            return {identity: rows[identity] for identity in ids if identity in rows}

        def _resolve_ids(self, ids):
            return dict(
                status="ok",
                routes=[],
                absent=[],
                unavailable=[],
                claims=[
                    dict(
                        id=identity,
                        partition_id=pending["target"]["partition_id"],
                        gate=manifest["owner"],
                        version=None,
                    )
                    for identity in ids
                ],
            )

        def _verify_receipt(self, actual_dataset, row, namespace, key):
            calls.append("original_receipt_validator")
            return super()._verify_receipt(actual_dataset, row, namespace, key)

        def release_remote(self):
            calls.append("released")

    @contextmanager
    def registered():
        yield authority

    store = module.PartitionedExactStore(reg["destination"])
    monkeypatch.setattr(store, "_authority", registered)
    monkeypatch.setattr(module, "_PartitionStore", Home)
    result = store.layout_verify_owners(pending["intent_id"], 7)
    if fault is None:
        assert result == expected
        assert calls.count("open") == 1
        assert calls.count("original_receipt_validator") == int(live is not None)
        assert "BODY_SENTINEL" not in str(result) and "METADATA_SENTINEL" not in str(result)
    else:
        assert result == {
            "status": "stale"
            if fault == "stale_version"
            else "conflict"
            if fault in {"multiple", "empty"}
            else "corrupt"
        }
    if fault in {"multiple", "empty"}:
        assert calls == []
    else:
        assert calls[-1] == "released" and calls.count("open") == 1
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize(
    "fault",
    [
        "extra",
        "intent",
        "version_bool",
        "missing_owner",
        "manifest",
        "live",
        "multiple",
        "proof_extra",
        "proof_missing",
        "memory_ids",
    ],
)
def test_grouped_owner_proof_summary_has_closed_original_bindings(fault):
    layout, _, _, exported, pending, observed = grouped_case()
    version = 1 if fault == "version_bool" else 7
    observed["owners"][0]["version"] = version
    expected = [{key: value for key, value in observed["owners"][0].items() if key != "live"}]
    assert layout.validate_grouped_owner_proof(observed, pending, exported, version) == expected
    changed = copy.deepcopy(observed)
    if fault == "extra":
        changed["manifest"] = exported["finalizations"][0]
    elif fault == "intent":
        changed["intent_hash"] = "f" * 64
    elif fault == "version_bool":
        changed["owners"][0]["version"] = True
    elif fault == "missing_owner":
        changed["owners"] = []
    elif fault == "manifest":
        changed["owners"][0]["manifest_hash"] = "f" * 64
    elif fault == "live":
        changed["owners"][0]["live"]["receipt_hash"] = "f" * 64
    elif fault == "proof_extra":
        changed["owners"][0]["verified"] = True
    elif fault == "proof_missing":
        del changed["owners"][0]["live"]
    elif fault == "memory_ids":
        changed["owners"][0]["live_receipt_ids"] = ["receipt:" + "f" * 64]
    else:
        changed["owners"] *= 2
    with pytest.raises(ValueError):
        layout.validate_grouped_owner_proof(changed, pending, exported, version)


@pytest.mark.parametrize("version", [True, 0])
def test_grouped_owner_proof_refuses_invalid_version_before_metadata(monkeypatch, version):
    from cognitive_castle import partitioned_exact_store as module

    store = module.PartitionedExactStore("/private/generation")
    monkeypatch.setattr(store, "_run", lambda action: pytest.fail("metadata must not open"))
    with pytest.raises(ValueError):
        store.layout_verify_owners("a" * 64, version)


def test_partition_protocol_directory_page_is_exact_pinned_physical_prefix():
    layout, _, q, envelope = qualification_case()
    pin = dict(
        format=layout.PARTITION_PIN,
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        partition_id=0,
        parent_identity=[1, 3],
        directory_identity=[1, 4],
        version=1,
        physical_rows=1,
        fragments=1,
        index=dict(version=None, segments=0, indexed_fragments=0, tail_fragments=1, tail_rows=1),
    )
    route = dict(
        format=layout.ROUTE,
        id="receipt:" + "a" * 64,
        kind="bulk_page",
        row_hash="b" * 64,
        gate="receipt:" + "c" * 64,
        partition_id=0,
        version=1,
        receipt="receipt:" + "d" * 64,
        state="staged",
    )
    page = dict(
        format="lance-layout-directory-page-v1",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        epoch="e" * 64,
        rebuild_token="f" * 64,
        partition_id=0,
        version=1,
        cursor_before=0,
        cursor_after=1,
        pin=pin,
        rows=[route],
        complete=True,
    )
    assert layout.validate_directory_page(page, q, envelope) == page
    for change in (
        {"complete": False},
        {"cursor_after": 0},
        {"epoch": True},
        {"rows": [{**route, "version": 2}]},
        {"rows": [{**route, "partition_id": 1}]},
    ):
        with pytest.raises(ValueError):
            layout.validate_directory_page({**page, **change}, q, envelope)


def test_partition_protocol_owner_span_checks_actual_reused_bytes_and_first_owner(monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle.partitioned_exact_store import _PartitionStore
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle.canonical_records import prepare_remember, _hash, _json
    from cognitive_castle.lance_exact_store import _StoreError
    import json

    original = prepare_remember("borrow", "same original")
    first, owner1 = bulk.prepare_owner(
        "first", "a" * 64, original.records, metadata="", columns="", scopes="", descriptor=""
    )
    second, owner2 = bulk.prepare_owner(
        "second", "b" * 64, original.records, metadata="", columns="", scopes="", descriptor=""
    )
    old = [*first, bulk.receipt_row(owner1, owner1["owner"])]
    new = [row for row in second if row["kind"] != "bulk_record"] + [
        bulk.receipt_row(owner2, owner2["owner"])
    ]
    physical = old + new
    by_id = {row["id"]: row for row in old}
    store = object.__new__(_PartitionStore)
    store.qualification = SimpleNamespace(
        owner=owner2["owner"], source_binding=owner2["source_binding"]
    )
    store.authority = SimpleNamespace(
        span=lambda owner: dict(first_row=len(old), next_row=len(physical))
    )
    monkeypatch.setattr(store, "_geometry", lambda dataset: {"physical_rows": len(physical)})
    monkeypatch.setattr(
        store,
        "_lookup_rows",
        lambda dataset, ids, **kwargs: {
            identity: by_id[identity] for identity in ids if identity in by_id
        },
    )

    def scanner(**options):
        assert options["offset"] == len(old) and options["limit"] == len(new)
        assert (
            options["batch_size"] == 32
            and options["batch_readahead"] == options["fragment_readahead"] == 0
        )
        return SimpleNamespace(to_batches=lambda: iter([SimpleNamespace(to_pylist=lambda: new)]))

    dataset = SimpleNamespace(scanner=scanner)
    store._verify_owner(dataset, owner2, {})
    row = by_id[original.records[0].id]
    provenance = json.loads(row["provenance"])
    row["text"] = "changed bytes"
    row["provenance"] = _json(
        {**provenance, "text_hash": _hash(row["text"]), "chunk_end": len(row["text"])}
    )
    with pytest.raises(_StoreError):
        store._verify_owner(dataset, owner2, {})


def test_partition_protocol_remote_release_precedes_next_acquisition_and_counts_reopens(
    monkeypatch,
):
    from types import SimpleNamespace
    from cognitive_castle import partitioned_exact_store as module
    from cognitive_castle.lance_exact_store import _StoreError

    original = module._PartitionStore
    store = object.__new__(original)
    store.remote_pid = store.remote_store = store.remote_dataset = None
    store.remote_opens = 0
    store.authority = SimpleNamespace(
        qualification={"max_remote_opens": 2}, owner_plan=lambda owner: {"source_binding": "source"}
    )
    events = []

    class Dataset:
        def __init__(self, pid):
            self.pid = pid
            events.append(("open", pid))

        def __del__(self):
            events.append(("release", self.pid))

    class Remote:
        def __init__(self, authority, pid, owner, source):
            self.pid = pid
            self.registry = {"mutation": None}

        def _open(self):
            return Dataset(self.pid)

        def actual_pin(self, dataset):
            return {}

    monkeypatch.setattr(module, "_PartitionStore", Remote)
    store._remote(1, "owner")
    store._remote(2, "owner")
    with pytest.raises(_StoreError):
        store._remote(1, "owner")
    assert events == [("open", 1), ("release", 1), ("open", 2), ("release", 2)]
    assert store.remote_opens == 2


def test_partition_protocol_remote_original_version_is_not_latest_but_cannot_be_future(monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle import partitioned_exact_store as module
    from cognitive_castle.lance_exact_store import _StoreError
    from cognitive_castle import migration_codec as migration

    _, _, _, exported, _, _, _ = fixture()
    row = next(row for row in exported["entries"] if row["kind"] == "bulk_record")
    route = dict(
        id=row["id"],
        kind=row["kind"],
        row_hash=migration.hashed(row),
        gate=row["digest"],
        partition_id=1,
        version=1,
        receipt="receipt:" + "f" * 64,
        state="published",
    )
    store = object.__new__(module._PartitionStore)
    store.pid = 0
    store.intent_id = None
    store.remote_dataset = SimpleNamespace(version=5)
    store.authority = SimpleNamespace(
        resolve=lambda ids, **kwargs: dict(
            status="ok", routes=[route], absent=[], claims=[], unavailable=[]
        )
    )
    monkeypatch.setattr(store, "_remote", lambda pid, owner: store.remote_dataset)
    monkeypatch.setattr(module, "lookup_many", lambda dataset, ids, **kwargs: {row["id"]: row})
    assert store._lookup_rows(None, [row["id"]], checked=True) == {row["id"]: row}
    route["version"] = 6
    with pytest.raises(_StoreError):
        store._lookup_rows(None, [row["id"]], checked=True)


def test_partition_protocol_public_gate_and_certified_absence_precede_native_open(monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle import partitioned_exact_store as module

    store = module.PartitionedExactStore("/private/generation")
    identity = "memory:" + "a" * 64
    monkeypatch.setattr(
        module,
        "_PartitionStore",
        lambda *a, **kw: pytest.fail("unpublished/absent route cannot open Lance"),
    )
    blocked = SimpleNamespace(
        resolve=lambda ids: dict(
            status="unavailable", routes=[], absent=[], unavailable=ids, claims=[]
        )
    )
    assert store._read(blocked, "read_sources", {"ids": [identity]}, [identity], None) == {
        "status": "unavailable"
    }
    absent = SimpleNamespace(
        resolve=lambda ids: dict(status="ok", routes=[], absent=ids, unavailable=[], claims=[])
    )
    assert store._read(absent, "read_sources", {"ids": [identity]}, [identity], None) == {
        "status": "not_found",
        "sources": [],
    }

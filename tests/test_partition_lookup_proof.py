"""One actual receipt per bounded lookup; no proof survives the invocation."""

import copy
import sys
from types import SimpleNamespace

import pytest

from test_lance_layout_codec import fixture


def lookup_case(monkeypatch):
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle import partitioned_exact_store as module

    layout, migration, reg, exported, _, _, target = fixture()
    pending = layout.prepare_layout_interval(reg, exported, target)
    rows = {row["id"]: copy.deepcopy(row) for row in exported["entries"]}
    for owner in exported["finalizations"]:
        rows[owner["owner"]] = bulk.receipt_row(owner, owner["owner"])
    ids = list(rows)
    assert len(ids) > 1
    receipt_id = layout.batch_receipt_id(pending)
    manifest = dict(
        format=bulk.FORMAT,
        state="committed_batch",
        batch_id=pending["intent_id"],
        source_binding=pending["source_binding"],
        digest=pending["digest"],
        proof=[[identity, migration.hashed(row)] for identity, row in rows.items()],
        records=pending["records"],
        published_owners=len(pending["owners"]),
        entries=pending["entries"],
    )
    rows[receipt_id] = bulk.receipt_row(manifest, receipt_id)
    locations = {
        route["id"]: dict(route, partition_id=0, state="reserved", version=1)
        for route in pending["routes"]
    }
    state = dict(geometry={"version": 1}, geometry_reads=0, receipt_reads=0)
    dataset = SimpleNamespace(version=1)
    store = object.__new__(module._PartitionStore)
    store.pid = 0
    store.intent_id = pending["intent_id"]
    store.registry = {"mutation": {"after": {"version": 1}}}

    def resolve(group, **kwargs):
        assert len(group) <= 32
        return dict(
            status="ok",
            routes=[],
            claims=[locations[i] for i in group],
            absent=[],
            unavailable=[],
        )

    def geometry(actual):
        assert actual is dataset
        state["geometry_reads"] += 1
        return state["geometry"]

    def lookup(actual, group, *, checked):
        assert actual is dataset and checked
        if group == [receipt_id]:
            state["receipt_reads"] += 1
        return {identity: rows[identity] for identity in group if identity in rows}

    def actual_pending(identity):
        assert identity == pending["intent_id"]
        return pending

    store.authority = SimpleNamespace(resolve=resolve, pending=actual_pending)
    monkeypatch.setattr(store, "_geometry", geometry)
    monkeypatch.setattr(module, "lookup_many", lookup)
    return SimpleNamespace(
        store=store,
        dataset=dataset,
        rows=rows,
        ids=ids,
        state=state,
        receipt_id=receipt_id,
        manifest=manifest,
        locations=locations,
        bulk=bulk,
        module=module,
    )


def test_lookup_reads_actual_batch_proof_once_per_call(monkeypatch):
    case = lookup_case(monkeypatch)
    actual = case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert actual == {identity: case.rows[identity] for identity in case.ids}
    assert case.state["receipt_reads"] == 1
    assert case.state["geometry_reads"] == 1
    assert not {"lance", "pyarrow", "numpy", "sqlite3"}.intersection(sys.modules)


def test_next_lookup_rereads_actual_receipt_and_detects_changed_last_hash(monkeypatch):
    case = lookup_case(monkeypatch)
    case.store._lookup_rows(case.dataset, case.ids, checked=True)
    changed = copy.deepcopy(case.manifest)
    changed["proof"][-1][1] = "0" * 64
    case.rows[case.receipt_id] = case.bulk.receipt_row(changed, case.receipt_id)
    with pytest.raises(case.module._StoreError, match="corrupt"):
        case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == 2
    assert case.state["geometry_reads"] == 2


def test_next_lookup_rechecks_geometry_without_cached_proof(monkeypatch):
    case = lookup_case(monkeypatch)
    case.state["geometry"] = {"version": 0}
    case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == 0
    case.state["geometry"] = {"version": 1}
    case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == 1
    assert case.state["geometry_reads"] == 2


@pytest.mark.parametrize("change", ["first_hash", "last_hash", "missing", "binding", "digest"])
def test_actual_receipt_validation_and_each_row_hash_remain_required(monkeypatch, change):
    case = lookup_case(monkeypatch)
    changed = copy.deepcopy(case.manifest)
    if change in {"first_hash", "last_hash"}:
        changed["proof"][0 if change == "first_hash" else -1][1] = "0" * 64
    elif change == "missing":
        changed["proof"].pop()
    elif change == "binding":
        changed["source_binding"] = "wrong-source"
    else:
        changed["digest"] = "0" * 64
    case.rows[case.receipt_id] = case.bulk.receipt_row(changed, case.receipt_id)
    with pytest.raises(case.module._StoreError, match="corrupt"):
        case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == 1


@pytest.mark.parametrize("mode", ["empty", "published", "no_intent"])
def test_lookup_does_not_read_context_when_unneeded(monkeypatch, mode):
    case = lookup_case(monkeypatch)
    if mode == "empty":
        case.rows.clear()
    elif mode == "published":
        for location in case.locations.values():
            location["state"] = "published"
    else:
        case.store.intent_id = None
    case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == case.state["geometry_reads"] == 0


def test_batch_receipt_lookup_keeps_geometry_check_but_avoids_recursive_proof(monkeypatch):
    case = lookup_case(monkeypatch)
    result = case.store._lookup_rows(case.dataset, [case.receipt_id], checked=True)
    assert result == {case.receipt_id: case.rows[case.receipt_id]}
    assert case.state["receipt_reads"] == 1  # the requested row, no context recursion
    assert case.state["geometry_reads"] == 1


@pytest.mark.parametrize("change, status", [("version", "stale"), ("row_hash", "corrupt")])
def test_route_version_and_hash_reject_before_context_read(monkeypatch, change, status):
    case = lookup_case(monkeypatch)
    case.locations[case.ids[0]][change] = 2 if change == "version" else "0" * 64
    with pytest.raises(case.module._StoreError, match=status):
        case.store._lookup_rows(case.dataset, case.ids, checked=True)
    assert case.state["receipt_reads"] == case.state["geometry_reads"] == 0

"""Packed facade paths retain every original owner on one physical handle."""

import copy
from contextlib import contextmanager
from types import SimpleNamespace

import pytest


def data():
    from test_layout_packed_intent import live_case
    from test_layout_packed_rows import case

    reg, packet, _ = live_case()
    intent, mutation, physical = case()
    return reg, packet, intent, mutation, physical


def grouped(monkeypatch, fault=None):
    from cognitive_castle import partitioned_exact_store as module
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_packed import validate_grouped_owner_proof

    reg, packet, intent, _, physical = data()
    rows = {r["id"]: r for r in physical}
    candidates = {r["id"]: r for r in intent["routes"]}
    owners = intent["owners"]
    second = owners[1]
    live_id = "receipt:" + migration.hashed(second["source_key"])
    if fault == "missing":
        del rows[live_id]
    elif fault == "live_digest":
        rows[live_id]["digest"] = "e" * 64
    elif fault == "owner_hash":
        owner = packet["components"][1]["export"]["finalizations"][0]
        from cognitive_castle import bulk_exact_codec as bulk

        rows[second["owner"]] = bulk.receipt_row({**owner, "text_hash": "e" * 64}, second["owner"])
    elif fault == "legacy_multiple":
        intent["format"] = module.layout.INTENT
    calls = []
    authority = SimpleNamespace(pending=lambda identity: intent)
    dataset = SimpleNamespace(version=7)
    original = module._PartitionStore

    class Home(original):
        def __init__(self, actual, pid, owner, source_binding, **context):
            assert actual is authority and pid == intent["target"]["partition_id"]
            assert owner == owners[0]["owner"] and source_binding == intent["source_binding"]
            assert context == dict(intent_id=intent["intent_id"], proof_version=7)
            calls.append("home")

        def _read(self, *, version=None):
            assert version == 7
            calls.append("open")
            return dataset, {}

        def _lookup_rows(self, actual, ids, *, checked=False):
            assert actual is dataset and len(ids) <= 32
            return {i: rows[i] for i in ids if i in rows}

        def _resolve_ids(self, ids):
            claims = [
                dict(
                    id=i,
                    partition_id=intent["target"]["partition_id"],
                    gate=candidates[i]["gate"],
                    version=None,
                )
                for i in ids
            ]
            if fault in {"gate", "partition", "version"} and any(
                c["gate"] == second["owner"] for c in claims
            ):
                claims[0][
                    {"gate": "gate", "partition": "partition_id", "version": "version"}[fault]
                ] = owners[0]["owner"] if fault == "gate" else 999
            return dict(status="ok", routes=[], absent=[], unavailable=[], claims=claims)

        def _verify_receipt(self, actual, row, namespace, key):
            calls.append(("receipt", namespace, key))
            return super()._verify_receipt(actual, row, namespace, key)

        def release_remote(self):
            calls.append("released")

    @contextmanager
    def registered():
        yield authority

    store = module.PartitionedExactStore(reg["destination"])
    monkeypatch.setattr(store, "_authority", registered)
    monkeypatch.setattr(module, "_PartitionStore", Home)
    result = store.layout_verify_owners(intent["intent_id"], 7)
    if fault is None:
        assert result.get("status") == "ok", result
        finalizations = [p["export"]["finalizations"][0] for p in packet["components"]]
        proofs = validate_grouped_owner_proof(result, intent, dict(finalizations=finalizations), 7)
        assert [p["owner"] for p in proofs] == [o["owner"] for o in owners]
        assert calls.count("open") == calls.count("home") == 1
        assert [c[1:] for c in calls if isinstance(c, tuple)] == [
            tuple(o["source_key"]) for o in owners
        ]
    else:
        assert result == dict(
            status="conflict"
            if fault in {"gate", "partition", "version", "legacy_multiple"}
            else "corrupt"
        )
    assert calls.count("released") == int(fault != "legacy_multiple")


def test_packed_group_verifies_every_original_live_receipt_on_one_handle(monkeypatch):
    grouped(monkeypatch)


@pytest.mark.parametrize(
    "fault",
    ["missing", "live_digest", "owner_hash", "gate", "partition", "version", "legacy_multiple"],
)
def test_packed_group_never_accepts_a_bad_later_owner(monkeypatch, fault):
    grouped(monkeypatch, fault)


def home_case():
    from cognitive_castle.partitioned_exact_store import _PartitionStore
    from cognitive_castle.lance_layout_packed_rows import owner_spans

    _, packet, intent, mutation, physical = data()
    home = object.__new__(_PartitionStore)
    spans = owner_spans(intent, mutation)
    home.intent_id = intent["intent_id"]
    home.registry = dict(mutation=mutation)
    home.qualification = SimpleNamespace(
        owner=intent["owners"][0]["owner"], source_binding=intent["source_binding"]
    )
    home.authority = SimpleNamespace(
        pending=lambda identity: intent, span=lambda owner: spans[owner]
    )
    home.release_remote = lambda: None
    before = {k: mutation["before"][k] for k in mutation["after"]}
    home._geometry = lambda dataset: before
    return home, packet, intent, mutation, physical, spans


def test_actual_append_reorders_original_rows_to_registered_owner_ranges(monkeypatch):
    from cognitive_castle.bulk_exact_store import OrderedBulkExactStore
    from cognitive_castle.lance_exact_store import _StoreError

    home, _, intent, mutation, rows, _ = home_case()
    saved = copy.deepcopy(rows)
    observed = []

    def before(self, dataset, actual):
        observed.extend(actual)
        raise _StoreError("fixture_stop")

    monkeypatch.setattr(OrderedBulkExactStore, "_before_append", before)
    with pytest.raises(_StoreError, match="fixture_stop"):
        home._before_append(None, rows)
    assert [r["id"] for r in observed] == mutation["new_ids"]
    assert {r["id"]: r for r in observed} == {r["id"]: r for r in saved}
    assert observed[-1]["id"] == intent["routes"][-1]["id"]


def test_fresh_owner_verification_uses_only_its_original_rows():
    home, packet, _, _, rows, _ = home_case()
    pending = {r["id"]: r for r in rows[:-1]}
    for part in packet["components"]:
        home._verify_owner(None, part["export"]["finalizations"][0], pending)


@pytest.mark.parametrize("damage", ["unknown", "changed", "missing"])
def test_pending_filtering_cannot_hide_foreign_or_missing_original_rows(damage):
    from cognitive_castle.lance_exact_store import _StoreError

    home, packet, _, _, rows, _ = home_case()
    pending = {r["id"]: copy.deepcopy(r) for r in rows[:-1]}
    if damage == "unknown":
        pending["memory:" + "e" * 64] = dict(rows[0], id="memory:" + "e" * 64)
    elif damage == "changed":
        pending[rows[0]["id"]]["text"] += "tampered"
    else:
        del pending[rows[0]["id"]]
    with pytest.raises(_StoreError):
        home._verify_owner(None, packet["components"][1]["export"]["finalizations"][0], pending)


@pytest.mark.parametrize("damage", ["span", "geometry"])
def test_future_owner_range_is_empty_only_before_the_exact_registered_append(damage):
    from cognitive_castle.lance_exact_store import _StoreError

    home, _, intent, _, _, spans = home_case()
    owner = intent["owners"][1]["owner"]
    if damage == "span":
        spans[owner]["first_row"] += 1
    else:
        home._geometry = lambda dataset: dict(version=99, physical_rows=40, fragments=3)
    with pytest.raises(_StoreError):
        list(home._owner_rows(None, owner, {}))


@pytest.mark.parametrize("operation", ["bulk_owner", "receipt"])
@pytest.mark.parametrize("foreign", [False, True])
def test_context_reads_select_only_the_exact_original_packed_owner(monkeypatch, operation, foreign):
    from cognitive_castle import partitioned_exact_store as module

    reg, _, intent, _, _ = data()
    selected = intent["owners"][1]
    authority = SimpleNamespace(pending=lambda identity: intent)
    calls = []
    original = module._PartitionStore

    class Home(original):
        def __init__(self, actual, pid, owner, source_binding, **context):
            assert actual is authority and owner == selected["owner"]
            assert pid == intent["target"]["partition_id"]
            assert source_binding == intent["source_binding"]
            assert context == dict(intent_id=intent["intent_id"], proof_version=7)
            calls.append("home")

        def bulk_owner(self, **arguments):
            return dict(status="ok", owner=arguments["owner"])

        def receipt(self, **arguments):
            return dict(status="ok", **arguments)

        def release_remote(self):
            calls.append("released")

    @contextmanager
    def registered():
        yield authority

    store = module.PartitionedExactStore(reg["destination"])
    monkeypatch.setattr(store, "_authority", registered)
    monkeypatch.setattr(module, "_PartitionStore", Home)
    if operation == "bulk_owner":
        args = dict(owner="receipt:" + "e" * 64 if foreign else selected["owner"], version=7)
    else:
        args = dict(
            namespace=selected["source_key"][0],
            key="foreign" if foreign else selected["source_key"][1],
        )
    result = store.layout_read(
        operation, args, context=dict(intent_id=intent["intent_id"], version=7)
    )
    if foreign:
        assert result == dict(status="conflict") and calls == []
    else:
        assert result["status"] == "ok" and calls == ["home", "released"]

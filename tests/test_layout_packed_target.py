"""Packed authority must bind every original owner to one actual reservation."""

import copy

import pytest


def case(monkeypatch):
    from test_layout_native_prefix_binding import authority
    from test_layout_source_packing import prefix_case
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    directory, job, plans, envelope, store, events, state = authority(monkeypatch)
    result = directory.bind_prefix(job, plans, envelope)
    reg, before, parts, _ = prefix_case()
    packet = pack_source_prefix(reg, before, parts[:2])
    intent = prepare_prefix_interval(reg, packet, result["target"])
    events.clear()
    return directory, state, intent, store, events


def test_packed_target_checks_complete_bound_original_set_without_writing(monkeypatch):
    directory, state, intent, store, events = case(monkeypatch)
    saved = copy.deepcopy(store)
    assert directory._prefix_target(state, intent) == intent["target"]
    assert store == saved
    assert not any(sql.startswith(("INSERT", "UPDATE", "SAVEPOINT")) for sql, _ in events)


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "published",
        "foreign_job",
        "different_home",
        "first_anchor",
        "wrong_registry_anchor",
        "sealed",
        "foreign_mutation",
        "foreign_pending",
        "wrong_cursor",
        "attachment",
        "plan_charge",
        "plan_reference",
        "span",
        "total_reservation",
        "corrupt_reservation",
    ],
)
def test_packed_target_refuses_incomplete_or_foreign_authority_without_writes(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, state, intent, store, events = case(monkeypatch)
    first, second = [o["owner"] for o in intent["owners"]]
    if damage == "missing":
        del store["homes"][second]
    elif damage == "extra":
        store["homes"]["receipt:" + "e" * 64] = copy.deepcopy(store["homes"][second])
    elif damage == "published":
        store["homes"][second]["published"] = 1
    elif damage == "foreign_job":
        store["homes"][second]["job"] = "e" * 64
    elif damage == "different_home":
        store["homes"][second]["pid"] = 1
    elif damage == "first_anchor":
        store["registry"][0]["active_owner"] = second
    elif damage == "wrong_registry_anchor":
        store["registry"][1]["active_owner"] = second
    elif damage == "sealed":
        store["registry"][0]["state"] = "sealed"
    elif damage == "foreign_mutation":
        store["registry"][0]["mutation"] = dict(kind="append", intent_id="e" * 64)
    elif damage == "foreign_pending":
        state["pending"] = dict(intent_id="e" * 64)
    elif damage == "wrong_cursor":
        state["staging_cursor"] = copy.deepcopy(intent["cursor_after"])
    elif damage == "attachment":
        state["layout"]["envelope"] = copy.deepcopy(state["layout"]["envelope"])
        state["layout"]["envelope"]["generation"]["inode"] += 1
    elif damage == "plan_charge":
        store["homes"][second]["plan"]["expansion"]["encoded_bytes"] += 1
    elif damage == "plan_reference":
        store["homes"][second]["plan"]["owner_key"] = ["changed-original-key"]
    elif damage == "span":
        store["spans"][second] = (0, 1, 0, 0)
    elif damage == "total_reservation":
        store["reservations"][0] = dict(store["homes"][first]["plan"]["expansion"])
    elif damage == "corrupt_reservation":
        # Even an oversized reservation cannot hide an invalid typed counter.
        store["reservations"][0]["encoded_bytes"] = True
    saved = copy.deepcopy(store)
    with pytest.raises((ValueError, _StoreError)):
        directory._prefix_target(state, intent)
    assert store == saved
    assert not any(sql.startswith(("INSERT", "UPDATE", "SAVEPOINT")) for sql, _ in events)


def test_packed_target_accepts_exact_pending_append_recovery(monkeypatch):
    directory, state, intent, store, _ = case(monkeypatch)
    state["pending"] = copy.deepcopy(intent)
    store["registry"][0]["mutation"] = dict(kind="append", intent_id=intent["intent_id"])
    assert directory._prefix_target(state, intent) == intent["target"]

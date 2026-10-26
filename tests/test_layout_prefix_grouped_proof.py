"""A complete source prefix must fit the unchanged 4096-byte owner proof."""

import copy
import json

import pytest


def live_packet_parts(count=4):
    from cognitive_castle import (
        bulk_exact_codec as bulk,
        lance_layout_codec as layout,
        migration_codec as migration,
    )
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.sqlite_export_codec import initial_cursor
    from test_layout_source_packing import prefix_case

    reg, _, _, envelope = prefix_case()
    before = initial_cursor(reg["source_binding"], reg["descriptor"]["store_id"])
    cursor, parts = before, []
    expansion = prefix_case()[2][0]["plan"]["expansion"]
    text = ("a" * 8191 + "€") * 7 + "b" * 8178
    for index in range(count):
        prepared = prepare_remember("proof-" + str(index), text)
        assert len(prepared.records) == 9
        key = [prepared.namespace, prepared.request_id]
        live = dict(namespace=key[0], key=key[1], digest=prepared.digest, receipt=prepared.receipt)
        entries, owner = bulk.prepare_owner(
            "LIVE:" + migration.hashed(["live", key]),
            reg["source_binding"],
            prepared.records,
            descriptor="ORIGINAL",
            live=live,
        )
        after = dict(cursor, phase="live", key=key)
        exported = dict(
            status="ok",
            reason=None,
            source_binding=reg["source_binding"],
            cursor_before=cursor,
            cursor_after=after,
            entries=entries,
            finalizations=[owner],
            counts={**migration.empty_counts(), "live_owners": 1, "members": 9},
            phase="live",
            phase_complete=True,
            complete=False,
            coverage="incomplete",
            frame_bytes=0,
        )
        for _ in range(3):
            exported["frame_bytes"] = len(migration.frame_bytes(exported))
        plan = layout.owner_plan(
            reg, migration.owner_reference(owner, "live", key), copy.deepcopy(expansion)
        )
        parts.append(dict(plan=plan, export=exported))
        cursor = after
    return reg, before, parts, envelope


def test_source_prefix_stops_before_exceeding_actual_grouped_owner_reply_budget():
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, parts, _ = live_packet_parts()
    packet = pack_source_prefix(reg, before, parts)
    assert 1 <= len(packet["components"]) < 4
    assert packet["components"] == parts[: len(packet["components"])]
    assert packet["cursor_after"] == parts[len(packet["components"]) - 1]["export"]["cursor_after"]


def proof_case(backend="sqlite"):
    from test_layout_packed_intent import case, live_case
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval

    reg, packet, target = live_case() if backend == "live" else case(backend)
    intent = prepare_prefix_interval(reg, packet, target)
    manifests = [p["export"]["finalizations"][0] for p in packet["components"]]
    owners = []
    for manifest in manifests:
        live = manifest["live"]
        owners.append(
            dict(
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
        )
    value = dict(
        status="ok",
        format=layout.GROUPED_OWNER_PROOF,
        intent_hash=migration.hashed(intent),
        owners=owners,
    )
    return intent, dict(finalizations=manifests), value


@pytest.mark.parametrize("backend", ["sqlite", "native", "live"])
def test_grouped_validation_preserves_every_original_owner_and_live_receipt(backend):
    from cognitive_castle.lance_layout_packed import validate_grouped_owner_proof

    intent, exported, value = proof_case(backend)
    result = validate_grouped_owner_proof(value, intent, exported, 7)
    assert len(result) == len(intent["owners"]) == 3
    assert [p["owner"] for p in result] == [p["owner"] for p in intent["owners"]]
    assert all("live" not in proof for proof in result)


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "duplicate",
        "order",
        "owner",
        "manifest",
        "version",
        "partition",
        "ids",
        "live",
        "hash",
        "unknown",
    ],
)
def test_grouped_validation_refuses_any_changed_owner_evidence(damage):
    from cognitive_castle.lance_layout_packed import validate_grouped_owner_proof

    intent, exported, value = proof_case("live")
    if damage == "missing":
        value["owners"].pop()
    elif damage == "duplicate":
        value["owners"][1] = copy.deepcopy(value["owners"][0])
    elif damage == "order":
        value["owners"].reverse()
    elif damage == "owner":
        value["owners"][1]["owner"] = "receipt:" + "f" * 64
    elif damage == "manifest":
        value["owners"][1]["manifest_hash"] = "f" * 64
    elif damage == "version":
        value["owners"][1]["version"] = 8
    elif damage == "partition":
        value["owners"][1]["partition_id"] += 1
    elif damage == "ids":
        value["owners"][1]["live_receipt_ids"] = []
    elif damage == "live":
        value["owners"][1]["live"]["digest"] = "f" * 64
    elif damage == "hash":
        value["intent_hash"] = "f" * 64
    else:
        value["owners"][1]["cached"] = True
    with pytest.raises(ValueError):
        validate_grouped_owner_proof(value, intent, exported, 7)


def test_full_grouped_response_budget_is_checked_including_maximum_scalar_widths():
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, parts, _ = live_packet_parts()
    packet = pack_source_prefix(reg, before, parts)
    maximum = 2**63 - 1
    response = dict(
        status="ok",
        format=layout.GROUPED_OWNER_PROOF,
        intent_hash="f" * 64,
        owners=[
            layout.owner_observation(p["export"]["finalizations"][0], maximum, maximum)
            for p in packet["components"]
        ],
    )
    assert (
        max(
            len(json.dumps(response).encode()),
            len(json.dumps(response, ensure_ascii=False).encode()),
        )
        <= 4096
    )

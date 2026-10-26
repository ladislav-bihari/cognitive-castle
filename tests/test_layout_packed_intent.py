"""A packed interval retains every original owner boundary and separate row gate."""

import copy
import sys

import pytest

from test_layout_source_packing import prefix_case


def case(backend="sqlite", count=3):
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    reg, before, parts, envelope = prefix_case(backend, count=count)
    packet = pack_source_prefix(reg, before, parts)
    fixed = layout.target(envelope, 0, [p["plan"] for p in packet["components"]])
    return reg, packet, fixed


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("count", [1, 3, 8])
def test_packed_intent_preserves_original_intervals_and_contiguous_gates(backend, count):
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_packed import (
        prepare_prefix_interval,
        validate_prefix_intent,
        INTENT,
    )
    from cognitive_castle import lance_layout_codec as layout

    reg, packet, fixed = case(backend, count)
    saved = copy.deepcopy((reg, packet, fixed))
    value = prepare_prefix_interval(reg, packet, fixed)
    assert validate_prefix_intent(value, reg) == value and value["format"] == INTENT
    assert (reg, packet, fixed) == saved
    assert (
        value["cursor_before"] == packet["cursor_before"]
        and value["cursor_after"] == packet["cursor_after"]
    )
    assert value["prefix_hash"] == packet["prefix_hash"] and len(value["owners"]) == count
    routes = value["routes"][:-1]
    position = 0
    for part, component in zip(packet["components"], value["components"], strict=True):
        original = migration.prepare_interval(reg, part["export"])
        assert component == dict(plan_hash=migration.hashed(part["plan"]), interval=original)
        owner = original["owners"][0]
        size = original["entries"] + 1 + int(owner["classification"] == "live")
        group = routes[position : position + size]
        assert {r["gate"] for r in group} == {owner["owner"]}
        assert group[original["entries"]]["id"] == owner["owner"]
        position += size
    assert position == len(routes)
    assert value["routes"][-1] == dict(
        id=layout.batch_receipt_id(value),
        kind="bulk_receipt",
        row_hash=None,
        gate=value["owners"][0]["owner"],
    )
    assert migration.expected_receipt(value)["published_owners"] == count
    with pytest.raises(ValueError):
        layout.validate_layout_intent(value, reg)
    with pytest.raises(ValueError):
        migration.validate_interval(value, reg)
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize(
    "damage",
    [
        "format",
        "unknown",
        "hash",
        "target",
        "target_order",
        "plan_order",
        "before",
        "after",
        "owner_before",
        "owner_after",
        "owner_count",
        "counter",
        "entries",
        "records",
        "coverage",
        "route_gate",
        "route_interleave",
        "route_duplicate",
        "route_hash",
        "batch_gate",
        "batch_order",
        "component_unknown",
    ],
)
def test_packed_intent_rejects_changed_original_association(backend, damage):
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval, validate_prefix_intent

    reg, packet, fixed = case(backend)
    value = prepare_prefix_interval(reg, packet, fixed)
    if damage == "format":
        value["format"] = "unknown"
    elif damage == "unknown":
        value["persisted_body"] = "forbidden"
    elif damage == "hash":
        value["prefix_hash"] = "f" * 64
    elif damage == "target":
        value["target"]["partition_id"] += 1
    elif damage == "target_order":
        value["target"]["owner_plan_hashes"].reverse()
    elif damage == "plan_order":
        value["components"].reverse()
    elif damage == "before":
        value["cursor_before"] = copy.deepcopy(value["cursor_after"])
    elif damage == "after":
        value["cursor_after"] = copy.deepcopy(value["cursor_before"])
    elif damage == "owner_before":
        value["components"][1]["interval"]["cursor_before"] = copy.deepcopy(value["cursor_before"])
    elif damage == "owner_after":
        value["components"][0]["interval"]["cursor_after"] = copy.deepcopy(value["cursor_after"])
    elif damage == "owner_count":
        value["owners"].pop()
    elif damage == "counter":
        value["counts"]["historical_occurrences"] -= 1
    elif damage in {"entries", "records"}:
        value[damage] += 1
    elif damage == "coverage":
        value["coverage"] = "complete"
    elif damage == "route_gate":
        value["routes"][0]["gate"] = value["owners"][1]["owner"]
    elif damage == "route_interleave":
        value["routes"][0], value["routes"][-2] = value["routes"][-2], value["routes"][0]
    elif damage == "route_duplicate":
        value["routes"][1] = copy.deepcopy(value["routes"][0])
    elif damage == "route_hash":
        value["routes"][0]["row_hash"] = None
    elif damage == "batch_gate":
        value["routes"][-1]["gate"] = value["owners"][1]["owner"]
    elif damage == "batch_order":
        value["routes"][-1], value["routes"][0] = value["routes"][0], value["routes"][-1]
    else:
        value["components"][0]["interval"]["body"] = "forbidden"
    with pytest.raises(ValueError):
        validate_prefix_intent(value, reg)


def test_packed_intent_requires_exact_admitted_prefix_before_bodyfree_prepare():
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval
    from cognitive_castle import lance_layout_codec as layout

    reg, packet, fixed = case()
    reg2, _, parts, envelope = prefix_case(count=3)
    assert reg2 == reg
    for subset in (parts[:2], parts[1:], list(reversed(parts))):
        changed = layout.target(envelope, 0, [p["plan"] for p in subset])
        with pytest.raises(ValueError):
            prepare_prefix_interval(reg, packet, changed)


def test_packed_intent_is_bodyfree_and_retains_complete_native_keys():
    import json
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval
    from cognitive_castle import migration_codec as migration

    reg, packet, fixed = case("native")
    value = prepare_prefix_interval(reg, packet, fixed)
    text = json.dumps(value)
    assert "DESCRIPTOR_SENTINEL" not in text and '"original":"columns"' not in text
    assert [p["interval"]["cursor_after"] for p in value["components"]] == [
        p["export"]["cursor_after"] for p in packet["components"]
    ]
    assert value["counts"]["historical_occurrences"] == 3
    migration.bounded(value)

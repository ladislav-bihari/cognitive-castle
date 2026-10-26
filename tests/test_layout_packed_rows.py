"""Physical row order and owner spans come from one frozen native mutation."""

import copy

import pytest


def case():
    from test_layout_packed_intent import live_case
    from test_layout_authority import authority_case
    from cognitive_castle import bulk_exact_codec as bulk, migration_codec as migration
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval

    reg, packet, target = live_case()
    intent = prepare_prefix_interval(reg, packet, target)
    rows = []
    for part in packet["components"]:
        rows.extend(copy.deepcopy(part["export"]["entries"]))
    for part in packet["components"]:
        owner = part["export"]["finalizations"][0]
        rows.append(bulk.receipt_row(owner, owner["owner"]))
        live = owner["live"]
        rows.append(
            dict(
                id="receipt:" + migration.hashed([live["namespace"], live["key"]]),
                kind="receipt",
                text="",
                provenance="",
                digest=live["digest"],
                receipt=migration._json(live["receipt"]),
            )
        )
    # The native import constructs this actual receipt; only its identity/order
    # are needed in this unit fixture (receipt correctness has separate checks).
    rows.append(
        dict(
            id=intent["routes"][-1]["id"],
            kind="bulk_receipt",
            text="",
            provenance="",
            digest=intent["digest"],
            receipt="native batch proof",
        )
    )
    pin = authority_case()[-1]
    pin.update(version=7, physical_rows=40, fragments=3)
    mutation = dict(
        kind="append",
        intent_id=intent["intent_id"],
        before=pin,
        after=dict(version=8, physical_rows=40 + len(rows), fragments=4),
        new_ids=[r["id"] for r in intent["routes"]],
    )
    return intent, mutation, rows


def test_packed_rows_keep_original_bytes_in_registered_physical_order():
    from cognitive_castle.lance_layout_packed_rows import ordered_rows, owner_spans

    intent, mutation, rows = case()
    saved = copy.deepcopy(rows)
    result = ordered_rows(intent, mutation, rows)
    assert rows == saved and {r["id"]: r for r in result} == {r["id"]: r for r in rows}
    assert [r["id"] for r in result] == mutation["new_ids"]
    spans = owner_spans(intent, mutation)
    offset = mutation["before"]["physical_rows"]
    for owner in intent["owners"]:
        size = sum(r["gate"] == owner["owner"] for r in intent["routes"][:-1])
        assert spans[owner["owner"]] == dict(
            first_row=offset, next_row=offset + size, first_fragment=3, next_fragment=4
        )
        offset += size
    assert offset == mutation["after"]["physical_rows"] - 1
    assert spans[intent["owners"][0]["owner"]]["next_row"] < mutation["after"]["physical_rows"] - 1


def test_reused_published_records_do_not_create_phantom_new_span_rows():
    from cognitive_castle.lance_layout_packed_rows import ordered_rows, owner_spans

    intent, mutation, rows = case()
    omitted = {r["id"] for r in intent["routes"] if r["kind"] == "bulk_record"}
    mutation["new_ids"] = [i for i in mutation["new_ids"] if i not in omitted]
    mutation["after"]["physical_rows"] -= len(omitted)
    result = ordered_rows(intent, mutation, [r for r in rows if r["id"] not in omitted])
    assert len(result) == len(mutation["new_ids"])
    spans = owner_spans(intent, mutation)
    assert sum(s["next_row"] - s["first_row"] for s in spans.values()) == len(result) - 1


@pytest.mark.parametrize(
    "damage",
    [
        "foreign_intent",
        "before_partition",
        "before_generation",
        "version",
        "rows",
        "fragments",
        "duplicate",
        "foreign_id",
        "reorder",
        "batch_middle",
        "missing_page",
        "missing_owner",
        "missing_live",
    ],
)
def test_packed_mutation_never_invents_or_interleaves_owner_ranges(damage):
    from cognitive_castle.lance_layout_packed_rows import owner_spans

    intent, mutation, _ = case()
    if damage == "foreign_intent":
        mutation["intent_id"] = "f" * 64
    elif damage == "before_partition":
        mutation["before"]["partition_id"] += 1
    elif damage == "before_generation":
        mutation["before"]["generation"]["inode"] += 1
    elif damage in {"version", "fragments", "rows"}:
        mutation["after"][
            {"version": "version", "fragments": "fragments", "rows": "physical_rows"}[damage]
        ] += 1
    elif damage == "duplicate":
        mutation["new_ids"][1] = mutation["new_ids"][0]
    elif damage == "foreign_id":
        mutation["new_ids"][1] = "receipt:" + "f" * 64
    elif damage == "reorder":
        mutation["new_ids"][0], mutation["new_ids"][-2] = (
            mutation["new_ids"][-2],
            mutation["new_ids"][0],
        )
    elif damage == "batch_middle":
        mutation["new_ids"][-1], mutation["new_ids"][1] = (
            mutation["new_ids"][1],
            mutation["new_ids"][-1],
        )
    else:
        kind = {
            "missing_page": "bulk_page",
            "missing_owner": "bulk_receipt",
            "missing_live": "receipt",
        }[damage]
        victim = next(r["id"] for r in intent["routes"] if r["kind"] == kind)
        mutation["new_ids"].remove(victim)
        mutation["after"]["physical_rows"] -= 1
    with pytest.raises(ValueError):
        owner_spans(intent, mutation)


@pytest.mark.parametrize("damage", ["unknown", "duplicate", "hash", "kind"])
def test_actual_rows_must_equal_every_reserved_original_candidate(damage):
    from cognitive_castle.lance_layout_packed_rows import ordered_rows

    intent, mutation, rows = case()
    if damage == "unknown":
        rows[0]["id"] = "memory:" + "f" * 64
    elif damage == "duplicate":
        rows[1] = copy.deepcopy(rows[0])
    elif damage == "hash":
        rows[0]["text"] += "altered"
    else:
        rows[0]["kind"] = "receipt"
    with pytest.raises(ValueError):
        ordered_rows(intent, mutation, rows)

"""Pure association of packed candidates with one registered append's row order.

These checks confer no authority: callers must validate the qualified partition
pin, published reuse, actual native geometry and receipts independently.
"""

from . import bulk_exact_codec as bulk
from . import lance_layout_codec as layout
from . import migration_codec as migration
from .lance_layout_packed import validate_prefix_intent


def _selected(intent, mutation):
    reg = migration.registration(intent["window"]["descriptor"], intent["destination"])
    validate_prefix_intent(intent, reg)
    layout._fields(mutation, {"kind", "intent_id", "before", "after", "new_ids"}, "packed mutation")
    if mutation["kind"] != "append" or mutation["intent_id"] != intent["intent_id"]:
        raise ValueError("packed append identity")
    before, after = mutation["before"], mutation["after"]
    if not isinstance(before, dict):
        raise ValueError("packed before pin")
    if (
        before.get("generation") != intent["target"]["generation"]
        or bulk.integer(before.get("partition_id")) != intent["target"]["partition_id"]
    ):
        raise ValueError("packed partition binding")
    layout._fields(after, {"version", "physical_rows", "fragments"}, "packed after pin")
    for key in after:
        bulk.integer(before.get(key))
        bulk.integer(after[key])
    ids = mutation["new_ids"]
    if not isinstance(ids, list) or not 1 <= len(ids) <= layout.MAX_ROUTES:
        raise ValueError("packed new ID bound")
    if any(not isinstance(i, str) for i in ids) or len(set(ids)) != len(ids):
        raise ValueError("packed new ID identity")
    wanted = set(ids)
    routes = intent["routes"]
    selected = [r for r in routes if r["id"] in wanted]
    if (
        [r["id"] for r in selected] != ids
        or ids[-1] != routes[-1]["id"]
        or any(r["id"] not in wanted and r["kind"] != "bulk_record" for r in routes)
    ):
        raise ValueError("packed complete ordered candidates")
    if after != dict(
        version=before["version"] + 1,
        physical_rows=before["physical_rows"] + len(ids),
        fragments=before["fragments"] + 1,
    ):
        raise ValueError("packed append geometry")
    return selected


def owner_spans(intent, mutation):
    """Return new physical owner ranges, excluding the global batch receipt."""
    selected = _selected(intent, mutation)
    before, after = mutation["before"], mutation["after"]
    offset = before["physical_rows"]
    result = {}
    for owner in intent["owners"]:
        count = sum(r["gate"] == owner["owner"] for r in selected[:-1])
        if not count:
            raise ValueError("packed empty owner span")
        result[owner["owner"]] = dict(
            first_row=offset,
            next_row=offset + count,
            first_fragment=before["fragments"],
            next_fragment=after["fragments"],
        )
        offset += count
    if offset != after["physical_rows"] - 1:
        raise ValueError("packed owner span coverage")
    return result


def ordered_rows(intent, mutation, rows):
    """Validate original row bytes and return their registered physical order."""
    selected = _selected(intent, mutation)
    if not isinstance(rows, list) or len(rows) != len(selected):
        raise ValueError("packed physical rows")
    by_id = {}
    for row in rows:
        layout._fields(row, {"id", "kind", "text", "provenance", "digest", "receipt"}, "packed row")
        if any(not isinstance(v, str) for v in row.values()) or row["id"] in by_id:
            raise ValueError("packed unique string rows")
        by_id[row["id"]] = row
    if set(by_id) != set(mutation["new_ids"]):
        raise ValueError("packed physical candidate set")
    result = []
    for candidate in selected:
        row = by_id[candidate["id"]]
        if row["kind"] != candidate["kind"] or (
            candidate["row_hash"] is not None and migration.hashed(row) != candidate["row_hash"]
        ):
            raise ValueError("packed original row association")
        result.append(row)
    return result

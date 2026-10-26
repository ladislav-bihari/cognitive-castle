"""Body-free whole-owner prefix intents; not yet accepted by queue or facade.

Original per-owner intervals and source cursors stay in their original domains.
Association hashes are not independent source authentication or publication proof.
"""

from copy import deepcopy
import json

from . import bulk_exact_codec as bulk
from . import lance_layout_codec as layout
from . import migration_codec as migration
from .canonical_records import _hash, _json
from .lance_layout_packing import MAX_OWNERS, validate_source_prefix

INTENT = "lance-layout-packed-interval-v1"
SOURCE_WINDOW = "lance-layout-prefix-source-v1"
_FIELDS = {
    "format",
    "packing",
    "job_id",
    "source_binding",
    "destination",
    "intent_id",
    "cursor_before",
    "cursor_after",
    "digest",
    "entries",
    "records",
    "owners",
    "counts",
    "coverage",
    "phase",
    "registration_format",
    "target",
    "window",
    "routes",
    "components",
    "prefix_hash",
}


def _window(reg, before, fixed):
    return dict(
        format=SOURCE_WINDOW,
        action="export",
        descriptor=reg["descriptor"],
        cursor=before,
        allowed_plan_hashes=fixed["owner_plan_hashes"],
        packing=layout.PACKING,
    )


def interval_id(reg, before, fixed, window, prefix_hash):
    return migration.hashed(
        [
            INTENT,
            reg["format"],
            reg["job_id"],
            reg["source_binding"],
            reg["destination"],
            layout.PACKING,
            before,
            fixed,
            window,
            prefix_hash,
        ]
    )


def _aggregate(components):
    originals = [part["interval"] for part in components]
    return dict(
        cursor_before=originals[0]["cursor_before"],
        cursor_after=originals[-1]["cursor_after"],
        entries=sum(p["entries"] for p in originals),
        records=sum(p["records"] for p in originals),
        owners=[p["owners"][0] for p in originals],
        counts={key: sum(p["counts"][key] for p in originals) for key in migration.empty_counts()},
        coverage="incomplete"
        if any(p["coverage"] == "incomplete" for p in originals)
        else "indexed-only",
        phase=originals[0]["phase"],
    )


def _native_boundary(reg, interval):
    if reg["format"] != migration.NATIVE_FORMAT:
        return
    # The same original full-export next-row rules as migration.validate_export.
    # Body-free migration intervals alone do not prove this relative transition.
    previous, following = (
        json.loads(interval[k]["key"][0]) for k in ("cursor_before", "cursor_after")
    )
    if (
        following["fragment"] < previous["fragment"]
        or following["offset"]
        != (previous["offset"] + 1 if following["fragment"] == previous["fragment"] else 1)
        or (
            following["fragment"] == previous["fragment"]
            and following["row_address"] <= previous.get("row_address", -1)
        )
    ):
        raise ValueError("packed original native boundary")


def _routes(value):
    routes = value["routes"]
    if not isinstance(routes, list) or not 1 <= len(routes) <= layout.MAX_ROUTES:
        raise ValueError("packed route bound")
    seen = set()
    for route in routes:
        layout._fields(route, layout.CANDIDATE_FIELDS, "packed candidate")
        if not isinstance(route["kind"], str) or route["kind"] not in layout.KINDS:
            raise ValueError("packed route kind")
        bulk.identity(route["id"], "memory" if route["kind"] == "bulk_record" else "receipt")
        bulk.identity(route["gate"])
        if route["id"] in seen:
            raise ValueError("duplicate packed route")
        seen.add(route["id"])
        if route["row_hash"] is not None:
            bulk.sha(route["row_hash"])
    offset = 0
    for part in value["components"]:
        original = part["interval"]
        owner = original["owners"][0]
        count = original["entries"]
        live = owner["classification"] == "live"
        size = count + 1 + int(live)
        group = routes[offset : offset + size]
        if len(group) != size or any(
            r["gate"] != owner["owner"] or r["row_hash"] is None for r in group
        ):
            raise ValueError("packed contiguous owner gate")
        entries = group[:count]
        if (
            any(r["kind"] not in {"bulk_record", "bulk_page"} for r in entries)
            or sum(r["kind"] == "bulk_record" for r in entries) != original["records"]
        ):
            raise ValueError("packed original entry cardinality")
        if group[count]["id"] != owner["owner"] or group[count]["kind"] != "bulk_receipt":
            raise ValueError("packed original owner marker")
        if live and (
            group[-1]["id"] != "receipt:" + migration.hashed(owner["source_key"])
            or group[-1]["kind"] != "receipt"
        ):
            raise ValueError("packed original live receipt")
        offset += size
    expected_batch = dict(
        id=layout.batch_receipt_id(value),
        kind="bulk_receipt",
        row_hash=None,
        gate=value["owners"][0]["owner"],
    )
    if offset + 1 != len(routes) or routes[-1] != expected_batch:
        raise ValueError("packed global batch route")


def validate_prefix_intent(value, reg):
    migration.validate_registration(reg)
    layout._fields(value, _FIELDS, "packed intent")
    if (
        value["format"] != INTENT
        or value["packing"] != layout.PACKING
        or value["registration_format"] != reg["format"]
        or any(value[key] != reg[key] for key in ("job_id", "source_binding", "destination"))
    ):
        raise ValueError("packed intent binding")
    migration.cursor(value["cursor_before"], reg)
    migration.cursor(value["cursor_after"], reg)
    if not isinstance(value["owners"], list):
        raise ValueError("packed original owners")
    for owner in value["owners"]:
        migration.validate_owner_reference(owner, reg)
    bulk.sha(value["prefix_hash"])
    bulk.sha(value["digest"])
    parts = value["components"]
    if not isinstance(parts, list) or not 1 <= len(parts) <= MAX_OWNERS:
        raise ValueError("packed component bound")
    cursor = value["cursor_before"]
    for part in parts:
        layout._fields(part, {"plan_hash", "interval"}, "packed original component")
        bulk.sha(part["plan_hash"])
        original = migration.validate_interval(part["interval"], reg)
        if (
            original["cursor_before"] != cursor
            or original["cursor_before"]["owner"] is not None
            or original["cursor_after"]["owner"] is not None
            or len(original["owners"]) != 1
            or original["phase"] != value["phase"]
        ):
            raise ValueError("packed original cursor chain")
        owner = original["owners"][0]
        if (
            original["entries"]
            != sum(field["pages"] for field in owner["fields"].values()) + 2 * owner["members"]
            or original["records"] != owner["members"]
        ):
            raise ValueError("packed complete original owner")
        _native_boundary(reg, original)
        cursor = original["cursor_after"]
    if any(value[key] != expected for key, expected in _aggregate(parts).items()):
        raise ValueError("packed original aggregate")
    for key in ("entries", "records"):
        bulk.integer(value[key])
    migration.counts(value["counts"])
    if value["entries"] + len(parts) > bulk.MAX_ENTRIES:
        raise ValueError("packed whole frame entries")
    fixed = layout.validate_target(value["target"])
    if (
        fixed["generation"]["path"] != reg["destination"]
        or fixed["owner_plan_hashes"] != [p["plan_hash"] for p in parts]
        or value["window"] != _window(reg, value["cursor_before"], fixed)
        or value["intent_id"]
        != interval_id(reg, value["cursor_before"], fixed, value["window"], value["prefix_hash"])
    ):
        raise ValueError("packed admitted source binding")
    _routes(value)
    return migration.bounded(value)


def prepare_prefix_interval(reg, packet, fixed):
    migration.validate_registration(reg)
    validate_source_prefix(packet, reg, packet["cursor_before"])
    layout.validate_target(fixed)
    parts = packet["components"]
    if fixed["generation"]["path"] != reg["destination"] or fixed["owner_plan_hashes"] != [
        migration.hashed(p["plan"]) for p in parts
    ]:
        raise ValueError("exact admitted source prefix required")
    components = [
        dict(
            plan_hash=migration.hashed(p["plan"]),
            interval=migration.prepare_interval(reg, p["export"]),
        )
        for p in parts
    ]
    window = _window(reg, packet["cursor_before"], fixed)
    identity = interval_id(reg, packet["cursor_before"], fixed, window, packet["prefix_hash"])
    entries = [r for p in parts for r in p["export"]["entries"]]
    owners = [o for p in parts for o in p["export"]["finalizations"]]
    value = dict(
        format=INTENT,
        packing=layout.PACKING,
        registration_format=reg["format"],
        job_id=reg["job_id"],
        source_binding=reg["source_binding"],
        destination=reg["destination"],
        intent_id=identity,
        digest=bulk.validate_batch(identity, reg["source_binding"], entries, owners),
        components=components,
        prefix_hash=packet["prefix_hash"],
        target=fixed,
        window=window,
        **_aggregate(components),
    )
    routes = []
    for part in parts:
        exported = part["export"]
        owner = exported["finalizations"][0]
        for row in exported["entries"]:
            gate = (
                row["digest"]
                if row["kind"] == "bulk_record"
                else bulk.decode(row["receipt"], 2048)["owner"]
            )
            if gate != owner["owner"]:
                raise ValueError("packed original entry gate")
            routes.append(layout._candidate(row, gate))
        routes.append(layout._candidate(bulk.receipt_row(owner, owner["owner"]), owner["owner"]))
        if owner["live"] is not None:
            live = owner["live"]
            row = dict(
                id="receipt:" + _hash(_json([live["namespace"], live["key"]])),
                kind="receipt",
                text="",
                provenance="",
                digest=live["digest"],
                receipt=_json(live["receipt"]),
            )
            routes.append(layout._candidate(row, owner["owner"]))
    routes.append(
        dict(
            id=layout.batch_receipt_id(value),
            kind="bulk_receipt",
            row_hash=None,
            gate=owners[0]["owner"],
        )
    )
    value["routes"] = routes
    return validate_prefix_intent(deepcopy(value), reg)

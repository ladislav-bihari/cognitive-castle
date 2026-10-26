"""Bounded transient prefixes of unchanged original source-owner exports.

These packets are source evidence, never placement or publication authority.
No database, engine or persistent proof is used here.
"""

from copy import deepcopy
from itertools import islice

from . import bulk_exact_codec as bulk
from . import lance_layout_codec as layout
from . import migration_codec as migration

PREFIX = "lance-layout-source-prefix-v1"
MAX_OWNERS = 8
_FIELDS = {
    "format",
    "source_binding",
    "cursor_before",
    "cursor_after",
    "components",
    "prefix_hash",
    "frame_bytes",
}
_SIZE_ERRORS = {"bulk entries", "bulk encoded bytes", "bulk decoded bytes", "export frame limit"}


def _component(value, reg, before):
    layout._fields(value, {"plan", "export"}, "prefix component")
    plan = layout.validate_owner_plan(value["plan"], reg)
    exported = migration.validate_export(value["export"], reg, before)
    if exported["status"] != "ok" or exported["complete"]:
        raise ValueError("prefix owner content")
    if exported["phase"] != plan["source_phase"]:
        raise ValueError("prefix source phase")
    if not exported["finalizations"]:
        if exported["cursor_after"]["owner"] != plan["owner_key"]:
            raise ValueError("prefix staged owner")
        return False
    if before["owner"] is not None:
        return False
    reference = migration.owner_reference(
        exported["finalizations"][0], exported["phase"], exported["cursor_after"]["key"]
    )
    if reference != {key: plan[key] for key in migration.OWNER_FIELDS}:
        raise ValueError("prefix original owner plan")
    return True


def _identities(component):
    exported = component["export"]
    ids = {row["id"] for row in exported["entries"]}
    for owner in exported["finalizations"]:
        ids.add(owner["owner"])
        if owner["live"] is not None:
            live = owner["live"]
            ids.add("receipt:" + migration.hashed([live["namespace"], live["key"]]))
    return ids


def _packet(reg, before, components):
    entries = [row for part in components for row in part["export"]["entries"]]
    owners = [owner for part in components for owner in part["export"]["finalizations"]]
    # Maximum-width interval identity budgets the eventual request, not a short
    # prototype key that would allow a larger last frame.
    bulk.validate_batch("f" * 64, reg["source_binding"], entries, owners)
    evidence = dict(
        format=PREFIX,
        source_binding=reg["source_binding"],
        cursor_before=deepcopy(before),
        cursor_after=deepcopy(components[-1]["export"]["cursor_after"]),
        components=deepcopy(components),
    )
    result = dict(evidence, prefix_hash=migration.hashed(evidence), frame_bytes=0)
    for _ in range(3):
        result["frame_bytes"] = len(migration.frame_bytes(result))
    # Budget the complete fixed source-worker success envelope as well as its
    # inner packet; a nearly full packet cannot consume the wrapper's bytes.
    migration.frame_bytes(dict(status="ok", reason=None, prefix=result))
    return result


def pack_source_prefix(reg, before, components):
    """Take only a deterministic whole-owner prefix; retain every original cursor."""
    migration.validate_registration(reg)
    migration.cursor(before, reg)
    selected, identities = [], set()
    cursor, phase, packet = before, None, None
    for component in islice(components, MAX_OWNERS):
        whole = _component(component, reg, cursor)
        if not whole:
            if not selected:
                raise ValueError("whole owner prefix required")
            break
        exported = component["export"]
        if selected and exported["phase"] != phase:
            break
        current_ids = _identities(component)
        if identities & current_ids:
            break
        try:
            prospective = _packet(reg, before, [*selected, component])
        except ValueError as exc:
            if str(exc) not in _SIZE_ERRORS or not selected:
                raise
            break
        selected.append(component)
        identities.update(current_ids)
        cursor, phase, packet = exported["cursor_after"], exported["phase"], prospective
    if packet is None:
        raise ValueError("whole owner prefix required")
    return packet


def validate_source_prefix(value, reg, before):
    layout._fields(value, _FIELDS, "source prefix fields")
    migration.validate_registration(reg)
    migration.frame_bytes(value)
    migration.cursor(value["cursor_before"], reg)
    migration.cursor(value["cursor_after"], reg)
    bulk.sha(value["prefix_hash"])
    bulk.integer(value["frame_bytes"])
    components = value["components"]
    if (
        value["format"] != PREFIX
        or value["source_binding"] != reg["source_binding"]
        or value["cursor_before"] != before
        or not isinstance(components, list)
        or not 1 <= len(components) <= MAX_OWNERS
    ):
        raise ValueError("source prefix binding")
    if pack_source_prefix(reg, before, components) != value:
        raise ValueError("source prefix evidence")
    return value


def admit_source_prefix(value, reg, fixed_target):
    """Select only the ordered initial prefix named by an actual placement result."""
    validate_source_prefix(value, reg, value["cursor_before"])
    layout.validate_target(fixed_target)
    hashes = fixed_target["owner_plan_hashes"]
    components = value["components"]
    if (
        fixed_target["generation"]["path"] != reg["destination"]
        or len(hashes) > len(components)
        or hashes != [migration.hashed(part["plan"]) for part in components[: len(hashes)]]
    ):
        raise ValueError("admitted original source prefix")
    return _packet(reg, value["cursor_before"], components[: len(hashes)])

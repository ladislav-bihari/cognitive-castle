"""Bounded transient prefixes of unchanged original source-owner exports.

These packets are source evidence, never placement or publication authority.
No database, engine or persistent proof is used here.
"""

from copy import deepcopy
from itertools import islice

from . import bulk_exact_codec as bulk
from . import lance_layout_codec as layout
from . import migration_codec as migration
from .canonical_records import _string

PREFIX = "lance-layout-source-prefix-v1"
MAX_OWNERS = 8
_REQUEST_FIELDS = {
    "format",
    "action",
    "generation",
    "qualification_hash",
    "allowed_plan_hashes",
    "packing",
}
_REPLY_FIELDS = {
    "format",
    "status",
    "reason",
    "action",
    "source_binding",
    "cursor_before",
    "cursor_after",
    "prefix",
    "fallback_plan",
    "terminal",
}
_FIELDS = {
    "format",
    "source_binding",
    "cursor_before",
    "cursor_after",
    "components",
    "prefix_hash",
    "frame_bytes",
}
_SIZE_ERRORS = {
    "bulk entries",
    "bulk encoded bytes",
    "bulk decoded bytes",
    "export frame limit",
    "grouped owner result bound",
}


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
    maximum = 2**63 - 1
    layout._grouped_reply_bound(
        dict(
            status="ok",
            format=layout.GROUPED_OWNER_PROOF,
            intent_hash="f" * 64,
            owners=[layout.owner_observation(owner, maximum, maximum) for owner in owners],
        )
    )
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
    # Both real worker replies must fit, including potentially long original
    # before/after keys. The packet alone is not the wire envelope.
    for action in ("prepare", "export"):
        migration.frame_bytes(_prefix_envelope(reg, before, action, prefix=result))
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


def validate_prefix_request(value, descriptor, before):
    layout._fields(value, _REQUEST_FIELDS, "source prefix request")
    if value["format"] != layout.PREFIX_SOURCE_REQUEST or value["action"] not in {
        "prepare",
        "export",
    }:
        raise ValueError("source prefix request version")
    # Reuse all original generation/qualification/registration/cursor checks.
    # The legacy plan format remains independent of this explicit new version.
    original = dict(value, format=layout.SOURCE_REQUEST, action="plan", allowed_plan_hashes=[])
    layout.validate_layout_source_window(original, descriptor, before)
    hashes = value["allowed_plan_hashes"]
    if before["owner"] is not None:
        raise ValueError("whole owner prefix request")
    if not isinstance(hashes, list) or (
        len(hashes) != 0 if value["action"] == "prepare" else not 1 <= len(hashes) <= MAX_OWNERS
    ):
        raise ValueError("source prefix selection bound")
    for digest in hashes:
        bulk.sha(digest)
    if len(set(hashes)) != len(hashes):
        raise ValueError("duplicate source prefix plan")
    migration.frame_bytes(dict(descriptor=descriptor, cursor=before, layout=value))
    return value


def _prefix_envelope(
    reg, before, action, *, prefix=None, fallback_plan=None, terminal=None, status="ok", reason=None
):
    after = (
        prefix["cursor_after"]
        if status == "ok" and action == "export" and prefix is not None
        else before
    )
    return dict(
        format=layout.PREFIX_SOURCE_RESULT,
        status=status,
        reason=reason,
        action=action,
        source_binding=reg["source_binding"],
        cursor_before=deepcopy(before),
        cursor_after=deepcopy(after),
        prefix=deepcopy(prefix),
        fallback_plan=deepcopy(fallback_plan),
        terminal=deepcopy(terminal),
    )


def prefix_result(
    descriptor,
    before,
    window,
    *,
    prefix=None,
    fallback_plan=None,
    terminal=None,
    status="ok",
    reason=None,
):
    validate_prefix_request(window, descriptor, before)
    reg = migration.registration(descriptor, window["generation"]["path"])
    result = _prefix_envelope(
        reg,
        before,
        window["action"],
        prefix=prefix,
        fallback_plan=fallback_plan,
        terminal=terminal,
        status=status,
        reason=reason,
    )
    return validate_prefix_result(result, window, descriptor, before)


def validate_prefix_result(value, window, descriptor, before):
    validate_prefix_request(window, descriptor, before)
    layout._fields(value, _REPLY_FIELDS, "source prefix reply")
    migration.frame_bytes(value)
    reg = migration.registration(descriptor, window["generation"]["path"])
    _string(value["status"], 64, "source prefix status")
    if value["reason"] is not None:
        _string(value["reason"], 1024, "source prefix reason")
    migration.cursor(value["cursor_before"], reg)
    migration.cursor(value["cursor_after"], reg)
    if (
        value["format"] != layout.PREFIX_SOURCE_RESULT
        or value["action"] != window["action"]
        or value["source_binding"] != reg["source_binding"]
        or value["cursor_before"] != before
    ):
        raise ValueError("source prefix reply binding")
    payloads = [value[name] for name in ("prefix", "fallback_plan", "terminal")]
    if value["status"] != "ok":
        if any(item is not None for item in payloads) or value["cursor_after"] != before:
            raise ValueError("source prefix refusal progress")
        return value
    if value["reason"] is not None or sum(item is not None for item in payloads) != 1:
        raise ValueError("source prefix success payload")
    packet = value["prefix"]
    if packet is not None:
        validate_source_prefix(packet, reg, before)
        if window["action"] == "export" and window["allowed_plan_hashes"] != [
            migration.hashed(part["plan"]) for part in packet["components"]
        ]:
            raise ValueError("frozen original source prefix")
    elif window["action"] != "prepare":
        raise ValueError("frozen source prefix cannot fall back")
    elif value["fallback_plan"] is not None:
        layout.validate_owner_plan(value["fallback_plan"], reg)
    else:
        terminal = migration.validate_export(value["terminal"], reg, before)
        if terminal["status"] != "ok" or not terminal["complete"]:
            raise ValueError("source prefix terminal evidence")
    after = packet["cursor_after"] if window["action"] == "export" else before
    if value["cursor_after"] != after:
        raise ValueError("source prefix outer cursor")
    return value

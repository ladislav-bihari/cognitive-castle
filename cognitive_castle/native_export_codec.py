"""Pure fixed contracts for an inactive pinned native archive exporter.

Registration is supplied by trusted snapshot qualification. Neither this codec
nor an export cursor certifies source coverage or permits production movement.
"""

from __future__ import annotations

import json
from pathlib import PurePosixPath
import stat

from .bulk_exact_codec import integer, sha
from .canonical_records import _hash, _json, _string
from .sqlite_export_codec import (
    CURSOR_FIELDS,
    IDENTITY_FIELDS,
    PACKING,
    empty_counts as empty_counts,
    file_identity as file_identity,
    frame_bytes,
    layout_result as layout_result,
    record_entry as record_entry,
)

FORMAT = "pinned-native-export-v1"
MAX_MANIFEST = 512 * 1024
MAX_SCHEMA = 64 * 1024
MAX_INVENTORY = 4096


def validate_identity(value, *, directory=False):
    if not isinstance(value, dict) or value.keys() != IDENTITY_FIELDS:
        raise ValueError("native identity")
    for item in value.values():
        integer(item)
    if value["mode"] & 0o077:
        raise ValueError("native private identity")
    if directory:
        if not stat.S_ISDIR(value["mode"]) or value["links"] < 1:
            raise ValueError("native directory identity")
    elif not stat.S_ISREG(value["mode"]) or value["links"] != 1 or value["mode"] & 0o222:
        raise ValueError("native immutable file identity")
    return value


def raw_cursor(value):
    if (
        not isinstance(value, dict)
        or not {"fragment", "offset"} <= value.keys()
        or value.keys() - {"fragment", "offset", "row_address"}
    ):
        raise ValueError("native raw cursor")
    for item in value.values():
        integer(item)
        if item >= 2**63:
            raise ValueError("native raw cursor range")
    if "row_address" in value and value["row_address"] >> 32 != value["fragment"]:
        raise ValueError("native raw address")
    return value


def validate_descriptor(value):
    if (
        not isinstance(value, dict)
        or value.keys()
        != {
            "format",
            "store_id",
            "snapshot_path",
            "snapshot_identity",
            "manifest_path",
            "manifest_identity",
            "manifest_hash",
            "archive_id",
            "original_snapshot_id",
            "version",
            "start_cursor",
        }
        or value["format"] != FORMAT
    ):
        raise ValueError("native source descriptor")
    for name in ("store_id", "archive_id", "original_snapshot_id"):
        _string(value[name], 256, name)
    for name in ("snapshot_path", "manifest_path"):
        path = _string(value[name], 4096, name)
        parsed = PurePosixPath(path)
        if not parsed.is_absolute() or str(parsed) != path or ".." in parsed.parts:
            raise ValueError("native source path")
    if PurePosixPath(value["snapshot_path"]).parent != PurePosixPath(value["manifest_path"]).parent:
        raise ValueError("native manifest location")
    validate_identity(value["snapshot_identity"], directory=True)
    validate_identity(value["manifest_identity"])
    sha(value["manifest_hash"])
    integer(value["version"])
    if not 0 < value["version"] < 2**63:
        raise ValueError("native source version")
    raw_cursor(value["start_cursor"])
    frame_bytes(value)
    return value


def source_binding(descriptor):
    validate_descriptor(descriptor)
    return _hash(_json([FORMAT, "registered-source", descriptor]))


def prepare_layout_request(descriptor, cursor, window):
    from .lance_layout_codec import validate_layout_source_window

    binding = source_binding(descriptor)
    validate_cursor(cursor, binding, descriptor["store_id"], descriptor["start_cursor"])
    validate_layout_source_window(window, descriptor, cursor)
    request = dict(descriptor=descriptor, cursor=cursor, layout=window)
    frame_bytes(request)
    return request


def validate_manifest(value, descriptor):
    """Check qualified snapshot evidence; complete never implies reconciled history."""
    if not isinstance(value, dict) or value.keys() != {
        "format",
        "state",
        "store_id",
        "snapshot_identity",
        "original",
        "start_cursor",
        "schema",
        "fragments",
        "inventory",
        "lineage",
    }:
        raise ValueError("native manifest schema")
    if len(_json(value).encode()) > MAX_MANIFEST:
        raise ValueError("native manifest limit")
    validate_identity(value["snapshot_identity"], directory=True)
    raw_cursor(value["start_cursor"])
    if not isinstance(value["original"], dict) or type(value["original"].get("version")) is not int:
        raise ValueError("native original types")
    for key in ("format", "store_id", "snapshot_identity", "start_cursor"):
        if _json(value[key]) != _json(descriptor[key]):
            raise ValueError("native manifest binding")
    if value["state"] != "complete" or value["original"] != dict(
        archive_id=descriptor["archive_id"],
        snapshot_id=descriptor["original_snapshot_id"],
        version=descriptor["version"],
    ):
        raise ValueError("native original binding")
    schema = value["schema"]
    if not isinstance(schema, dict) or schema.keys() != {"hash", "bytes"}:
        raise ValueError("native schema evidence")
    sha(schema["hash"])
    integer(schema["bytes"])
    if not 1 <= schema["bytes"] <= MAX_SCHEMA:
        raise ValueError("native schema limit")
    inventory, fragments = value["inventory"], value["fragments"]
    if (
        not isinstance(inventory, list)
        or not 1 <= len(inventory) <= MAX_INVENTORY
        or not isinstance(fragments, list)
        or len(fragments) > MAX_INVENTORY
        or not isinstance(value["lineage"], dict)
    ):
        raise ValueError("native manifest evidence")
    previous = None
    for item in inventory:
        if not isinstance(item, dict) or item.keys() != {
            "path",
            "snapshot_identity",
            "content_hash",
        }:
            raise ValueError("native inventory entry")
        name = _string(item["path"], 4096, "native inventory path")
        path = PurePosixPath(name)
        if (
            path.is_absolute()
            or str(path) != name
            or ".." in path.parts
            or name == "."
            or (previous is not None and name <= previous)
        ):
            raise ValueError("native inventory order/path")
        validate_identity(item["snapshot_identity"])
        sha(item["content_hash"])
        previous = name
    previous = -1
    for fragment in fragments:
        if not isinstance(fragment, dict) or "id" not in fragment:
            raise ValueError("native fragment evidence")
        identity = integer(fragment["id"])
        if identity <= previous or identity >= 2**31:
            raise ValueError("native fragment order")
        previous = identity
    return value


def initial_cursor(binding, store_id, start_cursor):
    sha(binding)
    _string(store_id, 256, "native store id")
    raw_cursor(start_cursor)
    return dict(
        format=FORMAT,
        packing=PACKING,
        source_binding=binding,
        store_id=store_id,
        phase="history",
        key=[_json(start_cursor)],
        owner=None,
        entry=0,
        field=None,
        member_ordinal=0,
        field_byte_offset=0,
        field_char_offset=0,
    )


def validate_cursor(value, binding, store_id, start_cursor):
    raw_cursor(start_cursor)
    if not isinstance(value, dict) or value.keys() != CURSOR_FIELDS:
        raise ValueError("native export cursor")
    if (
        value["format"] != FORMAT
        or value["packing"] != PACKING
        or value["source_binding"] != binding
        or value["store_id"] != store_id
        or value["phase"] not in {"history", "done"}
    ):
        raise ValueError("native export cursor binding")
    for name in ("entry", "member_ordinal", "field_byte_offset", "field_char_offset"):
        integer(value[name])
        if value[name] >= 2**63:
            raise ValueError("native export cursor range")
    if value["phase"] == "done":
        if value["key"] is not None or value["owner"] is not None:
            raise ValueError("native done cursor")
    else:
        key = value["key"]
        if not isinstance(key, list) or len(key) != 1:
            raise ValueError("native export key")
        _string(key[0], 512, "native raw key")
        position = raw_cursor(json.loads(key[0]))
        if _json(position) != key[0]:
            raise ValueError("native canonical raw key")
        if (position["fragment"], position["offset"]) < (
            start_cursor["fragment"],
            start_cursor["offset"],
        ):
            raise ValueError("native cursor before registration")
        if (position["fragment"], position["offset"]) == (
            start_cursor["fragment"],
            start_cursor["offset"],
        ) and position != start_cursor:
            raise ValueError("native start cursor representation")
    if value["owner"] is None:
        if value["field"] is not None or any(
            value[name]
            for name in ("entry", "member_ordinal", "field_byte_offset", "field_char_offset")
        ):
            raise ValueError("native unused cursor positions")
    else:
        sha(value["owner"])
        field, member = value["field"], value["member_ordinal"]
        size, chars = value["field_byte_offset"], value["field_char_offset"]
        if not value["entry"] or field not in {
            "metadata",
            "columns",
            "scopes",
            "descriptor",
            "membership",
            "record",
            "finalization",
        }:
            raise ValueError("native active cursor")
        if field in {"metadata", "columns", "scopes", "descriptor"}:
            if member or not chars <= size <= 4 * chars:
                raise ValueError("native field cursor")
        elif field == "finalization":
            if size or chars:
                raise ValueError("native finalization cursor")
        elif size != member or chars != member:
            raise ValueError("native member cursor")
    return value

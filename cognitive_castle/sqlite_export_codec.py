"""Pure private contracts for an inactive protected SQLite exporter.

The descriptor is registered by a trusted operator after snapshot qualification.
This module neither makes nor certifies a backup. Cursors describe replay/export
only; they never authorize a source or destination publication checkpoint.
"""

from __future__ import annotations

import json
from pathlib import PurePosixPath

from .bulk_exact_codec import integer, sha
from .canonical_records import _hash, _json, _string

FORMAT = "protected-sqlite-export-v1"
PACKING = "bulk-v1-ascii-1"
MAX_FRAME = 512 * 1024
PHASES = ("live", "history", "audit", "orphans", "done")
IDENTITY_FIELDS = {"device", "inode", "size", "mtime_ns", "ctime_ns", "mode", "uid", "links"}
CURSOR_FIELDS = {
    "format",
    "packing",
    "source_binding",
    "store_id",
    "phase",
    "key",
    "owner",
    "entry",
    "field",
    "member_ordinal",
    "field_byte_offset",
    "field_char_offset",
}


def empty_counts():
    return dict(
        live_owners=0,
        historical_occurrences=0,
        audit_owners=0,
        members=0,
        gaps=0,
        repaired_gaps=0,
        pending_stages=0,
        orphans=0,
    )


def frame_bytes(value):
    """Bound UTF8 decoded representation and the actual ASCII guardian frame."""
    decoded = _json(value).encode("utf-8")
    wire = json.dumps(value, allow_nan=False).encode("ascii")
    if len(decoded) > MAX_FRAME or len(wire) > MAX_FRAME:
        raise ValueError("export frame limit")
    return wire


def file_identity(info):
    return dict(
        device=info.st_dev,
        inode=info.st_ino,
        size=info.st_size,
        mtime_ns=info.st_mtime_ns,
        ctime_ns=info.st_ctime_ns,
        mode=info.st_mode,
        uid=info.st_uid,
        links=info.st_nlink,
    )


def validate_identity(value):
    if not isinstance(value, dict) or value.keys() != IDENTITY_FIELDS:
        raise ValueError("file identity")
    for item in value.values():
        integer(item)
    if value["links"] != 1:
        raise ValueError("file links")


def validate_descriptor(value):
    if (
        not isinstance(value, dict)
        or value.keys()
        != {
            "format",
            "store_id",
            "snapshot_id",
            "path",
            "identity",
            "manifest_path",
            "manifest_identity",
            "manifest_hash",
        }
        or value["format"] != FORMAT
    ):
        raise ValueError("source descriptor")
    for name in ("store_id", "snapshot_id"):
        _string(value[name], 256, name)
    for name in ("path", "manifest_path"):
        path = _string(value[name], 4096, name)
        if (
            not PurePosixPath(path).is_absolute()
            or str(PurePosixPath(path)) != path
            or ".." in PurePosixPath(path).parts
        ):
            raise ValueError("source path")
    validate_identity(value["identity"])
    validate_identity(value["manifest_identity"])
    sha(value["manifest_hash"])
    frame_bytes(value)
    return value


def source_binding(descriptor):
    validate_descriptor(descriptor)
    return _hash(_json([FORMAT, "registered-source", descriptor]))


def prepare_layout_request(descriptor, cursor, window):
    """Explicit alternate request; v1 descriptor and cursor domains are unchanged."""
    from .lance_layout_codec import validate_layout_source_window

    binding = source_binding(descriptor)
    validate_cursor(cursor, binding, descriptor["store_id"])
    validate_layout_source_window(window, descriptor, cursor)
    request = dict(descriptor=descriptor, cursor=cursor, layout=window)
    frame_bytes(request)
    return request


def layout_result(descriptor, before, window, inner, plans):
    """Wrap an unchanged v1 result; a plan never advances the source cursor."""
    from .lance_layout_codec import SOURCE_RESULT, validate_layout_source_result

    ok = inner["status"] == "ok"
    exported = (
        inner if ok and (window["action"] in {"export", "prepare"} or inner["complete"]) else None
    )
    result = dict(
        format=SOURCE_RESULT,
        status=inner["status"],
        reason=inner["reason"],
        action=window["action"],
        source_binding=before["source_binding"],
        cursor_before=before,
        cursor_after=inner["cursor_after"] if ok and window["action"] == "export" else before,
        plans=plans if ok else [],
        export=exported,
    )
    if window["action"] == "prepare":
        from .migration_codec import hashed

        result["preparation_hash"] = hashed([plans, exported]) if ok and plans else None
    validate_layout_source_result(result, window, descriptor, before)
    frame_bytes(result)
    return result


def record_entry(owner, ordinal, record):
    """The original six-string member row, also sized during owner planning."""
    from . import bulk_exact_codec as bulk

    return bulk.validate_entry(
        dict(
            id=record.id,
            kind="bulk_record",
            text=record.text,
            provenance=record.provenance_json,
            digest=owner,
            receipt=_json(
                dict(
                    format=bulk.FORMAT,
                    page=bulk.page_id(owner, "membership", ordinal),
                    ordinal=ordinal,
                )
            ),
        )
    )


def initial_cursor(binding, store_id):
    sha(binding)
    _string(store_id, 256, "store id")
    return dict(
        format=FORMAT,
        packing=PACKING,
        source_binding=binding,
        store_id=store_id,
        phase="live",
        key=None,
        owner=None,
        entry=0,
        field=None,
        member_ordinal=0,
        field_byte_offset=0,
        field_char_offset=0,
    )


def validate_cursor(value, binding, store_id):
    if not isinstance(value, dict) or value.keys() != CURSOR_FIELDS:
        raise ValueError("export cursor")
    if (
        value["format"] != FORMAT
        or value["packing"] != PACKING
        or value["source_binding"] != binding
        or value["store_id"] != store_id
        or value["phase"] not in PHASES
    ):
        raise ValueError("export cursor binding")
    for name in ("entry", "member_ordinal", "field_byte_offset", "field_char_offset"):
        integer(value[name])
    if value["field"] not in (
        None,
        "metadata",
        "columns",
        "scopes",
        "descriptor",
        "membership",
        "record",
        "finalization",
    ):
        raise ValueError("export cursor field")
    key = value["key"]
    widths = {"live": 2, "history": 1, "audit": 2, "orphans": 1, "done": 0}
    if key is not None:
        if not isinstance(key, list) or len(key) != widths[value["phase"]]:
            raise ValueError("export cursor key")
        for item in key:
            _string(item, 32768, "cursor key")
    if value["owner"] is not None:
        if key is None or value["phase"] in {"orphans", "done"}:
            raise ValueError("export cursor owner")
        _string(value["owner"], 256, "cursor owner")
    if value["owner"] is None and any(
        value[name]
        for name in ("entry", "member_ordinal", "field_byte_offset", "field_char_offset")
    ):
        raise ValueError("export cursor unused offsets")
    if value["owner"] is None and value["field"] is not None:
        raise ValueError("export cursor unused field")
    if value["phase"] == "done" and key is not None:
        raise ValueError("export cursor final key")
    return value

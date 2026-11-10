"""Closed body-free authority/checkpoint codecs, without SQLite or native engines."""

import json
from pathlib import PurePosixPath

from .canonical_records import _hash, _json, _string
from .capture_recovery_codec import _sha
from .memory_accounting import COUNTERS

FORMAT = "castle-capture-control-v1"
MIN_BYTES = 393216
MAX_BYTES = 4 * 1024**3


def _number(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError("control metadata number")
    return value


def _path(value, *, canonical=False):
    value = _string(value, 2048, "control path")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or canonical and str(path) != value:
        raise ValueError("control path")
    return value


def validate_authority(value, *, expected_hash):
    _sha(expected_hash)
    if (
        not isinstance(value, dict)
        or value.keys() != {"format", "directory", "database", "max_bytes"}
        or value["format"] != FORMAT
    ):
        raise ValueError("capture control authority")
    if type(value["max_bytes"]) is not int or not MIN_BYTES <= value["max_bytes"] <= MAX_BYTES:
        raise ValueError("capture control budget")
    for name in ("directory", "database"):
        identity = value[name]
        expected = {"path", "device", "inode"} if name == "directory" else {"device", "inode"}
        if not isinstance(identity, dict) or identity.keys() != expected:
            raise ValueError("capture control identity")
        _number(identity["device"])
        if _number(identity["inode"]) == 0:
            raise ValueError("capture control inode")
    _path(value["directory"]["path"], canonical=True)
    raw = _string(_json(value), 8192, "capture control authority")
    if _hash(raw) != expected_hash:
        raise ValueError("capture control authority hash")
    return json.loads(raw)


def validate_checkpoint(value):
    required = {"generation", "offset", "identity", "gaps", "unsupported", "skipping", "anchor"}
    optional = {
        "accounting",
        "accounting_start",
        "observed_size",
        "observed_mtime_ns",
        "skip_start",
        "pending",
        "workspace",
        "workspace_first_offset",
        "workspace_conflict_at",
        "workspace_conflict",
    }
    if (
        not isinstance(value, dict)
        or not required <= value.keys()
        or value.keys() - required - optional
    ):
        raise ValueError("capture checkpoint fields")
    for name in {
        "generation",
        "offset",
        "gaps",
        "unsupported",
        "accounting_start",
        "observed_size",
        "observed_mtime_ns",
        "skip_start",
        "workspace_first_offset",
        "workspace_conflict_at",
    }.intersection(value):
        _number(value[name])
    if type(value["skipping"]) is not bool:
        raise ValueError("capture checkpoint skipping")
    identity = value["identity"]
    if not isinstance(identity, list) or len(identity) != 3:
        raise ValueError("capture source identity")
    for item in identity:
        _number(item)
    _sha(value["anchor"])
    for name in (
        "accounting_start",
        "skip_start",
        "workspace_first_offset",
        "workspace_conflict_at",
    ):
        if name in value and value[name] > value["offset"]:
            raise ValueError("capture checkpoint position")
    if "observed_size" in value and value["offset"] > value["observed_size"]:
        raise ValueError("capture checkpoint offset")
    if "workspace" in value:
        _path(value["workspace"])
    if "workspace_conflict" in value and type(value["workspace_conflict"]) is not bool:
        raise ValueError("capture workspace conflict")
    if "accounting" in value:
        counters = value["accounting"]
        if not isinstance(counters, dict) or counters.keys() - set(COUNTERS):
            raise ValueError("capture accounting fields")
        for count in counters.values():
            _number(count)
    pending = value.get("pending")
    if pending is not None:
        if not isinstance(pending, dict) or pending.keys() != {"hash", "cursor"}:
            raise ValueError("capture checkpoint pending")
        _sha(pending["hash"])
        cursor = pending["cursor"]
        if not isinstance(cursor, list) or len(cursor) != 2:
            raise ValueError("capture checkpoint cursor")
        for item in cursor:
            _number(item)
    return json.loads(_string(_json(value), 8192, "capture checkpoint"))

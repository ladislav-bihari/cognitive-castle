"""Read-only trusted selection metadata; no activation, DB open or model load.

Absence means no cutover has been selected. An existing invalid selection raises
and must never be interpreted as permission to fall back to an older backend.
Metadata pins do not establish native health, corpus coverage or model quality.
"""

import os
from pathlib import Path
import stat
from .bounded_json import strict_loads
from .canonical_records import _hash, _json, _string
from .capture_state_codec import validate_authority, _path
from . import lance_layout_codec as layout
from .lance_layout_directory import load_layout_qualification

NAME = "canonical-selection.json"
MAX_BYTES = 16384


class SelectionUnavailable(ValueError):
    pass


def _parents(path):
    if len(path.parts) > 64 or not path.is_absolute() or path.resolve() != path:
        raise ValueError("selection path")
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("selection symlink")


def _directory(path):
    _parents(path)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("selection private directory")
    return info


def _identity(info):
    return tuple(
        getattr(info, "st_" + name)
        for name in ("dev", "ino", "size", "mtime_ns", "ctime_ns", "mode", "uid", "nlink")
    )


def _file(path, *, immutable=False, missing=False):
    _parents(path)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if missing:
            return None
        raise
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_mode & 0o077
            or before.st_nlink != 1
            or not 1 <= before.st_size <= MAX_BYTES
            or immutable
            and stat.S_IMODE(before.st_mode) != 0o400
        ):
            raise ValueError("selection private file")
        raw = stream.read(MAX_BYTES + 1)
        if len(raw) != before.st_size or _identity(before) != _identity(os.fstat(stream.fileno())):
            raise ValueError("selection file changed")
    if _identity(before) != _identity(path.lstat()):
        raise ValueError("selection path changed")
    return raw


def _validate(value):
    if (
        not isinstance(value, dict)
        or value.keys()
        != {
            "format",
            "backend",
            "envelope",
            "control_authority",
            "control_hash",
            "profile",
            "wing",
            "room",
        }
        or value["format"] != "castle-canonical-selection-v1"
        or value["backend"] != "lance"
    ):
        raise ValueError("canonical selection format")
    layout.validate_envelope(value["envelope"])
    validate_authority(value["control_authority"], expected_hash=value["control_hash"])
    profile = value["profile"]
    if not isinstance(profile, dict) or profile.keys() != {"path", "hash"}:
        raise ValueError("selection profile")
    _path(profile["path"], canonical=True)
    layout.bulk.sha(profile["hash"])
    for name in ("wing", "room"):
        _string(value[name], 512, name)


def load_selection(palace):
    """Load only bounded immutable metadata from the fixed trusted palace path."""
    try:
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            raise ValueError("private selection unsupported")
        palace = Path(palace).absolute()
        _parents(palace)
        if not palace.is_dir():
            raise ValueError("selection palace unavailable")
        directory = palace / ".memory"
        if not directory.exists() and not directory.is_symlink():
            return None
        _directory(directory)
        marker = directory / NAME
        raw = _file(marker, immutable=True, missing=True)
        if raw is None:
            return None
        value = strict_loads(raw, max_bytes=MAX_BYTES)
        _validate(value)
        if _json(value).encode() != raw:
            raise ValueError("selection canonical encoding")
        envelope = value["envelope"]
        load_layout_qualification(envelope["generation"], envelope["qualification_hash"])
        authority = value["control_authority"]
        root = Path(authority["directory"]["path"])
        info = _directory(root)
        if (info.st_dev, info.st_ino) != (
            authority["directory"]["device"],
            authority["directory"]["inode"],
        ):
            raise ValueError("selected control root changed")
        database = root / "control.sqlite3"
        _parents(database)
        info = database.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or info.st_nlink != 1
            or (info.st_dev, info.st_ino)
            != (authority["database"]["device"], authority["database"]["inode"])
        ):
            raise ValueError("selected control database changed")
        control = strict_loads(
            _file(root / "control-authority.json", immutable=True), max_bytes=MAX_BYTES
        )
        if validate_authority(control, expected_hash=value["control_hash"]) != authority:
            raise ValueError("selected control authority changed")
        # Verify the small manifest only; weight hashing belongs in the worker.
        if _hash(_file(Path(value["profile"]["path"])).decode("utf-8")) != value["profile"]["hash"]:
            raise ValueError("selected profile changed")
        if _file(marker, immutable=True) != raw:
            raise ValueError("selection changed during load")
        return value
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise SelectionUnavailable("canonical selection unavailable") from None

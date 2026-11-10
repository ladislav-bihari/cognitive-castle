"""Read-only trusted selection metadata; no activation, DB open or model load.

Absence means no cutover has been selected. An existing invalid selection raises
and must never be interpreted as permission to fall back to an older backend.
Metadata pins do not establish native health, corpus coverage or model quality.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import stat
from .bounded_json import strict_loads
from .canonical_records import _hash, _json, _string
from .capture_state_codec import validate_authority, _path
from . import lance_layout_codec as layout
from .lance_layout_directory import load_layout_qualification

NAME = "canonical-selection.json"
BACKEND = "canonical-backend.json"
MAX_BYTES = 16384


class SelectionUnavailable(ValueError):
    pass


class SelectionBusy(SelectionUnavailable):
    """An active canonical operation or cooperating selector owns the lease."""


@contextmanager
def selection_lease(palace, expected, *, exclusive=False):
    """Pin the existing private metadata directory without creating lock files.

    Operations hold a shared lease; a cooperating operator selector must hold
    the exclusive lease across validation and publication; exclusive acquisition
    also drains inherited worker admission. Both are nonblocking.
    This alone does not implement or authorize an activation/rollback writer.
    """
    if type(exclusive) is not bool:
        raise ValueError("selection lease mode")
    fd = drain = None
    try:
        import fcntl

        directory = Path(palace).absolute() / ".memory"
        before = _directory(directory)
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        opened = os.fstat(fd)
        if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError("selection directory changed")
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SelectionBusy("canonical selection busy") from None
        if exclusive:
            # Descendants inherit global admission. If a host dies, its shared
            # directory lease can vanish before its worker exits; the selector
            # must still drain that worker before changing authority.
            from .memory_runtime import _admission

            try:
                drain = _admission()
            except BlockingIOError:
                raise SelectionBusy("canonical workers still draining") from None
        after = _directory(directory)
        if (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino) or load_selection(
            palace
        ) != expected:
            raise ValueError("selection changed before lease")
    except BaseException as exc:
        for owned in (drain, fd):
            if owned is not None:
                os.close(owned)
        if isinstance(exc, SelectionBusy):
            raise
        if isinstance(exc, (OSError, ValueError, AttributeError, ImportError)):
            raise SelectionUnavailable("canonical selection lease unavailable") from None
        raise
    try:
        yield
    finally:
        if drain is not None:
            os.close(drain)
        os.close(fd)


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
        declaration_path = directory / BACKEND
        declaration = _file(declaration_path, immutable=True, missing=True)
        if declaration is None:
            if marker.exists() or marker.is_symlink():
                raise ValueError("orphan canonical selection")
            return None
        backend = strict_loads(declaration, max_bytes=MAX_BYTES)
        if (
            not isinstance(backend, dict)
            or backend.keys() != {"format", "backend", "selection_hash"}
            or backend["format"] != "castle-canonical-backend-v1"
            or backend["backend"] != "lance"
        ):
            raise ValueError("canonical backend declaration")
        layout.bulk.sha(backend["selection_hash"])
        if _json(backend).encode() != declaration:
            raise ValueError("backend canonical encoding")
        raw = _file(marker, immutable=True)
        if _hash(raw.decode("utf-8")) != backend["selection_hash"]:
            raise ValueError("canonical selection changed")
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
        if (
            _file(marker, immutable=True) != raw
            or _file(declaration_path, immutable=True) != declaration
        ):
            raise ValueError("selection changed during load")
        return value
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise SelectionUnavailable("canonical selection unavailable") from None

"""Inactive independent capture metadata; canonical text never belongs here.

Only the explicit operator creates an authority. Ordinary connections require its
trusted hash and pinned physical identities, retaining the shared SQLite guards.
No public/CLI route, native work, backend fallback or capture activation lives here.
"""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat

from . import capture_state_codec as codec
from .canonical_records import _hash, _json, _string
from .capture_recovery_codec import _sha
from .fast_memory import MemoryStore, _Corrupt, _Unavailable
from .private_sqlite import private_connection, private_path

DB_NAME = "control.sqlite3"
ARTIFACT = "control-authority.json"
APP_ID = 0x43434331
SCHEMA_VERSION = 2
_COLUMNS = {
    "capture_control_authority": [("id", "INTEGER"), ("value", "TEXT")],
    "checkpoints": [("source_key", "TEXT"), ("value", "TEXT")],
    "coverage_observations": [("sequence", "INTEGER"), ("identity", "TEXT"), ("value", "TEXT")],
    "sqlite_sequence": [("name", ""), ("seq", "")],
    "capture_sources": [("source_key", "TEXT"), ("state", "TEXT"), ("value", "TEXT")],
    "capture_generation_scopes": [
        ("source_key", "TEXT"),
        ("generation", "INTEGER"),
        ("workspace", "TEXT"),
        ("attribution", "TEXT"),
        ("state", "TEXT"),
    ],
    "capture_chunk_scopes": [
        ("chunk_id", "TEXT"),
        ("source_key", "TEXT"),
        ("generation", "INTEGER"),
        ("workspace", "TEXT"),
        ("attribution", "TEXT"),
        ("source_time", "INTEGER"),
    ],
    "capture_publication_intents": [
        ("request_key", "TEXT"),
        ("source_key", "TEXT"),
        ("generation", "INTEGER"),
        ("binding", "TEXT"),
        ("value", "TEXT"),
        ("state", "TEXT"),
    ],
    "capture_verified_chunks": [
        ("chunk_id", "TEXT"),
        ("source_key", "TEXT"),
        ("generation", "INTEGER"),
    ],
    "capture_count_baselines": [
        ("source_key", "TEXT"),
        ("generation", "INTEGER"),
        ("state", "TEXT"),
    ],
    "discovery_roots": [
        ("root_key", "TEXT"),
        ("path", "TEXT"),
        ("format", "TEXT"),
        ("sweep", "INTEGER"),
        ("completed_sweeps", "INTEGER"),
        ("complete_ns", "INTEGER"),
        ("errors", "INTEGER"),
    ],
    "discovery_dirs": [
        ("root_key", "TEXT"),
        ("path", "TEXT"),
        ("cursor", "TEXT"),
        ("identity", "TEXT"),
        ("done", "INTEGER"),
    ],
}
_INDEXES = {
    "capture_verified_source",
    "capture_source_state",
    "capture_source_workspace",
    "capture_chunk_workspace",
    "capture_chunk_generation",
    "capture_publication_state",
    "capture_publication_pending",
    "discovery_pending",
}


def _identity(path):
    info = path.lstat()
    return dict(device=info.st_dev, inode=info.st_ino)


class CaptureControlStore:
    def __init__(self, authority, *, expected_hash):
        self.authority = codec.validate_authority(authority, expected_hash=expected_hash)
        self.expected_hash = expected_hash
        self.directory = Path(self.authority["directory"]["path"])
        self.db_path = self.directory / DB_NAME
        self._creating = False

    @staticmethod
    def _private(path, directory=False):
        private_path(path, directory)

    @staticmethod
    def _error(exc):
        return MemoryStore._error(exc)

    def _write_budget(self):
        # Reserve the bounded immutable authority in addition to DB/journal/temp.
        return self.authority["max_bytes"] - 8192

    def _preflight(self):
        codec.validate_authority(self.authority, expected_hash=self.expected_hash)
        try:
            for path in (self.directory, *self.directory.parents):
                if path.is_symlink():
                    raise _Unavailable("control symlink ancestor")
            private_path(self.directory, True)
            private_path(self.db_path)
            if (
                _identity(self.directory)
                != {key: self.authority["directory"][key] for key in ("device", "inode")}
                or _identity(self.db_path) != self.authority["database"]
            ):
                raise _Unavailable("control identity changed")
            if any(
                path.name not in {DB_NAME, DB_NAME + "-journal", ARTIFACT}
                for path in self.directory.iterdir()
            ):
                raise _Unavailable("control unexpected artifact")
            artifact = self.directory / ARTIFACT
            private_path(artifact)
            if stat.S_IMODE(artifact.stat().st_mode) != 0o400:
                raise _Unavailable("control authority permissions")
            fd = os.open(artifact, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as source:
                raw = source.read(8193)
            if (
                len(raw) > 8192
                or codec.validate_authority(json.loads(raw), expected_hash=self.expected_hash)
                != self.authority
            ):
                raise _Unavailable("control authority changed")
        except (OSError, ValueError, UnicodeError, RecursionError) as exc:
            raise _Unavailable("control authority unavailable") from exc

    def _validate_schema(self, conn):
        self._preflight()
        conn.execute("PRAGMA trusted_schema=OFF")
        if (
            conn.execute("PRAGMA application_id").fetchone()[0] != APP_ID
            or conn.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION
        ):
            raise _Corrupt("control schema version")
        objects = conn.execute(
            "SELECT type,name,tbl_name FROM sqlite_master ORDER BY type,name LIMIT 65"
        ).fetchall()
        if len(objects) > 64 or {row[1] for row in objects if row[0] == "table"} != _COLUMNS.keys():
            raise _Corrupt("control schema tables")
        for kind, name, table in objects:
            if kind == "table":
                continue
            if (
                kind != "index"
                or (name not in _INDEXES and not name.startswith("sqlite_autoindex_"))
                or table not in _COLUMNS
            ):
                raise _Corrupt("control schema object")
        for name, expected in _COLUMNS.items():
            observed = [(r[1], r[2]) for r in conn.execute(f'PRAGMA table_info("{name}")')]
            if observed != expected:
                raise _Corrupt("control schema columns")
        rows = conn.execute(
            "SELECT id,substr(value,1,8193) FROM capture_control_authority LIMIT 2"
        ).fetchall()
        if (
            len(rows) != 1
            or rows[0][0] != 1
            or _string(rows[0][1], 8192, "control authority") != _json(self.authority)
        ):
            raise _Corrupt("control schema authority")

    def _initialize_schema(self, conn):
        if self._creating:
            from .memory_capture import _ensure
            from .memory_discovery import _ensure as ensure_discovery

            if conn.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone() is not None:
                raise _Corrupt("control initialization must be pristine")
            conn.execute(
                "CREATE TABLE checkpoints (source_key TEXT PRIMARY KEY,value TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE TABLE coverage_observations (sequence INTEGER PRIMARY KEY AUTOINCREMENT,identity TEXT UNIQUE NOT NULL,value TEXT NOT NULL)"
            )
            _ensure(conn)
            ensure_discovery(conn)
            conn.execute(
                "CREATE TABLE capture_verified_chunks (chunk_id TEXT PRIMARY KEY,source_key TEXT NOT NULL,generation INTEGER NOT NULL)"
            )
            conn.execute(
                "CREATE INDEX capture_verified_source ON capture_verified_chunks(source_key,generation)"
            )
            conn.execute(
                "CREATE TABLE capture_count_baselines (source_key TEXT,generation INTEGER,state TEXT NOT NULL,PRIMARY KEY(source_key,generation))"
            )
            conn.execute(
                "CREATE TABLE capture_control_authority (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO capture_control_authority VALUES (1,?)", (_json(self.authority),)
            )
            conn.execute(f"PRAGMA application_id={APP_ID}")
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        self._validate_schema(conn)

    @contextmanager
    def _connect(self, write=False):
        self._preflight()
        with private_connection(self, write) as conn:
            self._validate_schema(conn)
            yield conn

    def load_checkpoint(self, source_key):
        _sha(source_key)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT substr(value,1,8193) FROM checkpoints WHERE source_key=?", (source_key,)
            ).fetchone()
            if row is None:
                return None
            return codec.validate_checkpoint(json.loads(_string(row[0], 8192, "checkpoint")))

    def save_checkpoint(self, source_key, checkpoint, *, observations=None):
        _sha(source_key)
        checkpoint = codec.validate_checkpoint(checkpoint)
        raw = _json(checkpoint)
        if observations is not None and (
            not isinstance(observations, list) or len(observations) > 8
        ):
            raise ValueError("observations")
        encoded = [MemoryStore._observation(source_key, value) for value in observations or []]
        with self._connect(True) as conn:
            from .capture_control_counts import initialize_baseline

            old = conn.execute(
                "SELECT substr(value,1,8193) FROM checkpoints WHERE source_key=?", (source_key,)
            ).fetchone()
            previous = (
                None
                if old is None
                else codec.validate_checkpoint(json.loads(_string(old[0], 8192, "checkpoint")))
            )
            initialize_baseline(conn, source_key, checkpoint, previous)
            for observation in encoded:
                conn.execute(
                    "INSERT OR IGNORE INTO coverage_observations(identity,value) VALUES (?,?)",
                    (_hash(observation), observation),
                )
            conn.execute(
                "INSERT INTO checkpoints VALUES (?,?) ON CONFLICT(source_key) DO UPDATE SET value=excluded.value",
                (source_key, raw),
            )


def initialize_capture_control(directory, *, max_bytes):
    """Explicit operator for a new root only; failed artifacts remain preserved."""
    if type(max_bytes) is not int or not codec.MIN_BYTES <= max_bytes <= codec.MAX_BYTES:
        raise ValueError("control operator budget")
    directory = Path(directory)
    codec._path(str(directory), canonical=True)
    if not directory.is_absolute() or ".." in directory.parts:
        raise ValueError("control operator directory")
    for path in (directory, *directory.parents):
        if path.is_symlink():
            raise _Unavailable("control operator symlink")
    directory.mkdir(mode=0o700)
    fd = os.open(directory / DB_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    authority = dict(
        format=codec.FORMAT,
        directory=dict(path=str(directory), **_identity(directory)),
        database=_identity(directory / DB_NAME),
        max_bytes=max_bytes,
    )
    raw = _json(authority)
    expected_hash = _hash(raw)
    codec.validate_authority(authority, expected_hash=expected_hash)
    fd = os.open(directory / ARTIFACT, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as target:
        target.write(raw.encode())
        target.flush()
        os.fsync(target.fileno())
        os.fchmod(target.fileno(), 0o400)
    store = CaptureControlStore(authority, expected_hash=expected_hash)
    store._creating = True
    try:
        with store._connect(True):
            pass
    finally:
        store._creating = False
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    with store._connect():
        pass
    return dict(status="ok", authority=authority, authority_hash=expected_hash)

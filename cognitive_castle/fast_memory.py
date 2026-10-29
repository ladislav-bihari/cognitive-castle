"""Bounded, exact UTF-8 memory. No legacy backend or model dependencies."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

MAX_STORE_BYTES = 1024**3
DB_NAME = "memory.sqlite3"


def _json(value):
    def validate(item, depth=0):
        if depth > 32:
            raise ValueError("JSON nesting")
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON keys must be strings")
                validate(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                validate(child, depth + 1)
        elif item is not None and type(item) not in (str, int, float, bool):
            raise ValueError("invalid JSON value")

    try:
        validate(value)
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError("invalid JSON") from exc


def _string(value, maximum, name, *, empty=False):
    if not isinstance(value, str) or (not empty and not value):
        raise ValueError(name)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise ValueError(name) from exc
    if size > maximum:
        raise ValueError(name)
    return value


def _hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class _Unavailable(Exception):
    pass


class _Missing(_Unavailable):
    pass


class _Corrupt(Exception):
    pass


class _Quota(Exception):
    pass


class HistoryBudgetRefusal(_Quota):
    """Transient historical admission refusal, not a full SQLite shard."""

    REASONS = frozenset(
        {
            "history_aggregate_budget",
            "history_store_budget",
            "history_free_headroom",
            "history_scan_limit",
            "history_scan_deadline",
        }
    )

    def __init__(self, reason):
        if reason not in self.REASONS:
            raise ValueError("history budget reason")
        self.reason = reason
        super().__init__(reason)


class MemoryStore:
    def __init__(self, directory: Path):
        self.directory = Path(directory).absolute()
        self.db_path = self.directory / DB_NAME

    def _private(self, path, directory=False):
        info = path.lstat()
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        if not expected(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise _Unavailable("unsafe permissions or file type")
        if not directory and info.st_nlink != 1:
            raise _Unavailable("hard-linked storage")

    def _write_budget(self):
        return MAX_STORE_BYTES

    @contextmanager
    def _connect(self, write=False):
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "getuid"):
            raise _Unavailable("privacy primitives unavailable")
        # Do not resolve paths: every existing ancestor must reject symlinks.
        for path in reversed([self.directory, *self.directory.parents]):
            if path.is_symlink():
                raise _Unavailable("symlink ancestor")
        # Historical admission must precede creation and SQLite hot-journal
        # recovery. Read-only connections never reserve or expand capacity.
        budget = self._write_budget() if write else None
        if not self.directory.exists():
            if not write:
                raise _Missing("not initialized")
            self.directory.mkdir(mode=0o700)
        self._private(self.directory, True)
        for name in [DB_NAME, DB_NAME + "-journal", DB_NAME + "-wal", DB_NAME + "-shm"]:
            path = self.directory / name
            if path.exists() or path.is_symlink():
                self._private(path)
        if write and sum(p.stat().st_size for p in self.directory.iterdir()) >= budget:
            raise _Quota()
        if not self.db_path.exists():
            if not write:
                raise _Missing("not initialized")
            fd = os.open(self.db_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            os.close(fd)
        conn = sqlite3.connect(
            self.db_path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=0.05
        )
        conn.row_factory = sqlite3.Row
        deadline = time.monotonic() + (3 if write else 2)
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        try:
            conn.execute("PRAGMA cache_size=-4096")
            conn.execute("PRAGMA mmap_size=0")
            conn.execute("PRAGMA temp_store=FILE")
            if write:
                if conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
                    raise _Unavailable("rollback journal unavailable")
                # Reserve room for rollback journal and disk-backed temporary work.
                # This deliberately admits less data than the total sidecar budget.
                page_size = conn.execute("PRAGMA page_size").fetchone()[0]
                max_pages = max(1, budget // (3 * page_size))
                if conn.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0] > max_pages:
                    raise _Quota()
                conn.execute("BEGIN IMMEDIATE")
                new_chunks = (
                    conn.execute("SELECT 1 FROM sqlite_master WHERE name='chunks'").fetchone()
                    is None
                )
                for sql in [
                    "CREATE TABLE IF NOT EXISTS chunks (id TEXT PRIMARY KEY, text TEXT NOT NULL, provenance TEXT NOT NULL, text_hash TEXT NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL)",
                    "CREATE VIRTUAL TABLE IF NOT EXISTS lexical USING fts5(id UNINDEXED, text)",
                    "CREATE TABLE IF NOT EXISTS receipts (namespace TEXT, request_id TEXT, digest TEXT, ids TEXT, PRIMARY KEY(namespace, request_id))",
                    "CREATE TABLE IF NOT EXISTS checkpoints (source_key TEXT PRIMARY KEY, value TEXT NOT NULL)",
                    "CREATE TABLE IF NOT EXISTS coverage_observations (sequence INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT UNIQUE NOT NULL, value TEXT NOT NULL)",
                    "CREATE INDEX IF NOT EXISTS chunk_source_scope ON chunks(CASE WHEN json_valid(provenance) THEN json_extract(provenance,'$.source') END,CASE WHEN json_valid(provenance) THEN json_extract(provenance,'$.generation') END,CASE WHEN json_valid(provenance) THEN json_extract(provenance,'$.role') END)",
                ]:
                    conn.execute(sql)
                if new_chunks:
                    # Only index a table created empty in this transaction.
                    # Existing stores require the explicit bounded upgrade.
                    from .unified_memory import workspace_index_specs

                    for spec in workspace_index_specs():
                        if spec["table"] == "chunks":
                            conn.execute(spec["sql"])
            yield conn
            if write:
                conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _error(exc):
        if isinstance(exc, HistoryBudgetRefusal):
            return {"status": "quota", "reason": exc.reason}
        if isinstance(exc, _Corrupt):
            status = "corrupt"
        elif isinstance(exc, _Quota):
            status = "quota"
        elif isinstance(exc, sqlite3.Error):
            code = getattr(exc, "sqlite_errorcode", None)
            if isinstance(code, int):
                # Stable SQLite primary result codes; Python 3.10 lacks constants.
                status = {
                    5: "busy",
                    6: "busy",
                    13: "quota",
                    9: "deadline",
                    11: "corrupt",
                    26: "corrupt",
                }.get(code & 255, "unavailable")
            elif type(exc) in (sqlite3.OperationalError, sqlite3.DatabaseError):
                # Python 3.10 also lacks exception result codes. Match only exact
                # SQLite diagnostics, never substrings that could contain data.
                status = {
                    "database is locked": "busy",
                    "database table is locked": "busy",
                    "database or disk is full": "quota",
                    "interrupted": "deadline",
                    "database disk image is malformed": "corrupt",
                    "file is not a database": "corrupt",
                }.get(str(exc), "unavailable")
            else:
                status = "unavailable"
        else:
            status = "unavailable"
        return {"status": status}

    def remember(
        self,
        request_id: str,
        text: str,
        provenance: dict | None = None,
        *,
        namespace: str = "operator",
    ) -> dict:
        _string(request_id, 128, "request_id")
        _string(namespace, 128, "namespace")
        _string(text, 65536, "text")
        if provenance is not None and not isinstance(provenance, dict):
            raise ValueError("provenance")
        provenance_json = _json(provenance or {})
        _string(provenance_json, 512, "provenance")
        digest = _hash(_json([namespace, request_id, text, provenance or {}]))
        chunks, start, pending, size = [], 0, "", 0
        for character in text:
            width = len(character.encode())
            if size + width > 8192:
                chunks.append((start, pending))
                start += len(pending)
                pending, size = "", 0
            pending += character
            size += width
        if pending:
            chunks.append((start, pending))
        for offset, chunk in chunks:
            self._provenance(provenance or {}, _hash(chunk), offset, offset + len(chunk))
        ids = ["memory:" + _hash(f"{digest}:{i}") for i in range(len(chunks))]
        receipt = {
            "status": "ok",
            "ids": ids,
            "stored_text_bytes": len(text.encode()),
            "text_sha256": _hash(text),
        }
        try:
            with self._connect(True) as conn:
                old = conn.execute(
                    "SELECT digest, ids FROM receipts WHERE namespace=? AND request_id=?",
                    (namespace, request_id),
                ).fetchone()
                if old:
                    if old["digest"] != digest:
                        return {"status": "conflict"}
                    try:
                        stored_ids = json.loads(old["ids"])
                    except (ValueError, TypeError, RecursionError) as exc:
                        raise _Corrupt("invalid receipt JSON") from exc
                    if stored_ids != ids:
                        raise _Corrupt("invalid receipt identities")
                    return receipt
                for identity, (offset, chunk) in zip(ids, chunks):
                    conn.execute(
                        "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                        (
                            identity,
                            chunk,
                            provenance_json,
                            _hash(chunk),
                            offset,
                            offset + len(chunk),
                        ),
                    )
                    conn.execute("INSERT INTO lexical VALUES (?,?)", (identity, chunk))
                conn.execute(
                    "INSERT INTO receipts VALUES (?,?,?,?)",
                    (namespace, request_id, digest, _json(ids)),
                )
            return receipt
        except (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt) as exc:
            return self._error(exc)

    def receipt(self, namespace, key):
        """Read a committed request receipt and verify its bounded originals.

        Uses this explicitly selected backend only. A journal's expected IDs
        are never substituted for a missing or damaged canonical receipt.
        """
        _string(namespace, 128, "namespace")
        _string(key, 128, "request_id")
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT CAST(substr(CAST(digest AS BLOB),1,65) AS TEXT) AS digest,CAST(substr(CAST(ids AS BLOB),1,1025) AS TEXT) AS ids "
                    "FROM receipts WHERE namespace=? AND request_id=?",
                    (namespace, key),
                ).fetchone()
                if row is None:
                    return dict(status="not_found")
                return self._checked_receipt(conn, namespace, key, row)
        except _Missing:
            return dict(status="not_found")
        except (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt) as exc:
            return self._error(exc)

    def _checked_receipt(self, conn, namespace, key, row):
        from .canonical_records import prepare_remember

        try:
            ids = json.loads(_string(row["ids"], 1024, "receipt identities"))
            if not isinstance(ids, list) or not 1 <= len(ids) <= 9:
                raise ValueError("receipt identities")
            sources = []
            for identity in ids:
                if not isinstance(identity, str) or not re.fullmatch(
                    r"memory:[a-f0-9]{64}", identity
                ):
                    raise ValueError("receipt identity")
                original = conn.execute(
                    "SELECT id,CAST(substr(CAST(text AS BLOB),1,8193) AS TEXT) AS text,CAST(substr(CAST(provenance AS BLOB),1,513) AS TEXT) AS provenance,"
                    "CAST(substr(CAST(text_hash AS BLOB),1,65) AS TEXT) AS text_hash,start,end FROM chunks WHERE id=?",
                    (identity,),
                ).fetchone()
                if original is None:
                    raise _Corrupt("missing receipt original")
                sources.append(self._source(original))
            caller = sources[0]["provenance"]["caller"]
            prepared = prepare_remember(
                key, "".join(s["text"] for s in sources), caller, namespace=namespace
            )
            if (
                prepared.digest != row["digest"]
                or [record.source() for record in prepared.records] != sources
            ):
                raise ValueError("receipt original binding")
            return dict(status="ok", digest=prepared.digest, receipt=prepared.receipt)
        except (ValueError, KeyError, TypeError, RecursionError) as exc:
            raise _Corrupt("invalid committed receipt") from exc

    @staticmethod
    def _provenance(caller, text_hash, start, end):
        provenance = {
            "origin": "archive",
            "local_only": True,
            "storage_format": "sqlite-fts5-utf8-v1",
            "text_hash": text_hash,
            "chunk_start": start,
            "chunk_end": end,
            "caller": caller,
        }
        _string(_json(provenance), 1024, "final provenance")
        return provenance

    @classmethod
    def _source(cls, row):
        try:
            text = _string(row["text"], 8192, "persisted chunk")
            if (
                _hash(text) != row["text_hash"]
                or type(row["start"]) is not int
                or type(row["end"]) is not int
                or row["start"] < 0
                or row["end"] - row["start"] != len(text)
            ):
                raise ValueError("source checksum or offsets")
            caller = json.loads(_string(row["provenance"], 512, "persisted provenance"))
            if not isinstance(caller, dict):
                raise ValueError("persisted provenance")
            provenance = cls._provenance(caller, row["text_hash"], row["start"], row["end"])
            return {"id": row["id"], "text": row["text"], "provenance": provenance}
        except (ValueError, TypeError, RecursionError) as exc:
            raise _Corrupt("invalid source provenance") from exc

    def recall(self, query: str, *, limit: int = 8, match: str = "all") -> dict:
        """Return <=8192 bytes under compact ensure_ascii=False UTF-8 JSON.

        Transport adapters must bound their actual serialized envelopes separately.
        """
        _string(query, 1024, "query")
        terms = re.findall(r"\w+", query, re.UNICODE)
        if (
            not terms
            or len(terms) > 16
            or type(limit) is not int
            or not 1 <= limit <= 8
            or match not in ("all", "any")
        ):
            raise ValueError("query or limit")
        literal = (" AND " if match == "all" else " OR ").join(
            '"' + term.replace('"', '""') + '"' for term in terms
        )
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT c.* FROM lexical JOIN chunks c ON c.id=lexical.id WHERE lexical MATCH ? ORDER BY rank LIMIT ?",
                    (literal, limit),
                ).fetchall()
                result = {
                    "status": "ok",
                    "sources": [],
                    "coverage": "indexed_only",
                    "matching": match,
                }
                for row in rows:
                    source = self._source(row)
                    source["text"] = source["text"][:256]
                    source["provenance"]["preview"] = True
                    _string(_json(source["provenance"]), 1024, "final provenance")
                    result["sources"].append(source)
                    if len(_json(result).encode()) > 8192:
                        result["sources"].pop()
                        break
                return result
        except (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt) as exc:
            return self._error(exc)

    def read_sources(self, ids: list[str]) -> dict:
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8:
            raise ValueError("ids")
        for identity in ids:
            _string(identity, 128, "id")
            if not identity.startswith("memory:"):
                raise ValueError("id")
        try:
            with self._connect() as conn:
                sources = []
                for identity in ids:
                    row = conn.execute("SELECT * FROM chunks WHERE id=?", (identity,)).fetchone()
                    if row is None:
                        return {"status": "not_found", "sources": []}
                    sources.append(self._source(row))
                return {"status": "ok", "sources": sources}
        except (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt) as exc:
            return self._error(exc)

    def page(
        self,
        source_id: str,
        *,
        offset: int = 0,
        limit: int = 2048,
        expected_hash: str | None = None,
    ) -> dict:
        if (
            type(offset) is not int
            or offset < 0
            or type(limit) is not int
            or not 1 <= limit <= 2048
        ):
            raise ValueError("page bounds")
        if expected_hash is not None:
            _string(expected_hash, 128, "expected_hash")
        result = self.read_sources([source_id])
        if result["status"] != "ok":
            return result
        source = result["sources"][0]
        if expected_hash is not None and expected_hash != source["provenance"]["text_hash"]:
            return {"status": "stale"}
        text = source["text"]
        source.update(
            status="ok",
            text_hash=source["provenance"]["text_hash"],
            text=text[offset : offset + limit],
            offset=offset,
            next_offset=min(len(text), offset + limit),
            complete=offset + limit >= len(text),
        )
        return source

    def status(self, *, after: int = 0, limit: int = 16) -> dict:
        if type(after) is not int or not 0 <= after <= 2**63 - 1:
            raise ValueError("after")
        if type(limit) is not int or not 1 <= limit <= 16:
            raise ValueError("limit")
        try:
            with self._connect() as conn:
                count = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='coverage_observations'"
                ).fetchone()
                rows = (
                    conn.execute(
                        "SELECT sequence, value FROM coverage_observations WHERE sequence > ? ORDER BY sequence LIMIT ?",
                        (after, limit + 1),
                    ).fetchall()
                    if exists
                    else []
                )
                observations = []
                for row in rows[:limit]:
                    try:
                        observation = json.loads(_string(row["value"], 1024, "observation"))
                        source = observation.pop("source_key")
                        self._observation(source, observation)
                    except (ValueError, KeyError, TypeError, RecursionError) as exc:
                        raise _Corrupt("invalid coverage observation") from exc
                    candidate = {"sequence": row["sequence"], "source_key": source, **observation}
                    if len(_json(observations + [candidate]).encode()) > 12000:
                        break
                    observations.append(candidate)
                return {
                    "status": "ok",
                    "indexed_chunks": count,
                    "observations": observations,
                    "next_after": observations[-1]["sequence"]
                    if len(rows) > len(observations)
                    else None,
                    "observation_currentness": "recorded_only_source_not_reopened",
                    "coverage": "indexed_only",
                    "storage_format": "sqlite-fts5-utf8-v1",
                }
        except (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt) as exc:
            return self._error(exc)

    def load_checkpoint(self, source_key: str) -> dict | None:
        _string(source_key, 128, "source_key")
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT value FROM checkpoints WHERE source_key=?", (source_key,)
                ).fetchone()
                if row is None:
                    return None
                try:
                    value = json.loads(_string(row[0], 8192, "checkpoint"))
                    self._checkpoint(value)
                    return value
                except (ValueError, TypeError, RecursionError) as exc:
                    raise ValueError("corrupt checkpoint") from exc
        except _Missing:
            return None

    @staticmethod
    def _checkpoint(checkpoint):
        if not isinstance(checkpoint, dict):
            raise ValueError("checkpoint")
        return _string(_json(checkpoint), 8192, "checkpoint")

    @staticmethod
    def _observation(source_key, observation):
        _string(source_key, 128, "source_key")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", source_key):
            raise ValueError("opaque source key")
        allowed = {
            "generation",
            "reason",
            "start",
            "end",
            "record_start",
            "hash_scope",
            "sha256",
            "outcome",
            "previous_generation",
        }
        required = {"generation", "reason", "start", "end", "record_start", "hash_scope"}
        if (
            not isinstance(observation, dict)
            or not required <= observation.keys()
            or observation.keys() - allowed
        ):
            raise ValueError("observation fields")
        for field in ("generation", "start", "end", "record_start", "previous_generation"):
            if field in observation and (
                type(observation[field]) is not int or not 0 <= observation[field] <= 2**63 - 1
            ):
                raise ValueError("observation range")
        if not observation["record_start"] <= observation["start"] <= observation["end"]:
            raise ValueError("observation range")
        if observation["reason"] not in {
            "pending_tail",
            "pending_record",
            "unsupported",
            "oversized",
            "malformed",
            "changed",
            "write_attempt",
            "failed",
            "indexed_record",
        }:
            raise ValueError("observation reason")
        if observation["hash_scope"] not in {"record", "segment", "unknown"}:
            raise ValueError("hash scope")
        digest = observation.get("sha256")
        if observation["hash_scope"] == "unknown":
            if digest is not None:
                raise ValueError("unknown hash")
        elif not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
            raise ValueError("observation hash")
        if "outcome" in observation and observation["outcome"] not in {
            "unknown_or_interrupted",
            "busy",
            "quota",
            "deadline",
            "corrupt",
            "unavailable",
            "conflict",
            "ok",
        }:
            raise ValueError("observation outcome")
        return _string(_json({"source_key": source_key, **observation}), 1024, "observation")

    def save_checkpoint(
        self, source_key: str, checkpoint: dict, *, observations: list[dict] | None = None
    ) -> None:
        _string(source_key, 128, "source_key")
        value = self._checkpoint(checkpoint)
        if observations is not None and (
            not isinstance(observations, list) or len(observations) > 8
        ):
            raise ValueError("observations")
        encoded = [self._observation(source_key, observation) for observation in observations or []]
        with self._connect(True) as conn:
            for observation in encoded:
                conn.execute(
                    "INSERT OR IGNORE INTO coverage_observations(identity,value) VALUES (?,?)",
                    (_hash(observation), observation),
                )
            conn.execute(
                "INSERT INTO checkpoints VALUES (?,?) ON CONFLICT(source_key) DO UPDATE SET value=excluded.value",
                (source_key, value),
            )

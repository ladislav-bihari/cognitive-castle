"""Private durable reference queue. References alone do not prove exact publication.

Only the trusted coordinator may acknowledge a receipt after verifying it against
LanceVectorStore.receipt; model responses are never acknowledgement receipts.
No bodies, token arrays or vectors are accepted or persisted here.
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re

from .canonical_records import _string
from .lance_exact_store import LanceExactStore, _StoreError, _identity, _require_supervision
from .semantic_profile import PROFILE_ID, canonical_json, digest

TASK_RSS = 128 * 1024**2
TOTAL_RSS = 192 * 1024**2
SCOPE_LIMITS = (("workspace", 2048), ("wing", 512), ("room", 512))


def validate_hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("hash")
    return value


def validate_scope(scope):
    if not isinstance(scope, dict) or set(scope) != {"workspace", "wing", "room"}:
        raise ValueError("full trusted scope required")
    for name, maximum in SCOPE_LIMITS:
        _string(scope[name], maximum, name)
    return scope


def validate_references(references):
    if not isinstance(references, list) or not 1 <= len(references) <= 8:
        raise ValueError("references")
    identities = set()
    for reference in references:
        if not isinstance(reference, dict) or set(reference) != {
            "canonical_id",
            "source_hash",
            "exposure_hash",
            "scope",
        }:
            raise ValueError("reference signature")
        validate_scope(reference["scope"])
        _identity(reference["canonical_id"])
        validate_hash(reference["source_hash"])
        validate_hash(reference["exposure_hash"])
        identities.add(reference["canonical_id"])
    if len(identities) != len(references):
        raise ValueError("duplicate references")
    return references


def validate_receipt(receipt):
    fields = {
        "status",
        "receipt_id",
        "batch_key",
        "profile",
        "input_binding",
        "reference_binding",
        "start_window",
        "total_units",
        "count",
        "next_continuation",
        "unit_ids",
        "vector_digests",
    }
    if (
        not isinstance(receipt, dict)
        or set(receipt) != fields
        or receipt["status"] != "ok"
        or receipt["profile"] != PROFILE_ID
    ):
        raise ValueError("publication receipt signature")
    _string(receipt["batch_key"], 128, "batch key")
    if not isinstance(receipt["receipt_id"], str) or not re.fullmatch(
        r"vector-receipt:[a-f0-9]{64}", receipt["receipt_id"]
    ):
        raise ValueError("receipt identity")
    validate_hash(receipt["input_binding"])
    validate_hash(receipt["reference_binding"])
    start, total, count = (receipt[name] for name in ("start_window", "total_units", "count"))
    if (
        any(type(v) is not int for v in (start, total, count))
        or not 0 <= start < total <= 1048576
        or start % 32
        or count != min(32, total - start)
    ):
        raise ValueError("receipt bounds")
    end = start + count
    expected = {"binding": receipt["input_binding"], "next_window": end} if end < total else None
    if receipt["next_continuation"] != expected:
        raise ValueError("receipt continuation")
    for name in ("unit_ids", "vector_digests"):
        if not isinstance(receipt[name], list) or len(receipt[name]) != count:
            raise ValueError("receipt records")
        for value in receipt[name]:
            validate_hash(value)
    return receipt


class SemanticQueue:
    def __init__(self, directory: Path, *, writable=False):
        if type(writable) is not bool:
            raise ValueError("writable")
        self.generation = Path(directory).absolute()
        self.directory = self.generation / "coordination"
        self.writable = writable

    def _paths(self, create):
        _require_supervision()
        # Queue-only limits are stricter than the shared native store ceiling.
        import psutil
        from .brain_reader_guardian import _group_rss

        task = _group_rss(os.getpgrp())
        if task > TASK_RSS:
            raise _StoreError("worker_memory_limit")
        if task + psutil.Process(os.getppid()).parent().memory_info().rss > TOTAL_RSS:
            raise _StoreError("total_memory_limit")
        for path in (self.generation, *self.generation.parents):
            if path.is_symlink():
                raise _StoreError("unavailable")
        LanceExactStore._private(self.generation, directory=True)
        if not self.directory.exists():
            if not create:
                raise _StoreError("not_initialized")
            self.directory.mkdir(mode=0o700)
        LanceExactStore._private(self.directory, directory=True)
        for path in self.directory.iterdir():
            if path.name not in {"queue.sqlite3", "queue.sqlite3-journal"}:
                raise _StoreError("unavailable")
            LanceExactStore._private(path)

    @contextmanager
    def _connection(self, write=False):
        if write and not self.writable:
            raise _StoreError("unavailable")
        self._paths(write)
        import sqlite3

        path = self.directory / "queue.sqlite3"
        if not write and not path.exists():
            raise _StoreError("not_initialized")
        conn = sqlite3.connect(
            path.as_uri()
            + ("?mode=rw" if path.exists() and write else "?mode=rwc" if write else "?mode=ro"),
            uri=True,
            timeout=0,
        )
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA trusted_schema=OFF")
            conn.execute("PRAGMA cache_size=-512")
            conn.execute("PRAGMA mmap_size=0")
            if write:
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                version = conn.execute("PRAGMA user_version").fetchone()[0]
                if version == 0:
                    conn.execute(
                        "CREATE TABLE groups (seq INTEGER PRIMARY KEY, batch_key TEXT UNIQUE NOT NULL, binding TEXT NOT NULL, references_json TEXT NOT NULL, profile TEXT NOT NULL, input_binding TEXT NOT NULL, lane TEXT NOT NULL, state TEXT NOT NULL, position INTEGER NOT NULL, total INTEGER, last_receipt TEXT)"
                    )
                    conn.execute(
                        "CREATE TABLE schedule (id INTEGER PRIMARY KEY CHECK(id=1), new_pages INTEGER NOT NULL)"
                    )
                    conn.execute("INSERT INTO schedule VALUES (1,0)")
                    conn.execute(
                        "CREATE TABLE acknowledgements (batch_key TEXT NOT NULL, start INTEGER NOT NULL, receipt TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(batch_key,start))"
                    )
                    conn.execute("PRAGMA user_version=1")
                elif version not in (1, 2, 3, 4, 5):
                    raise _StoreError("corrupt")
                if version in (4, 5):
                    from .lance_layout_directory import LayoutDirectory

                    LayoutDirectory(conn)._schema()
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS intents (seq INTEGER PRIMARY KEY, intent_id TEXT UNIQUE NOT NULL, binding TEXT NOT NULL, intent_json TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0)"
                )
                if "intent_cursor" not in {
                    row[1] for row in conn.execute("PRAGMA table_info(schedule)")
                }:
                    conn.execute(
                        "ALTER TABLE schedule ADD COLUMN intent_cursor INTEGER NOT NULL DEFAULT 0"
                    )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS migration_jobs (job_id TEXT PRIMARY KEY, registration TEXT NOT NULL, staging_cursor TEXT NOT NULL, completed_frontier TEXT NOT NULL, pending TEXT, counts TEXT NOT NULL, coverage TEXT NOT NULL, state TEXT NOT NULL, last_ack TEXT, last_refusal TEXT, owner_references INTEGER NOT NULL DEFAULT 0, semantic_pending INTEGER NOT NULL DEFAULT 0)"
                )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS migration_owners (job_id TEXT NOT NULL, owner TEXT NOT NULL, reference TEXT NOT NULL, PRIMARY KEY(job_id,owner))"
                )
                if version in (4, 5):
                    from .lance_layout_directory import require_attachment_column

                    require_attachment_column(conn)
                else:
                    conn.execute("PRAGMA user_version=3")
            elif conn.execute("PRAGMA user_version").fetchone()[0] not in (1, 2, 3, 4, 5):
                raise _StoreError("corrupt")
            elif conn.execute("PRAGMA user_version").fetchone()[0] in (4, 5):
                from .lance_layout_directory import require_attachment_column

                require_attachment_column(conn)
            yield conn
            if write:
                conn.commit()
        finally:
            conn.close()

    @contextmanager
    def _layout_resume_connection(self):
        """Recover only an existing coordination DB; never initialize metadata."""
        if not self.writable:
            raise _StoreError("unavailable")
        self._paths(False)
        import sqlite3

        path = self.directory / "queue.sqlite3"
        conn = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA trusted_schema=OFF")
            conn.execute("PRAGMA cache_size=-512")
            conn.execute("PRAGMA mmap_size=0")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _group(row):
        return {
            "batch_key": row["batch_key"],
            "references": json.loads(row["references_json"]),
            "profile": row["profile"],
            "input_binding": row["input_binding"],
            "lane": row["lane"],
            "reference_binding": digest(json.loads(row["references_json"])),
            "state": row["state"],
            "position": row["position"],
            "total_units": row["total"],
            "last_receipt": json.loads(row["last_receipt"]) if row["last_receipt"] else None,
        }

    def _run(self, write, operation):
        try:
            _require_supervision()
        except _StoreError as exc:
            return {"status": exc.status}
        import sqlite3

        try:
            with self._connection(write) as conn:
                return operation(conn)
        except _StoreError as exc:
            return {"status": exc.status}
        except sqlite3.OperationalError as exc:
            return {"status": "busy" if "locked" in str(exc) else "corrupt"}
        except (OSError, sqlite3.DatabaseError, ValueError, KeyError, TypeError):
            return {"status": "unavailable"}

    def register(self, batch_key, references, profile, input_binding, lane):
        _string(batch_key, 128, "batch key")
        validate_references(references)
        validate_hash(input_binding)
        if profile != PROFILE_ID or lane not in {"new", "history"}:
            raise ValueError("profile or lane")
        binding = digest([references, profile, input_binding, lane])

        def register(conn):
            old = conn.execute("SELECT * FROM groups WHERE batch_key=?", (batch_key,)).fetchone()
            if old:
                return (
                    {"status": "conflict"}
                    if old["binding"] != binding
                    else {"status": "ok", **self._group(old)}
                )
            conn.execute(
                "INSERT INTO groups(batch_key,binding,references_json,profile,input_binding,lane,state,position) VALUES (?,?,?,?,?,?,'pending',0)",
                (
                    batch_key,
                    binding,
                    canonical_json(references),
                    profile,
                    input_binding,
                    lane,
                ),
            )
            return {
                "status": "ok",
                **self._group(
                    conn.execute("SELECT * FROM groups WHERE batch_key=?", (batch_key,)).fetchone()
                ),
            }

        return self._run(True, register)

    def select(self):
        def select(conn):
            streak = conn.execute("SELECT new_pages FROM schedule WHERE id=1").fetchone()[0]
            lanes = {
                lane: conn.execute(
                    "SELECT * FROM groups WHERE lane=? AND state IN ('pending','inflight') ORDER BY CASE state WHEN 'inflight' THEN 0 ELSE 1 END,seq LIMIT 1",
                    (lane,),
                ).fetchone()
                for lane in ("new", "history")
            }
            lane = (
                "history"
                if lanes["history"] is not None and (streak >= 3 or lanes["new"] is None)
                else "new"
            )
            row = lanes[lane]
            if row is None:
                return {"status": "idle"}
            conn.execute(
                "UPDATE groups SET state='inflight' WHERE batch_key=?", (row["batch_key"],)
            )
            conn.execute(
                "UPDATE schedule SET new_pages=? WHERE id=1",
                (min(streak + 1, 3) if lane == "new" else 0,),
            )
            group = self._group(row)
            group["state"] = "inflight"
            return {"status": "ok", "group": group}

        return self._run(True, select)

    def acknowledge(self, batch_key, receipt):
        _string(batch_key, 128, "batch key")
        validate_receipt(receipt)

        def acknowledge(conn):
            row = conn.execute("SELECT * FROM groups WHERE batch_key=?", (batch_key,)).fetchone()
            if row is None:
                return {"status": "not_found"}
            old = conn.execute(
                "SELECT receipt,result FROM acknowledgements WHERE batch_key=? AND start=?",
                (batch_key, receipt["start_window"]),
            ).fetchone()
            encoded = canonical_json(receipt)
            if old:
                return (
                    json.loads(old["result"])
                    if old["receipt"] == encoded
                    else {"status": "conflict"}
                )
            if (
                receipt["batch_key"] != batch_key
                or receipt["profile"] != row["profile"]
                or receipt["input_binding"] != row["input_binding"]
                or receipt["reference_binding"] != digest(json.loads(row["references_json"]))
                or receipt["start_window"] != row["position"]
                or row["state"] not in {"pending", "inflight"}
                or row["total"] not in (None, receipt["total_units"])
            ):
                return {"status": "conflict"}
            position = receipt["start_window"] + receipt["count"]
            state = "complete" if position == receipt["total_units"] else "pending"
            conn.execute(
                "UPDATE groups SET state=?,position=?,total=?,last_receipt=? WHERE batch_key=?",
                (state, position, receipt["total_units"], encoded, batch_key),
            )
            result = {
                "status": "ok",
                "state": state,
                "position": position,
                "total_units": receipt["total_units"],
                "receipt_id": receipt["receipt_id"],
            }
            conn.execute(
                "INSERT INTO acknowledgements VALUES (?,?,?,?)",
                (batch_key, receipt["start_window"], encoded, canonical_json(result)),
            )
            return result

        return self._run(True, acknowledge)

    def retry(self, batch_key, *, refused=False):
        _string(batch_key, 128, "batch key")
        if type(refused) is not bool:
            raise ValueError("refused")

        def retry(conn):
            row = conn.execute("SELECT * FROM groups WHERE batch_key=?", (batch_key,)).fetchone()
            if row is None:
                return {"status": "not_found"}
            if row["state"] != "complete":
                conn.execute(
                    "UPDATE groups SET state=? WHERE batch_key=?",
                    ("refused" if refused else "pending", batch_key),
                )
            return {
                "status": "ok",
                **self._group(
                    conn.execute("SELECT * FROM groups WHERE batch_key=?", (batch_key,)).fetchone()
                ),
            }

        return self._run(True, retry)

    def status(self):
        def status(conn):
            counts = {name: 0 for name in ("pending", "inflight", "complete", "refused")}
            for row in conn.execute("SELECT state,count(*) AS n FROM groups GROUP BY state"):
                counts[row["state"]] = row["n"]
            rows = conn.execute("SELECT * FROM groups ORDER BY seq LIMIT 64").fetchall()
            return {
                "status": "ok",
                "counts": counts,
                "groups": [
                    {
                        "batch_key": row["batch_key"],
                        "lane": row["lane"],
                        "state": row["state"],
                        "position": row["position"],
                        "total_units": row["total"],
                        "receipt_id": json.loads(row["last_receipt"])["receipt_id"]
                        if row["last_receipt"]
                        else None,
                    }
                    for row in rows
                ],
                "positions_truncated": sum(counts.values()) > len(rows),
                "coverage_complete": False,
            }

        return self._run(False, status)

    def intent_prepare(self, intent):
        from .semantic_intents import validate_intent

        validate_intent(intent)
        encoded = canonical_json(intent)

        def prepare(conn):
            old = conn.execute(
                "SELECT binding FROM intents WHERE intent_id=?", (intent["intent_id"],)
            ).fetchone()
            if old:
                return {"status": "ok" if old["binding"] == digest(intent) else "conflict"}
            conn.execute(
                "INSERT INTO intents(intent_id,binding,intent_json) VALUES (?,?,?)",
                (intent["intent_id"], digest(intent), encoded),
            )
            return {"status": "ok"}

        return self._run(True, prepare)

    def intents(self):
        def pending(conn):
            cursor = conn.execute("SELECT intent_cursor FROM schedule WHERE id=1").fetchone()[0]
            rows = conn.execute(
                "SELECT seq,intent_json FROM intents WHERE complete=0 ORDER BY CASE WHEN seq>? THEN 0 ELSE 1 END,seq LIMIT 2",
                (cursor,),
            ).fetchall()
            if rows:
                conn.execute("UPDATE schedule SET intent_cursor=? WHERE id=1", (rows[-1]["seq"],))
            return {"status": "ok", "intents": [json.loads(row["intent_json"]) for row in rows]}

        return self._run(True, pending)

    def intent_complete(self, intent_id, binding):
        validate_hash(intent_id)
        validate_hash(binding)

        def complete(conn):
            row = conn.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                return {"status": "not_found"}
            if row["binding"] != binding:
                return {"status": "conflict"}
            intent = json.loads(row["intent_json"])
            from .semantic_intents import validate_intent

            validate_intent(intent)
            for group in intent["groups"]:
                group_binding = digest(
                    [group[key] for key in ("references", "profile", "input_binding", "lane")]
                )
                old = conn.execute(
                    "SELECT binding FROM groups WHERE batch_key=?", (group["batch_key"],)
                ).fetchone()
                if old and old[0] != group_binding:
                    raise _StoreError("conflict")
                conn.execute(
                    "INSERT OR IGNORE INTO groups(batch_key,binding,references_json,profile,input_binding,lane,state,position) VALUES (?,?,?,?,?,?, 'pending',0)",
                    (
                        group["batch_key"],
                        group_binding,
                        canonical_json(group["references"]),
                        group["profile"],
                        group["input_binding"],
                        group["lane"],
                    ),
                )
            conn.execute("UPDATE intents SET complete=1 WHERE intent_id=?", (intent_id,))
            return {"status": "ok"}

        return self._run(True, complete)

    @staticmethod
    def _migration_row(conn, job_id):
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version < 3:
            raise _StoreError("not_found")
        if version in (4, 5):
            marker = conn.execute(
                "SELECT length(CAST(layout_attachment AS BLOB)) FROM migration_jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if marker and marker[0] is not None and marker[0] > 512:
                raise _StoreError("unavailable")
        lengths = conn.execute(
            "SELECT length(CAST(registration AS BLOB)),length(CAST(staging_cursor AS BLOB)),length(CAST(completed_frontier AS BLOB)),length(CAST(pending AS BLOB)),length(CAST(counts AS BLOB)),length(CAST(last_ack AS BLOB)),length(CAST(last_refusal AS BLOB)),length(CAST(coverage AS BLOB)),length(CAST(state AS BLOB)),typeof(owner_references),typeof(semantic_pending) FROM migration_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if lengths and (
            any(n is not None and n > 196608 for n in lengths[:7])
            or any(n is None or n > 64 for n in lengths[7:9])
            or any(kind != "integer" for kind in lengths[9:])
        ):
            raise _StoreError("unavailable")
        row = conn.execute("SELECT * FROM migration_jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise _StoreError("not_found")
        return row

    @staticmethod
    def _migration_state(row, layout=None):
        from . import migration_codec as codec

        marker = row["layout_attachment"] if "layout_attachment" in row.keys() else None
        if marker is not None:
            from .lance_layout_codec import validate_attachment

            if len(marker.encode("utf-8")) > 512:
                raise _StoreError("unavailable")
            expected = validate_attachment(json.loads(marker))
            if layout is None or expected["envelope_hash"] != codec.hashed(layout["envelope"]):
                raise _StoreError("unavailable")
        elif layout is not None:
            raise _StoreError("unavailable")

        reg = codec.validate_registration(json.loads(row["registration"]))
        staging = codec.cursor(json.loads(row["staging_cursor"]), reg)
        frontier = codec.cursor(json.loads(row["completed_frontier"]), reg)
        pending = json.loads(row["pending"]) if row["pending"] else None
        if pending:
            codec.validate_pending(pending, reg, layout)
            if pending["cursor_before"] != staging:
                raise _StoreError("corrupt")
        state = dict(
            status="ok",
            registration=reg,
            staging_cursor=staging,
            completed_frontier=frontier,
            pending=pending,
            counts=codec.counts(json.loads(row["counts"])),
            coverage=row["coverage"],
            state=row["state"],
            last_ack=json.loads(row["last_ack"]) if row["last_ack"] else None,
            last_refusal=json.loads(row["last_refusal"]) if row["last_refusal"] else None,
            owner_references=row["owner_references"],
            semantic_pending=row["semantic_pending"],
        )
        if layout is not None:
            state["layout"] = layout
        return codec.bounded(state) if layout is not None else state

    @staticmethod
    def _migration_v1_state(conn, job_id):
        from .lance_layout_directory import LayoutDirectory

        if LayoutDirectory(conn).attachment(job_id) is not None:
            raise _StoreError("conflict")
        return SemanticQueue._migration_state(SemanticQueue._migration_row(conn, job_id))

    def migration_open(self, registration):
        from . import migration_codec as codec
        from .sqlite_export_codec import empty_counts

        try:
            codec.validate_registration(registration)
        except codec.RegistrationConflict:
            return {"status": "conflict"}
        if registration["destination"] != str(self.generation):
            return {"status": "conflict"}
        encoded = canonical_json(registration)

        def operation(conn):
            old = conn.execute(
                "SELECT 1 FROM migration_jobs WHERE job_id=?", (registration["job_id"],)
            ).fetchone()
            if old:
                old = self._migration_row(conn, registration["job_id"])
            if old and old["registration"] != encoded:
                return {"status": "conflict"}
            if old:
                return self._migration_v1_state(conn, registration["job_id"])
            if old is None:
                before = canonical_json(codec.initial_source_cursor(registration))
                conn.execute(
                    "INSERT INTO migration_jobs(job_id,registration,staging_cursor,completed_frontier,counts,coverage,state) VALUES (?,?,?,?,?,'incomplete','active')",
                    (
                        registration["job_id"],
                        encoded,
                        before,
                        before,
                        canonical_json(empty_counts()),
                    ),
                )
            return self._migration_state(self._migration_row(conn, registration["job_id"]))

        return self._run(True, operation)

    def migration_state(self, job_id):
        validate_hash(job_id)

        def operation(conn):
            from .lance_layout_directory import _directory

            return self._layout_state(conn, job_id, _directory(conn))

        return self._run(False, operation)

    @staticmethod
    def _layout_state(conn, job_id, directory):
        from .lance_layout_directory import _NativeLayoutDirectory

        attachment = directory.attachment(job_id)
        state = SemanticQueue._migration_state(
            SemanticQueue._migration_row(conn, job_id), attachment
        )
        if attachment is not None and isinstance(directory, _NativeLayoutDirectory):
            state["native_layout"] = directory.context(job_id)
            from .migration_codec import bounded

            bounded(state)
        return state

    def layout_resume(self, registration):
        """Mutating-path recovery entry; no job/schema/authority creation."""
        from . import migration_codec as codec
        from .lance_layout_directory import _NativeLayoutDirectory

        try:
            codec.validate_registration(registration)
        except codec.RegistrationConflict:
            return {"status": "conflict"}
        if registration["destination"] != str(self.generation):
            return {"status": "conflict"}
        try:
            _require_supervision()
        except _StoreError as exc:
            return {"status": exc.status}
        import sqlite3

        try:
            with self._layout_resume_connection() as conn:
                directory = _NativeLayoutDirectory(conn)
                if directory.attachment(registration["job_id"]) is None:
                    raise _StoreError("unavailable")
                state = self._layout_state(conn, registration["job_id"], directory)
                if state["registration"] != registration:
                    raise _StoreError("conflict")
                return state
        except _StoreError as exc:
            return {"status": exc.status}
        except sqlite3.OperationalError as exc:
            return {"status": "busy" if "locked" in str(exc) else "unavailable"}
        except (OSError, sqlite3.DatabaseError, ValueError, KeyError, TypeError):
            return {"status": "unavailable"}

    def migration_prepare(self, job_id, expected_cursor, intent):
        from . import migration_codec as codec

        validate_hash(job_id)

        def operation(conn):
            state = self._migration_v1_state(conn, job_id)
            codec.cursor(expected_cursor, state["registration"])
            codec.validate_interval(intent, state["registration"])
            if (
                expected_cursor != state["staging_cursor"]
                or intent["cursor_before"] != expected_cursor
                or state["state"] == "snapshot_export_exhausted"
            ):
                return {"status": "conflict"}
            if state["pending"] is not None and state["pending"] != intent:
                return {"status": "conflict"}
            conn.execute(
                "UPDATE migration_jobs SET pending=?,state='active',last_refusal=NULL WHERE job_id=?",
                (canonical_json(intent), job_id),
            )
            return {"status": "ok", "intent_id": intent["intent_id"]}

        return self._run(True, operation)

    def migration_refuse(self, job_id, expected_cursor, refusal):
        from . import migration_codec as codec

        validate_hash(job_id)
        if not isinstance(refusal, dict) or refusal.keys() != {
            "status",
            "reason",
            "phase",
            "refused_operation",
            "refused_field",
            "orphans",
        }:
            raise ValueError("migration refusal")
        for name in ("status", "reason", "phase", "refused_operation", "refused_field"):
            if refusal[name] is not None:
                _string(refusal[name], 128, name)
        if (
            refusal["status"] == "ok"
            or type(refusal["orphans"]) is not int
            or refusal["orphans"] not in (0, 1)
        ):
            raise ValueError("migration refusal status")
        codec.bounded(refusal)

        def operation(conn):
            state = self._migration_v1_state(conn, job_id)
            codec.cursor(expected_cursor, state["registration"])
            if (
                expected_cursor != state["staging_cursor"]
                or state["state"] == "snapshot_export_exhausted"
            ):
                return {"status": "conflict"}
            counters = state["counts"]
            counters["orphans"] = max(counters["orphans"], refusal["orphans"])
            conn.execute(
                "UPDATE migration_jobs SET state='refused',last_refusal=?,counts=?,coverage='incomplete' WHERE job_id=?",
                (canonical_json(refusal), canonical_json(counters), job_id),
            )
            return {"status": "ok"}

        return self._run(True, operation)

    def migration_ack(self, job_id, intent_id, verified_summary):
        """Private ACK trusts current-tick coordinator proof, never queue rows."""
        from . import migration_codec as codec

        validate_hash(job_id)
        if intent_id is not None:
            validate_hash(intent_id)
        codec.bounded(verified_summary)

        def operation(conn):
            state = self._migration_v1_state(conn, job_id)
            summary_hash = codec.hashed(verified_summary)
            old = state["last_ack"]
            if old and old["intent_id"] == intent_id and old["summary_hash"] == summary_hash:
                return old["result"]
            if intent_id is None:
                return self._migration_terminal_ack(
                    conn, job_id, state, verified_summary, summary_hash
                )
            intent = state["pending"]
            if (
                intent is None
                or intent["intent_id"] != intent_id
                or intent["cursor_before"] != state["staging_cursor"]
            ):
                return {"status": "conflict"}
            codec.validate_verified(verified_summary, intent)
            return self._migration_content_ack(
                conn, job_id, state, intent, verified_summary["owner_versions"], summary_hash
            )

        return self._run(True, operation)

    @staticmethod
    def _migration_terminal_ack(conn, job_id, state, summary, summary_hash):
        from . import migration_codec as codec

        try:
            done = codec.terminal_cursor(state, summary)
        except ValueError:
            raise _StoreError("conflict") from None
        result = codec.bounded(
            dict(
                status="ok",
                job_id=job_id,
                intent_id=None,
                state="snapshot_export_exhausted",
                staging_cursor=done,
                completed_frontier=state["completed_frontier"],
                counts=state["counts"],
            )
        )
        last_ack = codec.bounded(dict(intent_id=None, summary_hash=summary_hash, result=result))
        conn.execute(
            "UPDATE migration_jobs SET staging_cursor=?,pending=NULL,coverage=?,state='snapshot_export_exhausted',last_ack=?,last_refusal=NULL WHERE job_id=?",
            (canonical_json(done), summary["coverage"], canonical_json(last_ack), job_id),
        )
        return result

    @staticmethod
    def _migration_content_values(conn, job_id, state, intent, versions):
        """Shared original progression; validate all values before any write."""
        from . import migration_codec as codec

        if len(versions) != len(intent["owners"]):
            raise ValueError("owner proof versions")
        counters = codec.counts(
            {name: state["counts"][name] + value for name, value in intent["counts"].items()}
        )
        references = []
        for owner, version in zip(intent["owners"], versions):
            if type(version) is not int or not 0 < version < 2**63:
                raise ValueError("owner dataset version")
            reference = dict(
                job_id=job_id,
                source_binding=intent["source_binding"],
                destination=intent["destination"],
                **owner,
                proof_version=codec.PROOF_VERSION,
                dataset_version=version,
            )
            if "target" in intent:
                reference["layout_target"] = intent["target"]
            codec.bounded(reference)
            if conn.execute(
                "SELECT 1 FROM migration_owners WHERE job_id=? AND owner=?",
                (job_id, owner["owner"]),
            ).fetchone():
                raise _StoreError("conflict")
            references.append(reference)
        return counters, references

    @staticmethod
    def _migration_content_ack(conn, job_id, state, intent, versions, summary_hash):
        from . import migration_codec as codec

        counters, references = SemanticQueue._migration_content_values(
            conn, job_id, state, intent, versions
        )
        after = intent["cursor_after"]
        frontier = after if intent["owners"] else state["completed_frontier"]
        result = codec.bounded(
            dict(
                status="ok",
                job_id=job_id,
                intent_id=intent["intent_id"],
                state="active",
                staging_cursor=after,
                completed_frontier=frontier,
                counts=counters,
            )
        )
        last_ack = codec.bounded(
            dict(intent_id=intent["intent_id"], summary_hash=summary_hash, result=result)
        )
        for reference in references:
            conn.execute(
                "INSERT INTO migration_owners VALUES (?,?,?)",
                (job_id, reference["owner"], canonical_json(reference)),
            )
        conn.execute(
            "UPDATE migration_jobs SET staging_cursor=?,completed_frontier=?,pending=NULL,counts=?,coverage=?,state='active',last_ack=?,last_refusal=NULL,owner_references=owner_references+?,semantic_pending=semantic_pending+? WHERE job_id=?",
            (
                canonical_json(after),
                canonical_json(frontier),
                canonical_json(counters),
                intent["coverage"],
                canonical_json(last_ack),
                len(references),
                sum(r["semantic_disposition"] == "pending" for r in references),
                job_id,
            ),
        )
        return result

    def layout_bind(self, job_id, plans, envelope):
        from .lance_layout_directory import _directory

        return self._run(True, lambda conn: _directory(conn).bind(job_id, plans, envelope))

    def layout_prepare(self, job_id, expected_cursor, intent):
        from .lance_layout_directory import _directory

        return self._run(
            True, lambda conn: _directory(conn).prepare(job_id, expected_cursor, intent)
        )

    def layout_ack(self, job_id, intent_id, verified_summary):
        from .lance_layout_directory import _directory

        return self._run(
            True, lambda conn: _directory(conn).ack(job_id, intent_id, verified_summary)
        )

    def layout_routes(self, ids):
        from .lance_layout_directory import _directory

        return self._run(False, lambda conn: _directory(conn).routes(ids))

    def layout_maintenance_prepare(self, partition_id, version):
        from .lance_layout_directory import _NativeLayoutDirectory

        return self._run(
            True,
            lambda conn: _NativeLayoutDirectory(conn).maintenance_prepare(partition_id, version),
        )

    def layout_rebuild_prepare(self, partition_id, version):
        from .lance_layout_directory import _NativeLayoutDirectory

        return self._run(
            True, lambda conn: _NativeLayoutDirectory(conn).rebuild_prepare(partition_id, version)
        )

    def layout_reconcile(self, page):
        from .lance_layout_directory import _NativeLayoutDirectory

        return self._run(True, lambda conn: _NativeLayoutDirectory(conn).reconcile(page))

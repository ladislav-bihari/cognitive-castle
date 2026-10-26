"""Placement metadata on the queue's existing transaction.

Schema4 supplies derived routes only. Opt-in schema5 additionally tracks private
registered authority and pre-write claims. No method commits or opens native
datasets. Exact metadata readers retain their exact supervisor's memory profile.
"""

from contextlib import contextmanager
import json
import os
from pathlib import Path

from . import lance_layout_codec as codec
from . import migration_codec as migration
from . import bulk_exact_codec as bulk
from .lance_exact_store import LanceExactStore, _StoreError, _require_supervision
from .semantic_profile import canonical_json

DDL = {
    "layout_jobs": "CREATE TABLE layout_jobs (job_id TEXT PRIMARY KEY, envelope TEXT NOT NULL)",
    "layout_partitions": "CREATE TABLE layout_partitions (partition_id INTEGER PRIMARY KEY, envelope_hash TEXT NOT NULL, reservation TEXT NOT NULL)",
    "layout_homes": "CREATE TABLE layout_homes (owner TEXT PRIMARY KEY, job_id TEXT NOT NULL, plan TEXT NOT NULL, plan_hash TEXT UNIQUE NOT NULL, partition_id INTEGER NOT NULL, published INTEGER NOT NULL CHECK(published IN (0,1)))",
    "layout_routes": "CREATE TABLE layout_routes (id TEXT PRIMARY KEY, kind TEXT NOT NULL, row_hash TEXT, partition_id INTEGER NOT NULL, gate TEXT NOT NULL, intent_id TEXT NOT NULL, job_id TEXT NOT NULL, version INTEGER, receipt TEXT, state TEXT NOT NULL CHECK(state IN ('reserved','staged','published')))",
}
INDEXES = {
    "layout_homes_job_gate": "CREATE INDEX layout_homes_job_gate ON layout_homes(job_id,published)",
    "layout_routes_job_state": "CREATE INDEX layout_routes_job_state ON layout_routes(job_id,state)",
}
NATIVE_DDL = {
    "layout_authority": "CREATE TABLE layout_authority (id INTEGER PRIMARY KEY CHECK(id=1), envelope TEXT NOT NULL, qualification_hash TEXT NOT NULL, epoch TEXT NOT NULL, ready INTEGER NOT NULL CHECK(ready IN (0,1)))",
    "layout_registry": "CREATE TABLE layout_registry (partition_id INTEGER PRIMARY KEY, pin TEXT NOT NULL, mutation TEXT, active_owner TEXT, state TEXT NOT NULL CHECK(state IN ('empty','writable','sealed')), rebuild_cursor INTEGER NOT NULL, rebuild_token TEXT NOT NULL, rebuild_version INTEGER NOT NULL, claim_count INTEGER NOT NULL, last_page TEXT)",
    "layout_claims": "CREATE TABLE layout_claims (id TEXT PRIMARY KEY, claim TEXT NOT NULL, job_id TEXT NOT NULL, partition_id INTEGER NOT NULL, gate TEXT NOT NULL, intent_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','confirmed')), seen_token TEXT)",
    "layout_spans": "CREATE TABLE layout_spans (owner TEXT PRIMARY KEY, first_row INTEGER NOT NULL, next_row INTEGER NOT NULL, first_fragment INTEGER NOT NULL, next_fragment INTEGER NOT NULL)",
}
NATIVE_INDEXES = {
    "layout_claims_intent": "CREATE INDEX layout_claims_intent ON layout_claims(intent_id,state)",
    "layout_registry_active": "CREATE INDEX layout_registry_active ON layout_registry(active_owner)",
    "layout_claims_job_state": "CREATE INDEX layout_claims_job_state ON layout_claims(job_id,state)",
}
BASE_TABLES = {
    "groups",
    "schedule",
    "acknowledgements",
    "intents",
    "migration_jobs",
    "migration_owners",
}


def require_attachment_column(conn):
    size = conn.execute(
        "SELECT length(CAST(sql AS BLOB)) FROM sqlite_master WHERE type='table' AND name='migration_jobs'"
    ).fetchone()
    if size is None or size[0] is None or size[0] > 4096:
        raise _StoreError("unavailable")
    expected = (
        (0, "job_id", "TEXT", 0, None, 1),
        (1, "registration", "TEXT", 1, None, 0),
        (2, "staging_cursor", "TEXT", 1, None, 0),
        (3, "completed_frontier", "TEXT", 1, None, 0),
        (4, "pending", "TEXT", 0, None, 0),
        (5, "counts", "TEXT", 1, None, 0),
        (6, "coverage", "TEXT", 1, None, 0),
        (7, "state", "TEXT", 1, None, 0),
        (8, "last_ack", "TEXT", 0, None, 0),
        (9, "last_refusal", "TEXT", 0, None, 0),
        (10, "owner_references", "INTEGER", 1, "0", 0),
        (11, "semantic_pending", "INTEGER", 1, "0", 0),
        (12, "layout_attachment", "TEXT", 0, None, 0),
    )
    columns = conn.execute("PRAGMA table_info(migration_jobs)").fetchmany(14)
    if [tuple(row) for row in columns] != list(expected):
        raise _StoreError("unavailable")
    sizes = conn.execute(
        "SELECT length(CAST(name AS BLOB)) FROM sqlite_master WHERE type='index' AND tbl_name='migration_jobs' LIMIT 2"
    ).fetchall()
    if len(sizes) != 1 or sizes[0][0] > 128:
        raise _StoreError("unavailable")
    indexes = conn.execute("PRAGMA index_list(migration_jobs)").fetchmany(2)
    if [tuple(row) for row in indexes] != [(0, "sqlite_autoindex_migration_jobs_1", 1, "pk", 0)]:
        raise _StoreError("unavailable")
    keys = conn.execute("PRAGMA index_xinfo(sqlite_autoindex_migration_jobs_1)").fetchmany(3)
    if [tuple(row) for row in keys] != [
        (0, 0, "job_id", 0, "BINARY", 1),
        (1, -1, None, 0, "BINARY", 0),
    ]:
        raise _StoreError("unavailable")


def validate_first_bind(row, reg):
    before = migration.initial_source_cursor(reg)
    if (
        row["pending"] is not None
        or json.loads(row["staging_cursor"]) != before
        or json.loads(row["completed_frontier"]) != before
        or migration.counts(json.loads(row["counts"])) != migration.empty_counts()
        or row["owner_references"] != 0
        or row["semantic_pending"] != 0
        or row["last_ack"] is not None
        or row["last_refusal"] is not None
        or row["state"] != "active"
        or row["coverage"] != "incomplete"
    ):
        _conflict()


def _conflict():
    # A normal conflict return would COMMIT earlier changes in SemanticQueue.
    raise _StoreError("conflict")


def _metadata(text):
    if not isinstance(text, str) or len(text.encode("utf-8")) > migration.MAX_METADATA:
        raise _StoreError("unavailable")
    return json.loads(text)


def _ids(ids):
    if not isinstance(ids, list) or not 1 <= len(ids) <= 256:
        raise ValueError("route request bound")
    for identity in ids:
        bulk.identity(
            identity,
            "memory" if isinstance(identity, str) and identity.startswith("memory:") else "receipt",
        )
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate route request")
    migration.bounded(ids)


class LayoutDirectory:
    def __init__(self, conn):
        self.conn = conn

    def _schema(self):
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        tables = DDL | (NATIVE_DDL if version == 5 else {})
        indexes = INDEXES | (NATIVE_INDEXES if version == 5 else {})
        maximum = len(BASE_TABLES) + len(tables) + len(indexes)
        lengths = self.conn.execute(
            "SELECT length(CAST(name AS BLOB)),length(CAST(type AS BLOB)),length(CAST(sql AS BLOB)) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 20"
        ).fetchall()
        if len(lengths) != maximum or any(
            a is None or a > 64 or b is None or b > 16 or c is None or c > 4096
            for a, b, c in lengths
        ):
            raise _StoreError("unavailable")
        rows = self.conn.execute(
            "SELECT name,type,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 20"
        ).fetchall()
        if len(rows) != maximum:
            raise _StoreError("unavailable")
        seen = set()
        for row in rows:
            name, kind, sql = row
            expected_kind = "index" if name in indexes else "table"
            if (
                kind != expected_kind
                or name not in BASE_TABLES | tables.keys() | indexes.keys()
                or name in seen
            ):
                raise _StoreError("unavailable")
            if name in tables and sql != tables[name]:
                raise _StoreError("unavailable")
            if name in indexes and sql != indexes[name]:
                raise _StoreError("unavailable")
            seen.add(name)
        if version not in (4, 5):
            raise _StoreError("unavailable")
        require_attachment_column(self.conn)

    def attachment(self, job_id):
        """Recognize attachment before any v1 pending decode or replay shortcut."""
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='layout_jobs'"
        ).fetchone()
        if version < 4:
            if exists:
                raise _StoreError("unavailable")
            return None
        if version not in (4, 5):
            raise _StoreError("unavailable")
        require_attachment_column(self.conn)
        marker_row = self.conn.execute(
            "SELECT length(CAST(layout_attachment AS BLOB)),CASE WHEN typeof(layout_attachment)='text' AND length(CAST(layout_attachment AS BLOB))<=512 THEN layout_attachment ELSE NULL END FROM migration_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if marker_row is not None and marker_row[0] is not None and marker_row[0] > 512:
            raise _StoreError("unavailable")
        if marker_row is not None and marker_row[0] is not None and marker_row[1] is None:
            raise _StoreError("unavailable")
        if marker_row is None or marker_row[1] is None:
            if (
                exists
                and self.conn.execute(
                    "SELECT 1 FROM layout_jobs WHERE job_id=?", (job_id,)
                ).fetchone()
            ):
                raise _StoreError("unavailable")
            return None
        if marker_row[0] > 512 or not exists:
            raise _StoreError("unavailable")
        marker = codec.validate_attachment(json.loads(marker_row[1]))
        self._schema()
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(envelope)='text' AND length(CAST(envelope AS BLOB))<=196608 THEN envelope ELSE NULL END FROM layout_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise _StoreError("unavailable")
        envelope = codec.validate_envelope(_metadata(row[0]))
        if marker["envelope_hash"] != migration.hashed(envelope):
            raise _StoreError("unavailable")
        self._identity(envelope)
        return migration.bounded(dict(**marker, envelope=envelope))

    def _identity(self, envelope):
        databases = self.conn.execute("PRAGMA database_list").fetchmany(2)
        if len(databases) != 1 or databases[0][1] != "main":
            raise _StoreError("unavailable")
        path = Path(databases[0][2])
        generation = Path(envelope["generation"]["path"])
        if path != generation / "coordination" / "queue.sqlite3":
            _conflict()
        _metadata_paths(generation)
        info = generation.stat()
        if (info.st_dev, info.st_ino) != (
            envelope["generation"]["device"],
            envelope["generation"]["inode"],
        ):
            _conflict()

    def _state(self, job_id):
        from .semantic_queue import SemanticQueue

        bulk.sha(job_id)
        attachment = self.attachment(job_id)
        if attachment is None:
            raise _StoreError("unavailable")
        row = SemanticQueue._migration_row(self.conn, job_id)
        state = SemanticQueue._migration_state(row, attachment)
        if state["registration"]["destination"] != attachment["envelope"]["generation"]["path"]:
            _conflict()
        return state

    def bind(self, job_id, plans, envelope):
        from .semantic_queue import SemanticQueue

        bulk.sha(job_id)
        codec.validate_envelope(envelope)
        if not isinstance(plans, list) or not 1 <= len(plans) <= codec.MAX_PLANS:
            raise ValueError("owner plan bound")
        migration.bounded(dict(job_id=job_id, plans=plans, envelope=envelope))
        row = SemanticQueue._migration_row(self.conn, job_id)
        reg = migration.validate_registration(json.loads(row["registration"]))
        self._identity(envelope)
        if reg["destination"] != envelope["generation"]["path"]:
            _conflict()
        for plan in plans:
            codec.validate_owner_plan(plan, reg)
        if len({p["owner"] for p in plans}) != len(plans):
            raise ValueError("duplicate owner plan")
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version in (4, 5):
            require_attachment_column(self.conn)
            attached = self.attachment(job_id)
        else:
            attached = None
        if attached is None:
            validate_first_bind(row, reg)
        if version == 3:
            self.conn.execute("ALTER TABLE migration_jobs ADD COLUMN layout_attachment TEXT")
            self.conn.execute("PRAGMA user_version=4")
        elif version not in (4, 5):
            raise _StoreError("unavailable")
        require_attachment_column(self.conn)
        # Only the one initial bind creates derived tables, inside this transaction.
        present = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE name LIKE 'layout_%' LIMIT 14"
        ).fetchall()
        expected_objects = (
            DDL.keys()
            | INDEXES.keys()
            | (NATIVE_DDL.keys() | NATIVE_INDEXES.keys() if version == 5 else set())
        )
        if present and {r[0] for r in present} != expected_objects:
            raise _StoreError("unavailable")
        if not present:
            if row["pending"] is not None or json.loads(row["staging_cursor"])["owner"] is not None:
                _conflict()
            for sql in (*DDL.values(), *INDEXES.values()):
                self.conn.execute(sql)
        self._schema()
        previous = self.conn.execute(
            "SELECT CASE WHEN typeof(envelope)='text' AND length(CAST(envelope AS BLOB))<=196608 THEN envelope ELSE NULL END FROM layout_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        encoded = canonical_json(envelope)
        if previous and previous[0] != encoded:
            _conflict()
        # One immutable generation envelope, including across jobs.
        other = self.conn.execute(
            "SELECT CASE WHEN typeof(envelope)='text' AND length(CAST(envelope AS BLOB))<=196608 THEN envelope ELSE NULL END FROM layout_jobs LIMIT 1"
        ).fetchone()
        if other and other[0] != encoded:
            _conflict()
        if previous is None and row["pending"] is not None:
            _conflict()
        updates, homes, targets = self._placement(job_id, plans, envelope)
        response = migration.bounded(
            dict(
                status="ok",
                layout=dict(
                    format=codec.ATTACHMENT,
                    envelope_hash=migration.hashed(envelope),
                    envelope=envelope,
                ),
                targets=targets,
            )
        )
        if attached is None:
            pristine = SemanticQueue._migration_state(row)
            migration.bounded({**pristine, "layout": response["layout"]})
        # Everything, including the complete response, was validated before writes.
        if previous is None:
            self.conn.execute("INSERT INTO layout_jobs VALUES (?,?)", (job_id, encoded))
            marker = codec.validate_attachment(
                dict(format=codec.ATTACHMENT, envelope_hash=migration.hashed(envelope))
            )
            self.conn.execute(
                "UPDATE migration_jobs SET layout_attachment=? WHERE job_id=?",
                (canonical_json(marker), job_id),
            )
        for pid, reservation in updates.items():
            self.conn.execute(
                "INSERT INTO layout_partitions VALUES (?,?,?) ON CONFLICT(partition_id) DO UPDATE SET reservation=excluded.reservation",
                (pid, migration.hashed(envelope), canonical_json(reservation)),
            )
        for plan, pid in homes:
            self.conn.execute(
                "INSERT INTO layout_homes VALUES (?,?,?,?,?,0)",
                (plan["owner"], job_id, canonical_json(plan), migration.hashed(plan), pid),
            )
        return response

    def _placement(self, job_id, plans, envelope, *, stop_partition=None):
        if stop_partition is not None:
            bulk.integer(stop_partition)
            if stop_partition >= envelope["max_partitions"]:
                raise ValueError("prefix placement partition")
        last = self.conn.execute(
            "SELECT partition_id,CASE WHEN length(envelope_hash)=64 THEN envelope_hash ELSE NULL END,CASE WHEN typeof(reservation)='text' AND length(CAST(reservation AS BLOB))<=4096 THEN reservation ELSE NULL END FROM layout_partitions ORDER BY partition_id DESC LIMIT 1"
        ).fetchone()
        partition = last[0] if last else 0
        usage = self._partition(partition, envelope) if last else dict.fromkeys(codec.EXPANSION, 0)
        if last and last[1] != migration.hashed(envelope):
            _conflict()
        if usage.keys() != set(codec.EXPANSION):
            raise _StoreError("corrupt")
        for value in usage.values():
            bulk.integer(value)
        if (
            partition >= envelope["max_partitions"]
            or partition < 0
            or any(usage[k] > envelope["limits"][k] for k in codec.EXPANSION)
        ):
            raise _StoreError("unavailable")
        updates, homes, targets = {}, [], []
        for plan in plans:
            old = self.conn.execute(
                "SELECT job_id,CASE WHEN typeof(plan)='text' AND length(CAST(plan AS BLOB))<=196608 THEN plan ELSE NULL END,partition_id,CASE WHEN length(CAST(plan_hash AS BLOB))=64 THEN plan_hash ELSE NULL END FROM layout_homes WHERE owner=? AND length(CAST(job_id AS BLOB))=64 AND typeof(partition_id)='integer'",
                (plan["owner"],),
            ).fetchone()
            if old:
                if (
                    old[0] != job_id
                    or old[1] != canonical_json(plan)
                    or old[3] != migration.hashed(plan)
                ):
                    _conflict()
                reservation = self._partition(old[2], envelope)
                if any(reservation[k] < plan["expansion"][k] for k in codec.EXPANSION):
                    raise _StoreError("unavailable")
                if stop_partition is not None and old[2] != stop_partition:
                    break
                targets.append(codec.target(envelope, old[2], [plan]))
                continue
            if any(plan["expansion"][k] > envelope["limits"][k] for k in codec.EXPANSION):
                if stop_partition is not None:
                    break
                _conflict()
            total = {k: usage[k] + plan["expansion"][k] for k in codec.EXPANSION}
            if any(total[k] > envelope["limits"][k] for k in codec.EXPANSION):
                partition += 1
                total = dict(plan["expansion"])
            if stop_partition is not None and partition != stop_partition:
                break
            if partition >= envelope["max_partitions"]:
                _conflict()
            usage = total
            updates[partition] = total
            homes.append((plan, partition))
            targets.append(codec.target(envelope, partition, [plan]))
        return updates, homes, targets

    def _target(self, state, intent):
        target = codec.validate_target(intent["target"])
        envelope = state["layout"]["envelope"]
        if (
            target["generation"] != envelope["generation"]
            or target["envelope_hash"] != migration.hashed(envelope)
            or target["partition_id"] >= envelope["max_partitions"]
        ):
            _conflict()
        # Task2 handles one owner per frame. Task4 adds a checked ordered prefix.
        if len(target["owner_plan_hashes"]) != 1:
            _conflict()
        home = self.conn.execute(
            "SELECT owner,job_id,CASE WHEN typeof(plan)='text' AND length(CAST(plan AS BLOB))<=196608 THEN plan ELSE NULL END,partition_id,published FROM layout_homes WHERE plan_hash=? AND length(CAST(owner AS BLOB))=72 AND length(CAST(job_id AS BLOB))=64 AND typeof(partition_id)='integer' AND typeof(published)='integer' AND published IN (0,1)",
            (target["owner_plan_hashes"][0],),
        ).fetchone()
        if (
            home is None
            or home[1] != state["registration"]["job_id"]
            or home[3] != target["partition_id"]
            or home[4]
        ):
            _conflict()
        plan = codec.validate_owner_plan(_metadata(home[2]), state["registration"])
        if migration.hashed(plan) != target["owner_plan_hashes"][0] or plan["owner"] != home[0]:
            raise _StoreError("unavailable")
        reservation = self._partition(home[3], envelope)
        if any(reservation[k] < plan["expansion"][k] for k in codec.EXPANSION):
            raise _StoreError("unavailable")
        if plan["source_phase"] != intent["phase"]:
            _conflict()
        if any(r["gate"] != home[0] for r in intent["routes"]):
            _conflict()
        if intent["owners"] and {k: plan[k] for k in migration.OWNER_FIELDS} != intent["owners"][0]:
            _conflict()
        owner_key = intent["cursor_after"]["owner"]
        if owner_key is not None and owner_key != plan["owner_key"]:
            _conflict()
        return home

    def _partition(self, partition_id, envelope):
        bulk.integer(partition_id)
        if partition_id >= envelope["max_partitions"]:
            raise _StoreError("unavailable")
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(envelope_hash)='text' AND length(CAST(envelope_hash AS BLOB))=64 THEN envelope_hash ELSE NULL END,CASE WHEN typeof(reservation)='text' AND length(CAST(reservation AS BLOB))<=4096 THEN reservation ELSE NULL END FROM layout_partitions WHERE partition_id=?",
            (partition_id,),
        ).fetchone()
        if row is None or row[0] != migration.hashed(envelope):
            raise _StoreError("unavailable")
        reservation = codec._expansion(_metadata(row[1]))
        if any(reservation[k] > envelope["limits"][k] for k in codec.EXPANSION):
            raise _StoreError("unavailable")
        return reservation

    def prepare(self, job_id, expected_cursor, intent):
        migration.bounded(dict(job_id=job_id, expected_cursor=expected_cursor, intent=intent))
        state = self._state(job_id)
        migration.cursor(expected_cursor, state["registration"])
        codec.validate_layout_intent(intent, state["registration"])
        if (
            expected_cursor != state["staging_cursor"]
            or intent["cursor_before"] != expected_cursor
            or state["state"] == "snapshot_export_exhausted"
        ):
            _conflict()
        replay = state["pending"] is not None
        if replay:
            if state["pending"] != intent:
                _conflict()
        self._target(state, intent)
        codec.prospective_ack(intent)
        migration.bounded({**state, "pending": intent, "state": "active", "last_refusal": None})
        inserts = []
        authorities = {}
        for candidate in intent["routes"]:
            old = self._route_row(candidate["id"])
            if old:
                if old["state"] == "reserved":
                    if (
                        not replay
                        or old["intent_id"] != intent["intent_id"]
                        or old["job_id"] != job_id
                        or any(old[k] != candidate[k] for k in ("kind", "row_hash", "gate"))
                        or old["partition_id"] != intent["target"]["partition_id"]
                        or old["version"] is not None
                        or old["receipt"] is not None
                    ):
                        _conflict()
                    continue
                visible = self._route(old, authorities)
                if old["kind"] != candidate["kind"]:
                    _conflict()
                if visible is None or (
                    visible["state"] != "published" and old["gate"] != candidate["gate"]
                ):
                    _conflict()
                if candidate["kind"] != "bulk_record" and (
                    old["row_hash"] != candidate["row_hash"] or old["gate"] != candidate["gate"]
                ):
                    _conflict()
            else:
                inserts.append(candidate)
        if replay:
            if inserts:
                raise _StoreError("unavailable")
            return dict(status="ok", intent_id=intent["intent_id"])
        for candidate in inserts:
            self.conn.execute(
                "INSERT INTO layout_routes VALUES (?,?,?,?,?,?,?,NULL,NULL,'reserved')",
                (
                    candidate["id"],
                    candidate["kind"],
                    candidate["row_hash"],
                    intent["target"]["partition_id"],
                    candidate["gate"],
                    intent["intent_id"],
                    job_id,
                ),
            )
        self.conn.execute(
            "UPDATE migration_jobs SET pending=?,state='active',last_refusal=NULL WHERE job_id=?",
            (canonical_json(intent), job_id),
        )
        return dict(status="ok", intent_id=intent["intent_id"])

    def _route_row(self, identity):
        # BLOB lengths bound Python allocations before any JSON/codec validation.
        return self.conn.execute(
            "SELECT * FROM layout_routes WHERE id=? AND typeof(id)='text' AND length(CAST(id AS BLOB))<=72 AND typeof(kind)='text' AND length(CAST(kind AS BLOB))<=16 AND (row_hash IS NULL OR (typeof(row_hash)='text' AND length(CAST(row_hash AS BLOB))=64)) AND typeof(gate)='text' AND length(CAST(gate AS BLOB))=72 AND typeof(intent_id)='text' AND length(CAST(intent_id AS BLOB))=64 AND typeof(job_id)='text' AND length(CAST(job_id AS BLOB))=64 AND (receipt IS NULL OR (typeof(receipt)='text' AND length(CAST(receipt AS BLOB))=72)) AND typeof(state)='text' AND length(CAST(state AS BLOB))<=16 AND typeof(partition_id)='integer' AND (version IS NULL OR typeof(version)='integer')",
            (identity,),
        ).fetchone()

    def _route(self, row, authorities=None):
        if row["state"] == "reserved":
            return None
        gate = self.conn.execute(
            "SELECT published,partition_id,job_id,CASE WHEN typeof(plan_hash)='text' AND length(CAST(plan_hash AS BLOB))=64 THEN plan_hash ELSE NULL END,CASE WHEN typeof(plan)='text' AND length(CAST(plan AS BLOB))<=196608 THEN plan ELSE NULL END FROM layout_homes WHERE owner=? AND typeof(job_id)='text' AND length(CAST(job_id AS BLOB))=64 AND typeof(partition_id)='integer' AND typeof(published)='integer' AND published IN (0,1)",
            (row["gate"],),
        ).fetchone()
        if gate is None or gate[1] != row["partition_id"] or gate[2] != row["job_id"]:
            raise _StoreError("corrupt")
        authorities = {} if authorities is None else authorities
        if gate[2] not in authorities:
            if len(authorities) >= 256:
                raise _StoreError("unavailable")
            state = self._state(gate[2])
            authorities[gate[2]] = dict(registration=state["registration"], layout=state["layout"])
            migration.bounded(authorities)
        authority = authorities[gate[2]]
        plan = codec.validate_owner_plan(_metadata(gate[4]), authority["registration"])
        if plan["owner"] != row["gate"] or migration.hashed(plan) != gate[3]:
            raise _StoreError("unavailable")
        reservation = self._partition(row["partition_id"], authority["layout"]["envelope"])
        if any(reservation[k] < plan["expansion"][k] for k in codec.EXPANSION):
            raise _StoreError("unavailable")
        if row["state"] == "published" and not gate[0]:
            raise _StoreError("corrupt")
        return codec.validate_route(
            dict(
                format=codec.ROUTE,
                id=row["id"],
                kind=row["kind"],
                row_hash=row["row_hash"],
                partition_id=row["partition_id"],
                gate=row["gate"],
                version=row["version"],
                receipt=row["receipt"],
                state="published" if gate[0] else "staged",
            )
        )

    def routes(self, ids):
        _ids(ids)
        self._schema()
        # An envelope is required even for a hit. No attachment means unavailable.
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(job_id)='text' AND length(CAST(job_id AS BLOB))=64 THEN job_id ELSE NULL END FROM layout_jobs LIMIT 1"
        ).fetchone()
        if row is None or self.attachment(row[0]) is None:
            raise _StoreError("unavailable")
        found, unavailable, authorities = [], [], {}
        for identity in ids:
            row = self._route_row(identity)
            route = self._route(row, authorities) if row is not None else None
            if route is None or route["state"] != "published":
                unavailable.append(identity)
            else:
                found.append(route)
        return migration.bounded(
            dict(
                status="ok" if not unavailable else "unavailable",
                routes=found,
                unavailable=unavailable,
            )
        )

    def ack(self, job_id, intent_id, verified_summary):
        from .semantic_queue import SemanticQueue

        if intent_id is not None:
            bulk.sha(intent_id)
        migration.bounded(
            dict(job_id=job_id, intent_id=intent_id, verified_summary=verified_summary)
        )
        state = self._state(job_id)
        summary_hash = migration.hashed(verified_summary)
        old = state["last_ack"]
        if old and old["intent_id"] == intent_id and old["summary_hash"] == summary_hash:
            return old["result"]
        if intent_id is None:
            if (
                self.conn.execute(
                    "SELECT 1 FROM layout_homes WHERE job_id=? AND published=0 LIMIT 1", (job_id,)
                ).fetchone()
                or self.conn.execute(
                    "SELECT 1 FROM layout_routes WHERE job_id=? AND state='reserved' LIMIT 1",
                    (job_id,),
                ).fetchone()
            ):
                _conflict()
            return SemanticQueue._migration_terminal_ack(
                self.conn, job_id, state, verified_summary, summary_hash
            )
        intent = state["pending"]
        if (
            intent is None
            or intent["intent_id"] != intent_id
            or intent["cursor_before"] != state["staging_cursor"]
        ):
            _conflict()
        codec.validate_layout_verified(verified_summary, intent)
        home = self._target(state, intent)
        updates = []
        authorities = {}
        for candidate, observed in zip(intent["routes"], verified_summary["batch"]["routes"]):
            row = self._route_row(candidate["id"])
            if row is None:
                _conflict()
            if row["state"] == "reserved":
                if (
                    row["intent_id"] != intent_id
                    or row["job_id"] != job_id
                    or observed["partition_id"] != row["partition_id"]
                    or observed["gate"] != row["gate"]
                    or observed["receipt"] != codec.batch_receipt_id(intent)
                    or observed["state"] != "staged"
                ):
                    _conflict()
                if (
                    candidate["row_hash"] is not None
                    and observed["row_hash"] != candidate["row_hash"]
                ):
                    _conflict()
                updates.append(observed)
            elif self._route(row, authorities) != observed:
                # Reuse accepts only an exact existing pinned route. The exact
                # worker still must compare original canonical bytes/membership.
                _conflict()
        versions = [p["version"] for p in verified_summary["owner_proofs"]]
        for proof in verified_summary["owner_proofs"]:
            receipt = next(
                r for r in verified_summary["batch"]["routes"] if r["id"] == proof["owner"]
            )
            if receipt["version"] != proof["version"]:
                _conflict()
        # Common original ACK validation happens before any route mutation.
        SemanticQueue._migration_content_values(self.conn, job_id, state, intent, versions)
        for observed in updates:
            self.conn.execute(
                "UPDATE layout_routes SET row_hash=?,version=?,receipt=?,state=? WHERE id=?",
                (
                    observed["row_hash"],
                    observed["version"],
                    observed["receipt"],
                    "published" if intent["owners"] else "staged",
                    observed["id"],
                ),
            )
        if intent["owners"]:
            self.conn.execute("UPDATE layout_homes SET published=1 WHERE owner=?", (home[0],))
        return SemanticQueue._migration_content_ack(
            self.conn, job_id, state, intent, versions, summary_hash
        )


def _metadata_paths(generation):
    for path in (generation, *generation.parents):
        if path.is_symlink():
            raise _StoreError("unavailable")
    LanceExactStore._private(generation, directory=True)
    coordination = generation / "coordination"
    LanceExactStore._private(coordination, directory=True)
    # Exactly the existing queue path and optional rollback journal, never a new DB.
    with os.scandir(coordination) as entries:
        for count, entry in enumerate(entries, 1):
            if count > 2 or entry.name not in {"queue.sqlite3", "queue.sqlite3-journal"}:
                raise _StoreError("unavailable")
            LanceExactStore._private(Path(entry.path))
    LanceExactStore._private(coordination / "queue.sqlite3")


def load_layout_qualification(generation, qualification_hash):
    """Read the immutable operator artifact, without SQLite or a profile change."""
    import hashlib
    import stat

    codec.validate_generation(generation)
    bulk.sha(qualification_hash)
    root = Path(generation["path"])

    def identity_of(info):
        return tuple(
            getattr(info, "st_" + name)
            for name in ("dev", "ino", "size", "mtime_ns", "ctime_ns", "mode", "uid", "nlink")
        )

    if len(root.parts) > 64:
        raise ValueError("qualified generation path bound")
    try:
        for path in (root, *root.parents):
            if path.is_symlink():
                raise ValueError("qualified generation symlink")
        for path in (root, root / "exact"):
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("qualified private generation")
        identity = root.stat()
        if (identity.st_dev, identity.st_ino) != (generation["device"], generation["inode"]):
            raise ValueError("qualified generation identity")
        artifact = root / "exact/layout-qualification.json"
        descriptor = os.open(artifact, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.getuid()
                or before.st_mode & 0o777 != 0o400
                or before.st_nlink != 1
                or not 1 <= before.st_size <= 64 * 1024
            ):
                raise ValueError("immutable qualification artifact")
            raw = os.read(descriptor, 64 * 1024 + 1)
            if len(raw) != before.st_size or identity_of(os.fstat(descriptor)) != identity_of(
                before
            ):
                raise ValueError("qualification artifact changed")
        finally:
            os.close(descriptor)
        after = artifact.lstat()
        if (
            identity_of(after) != identity_of(before)
            or hashlib.sha256(raw).hexdigest() != qualification_hash
        ):
            raise ValueError("qualification artifact digest/identity")
        value = codec.validate_qualification_body(
            json.loads(raw), expected_hash=qualification_hash, generation=generation
        )
        if canonical_json(value).encode() != raw:
            raise ValueError("canonical qualification artifact")
        identity = root.stat()
        if (identity.st_dev, identity.st_ino) != (generation["device"], generation["inode"]):
            raise ValueError("qualified generation changed")
        return value
    except OSError:
        raise ValueError("unavailable qualification artifact") from None


@contextmanager
def readonly_directory(generation):
    """Exact-supervised metadata access; never the queue's 128/192MiB guard."""
    _require_supervision()
    generation = Path(generation)
    if (
        not generation.is_absolute()
        or str(generation) != str(generation.absolute())
        or ".." in generation.parts
    ):
        raise _StoreError("unavailable")
    _metadata_paths(generation)
    import sqlite3

    path = generation / "coordination" / "queue.sqlite3"
    before = path.stat()
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=0)
    conn.row_factory = sqlite3.Row
    try:
        heap = conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()
        if heap is None or type(heap[0]) is not int or not 0 < heap[0] <= 16777216:
            raise _StoreError("unavailable")
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA trusted_schema=OFF")
        conn.execute("PRAGMA cache_size=-512")
        conn.execute("PRAGMA mmap_size=0")
        conn.execute("BEGIN")
        directory = _directory(conn)
        directory._schema()
        yield directory
        _metadata_paths(generation)
        after = path.stat()
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise _StoreError("unavailable")
    finally:
        conn.close()


def _directory(conn):
    return (
        _NativeLayoutDirectory(conn)
        if conn.execute("PRAGMA user_version").fetchone()[0] == 5
        else LayoutDirectory(conn)
    )


def _claim_shape(value):
    codec._fields(
        value,
        {
            "id",
            "kind",
            "partition_id",
            "gate",
            "intent_id",
            "state",
            "version",
            "receipt",
            "row_hash",
        },
        "native ID claim",
    )
    bulk.identity(value["id"], "memory" if value["kind"] == "bulk_record" else "receipt")
    bulk.identity(value["gate"])
    bulk.sha(value["intent_id"])
    bulk.integer(value["partition_id"])
    if value["kind"] not in codec.KINDS or value["state"] not in {"reserved", "confirmed"}:
        raise _StoreError("unavailable")
    bulk.identity(value["receipt"])
    if value["row_hash"] is not None:
        bulk.sha(value["row_hash"])
    if value["state"] == "confirmed":
        codec.positive(value["version"])
        bulk.sha(value["row_hash"])
    elif value["version"] is not None:
        raise _StoreError("unavailable")
    return value


def _empty_pins(envelope):
    """Only the trusted initializer enumerates the fixed empty containers."""
    import stat

    maximum = envelope["max_partitions"]
    if maximum > codec.MAX_PLANS:
        raise ValueError("bounded registered partition count")
    root = Path(envelope["generation"]["path"])
    for path in (root, *root.parents):
        if path.is_symlink():
            raise _StoreError("unavailable")
    for path in (root, root / "exact", root / "exact/partitions", root / "exact/tmp"):
        LanceExactStore._private(path, directory=True)
    if (root.stat().st_dev, root.stat().st_ino) != (
        envelope["generation"]["device"],
        envelope["generation"]["inode"],
    ):
        _conflict()
    with os.scandir(root / "exact") as entries:
        names = set()
        for count, entry in enumerate(entries, 1):
            if count > 2 or entry.name not in {"partitions", "tmp"}:
                raise _StoreError("unavailable")
            names.add(entry.name)
        if names != {"partitions", "tmp"}:
            raise _StoreError("unavailable")
    with os.scandir(root / "exact/tmp") as entries:
        if next(entries, None) is not None:
            raise _StoreError("unavailable")
    parent = root / "exact/partitions"
    parent_identity = [parent.stat().st_dev, parent.stat().st_ino]
    pins = {}
    with os.scandir(parent) as entries:
        for count, entry in enumerate(entries, 1):
            if (
                count > maximum
                or not entry.name.isascii()
                or not entry.name.isdecimal()
                or str(int(entry.name)) != entry.name
                or int(entry.name) >= maximum
            ):
                raise _StoreError("unavailable")
            info = entry.stat(follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                raise _StoreError("unavailable")
            path = Path(entry.path)
            LanceExactStore._private(path, directory=True)
            with os.scandir(path) as children:
                if next(children, None) is not None:
                    raise _StoreError("unavailable")
            pid = int(entry.name)
            pins[str(pid)] = dict(
                format=codec.PARTITION_PIN,
                generation=envelope["generation"],
                qualification_hash=envelope["qualification_hash"],
                partition_id=pid,
                parent_identity=parent_identity,
                directory_identity=[info.st_dev, info.st_ino],
                version=0,
                physical_rows=0,
                fragments=0,
                index=dict(
                    version=None, segments=0, indexed_fragments=0, tail_fragments=0, tail_rows=0
                ),
            )
    if len(pins) != maximum:
        raise _StoreError("unavailable")
    migration.bounded(pins)
    return pins


def _register_layout_authority(conn, job_id, envelope, qualification, *, expected_hash):
    """Private root-bound fresh setup, never an ordinary worker request setter.

    Filesystem artifacts deliberately survive a failed SQL commit. An artifact
    without authority requires protected restoration; this function never repairs.
    """
    codec.validate_qualification(qualification, envelope, expected_hash=expected_hash)
    from .semantic_queue import SemanticQueue

    bulk.sha(job_id)
    if not conn.in_transaction:
        raise _StoreError("unavailable")
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version == 5:
        registered = _NativeLayoutDirectory(conn)
        if registered.envelope != envelope or registered.qualification != qualification:
            _conflict()
        if registered.attachment(job_id) is None:
            row = SemanticQueue._migration_row(conn, job_id)
            reg = migration.validate_registration(_metadata(row["registration"]))
            validate_first_bind(row, reg)
            if reg["destination"] != envelope["generation"]["path"]:
                _conflict()
            marker = dict(format=codec.ATTACHMENT, envelope_hash=migration.hashed(envelope))
            conn.execute("INSERT INTO layout_jobs VALUES (?,?)", (job_id, canonical_json(envelope)))
            conn.execute(
                "UPDATE migration_jobs SET layout_attachment=? WHERE job_id=?",
                (canonical_json(marker), job_id),
            )
        return dict(status="ok", epoch=registered.epoch)
    if version != 3:
        raise _StoreError("unavailable")
    row = SemanticQueue._migration_row(conn, job_id)
    reg = migration.validate_registration(_metadata(row["registration"]))
    validate_first_bind(row, reg)
    jobs = conn.execute(
        "SELECT CASE WHEN length(CAST(job_id AS BLOB))=64 THEN job_id END,pending IS NOT NULL OR last_ack IS NOT NULL OR last_refusal IS NOT NULL OR owner_references<>0 OR semantic_pending<>0 OR state<>'active' FROM migration_jobs LIMIT 257"
    ).fetchall()
    if len(jobs) > codec.MAX_PLANS or any(
        identity is None or progressed for identity, progressed in jobs
    ):
        raise _StoreError("unavailable")
    directory = LayoutDirectory(conn)
    directory._identity(envelope)
    if (
        reg["destination"] != envelope["generation"]["path"]
        or conn.execute("SELECT 1 FROM sqlite_master WHERE name LIKE 'layout_%' LIMIT 1").fetchone()
    ):
        _conflict()
    pins = _empty_pins(envelope)
    for pin in pins.values():
        codec.validate_partition_pin(pin, qualification, envelope)
    epoch = migration.hashed(["lance-layout-authority-v1", envelope, pins])
    marker = dict(format=codec.ATTACHMENT, envelope_hash=migration.hashed(envelope))
    migration.bounded(dict(job_id=job_id, envelope=envelope, pins=pins, epoch=epoch))
    artifact = Path(reg["destination"]) / "exact/layout-qualification.json"
    raw = canonical_json(qualification).encode()
    if len(raw) > 64 * 1024:
        raise ValueError("qualification artifact bytes")
    fd = os.open(artifact, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(fd, raw[offset:])
            if written <= 0:
                raise OSError("qualification write")
            offset += written
        os.fsync(fd)
    finally:
        os.close(fd)
    if load_layout_qualification(envelope["generation"], expected_hash) != qualification:
        raise _StoreError("unavailable")
    conn.execute("ALTER TABLE migration_jobs ADD COLUMN layout_attachment TEXT")
    for sql in (*DDL.values(), *INDEXES.values(), *NATIVE_DDL.values(), *NATIVE_INDEXES.values()):
        conn.execute(sql)
    conn.execute("INSERT INTO layout_jobs VALUES (?,?)", (job_id, canonical_json(envelope)))
    conn.execute(
        "UPDATE migration_jobs SET layout_attachment=? WHERE job_id=?",
        (canonical_json(marker), job_id),
    )
    conn.execute(
        "INSERT INTO layout_authority VALUES (1,?,?,?,1)",
        (canonical_json(envelope), expected_hash, epoch),
    )
    for pid, pin in pins.items():
        pid = pin["partition_id"]
        token = migration.hashed([epoch, pid, "initial"])
        conn.execute(
            "INSERT INTO layout_registry VALUES (?,?,NULL,NULL,'empty',0,?,0,0,NULL)",
            (pid, canonical_json(pin), token),
        )
    conn.execute("PRAGMA user_version=5")
    return dict(status="ok", epoch=epoch)


class _NativeLayoutDirectory(LayoutDirectory):
    """Schema5 authority; derived routes never reconstruct ownership claims."""

    def __init__(self, conn):
        super().__init__(conn)
        self._schema()
        if conn.execute("PRAGMA user_version").fetchone()[0] != 5:
            raise _StoreError("unavailable")
        row = conn.execute(
            "SELECT CASE WHEN typeof(envelope)='text' AND length(CAST(envelope AS BLOB))<=196608 THEN envelope END,CASE WHEN length(CAST(qualification_hash AS BLOB))=64 THEN qualification_hash END,CASE WHEN length(CAST(epoch AS BLOB))=64 THEN epoch END FROM layout_authority WHERE id=1"
        ).fetchone()
        if row is None:
            raise _StoreError("unavailable")
        self.envelope = codec.validate_envelope(_metadata(row[0]))
        bulk.sha(row[1])
        self.epoch = bulk.sha(row[2])
        if row[1] != self.envelope["qualification_hash"]:
            raise _StoreError("unavailable")
        self._identity(self.envelope)
        self.qualification = load_layout_qualification(self.envelope["generation"], row[1])
        codec.validate_qualification(self.qualification, self.envelope, expected_hash=row[1])
        if not self._registry_complete():
            raise _StoreError("unavailable")

    def _registry_complete(self):
        maximum = self.envelope["max_partitions"]
        if not 1 <= maximum <= codec.MAX_PLANS:
            return False
        row = self.conn.execute(
            "SELECT count(*),min(partition_id),max(partition_id) FROM (SELECT partition_id FROM layout_registry LIMIT 257)"
        ).fetchone()
        # INTEGER PRIMARY KEY makes the bounded, contiguous identity set unique.
        return tuple(row) == (maximum, 0, maximum - 1)

    def _registry(self, pid):
        bulk.integer(pid)
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(pin)='text' AND length(CAST(pin AS BLOB))<=4096 THEN pin END,CASE WHEN mutation IS NULL THEN NULL WHEN typeof(mutation)='text' AND length(CAST(mutation AS BLOB))<=196608 THEN mutation END,CASE WHEN active_owner IS NULL THEN NULL WHEN length(CAST(active_owner AS BLOB))=72 THEN active_owner END,state,rebuild_cursor,rebuild_token,rebuild_version,claim_count,last_page,mutation IS NOT NULL,active_owner IS NOT NULL FROM layout_registry WHERE partition_id=? AND typeof(state)='text' AND length(CAST(state AS BLOB))<=16 AND typeof(rebuild_cursor)='integer' AND typeof(rebuild_version)='integer' AND typeof(claim_count)='integer' AND typeof(rebuild_token)='text' AND length(CAST(rebuild_token AS BLOB))=64 AND (last_page IS NULL OR (typeof(last_page)='text' AND length(CAST(last_page AS BLOB))<=256))",
            (pid,),
        ).fetchone()
        if row is None or row[9] and row[1] is None or row[10] and row[2] is None:
            raise _StoreError("unavailable")
        pin = codec.validate_partition_pin(_metadata(row[0]), self.qualification, self.envelope)
        if pin["partition_id"] != pid or row[3] not in {"empty", "writable", "sealed"}:
            raise _StoreError("unavailable")
        for number in (row[4], row[6], row[7]):
            bulk.integer(number)
        bulk.sha(row[5])
        if row[2] is not None:
            bulk.identity(row[2])
        mutation = _metadata(row[1]) if row[1] is not None else None
        maximum_cursor = pin["physical_rows"]
        allowed_versions = {0, pin["version"]}
        if mutation is not None:
            self._mutation_shape(mutation, pin)
            maximum_cursor = self._expected_pin(dict(pin=pin, mutation=mutation))["physical_rows"]
            allowed_versions.add(mutation["after"]["version"])
        if (
            row[4] > maximum_cursor
            or row[6] not in allowed_versions
            or row[7] != pin["physical_rows"]
        ):
            raise _StoreError("unavailable")
        return dict(
            pin=pin,
            mutation=mutation,
            active_owner=row[2],
            state=row[3],
            cursor=row[4],
            rebuild_token=row[5],
            rebuild_version=row[6],
            claim_count=row[7],
            last_page=_metadata(row[8]) if row[8] is not None else None,
        )

    def _mutation_shape(self, value, pin):
        codec._fields(
            value, {"kind", "intent_id", "before", "after", "new_ids"}, "partition mutation"
        )
        if value["kind"] not in {"append", "maintenance"} or value["before"] != pin:
            raise _StoreError("unavailable")
        after = value["after"]
        codec._fields(after, {"version", "physical_rows", "fragments"}, "mutation after")
        for number in after.values():
            bulk.integer(number)
        if value["kind"] == "append":
            bulk.sha(value["intent_id"])
            self._mutation_ids(value["new_ids"])
            if (
                len(value["new_ids"])
                > self.qualification["reservations"]["append"]["physical_rows"]
            ):
                raise _StoreError("unavailable")
            if after != dict(
                version=pin["version"] + 1,
                physical_rows=pin["physical_rows"] + len(value["new_ids"]),
                fragments=pin["fragments"] + 1,
            ):
                raise _StoreError("unavailable")
        elif (
            value["intent_id"] is not None
            or value["new_ids"]
            or after != {k: pin[k] + int(k == "version") for k in after}
        ):
            raise _StoreError("unavailable")

    @staticmethod
    def _mutation_ids(ids):
        if not isinstance(ids, list) or not 1 <= len(ids) <= codec.MAX_ROUTES:
            raise ValueError("mutation ID bound")
        for identity in ids:
            bulk.identity(
                identity,
                "memory"
                if isinstance(identity, str) and identity.startswith("memory:")
                else "receipt",
            )
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate mutation ID")

    def partition(self, pid):
        value = self._registry(pid)
        pin = value["pin"]
        path = Path(self.envelope["generation"]["path"]) / "exact/partitions" / str(pid)
        for candidate in (path, *path.parents):
            if candidate.is_symlink():
                raise _StoreError("unavailable")
        for candidate, identity in (
            (path.parent, pin["parent_identity"]),
            (path, pin["directory_identity"]),
        ):
            LanceExactStore._private(candidate, directory=True)
            info = candidate.lstat()
            if [info.st_dev, info.st_ino] != identity:
                raise _StoreError("unavailable")
        if value["claim_count"] != pin["physical_rows"]:
            raise _StoreError("unavailable")
        return value

    def _claim(self, identity):
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(claim)='text' AND length(CAST(claim AS BLOB))<=4096 THEN claim END,CASE WHEN typeof(job_id)='text' AND length(CAST(job_id AS BLOB))=64 THEN job_id END,partition_id,CASE WHEN length(CAST(gate AS BLOB))=72 THEN gate END,CASE WHEN length(CAST(intent_id AS BLOB))=64 THEN intent_id END,state FROM layout_claims WHERE id=? AND typeof(partition_id)='integer' AND typeof(state)='text' AND length(CAST(state AS BLOB))<=16",
            (identity,),
        ).fetchone()
        if row is None:
            # Type corruption must not turn an existing claim into absence.
            if self.conn.execute("SELECT 1 FROM layout_claims WHERE id=?", (identity,)).fetchone():
                raise _StoreError("unavailable")
            return None
        claim = _claim_shape(_metadata(row[0]))
        bulk.sha(row[1])
        if claim["id"] != identity or (
            claim["partition_id"],
            claim["gate"],
            claim["intent_id"],
            claim["state"],
        ) != tuple(row[2:]):
            raise _StoreError("unavailable")
        return claim

    def pending(self, intent_id):
        bulk.sha(intent_id)
        row = self.conn.execute(
            "SELECT CASE WHEN length(CAST(job_id AS BLOB))=64 THEN job_id END FROM layout_claims WHERE intent_id=? LIMIT 1",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise _StoreError("unavailable")
        state = self._state(bulk.sha(row[0]))
        intent = state["pending"]
        if intent is None or intent["intent_id"] != intent_id:
            raise _StoreError("unavailable")
        self._target(state, intent)
        registered = self.partition(intent["target"]["partition_id"])
        if (
            registered["mutation"] is None
            or registered["mutation"]["kind"] != "append"
            or registered["mutation"]["intent_id"] != intent_id
            or registered["active_owner"] != intent["routes"][0]["gate"]
        ):
            raise _StoreError("unavailable")
        return intent

    def owner_plan(self, owner):
        bulk.identity(owner)
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(plan)='text' AND length(CAST(plan AS BLOB))<=196608 THEN plan END,CASE WHEN length(CAST(job_id AS BLOB))=64 THEN job_id END,CASE WHEN length(CAST(plan_hash AS BLOB))=64 THEN plan_hash END,partition_id FROM layout_homes WHERE owner=? AND typeof(partition_id)='integer'",
            (owner,),
        ).fetchone()
        if row is None:
            raise _StoreError("unavailable")
        state = self._state(bulk.sha(row[1]))
        if state["layout"]["envelope"] != self.envelope:
            raise _StoreError("unavailable")
        plan = codec.validate_owner_plan(_metadata(row[0]), state["registration"])
        if plan["owner"] != owner or migration.hashed(plan) != row[2]:
            raise _StoreError("unavailable")
        self.partition(row[3])
        return plan

    def _ready_registry(self):
        row = self.conn.execute(
            "SELECT ready FROM layout_authority WHERE id=1 AND epoch=? AND typeof(ready)='integer'",
            (self.epoch,),
        ).fetchone()
        return (
            row is not None
            and row[0] == 1
            and self.conn.execute(
                "SELECT 1 FROM layout_registry WHERE mutation IS NOT NULL OR rebuild_cursor<>claim_count LIMIT 1"
            ).fetchone()
            is None
        )

    def _route_for_id(self, identity):
        row = self._route_row(identity)
        return super()._route(row) if row is not None else None

    def claim_route(self, identity):
        """Rebuild evidence comes from claims and gates, never a derived route."""
        claim = self._claim(identity)
        if claim is None or claim["state"] != "confirmed":
            return None
        self.owner_plan(claim["gate"])
        row = self.conn.execute(
            "SELECT published,partition_id FROM layout_homes WHERE owner=? AND typeof(published)='integer' AND published IN (0,1) AND typeof(partition_id)='integer'",
            (claim["gate"],),
        ).fetchone()
        if row is None or row[1] != claim["partition_id"]:
            raise _StoreError("unavailable")
        return codec.validate_route(
            dict(
                format=codec.ROUTE,
                **{k: claim[k] for k in codec.CANDIDATE_FIELDS},
                partition_id=claim["partition_id"],
                version=claim["version"],
                receipt=claim["receipt"],
                state="published" if row[0] else "staged",
            )
        )

    def resolve(self, ids, *, intent_id=None):
        _ids(ids)
        intent = self.pending(intent_id) if intent_id is not None else None
        gate = intent["routes"][0]["gate"] if intent else None
        found, absent, unavailable, claims = [], [], [], []
        for identity in ids:
            claim = self._claim(identity)
            if claim is None:
                complete = self._ready_registry() and self._route_row(identity) is None
                (absent if complete else unavailable).append(identity)
                continue
            if claim["state"] == "reserved":
                candidate = (
                    next((row for row in intent["routes"] if row["id"] == identity), None)
                    if intent
                    else None
                )
                if (
                    intent is not None
                    and claim["intent_id"] == intent_id
                    and claim["gate"] == gate
                    and candidate is not None
                    and all(claim[k] == candidate[k] for k in codec.CANDIDATE_FIELDS)
                    and claim["partition_id"] == intent["target"]["partition_id"]
                    and claim["receipt"] == codec.batch_receipt_id(intent)
                ):
                    claims.append(claim)
                else:
                    unavailable.append(identity)
                continue
            route = self._route_for_id(identity)
            if route is None or any(
                route[k] != claim[k]
                for k in ("id", "kind", "partition_id", "gate", "version", "receipt", "row_hash")
            ):
                unavailable.append(identity)
            elif route["state"] == "published" or intent is not None and route["gate"] == gate:
                self.partition(route["partition_id"])
                found.append(route)
            else:
                unavailable.append(identity)
        return migration.bounded(
            dict(
                status="unavailable" if unavailable else "ok",
                routes=found,
                absent=absent,
                unavailable=unavailable,
                claims=claims,
            )
        )

    def routes(self, ids):
        result = self.resolve(ids)
        found = {row["id"] for row in result["routes"]}
        unavailable = [identity for identity in ids if identity not in found]
        return migration.bounded(
            dict(
                status="unavailable" if unavailable else "ok",
                routes=result["routes"],
                unavailable=unavailable,
            )
        )

    def _prefix_homes(self, job_id):
        from .lance_layout_packing import MAX_OWNERS

        rows = self.conn.execute(
            "SELECT CASE WHEN typeof(owner)='text' AND length(CAST(owner AS BLOB))=72 THEN owner END,CASE WHEN typeof(partition_id)='integer' THEN partition_id END FROM layout_homes WHERE job_id=? AND published=0 LIMIT 9",
            (job_id,),
        ).fetchall()
        if len(rows) > MAX_OWNERS:
            raise _StoreError("unavailable")
        for owner, pid in rows:
            try:
                bulk.identity(owner)
                bulk.integer(pid)
            except ValueError:
                raise _StoreError("unavailable") from None
            if pid >= self.envelope["max_partitions"]:
                raise _StoreError("unavailable")
        if len({row[0] for row in rows}) != len(rows) or len({row[1] for row in rows}) > 1:
            raise _StoreError("unavailable")
        return [tuple(row) for row in rows]

    def _prefix_anchor(self):
        rows = self.conn.execute(
            "SELECT CASE WHEN typeof(active_owner)='text' AND length(CAST(active_owner AS BLOB))=72 THEN active_owner END,partition_id FROM layout_registry WHERE active_owner IS NOT NULL LIMIT 2"
        ).fetchall()
        if len(rows) > 1:
            raise _StoreError("unavailable")
        for owner, pid in rows:
            try:
                bulk.identity(owner)
                bulk.integer(pid)
            except ValueError:
                raise _StoreError("unavailable") from None
            if pid >= self.envelope["max_partitions"]:
                raise _StoreError("unavailable")
        return tuple(rows[0]) if rows else None

    def context(self, job_id):
        state = self._state(job_id)
        if state["layout"]["envelope"] != self.envelope:
            raise _StoreError("unavailable")
        homes = self._prefix_homes(job_id)
        if len(homes) > 1:
            from .lance_layout_packed import INTENT, validate_prefix_intent

            anchor = self._prefix_anchor()
            if anchor is None or anchor[0] not in {r[0] for r in homes} or anchor[1] != homes[0][1]:
                raise _StoreError("unavailable")
            pending = state["pending"]
            target = None
            if pending is not None:
                if pending["format"] != INTENT:
                    raise _StoreError("unavailable")
                validate_prefix_intent(pending, state["registration"])
                if {o["owner"] for o in pending["owners"]} != {r[0] for r in homes}:
                    raise _StoreError("unavailable")
                target = pending["target"]
            return migration.bounded(
                dict(
                    format="lance-layout-native-context-v2",
                    generation=self.envelope["generation"],
                    qualification_hash=self.envelope["qualification_hash"],
                    epoch=self.epoch,
                    first_owner=anchor[0],
                    bound_plans=[self.owner_plan(owner) for owner, _ in sorted(homes)],
                    target=target,
                )
            )
        plan = self.owner_plan(homes[0][0]) if homes else None
        target = codec.target(self.envelope, homes[0][1], [plan]) if homes else None
        return migration.bounded(
            dict(
                format="lance-layout-native-context-v1",
                generation=self.envelope["generation"],
                qualification_hash=self.envelope["qualification_hash"],
                epoch=self.epoch,
                plan=plan,
                target=target,
            )
        )

    def bind(self, job_id, plans, envelope):
        if envelope != self.envelope or not isinstance(plans, list) or len(plans) != 1:
            _conflict()
        row = self.conn.execute(
            "SELECT CASE WHEN typeof(active_owner)='text' AND length(CAST(active_owner AS BLOB))=72 THEN active_owner END FROM layout_registry WHERE active_owner IS NOT NULL LIMIT 1"
        ).fetchone()
        if row and row[0] != plans[0]["owner"]:
            _conflict()
        if self.attachment(job_id) is None:
            raise _StoreError("unavailable")
        response = super().bind(job_id, plans, envelope)
        pid = response["targets"][0]["partition_id"]
        registered = self.partition(pid)
        if registered["state"] == "sealed" or registered["mutation"] is not None:
            _conflict()
        owner = plans[0]["owner"]
        span = self.conn.execute("SELECT 1 FROM layout_spans WHERE owner=?", (owner,)).fetchone()
        if span is None:
            pin = registered["pin"]
            self.conn.execute(
                "INSERT INTO layout_spans VALUES (?,?,?,?,?)",
                (
                    owner,
                    pin["physical_rows"],
                    pin["physical_rows"],
                    pin["fragments"],
                    pin["fragments"],
                ),
            )
        self.conn.execute(
            "UPDATE layout_registry SET state='sealed' WHERE partition_id<? AND state='writable'",
            (pid,),
        )
        self.conn.execute(
            "UPDATE layout_registry SET active_owner=? WHERE partition_id=?", (owner, pid)
        )
        migration.bounded({**self._state(job_id), "native_layout": self.context(job_id)})
        return response

    def bind_prefix(self, job_id, plans, envelope):
        from .lance_layout_packing import MAX_OWNERS

        bulk.sha(job_id)
        if envelope != self.envelope:
            _conflict()
        if not isinstance(plans, list) or not 1 <= len(plans) <= MAX_OWNERS:
            raise ValueError("prefix plan bound")
        if any(not isinstance(plan, dict) for plan in plans):
            raise ValueError("prefix original plans")
        if (
            len({p["owner"] for p in plans}) != len(plans)
            or len({p["source_phase"] for p in plans}) != 1
        ):
            raise ValueError("prefix original plan order")
        migration.bounded(dict(job_id=job_id, plans=plans, envelope=envelope))
        state = self._state(job_id)
        for plan in plans:
            codec.validate_owner_plan(plan, state["registration"])
        if (
            state["pending"] is not None
            or state["staging_cursor"]["owner"] is not None
            or state["state"] == "snapshot_export_exhausted"
        ):
            _conflict()
        if self.attachment(job_id) is None:
            raise _StoreError("unavailable")
        homes, anchor = self._prefix_homes(job_id), self._prefix_anchor()
        old_owners = {r[0] for r in homes}
        if old_owners != {p["owner"] for p in plans[: len(homes)]}:
            _conflict()
        if homes:
            if anchor is None or anchor != (plans[0]["owner"], homes[0][1]):
                _conflict()
        elif anchor is not None:
            _conflict()
        self.conn.execute("SAVEPOINT castle_prefix_bind")
        try:
            selected, pid = [], None
            for plan in plans:
                old = self.conn.execute(
                    "SELECT published FROM layout_homes WHERE owner=?", (plan["owner"],)
                ).fetchone()
                if old is not None and (type(old[0]) is not int or old[0] != 0):
                    _conflict()
                if pid is not None:
                    _, _, proposed = self._placement(job_id, [plan], envelope, stop_partition=pid)
                    if not proposed:
                        break
                self.conn.execute("SAVEPOINT castle_prefix_owner")
                try:
                    response = super().bind(job_id, [plan], envelope)
                    if len(response["targets"]) != 1:
                        raise _StoreError("unavailable")
                    target = codec.validate_target(response["targets"][0])
                    actual = target["partition_id"]
                    if pid is not None and actual != pid:
                        self.conn.execute("ROLLBACK TO castle_prefix_owner")
                        self.conn.execute("RELEASE castle_prefix_owner")
                        break
                    registered = self.partition(actual)
                    if registered["state"] == "sealed" or registered["mutation"] is not None:
                        _conflict()
                    pid = actual
                except Exception:
                    self.conn.execute("ROLLBACK TO castle_prefix_owner")
                    self.conn.execute("RELEASE castle_prefix_owner")
                    raise
                self.conn.execute("RELEASE castle_prefix_owner")
                selected.append(plan)
            if not selected or not old_owners <= {p["owner"] for p in selected}:
                _conflict()
            pin = self.partition(pid)["pin"]
            placeholder = (
                pin["physical_rows"],
                pin["physical_rows"],
                pin["fragments"],
                pin["fragments"],
            )
            for plan in selected:
                old = self.conn.execute(
                    "SELECT first_row,next_row,first_fragment,next_fragment FROM layout_spans WHERE owner=?",
                    (plan["owner"],),
                ).fetchone()
                if old is None:
                    self.conn.execute(
                        "INSERT INTO layout_spans VALUES (?,?,?,?,?)", (plan["owner"], *placeholder)
                    )
                elif tuple(old) != placeholder:
                    raise _StoreError("unavailable")
            self.conn.execute(
                "UPDATE layout_registry SET state='sealed' WHERE partition_id<? AND state='writable'",
                (pid,),
            )
            self.conn.execute(
                "UPDATE layout_registry SET active_owner=? WHERE partition_id=?",
                (selected[0]["owner"], pid),
            )
            response = migration.bounded(
                dict(
                    status="ok",
                    format="lance-layout-prefix-binding-v1",
                    target=codec.target(envelope, pid, selected),
                )
            )
            migration.bounded({**self._state(job_id), "native_layout": self.context(job_id)})
        except Exception:
            self.conn.execute("ROLLBACK TO castle_prefix_bind")
            self.conn.execute("RELEASE castle_prefix_bind")
            raise
        self.conn.execute("RELEASE castle_prefix_bind")
        return response

    def _expected_pin(self, registered):
        mutation = registered["mutation"]
        pin = registered["pin"]
        if mutation is None:
            return pin
        after = {**pin, **mutation["after"]}
        if mutation["kind"] == "append":
            after["index"] = {
                **pin["index"],
                "tail_rows": pin["index"]["tail_rows"] + len(mutation["new_ids"]),
                "tail_fragments": pin["index"]["tail_fragments"] + 1,
            }
        return codec.validate_partition_pin(after, self.qualification, self.envelope)

    def prepare(self, job_id, expected_cursor, intent):
        state = self._state(job_id)
        codec.validate_layout_intent(intent, state["registration"])
        migration.bounded({**state, "pending": intent, "native_layout": self.context(job_id)})
        registered = self.partition(intent["target"]["partition_id"])
        if registered["mutation"] is not None:
            if (
                registered["mutation"]["kind"] != "append"
                or registered["mutation"]["intent_id"] != intent["intent_id"]
            ):
                _conflict()
            return super().prepare(job_id, expected_cursor, intent)
        if not self._ready_registry() or registered["active_owner"] != intent["routes"][0]["gate"]:
            _conflict()
        new = []
        for candidate in intent["routes"]:
            claim = self._claim(candidate["id"])
            if claim is None:
                if self._route_row(candidate["id"]) is not None:
                    raise _StoreError("unavailable")
                new.append(candidate)
            elif claim["state"] != "confirmed" or self._route_for_id(candidate["id"]) is None:
                raise _StoreError("unavailable")
        if not new or len(new) > self.qualification["reservations"]["append"]["physical_rows"]:
            _conflict()
        pin = registered["pin"]
        if pin["index"]["tail_rows"] + len(new) > 1024 or pin["index"]["tail_fragments"] + 1 > 32:
            raise _StoreError("index_required")
        mutation = dict(
            kind="append",
            intent_id=intent["intent_id"],
            before=pin,
            after=dict(
                version=pin["version"] + 1,
                physical_rows=pin["physical_rows"] + len(new),
                fragments=pin["fragments"] + 1,
            ),
            new_ids=[r["id"] for r in new],
        )
        migration.bounded(mutation)
        self._expected_pin({**registered, "mutation": mutation})
        response = super().prepare(job_id, expected_cursor, intent)
        for candidate in new:
            claim = _claim_shape(
                dict(
                    **candidate,
                    partition_id=pin["partition_id"],
                    intent_id=intent["intent_id"],
                    state="reserved",
                    version=None,
                    receipt=codec.batch_receipt_id(intent),
                )
            )
            self.conn.execute(
                "INSERT INTO layout_claims VALUES (?,?,?,?,?,?,?,NULL)",
                (
                    claim["id"],
                    canonical_json(claim),
                    job_id,
                    pin["partition_id"],
                    claim["gate"],
                    intent["intent_id"],
                    "reserved",
                ),
            )
        self.conn.execute(
            "UPDATE layout_registry SET mutation=? WHERE partition_id=?",
            (canonical_json(mutation), pin["partition_id"]),
        )
        self.conn.execute("UPDATE layout_authority SET ready=0 WHERE id=1")
        return response

    def span(self, owner):
        self.owner_plan(owner)
        row = self.conn.execute(
            "SELECT first_row,next_row,first_fragment,next_fragment FROM layout_spans WHERE owner=? AND typeof(first_row)='integer' AND typeof(next_row)='integer' AND typeof(first_fragment)='integer' AND typeof(next_fragment)='integer'",
            (owner,),
        ).fetchone()
        if row is None:
            raise _StoreError("unavailable")
        result = dict(zip(("first_row", "next_row", "first_fragment", "next_fragment"), row))
        for number in result.values():
            bulk.integer(number)
        active = self.conn.execute(
            "SELECT partition_id FROM layout_registry WHERE active_owner=? LIMIT 1", (owner,)
        ).fetchone()
        if active:
            pin = self._expected_pin(self.partition(active[0]))
            result.update(next_row=pin["physical_rows"], next_fragment=pin["fragments"])
        else:
            home = self.conn.execute(
                "SELECT partition_id FROM layout_homes WHERE owner=? AND typeof(partition_id)='integer'",
                (owner,),
            ).fetchone()
            if home is None:
                raise _StoreError("unavailable")
            pin = self.partition(home[0])["pin"]
        if (
            result["next_row"] < result["first_row"]
            or result["next_fragment"] < result["first_fragment"]
            or result["next_row"] > pin["physical_rows"]
            or result["next_fragment"] > pin["fragments"]
        ):
            raise _StoreError("unavailable")
        return result

    def _refresh_ready(self):
        dirty = self.conn.execute(
            "SELECT 1 FROM layout_registry WHERE mutation IS NOT NULL OR rebuild_cursor<>claim_count LIMIT 1"
        ).fetchone()
        self.conn.execute("UPDATE layout_authority SET ready=? WHERE id=1", (int(dirty is None),))

    def ack(self, job_id, intent_id, verified_summary):
        bulk.sha(job_id)
        if intent_id is not None:
            bulk.sha(intent_id)
        migration.bounded(
            dict(job_id=job_id, intent_id=intent_id, verified_summary=verified_summary)
        )
        state = self._state(job_id)
        old = state["last_ack"]
        if (
            old
            and old["intent_id"] == intent_id
            and old["summary_hash"] == migration.hashed(verified_summary)
        ):
            return old["result"]
        if intent_id is None:
            if not self._ready_registry():
                _conflict()
            return super().ack(job_id, intent_id, verified_summary)
        intent = self.pending(intent_id)
        if intent["job_id"] != job_id:
            _conflict()
        codec.validate_layout_verified(verified_summary, intent)
        registered = self.partition(intent["target"]["partition_id"])
        mutation = registered["mutation"]
        if mutation is None or mutation["intent_id"] != intent_id:
            _conflict()
        after = self._expected_pin(registered)
        updates = []
        for observed in verified_summary["batch"]["routes"]:
            claim = self._claim(observed["id"])
            if claim is None:
                _conflict()
            if claim["state"] == "reserved":
                if (
                    claim["intent_id"] != intent_id
                    or observed["version"] != after["version"]
                    or any(
                        observed[k] != claim[k]
                        for k in ("id", "kind", "partition_id", "gate", "receipt")
                    )
                ):
                    _conflict()
                if claim["row_hash"] is not None and claim["row_hash"] != observed["row_hash"]:
                    _conflict()
                updates.append(
                    {
                        **claim,
                        "state": "confirmed",
                        "row_hash": observed["row_hash"],
                        "version": observed["version"],
                    }
                )
            elif any(
                observed[k] != claim[k]
                for k in ("id", "kind", "partition_id", "gate", "row_hash", "version", "receipt")
            ) or observed != self.claim_route(observed["id"]):
                _conflict()
        if {v["id"] for v in updates} != set(mutation["new_ids"]):
            _conflict()
        response = super().ack(job_id, intent_id, verified_summary)
        for claim in updates:
            self.conn.execute(
                "UPDATE layout_claims SET claim=?,state='confirmed' WHERE id=?",
                (canonical_json(claim), claim["id"]),
            )
        token = migration.hashed([registered["rebuild_token"], after["version"], "append"])
        # The current batch proof can extend a certified old prefix. It cannot
        # certify earlier owners whose explicit route rebuild was interrupted.
        next_cursor = (
            after["physical_rows"]
            if registered["cursor"] >= registered["pin"]["physical_rows"]
            else 0
        )
        self.conn.execute(
            "UPDATE layout_registry SET pin=?,mutation=NULL,state='writable',rebuild_cursor=?,rebuild_version=?,rebuild_token=?,claim_count=?,last_page=NULL,active_owner=? WHERE partition_id=?",
            (
                canonical_json(after),
                next_cursor,
                after["version"],
                token,
                after["physical_rows"],
                None if intent["owners"] else registered["active_owner"],
                after["partition_id"],
            ),
        )
        self.conn.execute(
            "UPDATE layout_spans SET next_row=?,next_fragment=? WHERE owner=?",
            (after["physical_rows"], after["fragments"], registered["active_owner"]),
        )
        self._refresh_ready()
        return response

    def maintenance_prepare(self, pid, version):
        registered = self.partition(pid)
        pin = registered["pin"]
        bulk.integer(version)
        if (
            registered["mutation"] is not None
            and registered["mutation"]["kind"] == "maintenance"
            and version == pin["version"]
        ):
            return dict(status="ok", rebuild_token=registered["rebuild_token"])
        if (
            bulk.integer(version) != pin["version"]
            or not version
            or registered["state"] == "sealed"
            or registered["mutation"] is not None
            or not self._ready_registry()
        ):
            _conflict()
        if pin["version"] + 1 > self.qualification["partition"]["max_versions"]:
            _conflict()
        mutation = dict(
            kind="maintenance",
            intent_id=None,
            before=pin,
            after=dict(
                version=version + 1, physical_rows=pin["physical_rows"], fragments=pin["fragments"]
            ),
            new_ids=[],
        )
        token = migration.hashed([registered["rebuild_token"], version, "maintenance"])
        self.conn.execute(
            "UPDATE layout_registry SET mutation=?,rebuild_version=?,rebuild_token=?,last_page=NULL WHERE partition_id=?",
            (canonical_json(mutation), version + 1, token, pid),
        )
        self.conn.execute("UPDATE layout_authority SET ready=0 WHERE id=1")
        return dict(status="ok", rebuild_token=token)

    def rebuild_prepare(self, pid, version):
        registered = self.partition(pid)
        if bulk.integer(version) != registered["pin"]["version"]:
            _conflict()
        if registered["mutation"] is not None and registered["mutation"]["kind"] != "append":
            _conflict()
        token = migration.hashed([registered["rebuild_token"], version, "rebuild"])
        self.conn.execute(
            "UPDATE layout_registry SET rebuild_cursor=0,rebuild_version=?,rebuild_token=?,last_page=NULL WHERE partition_id=?",
            (0 if registered["mutation"] else version, token, pid),
        )
        self.conn.execute("UPDATE layout_authority SET ready=0 WHERE id=1")
        return dict(status="ok", rebuild_token=token)

    def reconcile(self, page):
        migration.bounded(dict(page=page))
        codec.validate_directory_page(page, self.qualification, self.envelope)
        registered = self.partition(page["partition_id"])
        if page["epoch"] != self.epoch or page["rebuild_token"] != registered["rebuild_token"]:
            _conflict()
        fingerprint = migration.hashed(page)
        if registered["last_page"] == dict(hash=fingerprint, after=page["cursor_after"]):
            return dict(
                status="ok", next_cursor=page["cursor_after"], registry_ready=self._ready_registry()
            )
        mutation = registered["mutation"]
        if mutation is not None and mutation["kind"] == "maintenance":
            return self._maintenance_page(page, registered, fingerprint)
        allowed = [registered["pin"]]
        if mutation is not None:
            allowed.append(self._expected_pin(registered))
        if (
            page["pin"] not in allowed
            or registered["rebuild_version"] not in (0, page["version"])
            or page["cursor_before"] != registered["cursor"]
        ):
            _conflict()
        updates = []
        for observed in page["rows"]:
            claim = self._claim(observed["id"])
            if claim is None:
                raise _StoreError("unavailable")
            seen = self.conn.execute(
                "SELECT CASE WHEN typeof(seen_token)='text' AND length(CAST(seen_token AS BLOB))=64 THEN seen_token END,seen_token IS NOT NULL FROM layout_claims WHERE id=?",
                (observed["id"],),
            ).fetchone()
            if seen and seen[1] and seen[0] is None:
                raise _StoreError("unavailable")
            if seen and seen[0] == registered["rebuild_token"]:
                _conflict()
            if claim["state"] == "confirmed":
                if any(
                    observed[k] != claim[k]
                    for k in (
                        "id",
                        "kind",
                        "partition_id",
                        "gate",
                        "row_hash",
                        "version",
                        "receipt",
                    )
                ):
                    _conflict()
                if observed != self.claim_route(observed["id"]):
                    _conflict()
                updates.append(observed)
            elif (
                mutation is None
                or claim["intent_id"] != mutation["intent_id"]
                or observed["id"] not in mutation["new_ids"]
                or observed["partition_id"] != page["partition_id"]
                or observed["gate"] != claim["gate"]
                or claim["row_hash"] is not None
                and observed["row_hash"] != claim["row_hash"]
            ):
                _conflict()
        # Derived enumeration never confirms a reserved claim or changes a gate.
        for observed in updates:
            claim = self._claim(observed["id"])
            job = self.conn.execute(
                "SELECT CASE WHEN typeof(job_id)='text' AND length(CAST(job_id AS BLOB))=64 THEN job_id END FROM layout_claims WHERE id=?",
                (observed["id"],),
            ).fetchone()[0]
            bulk.sha(job)
            self.conn.execute(
                "INSERT INTO layout_routes VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,row_hash=excluded.row_hash,partition_id=excluded.partition_id,gate=excluded.gate,intent_id=excluded.intent_id,job_id=excluded.job_id,version=excluded.version,receipt=excluded.receipt,state=excluded.state",
                (
                    observed["id"],
                    observed["kind"],
                    observed["row_hash"],
                    observed["partition_id"],
                    observed["gate"],
                    claim["intent_id"],
                    job,
                    observed["version"],
                    observed["receipt"],
                    observed["state"],
                ),
            )
        for observed in page["rows"]:
            self.conn.execute(
                "UPDATE layout_claims SET seen_token=? WHERE id=?",
                (registered["rebuild_token"], observed["id"]),
            )
        self.conn.execute(
            "UPDATE layout_registry SET rebuild_cursor=?,rebuild_version=?,last_page=? WHERE partition_id=?",
            (
                page["cursor_after"],
                page["version"],
                canonical_json(dict(hash=fingerprint, after=page["cursor_after"])),
                page["partition_id"],
            ),
        )
        self._refresh_ready()
        return dict(
            status="ok", next_cursor=page["cursor_after"], registry_ready=self._ready_registry()
        )

    def _maintenance_page(self, page, registered, fingerprint):
        mutation = registered["mutation"]
        pin = page["pin"]
        if (
            page["cursor_before"] != 0
            or {k: pin[k] for k in mutation["after"]} != mutation["after"]
            or any(
                pin[k] != registered["pin"][k]
                for k in (
                    "generation",
                    "qualification_hash",
                    "partition_id",
                    "parent_identity",
                    "directory_identity",
                )
            )
            or pin["index"]["segments"] != 1
            or pin["index"]["tail_rows"]
            or pin["index"]["tail_fragments"]
            or pin["index"]["indexed_fragments"] != pin["fragments"]
        ):
            _conflict()
        for observed in page["rows"]:
            if observed != self.claim_route(observed["id"]):
                _conflict()
        self.conn.execute(
            "UPDATE layout_registry SET pin=?,mutation=NULL,rebuild_cursor=?,rebuild_version=?,last_page=? WHERE partition_id=?",
            (
                canonical_json(pin),
                registered["cursor"],
                pin["version"],
                canonical_json(dict(hash=fingerprint, after=page["cursor_after"])),
                pin["partition_id"],
            ),
        )
        self._refresh_ready()
        return dict(
            status="ok", next_cursor=page["cursor_after"], registry_ready=self._ready_registry()
        )

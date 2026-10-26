"""Read-only streaming adapter, imported only inside the guarded source worker.

A completed externally registered snapshot is required. Every owner is verified
in bounded streaming rereads before its finalization can be returned. No capture,
converter, snapshot creation, indexing, publication or checkpoint write runs here.
"""

from __future__ import annotations

import codecs
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from . import bulk_exact_codec as bulk
from .canonical_records import CanonicalRecord, _hash, _json, _string
from .fast_memory import MemoryStore, _Corrupt
from .memory_history import _HistoryError
from .historical_export_evidence import historical_scope_header, pages, member_text
from .sqlite_export_codec import (
    FORMAT,
    PACKING,
    file_identity,
    empty_counts,
    frame_bytes,
    initial_cursor,
    source_binding,
    validate_cursor,
    record_entry,
    layout_result,
)

# Original table shapes, not a schema to install. Unknown source tables refuse.
TABLES = {
    "chunks": ("id text provenance text_hash start end", "id"),
    "receipts": ("namespace request_id digest ids", "namespace request_id"),
    "history_occurrences": (
        "identity cursor_key row_address alias metadata_json columns_json text_hash ids metadata_hash columns_hash",
        "identity",
    ),
    "history_occurrence_chunks": ("identity chunk_id", "identity chunk_id"),
    "history_aliases": ("alias identity", "alias identity"),
    "history_attempts": ("cursor_key next_cursor outcome", "cursor_key"),
    "history_gaps": ("cursor_key identity reason", "cursor_key"),
    "history_gap_resolutions": (
        "cursor_key identity reason next_cursor snapshot_id text_hash metadata_hash columns_hash",
        "cursor_key",
    ),
    "history_staged_rows": (
        "cursor_key identity snapshot_id source_cursor next_cursor metadata_json columns_json text_bytes text_characters next_character chunk_start carry processed_bytes state inflight_character retry_character retry_count text_hash expected_hash reason phase_inflight hash_failures publication_failures final_deferrals",
        "cursor_key",
    ),
    "history_staged_chunks": ("cursor_key start end id text text_hash", "cursor_key start"),
    "capture_chunk_scopes": (
        "chunk_id source_key generation workspace attribution source_time",
        "chunk_id",
    ),
    "capture_generation_scopes": (
        "source_key generation workspace attribution state",
        "source_key generation",
    ),
    "capture_sources": ("source_key state value", "source_key"),
    "checkpoints": ("source_key value", "source_key"),
    "coverage_observations": ("sequence identity value", "sequence"),
}
AUDIT_TABLES = tuple(
    sorted(
        set(TABLES)
        - {
            "chunks",
            "receipts",
            "history_occurrences",
            "history_occurrence_chunks",
            "history_staged_chunks",
        }
    )
)
CHUNK_COLUMNS = (
    "CAST(substr(CAST(id AS BLOB),1,72) AS TEXT) AS id,"
    "CAST(substr(CAST(text AS BLOB),1,8193) AS TEXT) AS text,"
    "CAST(substr(CAST(provenance AS BLOB),1,513) AS TEXT) AS provenance,"
    "CAST(substr(CAST(text_hash AS BLOB),1,65) AS TEXT) AS text_hash,"
    "CASE WHEN typeof(start)='integer' THEN start END AS start,"
    "CASE WHEN typeof(end)='integer' THEN end END AS end"
)


class Refusal(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason


class Source:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.binding = source_binding(descriptor)
        self.until = time.monotonic() + 1.65  # Leave guarded startup/response/cleanup headroom.
        self.conn = None
        self.tables = set()
        self.manifest = None
        self.active_field = None
        self.active_operation = "validate_snapshot"

    def check_time(self):
        if time.monotonic() >= self.until:
            # Retrying requires qualification; never return successful nonprogress.
            raise Refusal("qualification_required", "summary_slice_deadline")

    @staticmethod
    def private(path, expected):
        path = Path(path)
        # System ancestors may be shared; the registered storage directory and
        # every descendant must be owned/private. All ancestors reject links.
        for ancestor in [path, *path.parents]:
            info = ancestor.lstat()
            if stat.S_ISLNK(info.st_mode):
                raise Refusal("stale", "linked_source_path")
        for target in (path.parent, path):
            info = target.lstat()
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise Refusal("stale", "unsafe_source_permissions")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or file_identity(info) != expected:
            raise Refusal("stale", "source_identity_changed")
        return path

    def validate_identities(self):
        d = self.descriptor
        self.private(d["path"], d["identity"])
        self.private(d["manifest_path"], d["manifest_identity"])
        for suffix in ("-journal", "-wal", "-shm"):
            if os.path.lexists(d["path"] + suffix):
                raise Refusal("stale", "snapshot_sidecar_present")

    def validate_files(self):
        d = self.descriptor
        path = self.private(d["path"], d["identity"])
        manifest_path = self.private(d["manifest_path"], d["manifest_identity"])
        if path == manifest_path or path.parent != manifest_path.parent:
            raise Refusal("unsupported", "manifest_location")
        for suffix in ("-journal", "-wal", "-shm"):
            if os.path.lexists(str(path) + suffix):
                raise Refusal("stale", "snapshot_sidecar_present")
        fd = os.open(manifest_path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            if file_identity(os.fstat(handle.fileno())) != d["manifest_identity"]:
                raise Refusal("stale", "manifest_identity_changed")
            raw = handle.read(65537)
        if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != d["manifest_hash"]:
            raise Refusal("stale", "manifest_binding_changed")
        manifest = json.loads(raw)
        if (
            not isinstance(manifest, dict)
            or manifest.keys()
            != {
                "format",
                "state",
                "store_id",
                "snapshot_id",
                "identity",
                "content_hash",
                "schema_hash",
                "original",
                "catalog",
                "high_water",
                "coverage_state",
            }
            or any(
                manifest[name] != d[name]
                for name in ("format", "store_id", "snapshot_id", "identity")
            )
        ):
            raise Refusal("unsupported", "registered_manifest_shape")
        if manifest["state"] != "complete":
            raise Refusal("unsupported", "snapshot_not_complete")
        bulk.sha(manifest["content_hash"])
        bulk.sha(manifest["schema_hash"])
        bulk.integer(manifest["high_water"])
        original = manifest["original"]
        if (
            not isinstance(original, dict)
            or original.keys() != {"archive_id", "snapshot_id", "version"}
            or not isinstance(manifest["catalog"], dict)
            or not isinstance(manifest["coverage_state"], str)
        ):
            raise Refusal("unsupported", "registered_original_facts")
        for name in ("archive_id", "snapshot_id"):
            if original[name] is not None:
                _string(original[name], 256, name)
        if original["version"] is not None:
            bulk.integer(original["version"])
        digest = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            if file_identity(os.fstat(handle.fileno())) != d["identity"]:
                raise Refusal("stale", "source_identity_changed")
            while piece := handle.read(65536):
                self.check_time()
                digest.update(piece)
            if file_identity(os.fstat(handle.fileno())) != d["identity"]:
                raise Refusal("stale", "source_identity_changed")
        if digest.hexdigest() != manifest["content_hash"]:
            raise Refusal("stale", "snapshot_content_changed")
        self.private(d["path"], d["identity"])
        self.private(d["manifest_path"], d["manifest_identity"])
        if self.manifest is not None and manifest != self.manifest:
            raise Refusal("stale", "manifest_changed")
        self.manifest = manifest
        return path

    def open(self):
        path = self.validate_files()
        # immutable is safe only for the already registered completed protected
        # view; sidecars have been refused. Never applied to live source storage.
        self.conn = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, timeout=0.05)
        self.conn.row_factory = sqlite3.Row
        self.conn.set_progress_handler(lambda: int(time.monotonic() >= self.until), 1000)
        for pragma in (
            "query_only=ON",
            "cache_size=-2048",
            "mmap_size=0",
            "temp_store=MEMORY",
            "trusted_schema=OFF",
        ):
            self.conn.execute("PRAGMA " + pragma)
        schema_digest = hashlib.sha256()
        schema_digest.update(b"[")
        first = True
        for row in self.conn.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
        ):
            self.check_time()
            if any(isinstance(v, str) and len(v.encode()) > 8192 for v in row):
                raise Refusal("unsupported", "schema_statement_size")
            if not first:
                schema_digest.update(b",")
            schema_digest.update(_json(list(row)).encode())
            first = False
            if row["type"] == "table":
                name = row["name"]
                if name in TABLES:
                    columns = list(self.conn.execute(f'PRAGMA table_info("{name}")'))
                    wanted, keys = TABLES[name]
                    if [r["name"] for r in columns] != wanted.split() or [
                        r["name"] for r in sorted(columns, key=lambda r: r["pk"]) if r["pk"]
                    ] != keys.split():
                        raise Refusal("unsupported", "source_table_shape")
                    self.tables.add(name)
                elif name not in {
                    "sqlite_sequence",
                    "lexical",
                    "lexical_data",
                    "lexical_idx",
                    "lexical_content",
                    "lexical_docsize",
                    "lexical_config",
                }:
                    raise Refusal("unsupported", "unknown_source_table")
        schema_digest.update(b"]")
        if schema_digest.hexdigest() != self.manifest["schema_hash"]:
            raise Refusal("stale", "schema_binding_changed")
        if not {"chunks", "receipts"} <= self.tables:
            raise Refusal("unsupported", "missing_source_tables")
        required_history = {"history_attempts", "history_occurrences", "history_gaps"}
        if not required_history <= self.tables:
            for table in required_history & self.tables:
                if self.conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                    raise Refusal("corrupt", "missing_history_lineage_tables")
        if "history_occurrences" in self.tables and any(
            v is None for v in self.manifest["original"].values()
        ):
            raise Refusal("unsupported", "missing_archive_binding")

    def raw(self, table, field, where, args):
        """Read a UTF8 field as bytes with <=3 bytes of decoder carry."""
        decoder = codecs.getincrementaldecoder("utf-8")("strict")
        offset = 0
        while True:
            self.check_time()
            row = self.conn.execute(
                f"SELECT substr(CAST(({field}) AS BLOB),?,8192) FROM {table} WHERE {where}",
                (offset + 1, *args),
            ).fetchone()
            if row is None or row[0] is None:
                raise Refusal("corrupt", "missing_field")
            piece = row[0]
            if not isinstance(piece, bytes):
                raise Refusal("corrupt", "field_bytes")
            if not piece:
                tail = decoder.decode(b"", final=True)
                if tail:
                    yield tail
                break
            offset += len(piece)
            text = decoder.decode(piece, final=False)
            if text:
                yield text
            if len(decoder.getstate()[0]) > 3:
                raise Refusal("corrupt", "utf8_carry")

    def value(self, table, field, where, args):
        row = self.conn.execute(
            f"SELECT typeof(({field})),length(CAST(({field}) AS BLOB)) FROM {table} WHERE {where}",
            args,
        ).fetchone()
        if row is None:
            raise Refusal("corrupt", "missing_evidence")
        kind = row[0]
        if kind == "text":
            yield '"'
            # SQLite can return NULL for substr(CAST('' AS BLOB),...), which
            # is distinct from an original NULL. Preserve empty TEXT explicitly.
            if row[1]:
                for text in self.raw(table, field, where, args):
                    yield _json(text)[1:-1]
            yield '"'
        elif kind in {"integer", "real", "null"}:
            yield _json(
                self.conn.execute(f"SELECT ({field}) FROM {table} WHERE {where}", args).fetchone()[
                    0
                ]
            )
        else:
            raise Refusal("unsupported", "binary_source_field")

    def object(self, table, where, args, fields=None):
        yield "{"
        for ordinal, field in enumerate(fields or TABLES[table][0].split()):
            if ordinal:
                yield ","
            yield _json(field) + ":"
            yield from self.value(table, field, where, args)
        yield "}"

    def small(self, table, columns, where, args, maximum=16384):
        # A SQL projection is bounded before Python decoding, including keys.
        expressions = []
        for column in columns.split():
            expressions.extend(
                (
                    f"CASE WHEN typeof({column})='text' AND length(CAST({column} AS BLOB))=0 THEN X'' ELSE substr(CAST({column} AS BLOB),1,{maximum + 1}) END AS {column}",
                    f"typeof({column}) AS {column}_type",
                )
            )
        row = self.conn.execute(
            f"SELECT {','.join(expressions)} FROM {table} WHERE {where}", args
        ).fetchone()
        if row is None:
            raise Refusal("corrupt", "missing_owner")
        result = {}
        for column in columns.split():
            value = row[column]
            if value is None:
                result[column] = None
                continue
            if len(value) > maximum:
                raise Refusal("unsupported", "source_key_size")
            value = value.decode("utf-8")
            result[column] = int(value) if row[column + "_type"] == "integer" else value
        return result

    def keys(self, table, after=None):
        columns = TABLES[table][1].split()
        if after is None:
            where, args = "1", ()
        else:
            where, args = (
                f"({','.join(columns)}) > ({','.join('?' for _ in columns)})",
                tuple(after),
            )
        projections = []
        for column in columns:
            projections.append(
                f"CASE WHEN typeof({column})='text' THEN CASE WHEN {column}='' THEN X'' ELSE substr(CAST({column} AS BLOB),1,16385) END ELSE {column} END"
            )
        sql = f"SELECT {','.join(projections)} FROM {table} WHERE {where} ORDER BY {','.join(columns)} LIMIT 1"
        row = self.conn.execute(sql, args).fetchone()
        if row is None:
            return None
        result = []
        for value in row:
            if isinstance(value, bytes):
                if len(value) > 16384:
                    raise Refusal("unsupported", "original_key_size")
                value = value.decode("utf-8")
                if len(value.encode("utf-8")) > 16384:
                    raise Refusal("unsupported", "original_key_size")
            elif type(value) is not int:
                raise Refusal("corrupt", "original_key_type")
            result.append(value)
        return result

    @staticmethod
    def alias_query():
        return "SELECT CASE WHEN alias='' THEN '' ELSE CAST(substr(CAST(alias AS BLOB),1,4097) AS TEXT) END FROM history_aliases WHERE identity=? ORDER BY alias"

    def prefix_bounds(self, phase, key):
        """Conservative scalar/129-ID preview, never full owner verification."""
        self.check_time()
        overflow = dict(rows=bulk.MAX_ENTRIES + 1, decoded=0)
        if phase == "audit":
            table, encoded = key
            args = json.loads(encoded)
            where = " AND ".join(column + "=?" for column in TABLES[table][1].split())
        elif phase == "history":
            table, where, args = "history_occurrences", "identity=?", key
        elif phase == "live":
            table, where, args = "receipts", "namespace=? AND request_id=?", key
        else:
            raise Refusal("corrupt", "prefix_preview_phase")
        lengths = ",".join(
            "coalesce(length(CAST(" + column + " AS BLOB)),0)"
            for column in TABLES[table][0].split()
        )
        if phase != "audit":
            lengths = (
                "length(CAST(ids AS BLOB)),("
                + "+".join(
                    "coalesce(length(CAST(" + column + " AS BLOB)),0)"
                    for column in TABLES[table][0].split()
                )
                + ")"
            )
        row = self.conn.execute(f"SELECT {lengths} FROM {table} WHERE {where}", args).fetchone()
        if row is None:
            raise Refusal("corrupt", "missing_prefix_preview")
        for value in row:
            bulk.integer(value)
        if phase == "audit":
            return dict(rows=1, decoded=sum(row))
        ids_bytes, decoded = row
        # JSON parsing is attempted only after this original scalar admission.
        # Extra whitespace conservatively selects the unchanged single-owner path.
        if ids_bytes > 129 * 75 + 2:
            return overflow
        raw_ids = list(
            self.conn.execute(
                f"SELECT substr(CAST(j.value AS BLOB),1,72),j.type FROM {table} r,json_each(r.ids) j WHERE {where} LIMIT 129",
                args,
            )
        )
        if len(raw_ids) >= 129:
            return overflow
        ids = []
        for identity, kind in raw_ids:
            if kind != "text" or not isinstance(identity, bytes):
                raise Refusal("corrupt", "prefix_member_type")
            ids.append(bulk.identity(identity.decode("utf-8"), "memory"))
        if phase == "history" and "history_occurrence_chunks" in self.tables:
            relational = list(
                self.conn.execute(
                    "SELECT substr(CAST(chunk_id AS BLOB),1,72) FROM history_occurrence_chunks WHERE identity=? ORDER BY chunk_id LIMIT 129",
                    key,
                )
            )
            if len(relational) >= 129:
                return overflow
            if not ids:
                ids = [bulk.identity(item[0].decode("utf-8"), "memory") for item in relational]
        if phase == "history" and "history_aliases" in self.tables:
            query = (
                "SELECT length(CAST(alias AS BLOB)) FROM history_aliases WHERE identity=? LIMIT 129"
            )
            for statement in (query, self.alias_query()):
                plan = list(self.conn.execute("EXPLAIN QUERY PLAN " + statement, key))
                if not any(
                    "SEARCH history_aliases" in item[3] and "ANY(" not in item[3] for item in plan
                ):
                    return overflow
            aliases = list(self.conn.execute(query, key))
            if len(aliases) >= 129:
                return overflow
            for item in aliases:
                bulk.integer(item[0])
                decoded += item[0]
        for identity in ids:
            self.check_time()
            sizes = self.conn.execute(
                "SELECT length(CAST(text AS BLOB)),length(CAST(provenance AS BLOB)) FROM chunks WHERE id=?",
                (identity,),
            ).fetchone()
            if sizes is None:
                raise Refusal("corrupt", "missing_prefix_member")
            for size in sizes:
                bulk.integer(size)
                decoded += size
        self.check_time()
        return dict(rows=2 * len(ids) + 5, decoded=decoded)

    def member_ids(self, phase, key):
        """Validate and stream both original membership representations in order."""
        if phase == "audit":
            return
        if phase == "live":
            table, where, args = "receipts", "namespace=? AND request_id=?", tuple(key)
        else:
            table, where, args = "history_occurrences", "identity=?", tuple(key)
        valid = self.conn.execute(
            f"SELECT json_valid(ids),CASE WHEN json_valid(ids) THEN json_type(ids) END FROM {table} WHERE {where}",
            args,
        ).fetchone()
        if valid is None or tuple(valid) != (1, "array"):
            raise Refusal("corrupt", "member_ids_json")
        json_count = self.conn.execute(
            f"SELECT json_array_length(ids) FROM {table} WHERE {where}", args
        ).fetchone()[0]
        relational = phase == "history" and "history_occurrence_chunks" in self.tables
        if relational:
            missing = self.conn.execute(
                "SELECT 1 FROM history_occurrence_chunks m LEFT JOIN chunks c ON c.id=m.chunk_id WHERE m.identity=? AND c.id IS NULL LIMIT 1",
                key,
            ).fetchone()
            if missing:
                raise Refusal("corrupt", "missing_history_member")
            relational_count = self.conn.execute(
                "SELECT count(*) FROM history_occurrence_chunks WHERE identity=?", key
            ).fetchone()[0]
        else:
            relational_count = 0
        if json_count and relational_count:
            if json_count != relational_count:
                raise Refusal("corrupt", "dual_membership_conflict")
            # Compare the two original representations in order, never union.
            dual = self.conn.execute(
                "SELECT CAST(substr(CAST(m.chunk_id AS BLOB),1,72) AS TEXT) FROM history_occurrence_chunks m JOIN chunks c ON c.id=m.chunk_id WHERE m.identity=? ORDER BY c.start,c.id",
                key,
            )
        else:
            dual = None
        if json_count:
            ids = self.conn.execute(
                f"SELECT j.key,substr(CAST(j.value AS BLOB),1,72),j.type FROM {table} r,json_each(r.ids) j WHERE {where} ORDER BY j.key",
                args,
            )
            for ordinal, identity, kind in ids:
                self.check_time()
                if kind != "text" or type(ordinal) is not int:
                    raise Refusal("corrupt", "member_identity_type")
                identity = identity.decode("utf-8")
                bulk.identity(identity, "memory")
                if dual is not None and dual.fetchone()[0] != identity:
                    raise Refusal("corrupt", "dual_membership_conflict")
                if (
                    self.conn.execute("SELECT 1 FROM chunks WHERE id=?", (identity,)).fetchone()
                    is None
                ):
                    raise Refusal("corrupt", "missing_member")
                yield ordinal, identity
        elif relational_count:
            # Sort only original order keys. Including projected text here would
            # materialize the entire occurrence inside SQLite's sorter heap.
            ordered = self.conn.execute(
                "SELECT c.id FROM history_occurrence_chunks m JOIN chunks c ON c.id=m.chunk_id "
                "WHERE m.identity=? ORDER BY c.start,c.id",
                key,
            )
            for ordinal, member in enumerate(ordered):
                self.check_time()
                identity = bulk.identity(member[0], "memory")
                yield ordinal, identity

    def members(self, phase, key, start_ordinal=0):
        for ordinal, identity in self.member_ids(phase, key):
            # Identity/membership checks still cover the complete prefix. Only
            # text already verified by this invocation's owner() is skipped.
            if ordinal < start_ordinal:
                continue
            row = self.conn.execute(
                "SELECT " + CHUNK_COLUMNS + " FROM chunks WHERE id=?", (identity,)
            ).fetchone()
            if row is None:
                raise Refusal("corrupt", "missing_member")
            yield ordinal, CanonicalRecord.from_source(MemoryStore._source(row))

    def scopes(self, phase, key):
        if phase == "history":
            kinds = {}
            for column in ("columns_json", "metadata_json"):
                valid = self.conn.execute(
                    f"SELECT json_valid({column}),CASE WHEN json_valid({column}) THEN json_type({column}) END FROM history_occurrences WHERE identity=?",
                    key,
                ).fetchone()
                if tuple(valid) != (1, "object"):
                    raise Refusal("corrupt", "historical_scope_json")
            for scope in ("workspace", "wing", "room"):
                kinds[scope] = tuple(
                    self.conn.execute(
                        f"SELECT json_type({column},'$.{scope}') FROM history_occurrences WHERE identity=?",
                        key,
                    ).fetchone()[0]
                    for column in ("columns_json", "metadata_json")
                )
            yield from historical_scope_header(
                self.value("history_occurrences", "columns_json", "identity=?", key),
                self.value("history_occurrences", "metadata_json", "identity=?", key),
                kinds,
            )
        else:
            yield (
                '{"format":"sqlite-source-scopes-v1","precedence":["columns","metadata","caller"],"origin":'
                + _json(phase)
                + ',"chunks":['
            )
        for ordinal, identity in self.member_ids(phase, key):
            if ordinal:
                yield ","
            yield '{"id":' + _json(identity) + ',"caller":'
            yield from self.value("chunks", "provenance", "id=?", (identity,))
            yield ',"capture_mapping":'
            found = (
                "capture_chunk_scopes" in self.tables
                and self.conn.execute(
                    "SELECT 1 FROM capture_chunk_scopes WHERE chunk_id=?", (identity,)
                ).fetchone()
            )
            if found:
                yield '{"present":true,"row":'
                yield from self.object("capture_chunk_scopes", "chunk_id=?", (identity,))
                yield ',"generation":'
                if "capture_generation_scopes" in self.tables:
                    mapping = self.conn.execute(
                        "SELECT source_key,generation FROM capture_chunk_scopes WHERE chunk_id=?",
                        (identity,),
                    ).fetchone()
                    generation = self.conn.execute(
                        "SELECT 1 FROM capture_generation_scopes WHERE source_key=? AND generation=?",
                        tuple(mapping),
                    ).fetchone()
                    if generation:
                        yield from self.object(
                            "capture_generation_scopes",
                            "source_key=? AND generation=?",
                            tuple(mapping),
                        )
                    else:
                        raise Refusal("corrupt", "missing_capture_generation")
                else:
                    raise Refusal("corrupt", "missing_capture_generation_table")
                yield "}"
            else:
                yield '{"present":false}'
            yield "}"
        yield "]}"

    def fields(self, phase, key):
        if phase == "history":
            yield "metadata", self.raw("history_occurrences", "metadata_json", "identity=?", key)
            yield "columns", self.raw("history_occurrences", "columns_json", "identity=?", key)
        else:
            yield "metadata", iter(())
            yield "columns", iter(())
        yield "scopes", self.scopes(phase, key) if phase != "audit" else iter(())
        yield "descriptor", self.descriptor_text(phase, key)

    def descriptor_text(self, phase, key):
        yield '{"format":' + _json(FORMAT) + ',"packing":' + _json(PACKING)
        yield ',"kind":' + _json("AUDIT" if phase == "audit" else phase)
        yield (
            ',"source_binding":'
            + _json(self.binding)
            + ',"registered_manifest":'
            + _json(self.manifest)
        )
        if phase == "audit":
            table, encoded = key
            original_key = json.loads(encoded)
            columns = TABLES[table][1].split()
            where = " AND ".join(c + "=?" for c in columns)
            yield (
                ',"original_table":'
                + _json(table)
                + ',"original_key":'
                + encoded
                + ',"original_version":'
                + _json(self.manifest["original"])
            )
            if table == "history_staged_rows":
                # This descriptor references unpublished original work. Carry,
                # chunks and unverified fields never become canonical evidence.
                fields = "cursor_key identity snapshot_id source_cursor next_cursor text_bytes text_characters next_character chunk_start processed_bytes state inflight_character retry_character retry_count text_hash expected_hash reason phase_inflight hash_failures publication_failures final_deferrals".split()
                yield ',"pending_stage":true,"recovery":"pinned-archive-cursor","row":'
                yield from self.object(table, where, original_key, fields)
            else:
                yield ',"row":'
                yield from self.object(table, where, original_key)
        elif phase == "live":
            yield ',"original_table":"receipts","row":'
            yield from self.object("receipts", "namespace=? AND request_id=?", key)
        else:
            yield ',"original_table":"history_occurrences","row":'
            yield from self.object(
                "history_occurrences",
                "identity=?",
                key,
                "identity cursor_key row_address alias text_hash metadata_hash columns_hash".split(),
            )
            yield ',"aliases":['
            if "history_aliases" in self.tables:
                first = True
                for row in self.conn.execute(
                    self.alias_query(),
                    key,
                ):
                    if not first:
                        yield ","
                    first = False
                    if not isinstance(row[0], str) or len(row[0].encode("utf-8")) > 4096:
                        raise Refusal("corrupt", "historical_alias_size")
                    yield _json(row[0])
            yield "]"
        yield "}"

    def owner_key(self, phase, key):
        if phase == "history":
            return bulk.sha(key[0])
        return ("AUDIT:" if phase == "audit" else "LIVE:") + _hash(_json([phase, key]))

    def owner(self, phase, key, *, planning=None):
        owner_key = self.owner_key(phase, key)
        owner = bulk.owner_id(owner_key, self.binding)
        summaries = {}
        self.active_operation = "owner_summary"
        for field, pieces in self.fields(phase, key):
            self.active_field = field
            digest = hashlib.sha256()
            chars = size = count = 0
            for text in pages(pieces):
                self.check_time()
                if planning is not None:
                    planning.add_entry(bulk.page(owner, field, count, text, chars, size))
                digest.update(text.encode())
                chars += len(text)
                size += len(text.encode())
                count += 1
            summaries[field] = dict(hash=digest.hexdigest(), bytes=size, chars=chars, pages=count)
        whole, members, digest = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
        count = text_chars = text_bytes = 0
        live = None
        caller = None
        ids = []  # Production remembers have <=9 original IDs, never history.
        if phase == "live":
            receipt = self.small(
                "receipts", "namespace request_id digest", "namespace=? AND request_id=?", key
            )
            _string(receipt["namespace"], 128, "namespace")
            _string(receipt["request_id"], 128, "request id")
            bulk.sha(receipt["digest"])
            digest.update((_json(key)[:-1] + ',"').encode())
        elif phase == "history":
            occurrence = self.small(
                "history_occurrences",
                "identity cursor_key row_address alias text_hash metadata_hash columns_hash",
                "identity=?",
                key,
            )
            validate_attempt(self, occurrence["cursor_key"], occurrence)
            original = self.manifest["original"]
            address = occurrence["row_address"]
            bulk.integer(address)
            expected = _hash(
                _json(
                    [
                        original["archive_id"],
                        original["snapshot_id"],
                        original["version"],
                        address >> 32,
                        address,
                    ]
                )
            )
            if expected != key[0] or any(
                summaries[field]["hash"] != occurrence[field + "_hash"]
                for field in ("metadata", "columns")
            ):
                raise Refusal("corrupt", "original_occurrence_hash_or_identity")
            alias_matches = self.conn.execute(
                "SELECT json_type(columns_json,'$.id')='text' AND json_extract(columns_json,'$.id') IS alias FROM history_occurrences WHERE identity=?",
                key,
            ).fetchone()[0]
            if not alias_matches:
                raise Refusal("corrupt", "historical_alias_columns")
            if (
                "history_aliases" not in self.tables
                or not self.conn.execute(
                    "SELECT 1 FROM history_aliases WHERE alias=? AND identity=?",
                    (occurrence["alias"], key[0]),
                ).fetchone()
            ):
                raise Refusal("corrupt", "missing_alias_relation")
        self.active_field = "membership"
        for ordinal, record in self.members(phase, key):
            self.check_time()
            provenance = record.source()["provenance"]
            if ordinal != count or provenance["chunk_start"] != text_chars:
                raise Refusal("corrupt", "member_offsets")
            if phase == "live":
                if ordinal >= 9:
                    raise Refusal("unsupported", "nonproduction_remember_shape")
                if record.id != "memory:" + _hash(f"{receipt['digest']}:{ordinal}"):
                    raise Refusal("corrupt", "original_remember_identity")
                current = _json(provenance["caller"])
                if caller is None:
                    caller = current
                if current != caller:
                    raise Refusal("corrupt", "remember_caller_mismatch")
                ids.append(record.id)
                digest.update(_json(record.text)[1:-1].encode())
            elif phase == "history":
                if record.id != "memory:" + _hash(_json([key[0], text_chars])):
                    raise Refusal("corrupt", "historical_chunk_identity")
                expected_caller = provenance["caller"]
                if any(
                    expected_caller.get(name) != value
                    for name, value in {
                        "archive_id": original["archive_id"],
                        "version": original["version"],
                        "metadata_id": key[0],
                    }.items()
                ):
                    raise Refusal("corrupt", "historical_caller_binding")
            member = member_text(owner, ordinal, record, summaries["scopes"])
            if planning is not None:
                planning.add_entry(
                    bulk.page(owner, "membership", ordinal, member, ordinal, ordinal)
                )
                planning.add_entry(record_entry(owner, ordinal, record))
            members.update(member.encode())
            whole.update(record.text.encode())
            text_chars += len(record.text)
            text_bytes += len(record.text.encode())
            count += 1
        if phase == "live":
            if not count or text_bytes > 65536:
                raise Refusal("unsupported", "nonproduction_remember_shape")
            digest.update(('",' + caller + "]").encode())
            if digest.hexdigest() != receipt["digest"]:
                raise Refusal("corrupt", "original_remember_digest")
            live = dict(
                namespace=key[0],
                key=key[1],
                digest=receipt["digest"],
                receipt=dict(
                    status="ok",
                    ids=ids,
                    stored_text_bytes=text_bytes,
                    text_sha256=whole.hexdigest(),
                ),
            )
        elif phase == "history" and whole.hexdigest() != occurrence["text_hash"]:
            raise Refusal("corrupt", "original_text_hash")
        manifest = dict(
            format=bulk.FORMAT,
            state="published_owner",
            owner=owner,
            owner_key=owner_key,
            source_binding=self.binding,
            members=count,
            membership_hash=members.hexdigest(),
            text_hash=whole.hexdigest(),
            text_bytes=text_bytes,
            text_chars=text_chars,
            fields=summaries,
            metadata_id=key[0] if phase == "history" else None,
            live=live,
        )
        return bulk.validate_owner(manifest)

    def entries(self, phase, key, manifest, start_entry=0):
        """Emit a suffix after owner() has verified this invocation's full owner."""
        bulk.integer(start_entry)
        owner = manifest["owner"]
        field_start = 0
        for field, pieces in self.fields(phase, key):
            field_end = field_start + manifest["fields"][field]["pages"]
            if start_entry >= field_end:
                field_start = field_end
                continue
            chars = size = 0
            for ordinal, text in enumerate(pages(pieces)):
                if field_start + ordinal >= start_entry:
                    yield (
                        bulk.page(owner, field, ordinal, text, chars, size),
                        (field, 0, size, chars),
                    )
                chars += len(text)
                size += len(text.encode())
            field_start = field_end
        first_member = max(0, (start_entry - field_start) // 2)
        for ordinal, record in self.members(phase, key, start_ordinal=first_member):
            if field_start + 2 * ordinal >= start_entry:
                text = member_text(owner, ordinal, record, manifest["fields"]["scopes"])
                yield (
                    bulk.page(owner, "membership", ordinal, text, ordinal, ordinal),
                    ("membership", ordinal, ordinal, ordinal),
                )
            yield (
                bulk.validate_entry(
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
                ),
                ("record", ordinal, 0, 0),
            )


def validate_attempt(source, cursor_key, occurrence=None):
    """Validate one indexed original receipt in either enumeration direction."""
    conn = source.conn
    from .memory_history import _validate_inventory_receipt

    row = conn.execute(
        "SELECT CAST(substr(CAST(a.cursor_key AS BLOB),1,65) AS TEXT) AS cursor_key,length(CAST(a.cursor_key AS BLOB)) AS cursor_key_length,"
        "CAST(substr(CAST(a.next_cursor AS BLOB),1,513) AS TEXT) AS next_cursor,length(CAST(a.next_cursor AS BLOB)) AS next_cursor_length,"
        "CAST(substr(CAST(a.outcome AS BLOB),1,32) AS TEXT) AS outcome,length(CAST(a.outcome AS BLOB)) AS outcome_length,"
        "CAST(substr(CAST(o.identity AS BLOB),1,65) AS TEXT) AS occurrence_identity,length(CAST(o.identity AS BLOB)) AS occurrence_identity_length,"
        "CASE WHEN typeof(o.row_address)='integer' THEN o.row_address END AS row_address,CAST(substr(CAST(g.identity AS BLOB),1,65) AS TEXT) AS gap_identity,length(CAST(g.identity AS BLOB)) AS gap_identity_length "
        "FROM history_attempts a LEFT JOIN history_occurrences o ON o.cursor_key=a.cursor_key "
        "LEFT JOIN history_gaps g ON g.cursor_key=a.cursor_key WHERE a.cursor_key=?",
        (cursor_key,),
    ).fetchone()
    if row is None:
        raise Refusal("corrupt", "missing_history_attempt")
    # The validator checks encoding lengths/cursors/physical identity against
    # original archive facts. Bounded SQL fields are checked before fetching.
    if any(isinstance(v, str) and len(v.encode()) > 512 for v in row):
        raise Refusal("corrupt", "source_attempt_size")
    _validate_inventory_receipt(source.manifest["original"], row)
    if occurrence is not None and (
        row["outcome"] != "indexed"
        or row["occurrence_identity"] != occurrence["identity"]
        or row["row_address"] != occurrence["row_address"]
    ):
        raise Refusal("corrupt", "historical_attempt_binding")


def validate_audit(source, key):
    table, encoded = key
    original_key = json.loads(encoded)
    conn = source.conn
    if table == "history_attempts":
        validate_attempt(source, original_key[0])
    elif table == "history_gaps":
        found = conn.execute(
            "SELECT 1 FROM history_attempts a JOIN history_gaps g USING(cursor_key) WHERE a.cursor_key=? AND a.outcome='gap'",
            original_key,
        ).fetchone()
        if not found:
            raise Refusal("corrupt", "unbound_gap")
    elif table == "history_gap_resolutions":
        from .memory_history import _valid_repair_cursor

        row = source.small(table, TABLES[table][0], "cursor_key=?", original_key)
        occurrence = source.small(
            "history_occurrences",
            "identity cursor_key row_address text_hash metadata_hash columns_hash",
            "identity=?",
            (row["identity"],),
        )
        following = json.loads(row["next_cursor"])
        if (
            not _valid_repair_cursor(following)
            or following.get("row_address") != occurrence["row_address"]
            or row["cursor_key"] != occurrence["cursor_key"]
            or row["snapshot_id"] != source.manifest["original"]["snapshot_id"]
            or any(row[f] != occurrence[f] for f in ("text_hash", "metadata_hash", "columns_hash"))
            or conn.execute(
                "SELECT 1 FROM history_gaps WHERE cursor_key=?", original_key
            ).fetchone()
            or not conn.execute(
                "SELECT 1 FROM history_attempts WHERE cursor_key=? AND outcome='indexed' AND next_cursor=?",
                (row["cursor_key"], row["next_cursor"]),
            ).fetchone()
        ):
            raise Refusal("corrupt", "gap_repair_lineage")
    elif table == "history_aliases":
        if not conn.execute(
            "SELECT 1 FROM history_occurrences WHERE identity=? AND alias=?",
            (original_key[1], original_key[0]),
        ).fetchone():
            raise Refusal("corrupt", "orphan_alias")
    elif table == "capture_chunk_scopes":
        if not conn.execute("SELECT 1 FROM chunks WHERE id=?", original_key).fetchone():
            raise Refusal("corrupt", "orphan_capture_mapping")


def next_unit(source, cursor, *, same_phase=False):
    """Keyset scan; the cursor key means current owner or last completed key."""
    phase, key = cursor["phase"], cursor["key"]
    if cursor["owner"] is not None:
        if cursor["owner"] != source.owner_key(phase, key):
            raise Refusal("corrupt", "cursor_owner_binding")
        return phase, key
    while phase != "done":
        source.check_time()
        if phase in {"live", "history"}:
            table = "receipts" if phase == "live" else "history_occurrences"
            found = source.keys(table, key) if table in source.tables else None
            if found is not None:
                return phase, found
        elif phase == "audit":
            table_after, row_after = key if key else ("", None)
            for table in AUDIT_TABLES:
                if table < table_after or table not in source.tables:
                    continue
                found = source.keys(
                    table,
                    json.loads(row_after)
                    if table == table_after and row_after is not None
                    else None,
                )
                if found is not None:
                    return phase, [table, _json(found)]
        elif phase == "orphans":
            # Only exact original owners count. No fabricated occurrence can
            # conceal leftovers. The JSON/sort plan remains under fixed guards.
            tests = ["NOT EXISTS (SELECT 1 FROM receipts r,json_each(r.ids) j WHERE j.value=c.id)"]
            if "history_occurrences" in source.tables:
                tests.append(
                    "NOT EXISTS (SELECT 1 FROM history_occurrences h,json_each(h.ids) j WHERE j.value=c.id)"
                )
            if "history_occurrence_chunks" in source.tables:
                tests.append(
                    "NOT EXISTS (SELECT 1 FROM history_occurrence_chunks m WHERE m.chunk_id=c.id)"
                )
                bad = source.conn.execute(
                    "SELECT 1 FROM history_occurrence_chunks m LEFT JOIN history_occurrences o ON o.identity=m.identity WHERE o.identity IS NULL LIMIT 1"
                ).fetchone()
                if bad:
                    raise Refusal("corrupt", "orphan_occurrence_membership")
            row = source.conn.execute(
                "SELECT c.id FROM chunks c WHERE " + " AND ".join(tests) + " ORDER BY c.id LIMIT 1"
            ).fetchone()
            if row:
                raise Refusal("incomplete", "orphan_chunk")
        if same_phase:
            return phase, None
        phase = {"live": "history", "history": "audit", "audit": "orphans", "orphans": "done"}[
            phase
        ]
        key = None
    return "done", None


def _export_owner(source, result, cursor, phase, key, *, manifest=None):
    if manifest is None:
        if phase == "audit":
            validate_audit(source, key)
        manifest = source.owner(phase, key)
    start = cursor["entry"] if cursor["owner"] is not None else 0
    base = initial_cursor(source.binding, source.descriptor["store_id"])
    base.update(phase=phase, key=key)
    expected = None
    source.active_operation = "emit_entries"
    entries = source.entries(phase, key, manifest, start_entry=start)
    # entry_batches reads one overflow entry before yielding its first batch.
    # Retain that position, not a second batch or a replay of the selected rows.
    last_position = None
    consumed = 0

    def rows():
        nonlocal expected, last_position, consumed
        for entry, position in entries:
            source.check_time()
            if expected is None:
                expected = position
            last_position = position
            consumed += 1
            yield entry

    batches = bulk.entry_batches(rows())
    try:
        selected = next(batches, [])
    finally:
        batches.close()
        entries.close()
    next_position = (start + len(selected), last_position) if consumed > len(selected) else None
    total = sum(field["pages"] for field in manifest["fields"].values()) + 2 * manifest["members"]
    if cursor["owner"] is not None:
        given = (
            cursor["field"],
            cursor["member_ordinal"],
            cursor["field_byte_offset"],
            cursor["field_char_offset"],
        )
        if expected is None:
            expected = ("finalization", manifest["members"], 0, 0)
        if given != expected:
            raise Refusal("corrupt", "cursor_position_mismatch")
    if start > total:
        raise Refusal("corrupt", "cursor_past_owner")
    result["entries"] = selected
    if next_position is not None:
        entry_ordinal, position = next_position
        base.update(
            owner=manifest["owner_key"],
            entry=entry_ordinal,
            field=position[0],
            member_ordinal=position[1],
            field_byte_offset=position[2],
            field_char_offset=position[3],
        )
    else:
        # Keep finalization separate when it would exceed byte/count
        # admission. It is the only phase that increments owner counts.
        try:
            bulk.validate_batch("export", source.binding, selected, [manifest])
            frame_bytes({**result, "entries": selected, "finalizations": [manifest]})
        except ValueError:
            base.update(
                owner=manifest["owner_key"],
                entry=total,
                field="finalization",
                member_ordinal=manifest["members"],
            )
        else:
            result["finalizations"] = [manifest]
            result["phase_complete"] = True
            counts = result["counts"]
            counts[
                {
                    "live": "live_owners",
                    "history": "historical_occurrences",
                    "audit": "audit_owners",
                }[phase]
            ] = 1
            counts["members"] = manifest["members"]
            if phase == "audit":
                for table, counter in (
                    ("history_gaps", "gaps"),
                    ("history_gap_resolutions", "repaired_gaps"),
                    ("history_staged_rows", "pending_stages"),
                ):
                    if key[0] == table:
                        counts[counter] = 1
    result["cursor_after"] = base
    if not selected and not result["finalizations"]:
        raise Refusal("qualification_required", "export_no_progress")
    bulk.validate_batch("export", source.binding, result["entries"], result["finalizations"])


def _normalize_refusal(result, before, source):
    """No refused interval can retain staged finalization/completion claims."""
    result.update(
        entries=[],
        finalizations=[],
        cursor_after=before,
        complete=False,
        phase_complete=False,
        counts=empty_counts(),
        frame_bytes=0,
        refused_field=source.active_field,
        refused_operation=source.active_operation,
    )
    if result.get("reason", "").startswith("orphan"):
        result["counts"]["orphans"] = 1
        result["coverage"] = "incomplete"
    return result


def export_slice(descriptor, cursor, *, layout=None):
    """One private bounded replay interval; no destination progress is implied."""
    source = Source(descriptor)
    validate_cursor(cursor, source.binding, descriptor["store_id"])
    before = dict(cursor)
    result = dict(
        status="ok",
        source_binding=source.binding,
        cursor_before=before,
        cursor_after=dict(before),
        entries=[],
        finalizations=[],
        counts=empty_counts(),
        phase=cursor["phase"],
        phase_complete=False,
        complete=False,
        reason=None,
        coverage="indexed-only",
        frame_bytes=0,
    )
    plans, prefix_payload = [], {}
    is_prefix = (
        isinstance(layout, dict) and layout.get("format") == "lance-layout-source-prefix-request-v1"
    )
    try:
        if layout is not None:
            from .lance_layout_codec import LayoutPlanningCounters, validate_layout_source_window
            from .lance_layout_directory import load_layout_qualification
            from .migration_codec import registration, hashed

            validate_layout_source_window(layout, descriptor, cursor)
            qualification = load_layout_qualification(
                layout["generation"], layout["qualification_hash"]
            )
            reg = registration(descriptor, layout["generation"]["path"])
            planning = LayoutPlanningCounters(qualification)
        source.open()
        phase, key = next_unit(source, cursor)
        result["phase"] = phase
        result["phase_complete"] = False
        if phase == "done":
            after = initial_cursor(source.binding, descriptor["store_id"])
            after["phase"] = "done"
            result.update(cursor_after=after, complete=True, phase_complete=True)
        elif is_prefix:
            incomplete = any(
                table in source.tables
                and source.conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                for table in ("history_gaps", "history_staged_rows")
            )
            result["coverage"] = "incomplete" if incomplete else "indexed-only"
            prefix_payload = _prefix_payload(
                source, reg, before, layout, qualification, phase, key, result["coverage"]
            )
        elif layout is not None:
            if phase == "audit":
                validate_audit(source, key)
            manifest = source.owner(phase, key, planning=planning)
            plan = planning.finish(reg, manifest, phase, key)
            source.check_time()
            if layout["action"] == "export" and layout["allowed_plan_hashes"] != [hashed(plan)]:
                raise Refusal("conflict", "layout_owner_plan_mismatch")
            plans = [plan]
            if layout["action"] == "export":
                _export_owner(source, result, cursor, phase, key, manifest=manifest)
        else:
            _export_owner(source, result, cursor, phase, key)
        # Snapshot facts are checked both sides of *every* successful slice.
        source.validate_files()
        if not is_prefix or phase == "done":
            incomplete = any(
                table in source.tables
                and source.conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                for table in ("history_gaps", "history_staged_rows")
            )
            result["coverage"] = "incomplete" if incomplete else "indexed-only"
        # Includes the extra accounting fields themselves and ASCII escaping.
        for _ in range(3):
            result["frame_bytes"] = len(frame_bytes(result))
        frame_bytes(result)
    except Refusal as exc:
        result.update(status=exc.status, reason=exc.reason)
    except MemoryError:
        result.update(status="qualification_required", reason="sqlite_summary_limit")
    except (_Corrupt, _HistoryError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result.update(status="corrupt", reason="source_contract")
    except sqlite3.Error as exc:
        code = getattr(exc, "sqlite_errorcode", 0) & 255
        result.update(
            status={
                5: "busy",
                6: "busy",
                9: "qualification_required",
                11: "corrupt",
                26: "corrupt",
                7: "qualification_required",
            }.get(code, "unavailable"),
            reason="sqlite_summary_limit" if code in (7, 9) else "sqlite_source",
        )
    except OSError:
        result.update(status="stale", reason="source_files")
    finally:
        if source.conn is not None:
            source.conn.close()
            try:
                source.validate_identities()
            except (Refusal, OSError):
                result.update(status="stale", reason="source_post_slice_identity")
        if result["status"] != "ok":
            _normalize_refusal(result, before, source)
    if is_prefix:
        return _prefix_response(source, descriptor, before, layout, result, prefix_payload)
    if layout is not None:
        return _layout_response(source, descriptor, before, layout, result, plans)
    return result


def _layout_response(source, descriptor, before, layout, result, plans):
    try:
        wrapped = layout_result(descriptor, before, layout, result, plans)
        if result["status"] == "ok":
            source.check_time()
        return wrapped
    except Refusal as exc:
        result.update(status=exc.status, reason=exc.reason)
    except MemoryError:
        result.update(status="qualification_required", reason="sqlite_summary_limit")
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result.update(status="qualification_required", reason="layout_source_frame_limit")
    _normalize_refusal(result, before, source)
    return layout_result(descriptor, before, layout, result, [])


def _prefix_fits(source, phase, key, packet):
    bounds = source.prefix_bounds(phase, key)
    rows, decoded = bounds["rows"], bounds["decoded"]
    if packet is not None:
        for part in packet["components"]:
            exported = part["export"]
            rows += len(exported["entries"]) + len(exported["finalizations"])
            decoded += sum(
                len(value.encode()) for row in exported["entries"] for value in row.values()
            )
            decoded += sum(len(_json(owner).encode()) for owner in exported["finalizations"])
    return rows <= bulk.MAX_ENTRIES and decoded <= bulk.MAX_DECODED_BYTES


def _prefix_payload(source, reg, before, window, qualification, phase, key, coverage):
    from .lance_layout_codec import LayoutPlanningCounters
    from .lance_layout_packing import MAX_OWNERS, _SIZE_ERRORS, pack_source_prefix
    from .migration_codec import hashed

    selected, packet, cursor = [], None, dict(before)
    frozen = window["action"] == "export"
    limit = len(window["allowed_plan_hashes"]) if frozen else MAX_OWNERS
    original_phase = phase
    for index in range(limit):
        source.check_time()
        fits = _prefix_fits(source, phase, key, packet)
        if not fits and selected:
            break
        if phase == "audit":
            validate_audit(source, key)
        planning = LayoutPlanningCounters(qualification)
        owner = source.owner(phase, key, planning=planning)
        plan = planning.finish(reg, owner, phase, key)
        source.check_time()
        if frozen and window["allowed_plan_hashes"][index] != hashed(plan):
            raise Refusal("conflict", "layout_owner_plan_mismatch")
        if not fits or plan["expansion"]["batches"] != 1:
            if not selected and not frozen:
                return dict(fallback_plan=plan)
            break
        exported = dict(
            status="ok",
            reason=None,
            source_binding=source.binding,
            cursor_before=dict(cursor),
            cursor_after=dict(cursor),
            entries=[],
            finalizations=[],
            counts=empty_counts(),
            phase=phase,
            phase_complete=False,
            complete=False,
            coverage=coverage,
            frame_bytes=0,
        )
        _export_owner(source, exported, cursor, phase, key, manifest=owner)
        for _ in range(3):
            exported["frame_bytes"] = len(frame_bytes(exported))
        try:
            candidate = pack_source_prefix(
                reg, before, [*selected, dict(plan=plan, export=exported)]
            )
        except ValueError as exc:
            if str(exc) not in _SIZE_ERRORS | {"whole owner prefix required"}:
                raise
            if not selected and not frozen:
                return dict(fallback_plan=plan)
            break
        if len(candidate["components"]) != len(selected) + 1:
            break
        packet, selected, cursor = (
            candidate,
            candidate["components"],
            dict(exported["cursor_after"]),
        )
        if index + 1 == limit:
            break
        phase, key = next_unit(source, cursor, same_phase=True)
        if key is None or phase != original_phase:
            break
    if packet is None or (frozen and len(selected) != limit):
        raise Refusal("conflict", "layout_source_prefix_mismatch")
    source.check_time()
    return dict(prefix=packet)


def _prefix_response(source, descriptor, before, window, result, payload):
    from .lance_layout_packing import prefix_result

    try:
        if result["status"] == "ok":
            if result["complete"]:
                payload = dict(terminal=result)
            wrapped = prefix_result(descriptor, before, window, **payload)
            source.check_time()
            return wrapped
    except Refusal as exc:
        result.update(status=exc.status, reason=exc.reason)
    except MemoryError:
        result.update(status="qualification_required", reason="sqlite_summary_limit")
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result.update(status="qualification_required", reason="layout_source_frame_limit")
    return prefix_result(
        descriptor, before, window, status=result["status"], reason=result["reason"]
    )

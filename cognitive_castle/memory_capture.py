"""Durable bounded source queue and observed coverage for exact local memory."""

from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path
import sqlite3
import time

from .fast_memory import _Corrupt, _Missing, _Quota, _Unavailable, _json, _string
from .memory_ingest import _open_source, ingest_slice

OMISSIONS = (
    "duplicate_records",  # Historical, unverified duplicate classification.
    "tool_records",
    "tool_blocks",
    "attachment_blocks",
    "unsupported_records",
    "unsupported_blocks",
    "malformed_records",
    "oversized_records",
)
EXCLUSIONS = ("metadata_records", "internal_records", "internal_blocks")
ERRORS = (OSError, sqlite3.Error, _Unavailable, _Quota, _Corrupt)


def source_key(path, format):
    return hashlib.sha256((str(Path(path).absolute()) + "\0" + format).encode()).hexdigest()


def _ensure(conn):
    from .capture_publication import ensure_journal

    ensure_journal(conn)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS capture_sources (source_key TEXT PRIMARY KEY, state TEXT NOT NULL, value TEXT NOT NULL)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS capture_source_state ON capture_sources(state)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS capture_source_workspace ON capture_sources(CASE WHEN json_valid(value) THEN json_extract(value,'$.workspace') END)"
    )

    conn.execute(
        "CREATE TABLE IF NOT EXISTS capture_generation_scopes (source_key TEXT,generation INTEGER,"
        "workspace TEXT,attribution TEXT NOT NULL,state TEXT NOT NULL,PRIMARY KEY(source_key,generation))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS capture_chunk_scopes (chunk_id TEXT PRIMARY KEY,source_key TEXT NOT NULL,"
        "generation INTEGER NOT NULL,workspace TEXT NOT NULL,attribution TEXT NOT NULL,source_time INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS capture_chunk_workspace ON capture_chunk_scopes(workspace,source_time DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS capture_chunk_generation ON capture_chunk_scopes(source_key,generation)"
    )


def _exists(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE name='capture_sources'").fetchone()


def _decode(row):
    try:
        value = json.loads(_string(row["value"], 16384, "source"))
        if not isinstance(value, dict) or value["source_key"] != row["source_key"]:
            raise ValueError("source identity")
        return value
    except (ValueError, KeyError, TypeError, RecursionError) as exc:
        raise _Corrupt("invalid capture source") from exc


def _write(conn, value):
    raw = _string(_json(value), 16384, "source")
    conn.execute(
        "INSERT INTO capture_sources VALUES (?,?,?) ON CONFLICT(source_key) DO UPDATE SET state=excluded.state,value=excluded.value",
        (value["source_key"], value["state"], raw),
    )


def queue_source(store, path, *, allowed_root, format, workspace=None):
    """Register observed metadata durably before any transcript text is processed."""
    if format not in ("codex", "claude"):
        raise ValueError("unsupported transcript format")
    for label, value in (("path", path), ("root", allowed_root)):
        _string(str(value), 2048, label)
    if workspace is not None:
        _string(workspace, 2048, "workspace")
        if not Path(workspace).is_absolute() or ".." in Path(workspace).parts:
            raise ValueError("absolute workspace required")
    opened, info, path = _open_source(path, allowed_root)
    opened.close()
    key = source_key(path, format)
    identity = [info.st_dev, info.st_ino, getattr(info, "st_birthtime_ns", 0)]
    try:
        with store._connect(True) as conn:
            _ensure(conn)
            row = conn.execute(
                "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                (key,),
            ).fetchone()
            value = _decode(row) if row else {"source_key": key, "state": "pending"}
            changed = any(
                value.get(k) != v
                for k, v in (
                    ("observed_size", info.st_size),
                    ("observed_mtime_ns", info.st_mtime_ns),
                    ("identity", identity),
                )
            )
            value.update(
                path=str(path),
                root=str(Path(allowed_root).absolute()),
                format=format,
                observed_size=info.st_size,
                observed_mtime_ns=info.st_mtime_ns,
                identity=identity,
                observed_at_ns=time.time_ns(),
            )
            if workspace is not None:
                value["host_workspace_hint"] = workspace
                value["host_workspace_observation"] = {
                    "size": info.st_size,
                    "mtime_ns": info.st_mtime_ns,
                    "identity": identity,
                }
                value["workspace"] = workspace
                value["workspace_attribution"] = "host_event"
            if changed:
                value.update(state="pending", processed_bytes=0, pending_bytes=info.st_size)
            _write(conn, value)
        return {"status": "ok", "source_key": key, "state": value["state"]}
    except ERRORS as exc:
        return store._error(exc)


_CHUNK_SOURCE = "CASE WHEN json_valid(provenance) THEN json_extract(provenance,'$.source') END"
_CHUNK_GENERATION = (
    "CASE WHEN json_valid(provenance) THEN json_extract(provenance,'$.generation') END"
)


def _generation_scope(conn, key, checkpoint, attempt):
    """Reserve immutable generation attribution; never reassign an old scope."""
    generation = checkpoint["generation"]
    metadata_start = checkpoint.get("workspace_first_offset")
    metadata = checkpoint.get("workspace") if metadata_start is not None else None
    observation = attempt.get("hint_observation")
    hint = (
        attempt.get("hint")
        if observation
        == {
            "size": checkpoint["observed_size"],
            "mtime_ns": checkpoint["observed_mtime_ns"],
            "identity": checkpoint["identity"],
        }
        else None
    )
    candidate = metadata or hint
    attribution = "transcript_metadata" if metadata else "host_event" if hint else "unattributed"
    old = conn.execute(
        "SELECT * FROM capture_generation_scopes WHERE source_key=? AND generation=?",
        (key, generation),
    ).fetchone()
    workspace = old["workspace"] if old and old["workspace"] else candidate
    prior_conflict = old is not None and old["state"] == "conflict"
    changed_scope = bool(old and old["workspace"] and candidate and candidate != old["workspace"])
    conflict = prior_conflict or changed_scope or bool(checkpoint.get("workspace_conflict"))
    state = "conflict" if conflict else "scoped" if workspace else "unattributed"
    if old and old["workspace"] and (attribution == "unattributed" or changed_scope):
        attribution = old["attribution"]
    # Workspace is assigned once per generation. A conflict only inhibits future
    # mappings; it cannot move old evidence or invalidate previously pinned refs.
    conn.execute(
        "INSERT INTO capture_generation_scopes VALUES (?,?,?,?,?) ON CONFLICT(source_key,generation) "
        "DO UPDATE SET workspace=COALESCE(capture_generation_scopes.workspace,excluded.workspace),"
        "attribution=excluded.attribution,state=excluded.state",
        (key, generation, workspace, attribution, state),
    )
    return dict(
        workspace=workspace,
        attribution=attribution,
        state=state,
        can_map=bool(workspace and not prior_conflict and not changed_scope),
        metadata_start=metadata_start,
        conflict_at=checkpoint.get("workspace_conflict_at"),
    )


def _scope_chunks(conn, key, checkpoint, attempt):
    """Summarize scope; only explicit legacy attempts use their old watermark."""
    generation = checkpoint["generation"]
    scope = _generation_scope(conn, key, checkpoint, attempt)
    workspace, attribution, state = scope["workspace"], scope["attribution"], scope["state"]
    metadata_start = scope["metadata_start"]
    where = " WHERE " + _CHUNK_SOURCE + "=? AND " + _CHUNK_GENERATION + "=?"
    if "after_rowid" in attempt and scope["can_map"]:
        extra = " AND rowid>?"
        parameters = [
            workspace,
            attribution,
            checkpoint["observed_mtime_ns"],
            key,
            generation,
            attempt["after_rowid"],
        ]
        if metadata_start is not None:
            extra += " AND json_extract(provenance,'$.record_start')>=?"
            parameters.append(metadata_start)
        if checkpoint.get("workspace_conflict_at") is not None:
            extra += " AND json_extract(provenance,'$.record_start')<?"
            parameters.append(checkpoint["workspace_conflict_at"])
        conn.execute(
            "INSERT OR IGNORE INTO capture_chunk_scopes "
            "SELECT id,"
            + _CHUNK_SOURCE
            + ","
            + _CHUNK_GENERATION
            + ",?,?,? FROM chunks"
            + where
            + extra,
            parameters,
        )
    total = conn.execute("SELECT count(*) FROM chunks" + where, (key, generation)).fetchone()[0]
    mapped = conn.execute(
        "SELECT count(*) FROM capture_chunk_scopes WHERE source_key=? AND generation=?",
        (key, generation),
    ).fetchone()[0]
    observed_scope = state
    if state != "conflict" and mapped < total:
        observed_scope = "partial" if mapped else "unattributed"
    return dict(
        workspace=workspace,
        workspace_attribution=attribution,
        workspace_scope=observed_scope,
        scoped_chunks=mapped,
        unscoped_chunks=total - mapped,
    )


def _prepare_scope(store, key, checkpoint):
    """Recover old mapping attempts, then reserve an explicit-ID capture attempt."""
    with store._connect(True) as conn:
        _ensure(conn)
        source = _decode(
            conn.execute(
                "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                (key,),
            ).fetchone()
        )
        if source.get("scope_attempt") and checkpoint is not None:
            source.update(_scope_chunks(conn, key, checkpoint, source["scope_attempt"]))
            source["generation"] = checkpoint["generation"]
        source["workspace_scope"] = "pending"
        source["scope_attempt"] = dict(
            format="canonical-receipt-v1",
            token=secrets.token_hex(16),
            hint=source.get("host_workspace_hint"),
            hint_observation=source.get("host_workspace_observation"),
        )
        _write(conn, source)
    return source


def capture_one(store, key):
    """Advance one known source; old receipts and observations remain immutable."""
    _string(key, 128, "source_key")
    try:
        with store._connect() as conn:
            if not _exists(conn):
                return {"status": "not_found"}
            row = conn.execute(
                "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                (key,),
            ).fetchone()
            if row is None:
                return {"status": "not_found"}
            source = _decode(row)
        from .capture_publication import CapturePublication, recover_publications

        recovered = recover_publications(store, key)
        if recovered["status"] != "ok":
            return recovered
        source = _prepare_scope(store, key, store.load_checkpoint(key))
        publication = CapturePublication(store, key, source["scope_attempt"])
        try:
            result = ingest_slice(
                store,
                Path(source["path"]),
                allowed_root=Path(source["root"]),
                format=source["format"],
                on_prepared=publication.prepare,
                on_published=publication.confirm,
            )
        except (ValueError, OSError):
            result = {"status": "unavailable", "reason": "source_unavailable"}
        checkpoint = store.load_checkpoint(key) if result["status"] == "ok" else None
        with store._connect(True) as conn:
            current = _decode(
                conn.execute(
                    "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                    (key,),
                ).fetchone()
            )
            current.update(last_attempt_ns=time.time_ns(), last_status=result["status"])
            if result["status"] == "ok":
                current.pop("last_error", None)
                # Another hook may have queued a newer observation while this slice ran.
                requeued = current["observed_at_ns"] != source["observed_at_ns"]
                newer = requeued and (
                    current["observed_size"] != result["observed_size"]
                    or current["observed_mtime_ns"] != result["observed_mtime_ns"]
                )
                current.update(
                    accounting=result["accounting"],
                    accounting_start=result["accounting_start"],
                    generation=result["generation"],
                    text_coverage=result["text_coverage"],
                )
                if not newer:
                    current.update(
                        observed_size=result["observed_size"],
                        observed_mtime_ns=result["observed_mtime_ns"],
                        processed_bytes=result["offset"],
                        pending_bytes=result["pending_bytes"],
                        state="pending"
                        if result["pending_bytes"] or result["text_coverage"] == "pending"
                        else "caught_up",
                    )
                current.update(_scope_chunks(conn, key, checkpoint, source["scope_attempt"]))
                current.pop("scope_attempt", None)
                current["publication_pending"] = publication.pending(conn)
                result["publication_pending"] = current["publication_pending"]
                if current["publication_pending"]:
                    current["state"] = "pending"
            else:
                current.update(state="failed", last_error=result.get("reason", result["status"]))
            _write(conn, current)
        return result
    except ERRORS as exc:
        return store._error(exc)


def source_page(store, *, after="", limit=8, pending_only=False):
    _string(after, 128, "after", empty=True)
    if type(limit) is not int or not 1 <= limit <= 16:
        raise ValueError("limit")
    if type(pending_only) is not bool:
        raise ValueError("pending_only")
    try:
        with store._connect() as conn:
            if not _exists(conn):
                return {"status": "ok", "sources": [], "next_after": None}
            from .capture_publication import pending_publications

            where = " AND state IN ('pending','failed')" if pending_only else ""
            if (
                pending_only
                and conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE name='capture_publication_intents'"
                ).fetchone()
            ):
                where = " AND (state IN ('pending','failed') OR EXISTS (SELECT 1 FROM capture_publication_intents p WHERE p.source_key=capture_sources.source_key AND p.state IS NOT 'applied'))"
            rows = conn.execute(
                f"SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key>?{where} ORDER BY source_key LIMIT ?",
                (after, limit + 1),
            ).fetchall()
            sources = []
            for row in rows[:limit]:
                value = _decode(row)
                value["publication_pending"] = pending_publications(conn, value["source_key"])
                if value["publication_pending"] and value["state"] == "caught_up":
                    value["state"] = "pending"
                if len(_json(sources + [value]).encode()) > 12000:
                    if not sources:
                        return {"status": "unavailable", "reason": "source_page_limit"}
                    break
                sources.append(value)
            return {
                "status": "ok",
                "sources": sources,
                "next_after": sources[-1]["source_key"] if len(rows) > len(sources) else None,
            }
    except _Missing:
        return {"status": "ok", "sources": [], "next_after": None}
    except ERRORS as exc:
        return store._error(exc)


def coverage(store):
    """Current aggregate over observed sources, never a claim of root discovery."""
    result = dict(
        status="ok",
        observed_sources=0,
        pending_sources=0,
        failed_sources=0,
        publication_pending=0,
        observed_bytes=0,
        processed_bytes=0,
        pending_bytes=0,
        indexed_text_bytes=0,
        accounting_incomplete_sources=0,
        unscoped_chunks=0,
        workspace_conflict_sources=0,
        workspace_pending_sources=0,
        omitted=dict.fromkeys(OMISSIONS, 0),
        excluded=dict.fromkeys(EXCLUSIONS, 0),
        coverage="empty",
        source_currentness="last_observed_only",
        discovery="not_established",
    )
    metrics = {
        "observed_bytes": "observed_size",
        "processed_bytes": "processed_bytes",
        "pending_bytes": "pending_bytes",
        "indexed_text_bytes": "accounting.text_bytes",
        "unscoped_chunks": "unscoped_chunks",
    }
    metrics.update({key: "accounting." + key for key in OMISSIONS + EXCLUSIONS})
    try:
        with store._connect() as conn:
            if not _exists(conn):
                return result
            # Aggregate in SQLite without constructing an unbounded Python source list.
            sql = ",".join(
                f"COALESCE(SUM(json_extract(value,'$.{path}')),0) AS {key}"
                for key, path in metrics.items()
            )
            row = conn.execute(
                "SELECT count(*) AS observed_sources, "
                "COALESCE(SUM(state='pending'),0) AS pending_sources, "
                "COALESCE(SUM(state='failed'),0) AS failed_sources, "
                "COALESCE(SUM(json_extract(value,'$.workspace_scope')='conflict'),0) AS workspace_conflict_sources, "
                "COALESCE(SUM(json_extract(value,'$.workspace_scope')='pending'),0) AS workspace_pending_sources, "
                "COALESCE(SUM(json_extract(value,'$.accounting_start')>0),0) AS accounting_incomplete_sources, "
                + sql
                + " FROM capture_sources"
            ).fetchone()
            for key in result.keys() & row.keys():
                result[key] = row[key]
            from .capture_publication import pending_publications

            result["publication_pending"] = pending_publications(conn)
            result["omitted"] = {key: row[key] for key in OMISSIONS}
            result["excluded"] = {key: row[key] for key in EXCLUSIONS}
            if result["observed_sources"]:
                result["coverage"] = (
                    "partial"
                    if result["pending_sources"]
                    or result["publication_pending"]
                    or result["failed_sources"]
                    or result["accounting_incomplete_sources"]
                    else "gaps"
                    if any(result["omitted"].values())
                    else "complete_supported_text_at_last_observation"
                )
        return result
    except _Missing:
        return result
    except ERRORS as exc:
        return store._error(exc)

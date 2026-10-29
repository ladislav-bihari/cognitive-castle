"""Body-free capture intents; only actual canonical receipts authorize ID scope.

The selected store supplies both append and receipt reads. This module never
chooses a fallback backend, hydrates text, or treats expected IDs as proof.
"""

import json
import re

from .canonical_records import _hash, _json, _string, validate_remember_receipt
from .fast_memory import _Corrupt

MAX_INTENT_BYTES = 8192


def ensure_journal(conn):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS capture_publication_intents ("
        "request_key TEXT PRIMARY KEY,source_key TEXT NOT NULL,generation INTEGER NOT NULL,"
        "binding TEXT NOT NULL,value TEXT NOT NULL,state TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS capture_publication_state ON capture_publication_intents(state)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS capture_publication_pending "
        "ON capture_publication_intents(source_key,state,request_key)"
    )


def _identity(namespace, key):
    return _hash(_json([namespace, key]))


def _row(conn, identity):
    return conn.execute(
        "SELECT CAST(substr(CAST(request_key AS BLOB),1,65) AS TEXT) AS request_key,"
        "CAST(substr(CAST(source_key AS BLOB),1,65) AS TEXT) AS source_key,"
        "CASE WHEN typeof(generation)='integer' THEN generation ELSE NULL END AS generation,CAST(substr(CAST(binding AS BLOB),1,65) AS TEXT) AS binding,"
        "CAST(substr(CAST(value AS BLOB),1,8193) AS TEXT) AS value,CAST(substr(CAST(state AS BLOB),1,16) AS TEXT) AS state "
        "FROM capture_publication_intents WHERE request_key=?",
        (identity,),
    ).fetchone()


def _decode(row):
    try:
        raw = _string(row["value"], MAX_INTENT_BYTES, "publication intent")
        value = json.loads(raw)
        fields = {
            "namespace",
            "key",
            "digest",
            "receipt",
            "source_key",
            "generation",
            "record_start",
            "workspace",
            "attribution",
            "map",
            "source_time",
        }
        if not isinstance(value, dict) or value.keys() != fields or _hash(raw) != row["binding"]:
            raise ValueError("intent binding")
        if row["state"] not in {"pending", "applied"} or value["namespace"] != "transcript":
            raise ValueError("intent state")
        _string(value["key"], 128, "request key")
        for name in ("source_key", "digest"):
            if not isinstance(value[name], str) or not re.fullmatch(r"[a-f0-9]{64}", value[name]):
                raise ValueError("intent hash")
        for name in ("generation", "record_start", "source_time"):
            if type(value[name]) is not int or not 0 <= value[name] < 2**63:
                raise ValueError("intent position")
        if (
            row["request_key"] != _identity(value["namespace"], value["key"])
            or row["source_key"] != value["source_key"]
            or row["generation"] != value["generation"]
        ):
            raise ValueError("intent identity")
        if value["workspace"] is not None:
            from pathlib import PurePosixPath

            path = _string(value["workspace"], 2048, "workspace")
            if not PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts:
                raise ValueError("workspace")
        if (
            type(value["map"]) is not bool
            or value["map"]
            and not value["workspace"]
            or value["attribution"] not in {"transcript_metadata", "host_event", "unattributed"}
        ):
            raise ValueError("intent scope")
        receipt = value["receipt"]
        if (
            not isinstance(receipt, dict)
            or receipt.keys() != {"status", "ids", "stored_text_bytes", "text_sha256"}
            or receipt["status"] != "ok"
            or not isinstance(receipt["ids"], list)
            or not 1 <= len(receipt["ids"]) <= 9
            or type(receipt["stored_text_bytes"]) is not int
            or not 1 <= receipt["stored_text_bytes"] <= 65536
            or not isinstance(receipt["text_sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", receipt["text_sha256"])
            or receipt["ids"]
            != ["memory:" + _hash(f"{value['digest']}:{i}") for i in range(len(receipt["ids"]))]
        ):
            raise ValueError("expected receipt")
        return value
    except (ValueError, KeyError, TypeError, RecursionError) as exc:
        raise _Corrupt("invalid capture publication intent") from exc


def _core(prepared, source_key):
    caller = prepared.records[0].source()["provenance"]["caller"]
    if prepared.namespace != "transcript" or caller.get("source") != source_key:
        raise _Corrupt("capture publication source")
    return dict(
        namespace=prepared.namespace,
        key=prepared.request_id,
        digest=prepared.digest,
        receipt=prepared.receipt,
        source_key=source_key,
        generation=caller["generation"],
        record_start=caller["record_start"],
    )


def _apply(conn, identity, intent):
    if intent["map"]:
        for chunk in intent["receipt"]["ids"]:
            values = (
                chunk,
                intent["source_key"],
                intent["generation"],
                intent["workspace"],
                intent["attribution"],
                intent["source_time"],
            )
            old = conn.execute(
                "SELECT * FROM capture_chunk_scopes WHERE chunk_id=?", (chunk,)
            ).fetchone()
            if old is not None and tuple(old) != values:
                raise _Corrupt("publication scope conflict")
            conn.execute("INSERT OR IGNORE INTO capture_chunk_scopes VALUES (?,?,?,?,?,?)", values)
    conn.execute(
        "UPDATE capture_publication_intents SET state='applied' WHERE request_key=?", (identity,)
    )


class CapturePublication:
    def __init__(self, store, source_key, attempt):
        self.store, self.key, self.attempt = store, source_key, attempt

    def prepare(self, prepared):
        from .memory_capture import _decode as decode_source, _generation_scope

        core = _core(prepared, self.key)
        identity = _identity(prepared.namespace, prepared.request_id)
        checkpoint = self.store.load_checkpoint(self.key)
        if checkpoint is None or checkpoint["generation"] != core["generation"]:
            raise _Corrupt("capture publication checkpoint")
        with self.store._connect(True) as conn:
            ensure_journal(conn)
            old = _row(conn, identity)
            if old is not None:
                value = _decode(old)
                if any(value[k] != v for k, v in core.items()):
                    raise _Corrupt("capture publication request conflict")
                return dict(status="ok")
            current = conn.execute(
                "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                (self.key,),
            ).fetchone()
            if decode_source(current).get("scope_attempt") != self.attempt:
                raise _Corrupt("capture publication attempt changed")
            scope = _generation_scope(conn, self.key, checkpoint, self.attempt)
            start = core["record_start"]
            allowed = (
                scope["can_map"]
                and (scope["metadata_start"] is None or start >= scope["metadata_start"])
                and (scope["conflict_at"] is None or start < scope["conflict_at"])
            )
            value = dict(
                core,
                workspace=scope["workspace"],
                attribution=scope["attribution"],
                map=bool(allowed),
                source_time=checkpoint["observed_mtime_ns"],
            )
            raw = _string(_json(value), MAX_INTENT_BYTES, "publication intent")
            bound = _hash(raw)
            _decode(
                dict(
                    request_key=identity,
                    source_key=self.key,
                    generation=core["generation"],
                    value=raw,
                    binding=bound,
                    state="pending",
                )
            )
            conn.execute(
                "INSERT INTO capture_publication_intents VALUES (?,?,?,?,?,?)",
                (identity, self.key, core["generation"], bound, raw, "pending"),
            )
        return dict(status="ok")

    def confirm(self, prepared, receipt):
        validate_remember_receipt(prepared, receipt)
        identity = _identity(prepared.namespace, prepared.request_id)
        core = _core(prepared, self.key)
        with self.store._connect(True) as conn:
            row = _row(conn, identity)
            if row is None:
                raise _Corrupt("missing publication intent")
            intent = _decode(row)
            if any(intent[k] != v for k, v in core.items()):
                raise _Corrupt("publication confirmation conflict")
            _apply(conn, identity, intent)
        return dict(status="ok")

    def pending(self, conn):
        return pending_publications(conn, self.key)


def pending_publications(conn, source_key=None):
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='capture_publication_intents'"
    ).fetchone():
        return 0
    where = "state IS NOT 'applied'"
    args = ()
    if source_key is not None:
        where += " AND source_key=?"
        args = (source_key,)
    return conn.execute(
        "SELECT count(*) FROM capture_publication_intents WHERE " + where, args
    ).fetchone()[0]


def _recovery_source(conn, source_key):
    from .memory_capture import _decode as decode_source

    row = conn.execute(
        "SELECT source_key,state,CAST(substr(CAST(value AS BLOB),1,16385) AS TEXT) AS value "
        "FROM capture_sources WHERE source_key=?",
        (source_key,),
    ).fetchone()
    if row is None:
        raise _Corrupt("missing capture recovery source")
    return decode_source(row)


def recover_publications(store, source_key):
    """Probe at most two intents fairly; absence never blocks fresh capture.

    Expected IDs stay pending until an actual canonical receipt confirms them.
    A durable keyset cursor prevents old absent requests starving later ones.
    """
    empty = dict(status="ok", checked=0, recovered=0, pending=0)
    with store._connect() as conn:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='capture_publication_intents'"
        ).fetchone():
            return empty
        source = _recovery_source(conn, source_key)
        after = source.get("publication_recovery_after", "")
        if not isinstance(after, str) or after and not re.fullmatch(r"[a-f0-9]{64}", after):
            raise _Corrupt("invalid capture recovery cursor")
        query = (
            "SELECT CAST(substr(CAST(request_key AS BLOB),1,65) AS TEXT) AS request_key "
            "FROM capture_publication_intents WHERE source_key=? AND state IS NOT 'applied' "
            "AND request_key>? ORDER BY request_key LIMIT 2"
        )
        rows = conn.execute(query, (source_key, after)).fetchall()
        if not rows and after:
            rows = conn.execute(query, (source_key, "")).fetchall()
        intents = [(row[0], _decode(_row(conn, row[0]))) for row in rows]
    if not intents:
        return empty
    recovered = 0
    for identity, intent in intents:
        proof = store.receipt(namespace=intent["namespace"], key=intent["key"])
        if proof.get("status") == "not_found":
            continue
        if proof.get("status") != "ok":
            return proof
        if proof.get("digest") != intent["digest"] or proof.get("receipt") != intent["receipt"]:
            raise _Corrupt("canonical publication receipt conflict")
        with store._connect(True) as conn:
            current = _decode(_row(conn, identity))
            if current != intent:
                raise _Corrupt("publication intent changed during recovery")
            _apply(conn, identity, intent)
        recovered += 1
    with store._connect(True) as conn:
        from .memory_capture import _write

        source = _recovery_source(conn, source_key)
        source["publication_recovery_after"] = intents[-1][0]
        _write(conn, source)
        pending = pending_publications(conn, source_key)
    return dict(status="ok", checked=len(intents), recovered=recovered, pending=pending)

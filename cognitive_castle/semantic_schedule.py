"""Schema6 body-free scheduling, used only inside the queue transaction.

Only explicit operator preparation creates this extension. Projection rows are
routing metadata, never canonical/source/vector proof. No database/model imports.
"""

import json

from .lance_exact_store import _StoreError
from .semantic_profile import canonical_json, digest

FORMAT = "castle-index-schedule-v1"
PAGE = 8
MAX_INTENT_BYTES = 196608
DDL = {
    "index_schedule": "CREATE TABLE index_schedule (id INTEGER PRIMARY KEY CHECK(id=1), next_work TEXT NOT NULL CHECK(next_work IN ('recovery','index')), new_intents INTEGER NOT NULL CHECK(new_intents BETWEEN 0 AND 3), new_cursor INTEGER NOT NULL CHECK(new_cursor>=0), history_cursor INTEGER NOT NULL CHECK(history_cursor>=0), backfill_cursor INTEGER NOT NULL CHECK(backfill_cursor>=0), ready INTEGER NOT NULL CHECK(ready IN (0,1)))",
    "index_intents": "CREATE TABLE index_intents (intent_id TEXT PRIMARY KEY NOT NULL, seq INTEGER UNIQUE NOT NULL, lane TEXT NOT NULL CHECK(lane IN ('new','history')), complete INTEGER NOT NULL CHECK(complete IN (0,1)))",
}
INDEXES = {
    "index_intents_pending": "CREATE INDEX index_intents_pending ON index_intents(lane,complete,seq)",
    "index_groups_pending": "CREATE INDEX index_groups_pending ON groups(lane,state,seq)",
}
# Project only bounded fields before SQLite copies a potentially corrupt value.
PROJECTION = "i.seq,CASE WHEN typeof(i.intent_id)='text' AND length(CAST(i.intent_id AS BLOB))=64 THEN i.intent_id END AS intent_id,CASE WHEN typeof(i.binding)='text' AND length(CAST(i.binding AS BLOB))=64 THEN i.binding END AS binding,CASE WHEN typeof(i.intent_json)='text' AND length(CAST(i.intent_json AS BLOB))<=196608 THEN i.intent_json END AS intent_json,i.complete"


def state(conn):
    row = conn.execute("SELECT * FROM index_schedule WHERE id=1").fetchone()
    if row is None or row["next_work"] not in {"recovery", "index"}:
        raise _StoreError("corrupt")
    values = dict(row)
    for key in ("new_intents", "new_cursor", "history_cursor", "backfill_cursor", "ready"):
        if type(values[key]) is not int or not 0 <= values[key] < 2**63:
            raise _StoreError("corrupt")
    if values["new_intents"] > 3 or values["ready"] not in (0, 1):
        raise _StoreError("corrupt")
    return values


def decode(row):
    from .semantic_intents import validate_intent

    if (
        row["intent_json"] is None
        or type(row["seq"]) is not int
        or not 0 < row["seq"] < 2**63
        or type(row["complete"]) is not int
        or row["complete"] not in (0, 1)
    ):
        raise _StoreError("corrupt")
    try:
        value = json.loads(row["intent_json"])
        validate_intent(value)
        if (
            canonical_json(value) != row["intent_json"]
            or value["intent_id"] != row["intent_id"]
            or digest(value) != row["binding"]
        ):
            raise ValueError("intent metadata changed")
    except (ValueError, TypeError, KeyError, RecursionError):
        raise _StoreError("corrupt") from None
    lane = "new" if any(g["lane"] == "new" for g in value["groups"]) else "history"
    return value, lane


def project(conn, identity):
    row = conn.execute(
        "SELECT " + PROJECTION + " FROM intents i WHERE i.intent_id=?", (identity,)
    ).fetchone()
    if row is None:
        raise _StoreError("corrupt")
    _, lane = decode(row)
    expected = (row["intent_id"], row["seq"], lane, row["complete"])
    previous = conn.execute("SELECT * FROM index_intents WHERE intent_id=?", (identity,)).fetchone()
    if previous is not None and tuple(previous) != expected:
        raise _StoreError("corrupt")
    if previous is None:
        conn.execute("INSERT INTO index_intents VALUES (?,?,?,?)", expected)


def initialize(conn):
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version == 5:
        for sql in (*DDL.values(), *INDEXES.values()):
            conn.execute(sql)
        conn.execute("INSERT INTO index_schedule VALUES (1,'recovery',0,0,0,0,0)")
        conn.execute("PRAGMA user_version=6")
    elif version != 6:
        raise _StoreError("unavailable")
    current = state(conn)
    processed = 0
    if not current["ready"]:
        rows = conn.execute(
            "SELECT seq,intent_id FROM intents WHERE seq>? ORDER BY seq LIMIT 8",
            (current["backfill_cursor"],),
        ).fetchall()
        for row in rows:
            project(conn, row["intent_id"])
        cursor = rows[-1]["seq"] if rows else current["backfill_cursor"]
        pending = conn.execute("SELECT 1 FROM intents WHERE seq>? LIMIT 1", (cursor,)).fetchone()
        conn.execute(
            "UPDATE index_schedule SET backfill_cursor=?,ready=? WHERE id=1",
            (cursor, int(pending is None)),
        )
        processed = len(rows)
        current = state(conn)
    return dict(
        status="ok" if current["ready"] else "in_progress",
        format=FORMAT,
        processed=processed,
        backfill_cursor=current["backfill_cursor"],
        ready=bool(current["ready"]),
    )


def _candidate(conn, lane, cursor, excluded):
    for threshold in (cursor, 0):
        row = conn.execute(
            "SELECT " + PROJECTION + ",p.lane AS projected_lane,p.complete AS projected_complete "
            "FROM index_intents p JOIN intents i ON i.seq=p.seq AND i.intent_id=p.intent_id "
            "WHERE p.lane=? AND p.complete=0 AND p.seq>? AND p.seq<>? ORDER BY p.seq LIMIT 1",
            (lane, threshold, excluded),
        ).fetchone()
        if row is not None:
            value, actual_lane = decode(row)
            if actual_lane != lane or row["complete"] != row["projected_complete"]:
                raise _StoreError("corrupt")
            return row["seq"], value
    return None


def _recover(conn, current):
    chosen = []
    excluded = 0
    for _ in range(2):
        candidates = {
            lane: _candidate(conn, lane, current[lane + "_cursor"], excluded)
            for lane in ("new", "history")
        }
        lane = (
            "history"
            if candidates["history"] is not None
            and (current["new_intents"] >= 3 or candidates["new"] is None)
            else "new"
        )
        candidate = candidates[lane]
        if candidate is None:
            break
        excluded, value = candidate
        chosen.append(value)
        current[lane + "_cursor"] = excluded
        current["new_intents"] = min(3, current["new_intents"] + 1) if lane == "new" else 0
    if chosen:
        conn.execute(
            "UPDATE index_schedule SET next_work='index',new_intents=?,new_cursor=?,history_cursor=? WHERE id=1",
            (current["new_intents"], current["new_cursor"], current["history_cursor"]),
        )
        return dict(status="ok", intents=chosen, selection=None)
    return None


def prepare(conn, select_group):
    current = state(conn)
    if not current["ready"]:
        return dict(status="unavailable", reason="index_schedule_not_ready")
    for category in (
        ("recovery", "index") if current["next_work"] == "recovery" else ("index", "recovery")
    ):
        if category == "recovery":
            result = _recover(conn, current)
            if result is not None:
                return result
        else:
            selection = select_group(conn)
            if selection["status"] != "idle":
                conn.execute("UPDATE index_schedule SET next_work='recovery' WHERE id=1")
                return dict(status="ok", intents=[], selection=selection)
    return dict(status="ok", intents=[], selection=dict(status="idle"))

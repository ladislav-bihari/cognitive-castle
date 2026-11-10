"""Body-free verified ID accounting. Expected/prepared IDs never count as proof."""

from .canonical_records import _hash
from .fast_memory import _Corrupt


def is_control(conn):
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='capture_control_authority' AND type='table'"
        ).fetchone()
        is not None
    )


def initialize_baseline(conn, key, checkpoint, previous):
    first_generation = previous is None or previous["generation"] < checkpoint["generation"]
    empty = (
        checkpoint["offset"] == 0
        and checkpoint["anchor"] == _hash("")
        and not checkpoint["gaps"]
        and not checkpoint["unsupported"]
        and not checkpoint["skipping"]
        and checkpoint.get("pending") is None
        and checkpoint.get("accounting_start", 0) == 0
        and not any(checkpoint.get("accounting", {}).values())
        and "skip_start" not in checkpoint
    )
    state = "fresh" if first_generation and empty else "pending_import"
    conn.execute(
        "INSERT OR IGNORE INTO capture_count_baselines VALUES (?,?,?)",
        (key, checkpoint["generation"], state),
    )


def record_confirmed(conn, intent):
    for identity in intent["receipt"]["ids"]:
        prior = conn.execute(
            "SELECT source_key,generation FROM capture_verified_chunks WHERE chunk_id=?",
            (identity,),
        ).fetchone()
        owner = (intent["source_key"], intent["generation"])
        if prior is not None and tuple(prior) != owner:
            raise _Corrupt("verified capture ID ownership conflict")
        conn.execute(
            "INSERT OR IGNORE INTO capture_verified_chunks VALUES (?,?,?)", (identity, *owner)
        )


def summary(conn, key, checkpoint, attempt, scope):
    generation = checkpoint["generation"]
    baseline = conn.execute(
        "SELECT state FROM capture_count_baselines WHERE source_key=? AND generation=?",
        (key, generation),
    ).fetchone()
    if baseline is not None and baseline[0] not in {"fresh", "pending_import"}:
        raise _Corrupt("invalid capture count baseline")
    complete = baseline is not None and baseline[0] == "fresh" and "after_rowid" not in attempt
    total = conn.execute(
        "SELECT count(*) FROM capture_verified_chunks WHERE source_key=? AND generation=?",
        (key, generation),
    ).fetchone()[0]
    mapped = conn.execute(
        "SELECT count(*) FROM capture_verified_chunks v JOIN capture_chunk_scopes s ON s.chunk_id=v.chunk_id AND s.source_key=v.source_key AND s.generation=v.generation WHERE v.source_key=? AND v.generation=?",
        (key, generation),
    ).fetchone()[0]
    state = scope["state"]
    if state != "conflict":
        if not complete:
            state = "pending"
        elif mapped < total:
            state = "partial" if mapped else "unattributed"
    return dict(
        workspace=scope["workspace"],
        workspace_attribution=scope["attribution"],
        workspace_scope=state,
        scoped_chunks=mapped,
        unscoped_chunks=total - mapped if complete else None,
        known_unscoped_chunks=total - mapped,
        scope_accounting_complete=complete,
    )

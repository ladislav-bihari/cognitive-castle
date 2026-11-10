"""Inactive source preparation/confirmation inside the supervised SQL worker only."""

from pathlib import Path
import time
from . import memory_capture as capture
from .capture_publication import CapturePublication, _identity, _row, _decode
from .capture_control_codec import validate_publication
from .fast_memory import _Corrupt
from .memory_ingest import ingest_slice


def _source(store, key):
    with store._connect() as conn:
        row = conn.execute(
            "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
            (key,),
        ).fetchone()
        return None if row is None else capture._decode(row)


def _finish(store, key, source, journal, result):
    status = result["status"]
    checkpoint = store.load_checkpoint(key) if status in {"ok", "prepared"} else None
    with store._connect(True) as conn:
        current = capture._decode(
            conn.execute(
                "SELECT source_key,state,substr(value,1,16385) AS value FROM capture_sources WHERE source_key=?",
                (key,),
            ).fetchone()
        )
        if current.get("scope_attempt") != source["scope_attempt"]:
            raise _Corrupt("capture source attempt changed")
        current.update(last_attempt_ns=time.time_ns(), last_status=status)
        if checkpoint is not None:
            current.pop("last_error", None)
            newer = current["observed_at_ns"] != source["observed_at_ns"] and (
                current["observed_size"] != checkpoint["observed_size"]
                or current["observed_mtime_ns"] != checkpoint["observed_mtime_ns"]
            )
            current.update(
                accounting=checkpoint["accounting"],
                accounting_start=checkpoint["accounting_start"],
                generation=checkpoint["generation"],
                text_coverage="pending" if status == "prepared" else result["text_coverage"],
            )
            if not newer:
                current.update(
                    observed_size=checkpoint["observed_size"],
                    observed_mtime_ns=checkpoint["observed_mtime_ns"],
                    processed_bytes=checkpoint["offset"],
                    pending_bytes=max(0, checkpoint["observed_size"] - checkpoint["offset"]),
                    state="pending"
                    if status == "prepared"
                    or result.get("pending_bytes")
                    or result.get("text_coverage") == "pending"
                    else "caught_up",
                )
            current.update(capture._scope_chunks(conn, key, checkpoint, source["scope_attempt"]))
            if status == "ok":
                current.pop("scope_attempt", None)
            current["publication_pending"] = journal.pending(conn)
            result["publication_pending"] = current["publication_pending"]
            if current["publication_pending"] or current.get("scope_accounting_complete") is False:
                current["state"] = "pending"
        else:
            current.update(state="failed", last_error=result.get("reason", status))
        capture._write(conn, current)
    return result


def prepare_source(store, key, publication=None):
    publication = validate_publication(publication)
    if _source(store, key) is None:
        return dict(status="not_found", publication_consumed=False)
    source = capture._prepare_scope(store, key, store.load_checkpoint(key))
    journal = CapturePublication(store, key, source["scope_attempt"])
    holder = {}
    accepted = False
    consumed = False

    def reserve(prepared):
        result = journal.prepare(prepared)
        if result.get("status") == "ok":
            with store._connect() as conn:
                intent = _decode(_row(conn, _identity(prepared.namespace, prepared.request_id)))
            holder.update(
                prepared=prepared,
                scope={name: intent[name] for name in ("workspace", "attribution", "map")},
            )
        return result

    def publish(request_id, text, provenance, *, namespace):
        nonlocal accepted
        prepared = holder["prepared"]
        if publication is not None and not accepted:
            if publication["request_id"] != request_id or publication["digest"] != prepared.digest:
                return dict(status="conflict", reason="capture_publication_changed")
            accepted = True
            return publication["receipt"]
        return dict(
            status="prepared",
            preparation=dict(
                request_id=request_id, text=text, provenance=provenance, namespace=namespace
            ),
        )

    def confirm(prepared, receipt):
        nonlocal consumed
        result = journal.confirm(prepared, receipt)
        if result.get("status") == "ok":
            consumed = True
        return result

    try:
        result = ingest_slice(
            store,
            Path(source["path"]),
            allowed_root=Path(source["root"]),
            format=source["format"],
            publisher=publish,
            on_prepared=reserve,
            on_published=confirm,
        )
    except (ValueError, OSError):
        result = dict(status="unavailable", reason="source_unavailable")
    if result.get("status") == "prepared":
        result["scope"] = holder["scope"]
    result = _finish(store, key, source, journal, result)
    result.update(format="capture-control-step-v1", publication_consumed=consumed)
    return result

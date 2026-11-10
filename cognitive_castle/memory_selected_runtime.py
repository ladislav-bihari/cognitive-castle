"""Inactive selected-Lance host adapter; no storage initialization or fallback.

Unsupported canonical routes refuse until their complete adapters are ready.
No activation writer exists here and selected metadata is not a readiness proof.
"""

import math
from pathlib import Path
import time
from .canonical_records import prepare_remember
from .capture_control_runtime import CaptureControlClient
from .live_publication_codec import prepare_publication
from . import live_publication, migration_codec, semantic_runtime
from .memory_selection import (
    load_selection,
    SelectionUnavailable,
    SelectionBusy,
    selection_lease,
)

SUPPORTED = {
    "remember",
    "read_sources",
    "page",
    "coverage",
    "source_page",
    "queue_source",
    "capture_one",
    "discover_sources",
    "discovery_status",
    "status",
}


def layout_hash(selected):
    return migration_codec.hashed(selected["envelope"])


def _current(palace, selected):
    try:
        return load_selection(palace) == selected
    except SelectionUnavailable:
        return False


def _status(session, control, generation, selected, arguments):
    """Compose recorded metadata; this does not qualify corpus/native health."""
    from .capture_control_codec import validate_operation

    arguments = validate_operation("control_status", arguments)
    page = control.call("control_status", arguments)
    if page.get("status") != "ok":
        return page
    ready = session._ready()
    if ready is not None:
        return ready
    pin = layout_hash(selected)
    queued = session.store(generation, "queue_status", dict(envelope_hash=pin))
    if queued.get("status") != "ok":
        return queued
    registered = queued.get("registered")
    if (
        not isinstance(registered, dict)
        or registered.keys()
        != {
            "envelope_hash",
            "confirmed_records",
            "pending_claims",
            "count_basis",
            "native_rows_rechecked",
        }
        or registered["envelope_hash"] != pin
        or registered["count_basis"] != "registered_claims"
        or registered["native_rows_rechecked"] is not False
        or any(
            type(registered[k]) is not int or not 0 <= registered[k] < 2**63
            for k in ("confirmed_records", "pending_claims")
        )
    ):
        return dict(status="unavailable", reason="selected_status_contract")
    ready = session._ready()
    if ready is not None:
        return ready
    captured = control.call("coverage", {})
    if captured.get("status") != "ok":
        return captured
    return dict(
        page,
        indexed_chunks=registered["confirmed_records"],
        canonical=registered,
        generation=pin,
        storage_format=selected["envelope"]["format"],
        capture=captured,
        semantic=dict(
            counts=queued["counts"],
            groups=queued["groups"][:8],
            positions_truncated=queued["positions_truncated"] or len(queued["groups"]) > 8,
            coverage_complete=False,
        ),
        coverage="indexed_only",
        coverage_detail=dict(
            complete=False,
            limitations=[
                "canonical_corpus_reconciliation_pending",
                "semantic_coverage_unqualified",
                "historical_capture_metadata_pending",
            ],
        ),
    )


def _perform(session, selected, operation, arguments):
    generation = Path(selected["envelope"]["generation"]["path"])
    if operation in {"read_sources", "page"}:
        if operation == "page":
            arguments = dict(arguments)
            if arguments.pop("field", "text") != "text":
                return dict(status="unavailable", reason="canonical_route_not_ready")
        return session.exact(
            generation,
            "layout_read",
            dict(operation=operation, arguments=arguments, envelope_hash=layout_hash(selected)),
        )
    if operation == "remember":
        canonical = prepare_remember(**arguments)
        scope = dict(workspace=None, wing=selected["wing"], room=selected["room"])
        publication = prepare_publication(canonical, [scope for _ in canonical.records])
        return live_publication.publish(session, generation, publication, layout_hash(selected))
    control = CaptureControlClient(
        session, selected["control_authority"], expected_hash=selected["control_hash"]
    )
    if operation == "status":
        return _status(session, control, generation, selected, arguments)
    if operation == "capture_one":
        from .capture_live_coordinator import advance

        return advance(
            control,
            arguments["key"],
            generation,
            layout_hash(selected),
            wing=selected["wing"],
            room=selected["room"],
        )
    return control.call("source_queue" if operation == "queue_source" else operation, arguments)


def run_selected(palace, selected, operation, arguments, *, until, background):
    """Borrow original operation deadline; a selected failure never uses SQLite."""
    if type(until) not in (int, float) or not math.isfinite(until) or type(background) is not bool:
        raise ValueError("canonical scheduling")
    if time.monotonic() >= until:
        return dict(status="unavailable", reason="semantic_deadline")
    if operation not in SUPPORTED:
        return dict(status="unavailable", reason="canonical_route_not_ready")
    if not _current(palace, selected):
        return dict(status="unavailable", reason="canonical_selection_changed")
    try:
        with selection_lease(palace, selected):
            return _run_leased(palace, selected, operation, arguments, until, background)
    except SelectionBusy:
        return dict(status="unavailable", reason="canonical_selection_busy")
    except SelectionUnavailable:
        return dict(status="unavailable", reason="canonical_selection_changed")


def _run_leased(palace, selected, operation, arguments, until, background):
    with semantic_runtime.SemanticSession(
        Path(selected["profile"]["path"]), interactive=not background
    ) as session:
        session._until = min(session._until, until)
        ready = session._ready()
        if ready is not None:
            return ready
        result = _perform(session, selected, operation, arguments)
        if not _current(palace, selected):
            return dict(status="unavailable", reason="canonical_selection_changed")
        ready = session._ready()
        return ready if ready is not None else result

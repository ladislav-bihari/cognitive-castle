"""Inactive capture handoff to registered Lance, using one caller-owned lease.

Control metadata never substitutes for native publication proof. This module
opens no database, acquires no lease and selects no production generation.
"""

from .canonical_records import prepare_remember, validate_remember_receipt, _string
from .capture_control_codec import validate_operation
from .bulk_exact_codec import sha
from .live_publication_codec import prepare_publication
from . import live_publication


def advance(control, source_key, generation, envelope_hash, *, wing, room):
    """Attempt one pending publication, then confirm only its actual receipt.

    A fresh native append may yield for replay in a later caller lease. No source
    checkpoint advances on that response. The caller retains responsibility for
    trusted generation selection, exposure policy and bounded scheduling.
    """
    arguments = validate_operation("source_prepare", dict(source_key=source_key))
    sha(envelope_hash)
    _string(wing, 512, "wing")
    _string(room, 512, "room")
    session = control.session
    ready = session._ready()
    if ready is not None:
        return ready
    result = control.call("source_prepare", arguments)
    if not isinstance(result, dict):
        return dict(status="unavailable", reason="capture_control_response")
    if result.get("status") != "prepared":
        return result
    ready = session._ready()
    if ready is not None:
        return ready
    try:
        scope = result["scope"]
        if not isinstance(scope, dict) or scope.keys() != {"workspace", "attribution", "map"}:
            raise ValueError("capture scope")
        if scope["map"] is not True or scope["workspace"] is None:
            return dict(status="in_progress", reason="capture_scope_unresolved")
        preparation = result["preparation"]
        if not isinstance(preparation, dict) or preparation.keys() != {
            "request_id",
            "text",
            "provenance",
            "namespace",
        }:
            raise ValueError("capture preparation")
        canonical = prepare_remember(**preparation)
        publication = prepare_publication(
            canonical,
            [dict(workspace=scope["workspace"], wing=wing, room=room) for _ in canonical.records],
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        return dict(status="unavailable", reason="capture_preparation_contract")
    receipt = live_publication.publish(session, generation, publication, envelope_hash)
    ready = session._ready()
    if ready is not None:
        return ready
    if not isinstance(receipt, dict):
        return dict(status="unavailable", reason="capture_publication_response")
    if receipt.get("status") != "ok":
        return receipt
    try:
        receipt = validate_remember_receipt(canonical, receipt)
    except (ValueError, TypeError, KeyError):
        return dict(status="conflict", reason="capture_publication_receipt")
    result = control.call(
        "source_prepare",
        dict(
            source_key=source_key,
            publication=dict(
                request_id=canonical.request_id, digest=canonical.digest, receipt=receipt
            ),
        ),
    )
    ready = session._ready()
    if ready is not None:
        return ready
    if not isinstance(result, dict):
        return dict(status="unavailable", reason="capture_confirmation_response")
    if (
        result.get("status") in {"ok", "prepared"}
        and result.get("publication_consumed") is not True
    ):
        return dict(status="in_progress", reason="capture_confirmation_unconsumed")
    return result

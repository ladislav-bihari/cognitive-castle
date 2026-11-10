"""Inactive direct publication through the existing registered layout protocol.

One caller-owned session supplies admission, deadlines and all workers. Exact
success requires an actual registered receipt, never an expected preparation or
first-append response. The durable semantic outbox remains for bounded indexing.
This module does not select/activate generations or change ordinary routes.
"""

from pathlib import Path

from . import lance_layout_codec as layout
from . import live_publication_codec as codec
from . import migration_codec as migration
from .canonical_records import validate_remember_receipt


def _packet(publication):
    # Iteration validates identical canonical regeneration before any storage I/O.
    return sum(1 for _ in codec.batches(publication))


def source_result(publication, registration, before, window, qualification):
    """Pure direct-source plan/export response, not archive or publication proof."""
    from .sqlite_export_codec import layout_result

    _packet(publication)
    migration.validate_registration(registration)
    descriptor = codec.source_descriptor(publication.intent)
    if registration != migration.registration(descriptor, registration["destination"]):
        raise ValueError("direct source registration")
    layout.validate_layout_source_window(window, descriptor, before)
    layout.validate_qualification_body(
        qualification,
        expected_hash=window["qualification_hash"],
        generation=window["generation"],
    )
    if window["generation"]["path"] != registration["destination"]:
        raise ValueError("direct source generation")
    inner = codec.source_slice(publication, before)
    plans = []
    if not inner["complete"]:
        counter = layout.LayoutPlanningCounters(qualification)
        for entry in publication.entries:
            counter.add_entry(entry)
        semantic = publication.intent["semantic"]
        plans.append(
            counter.finish(
                registration,
                publication.manifest,
                "live",
                [semantic["namespace"], semantic["key"]],
                separate_finalization=True,
            )
        )
    return layout_result(descriptor, before, window, inner, plans)


class _DirectSession:
    """Borrow the existing lease; override only the explicitly direct producer."""

    def __init__(self, session, publication, registration, qualification):
        self.session = session
        self.publication = publication
        self.registration = registration
        self.qualification = qualification

    def __getattr__(self, name):
        return getattr(self.session, name)

    def layout_source(self, descriptor, cursor, window):
        ready = self.session._ready()
        if ready is not None:
            return ready
        if descriptor != self.registration["descriptor"]:
            return {"status": "conflict", "reason": "direct_source_binding"}
        try:
            result = source_result(
                self.publication, self.registration, cursor, window, self.qualification
            )
        except (ValueError, KeyError, TypeError):
            return {"status": "conflict", "reason": "direct_source_contract"}
        ready = self.session._ready()
        return ready if ready is not None else result


def step(session, generation, publication, envelope_hash):
    """One bounded interval; a fresh append yields for a later cold lease."""
    from .lance_layout_directory import load_layout_qualification
    from .migration_coordinator import _layout_envelope, _layout_tick

    ready = session._ready()
    if ready is not None:
        return ready
    _packet(publication)
    migration.bulk.sha(envelope_hash)
    generation = Path(generation)
    registration = migration.registration(
        codec.source_descriptor(publication.intent), str(generation)
    )
    state = session.store(
        generation,
        "queue_live_open",
        dict(intent=publication.intent, envelope_hash=envelope_hash),
    )
    if not isinstance(state, dict) or state.get("status") != "ok":
        return state if isinstance(state, dict) else {"status": "unavailable"}
    try:
        if state["registration"] != registration:
            raise ValueError("direct publication registration")
        envelope = _layout_envelope(state, registration)
        if migration.hashed(envelope) != envelope_hash:
            raise ValueError("direct publication operator binding")
        qualification = load_layout_qualification(
            envelope["generation"], envelope["qualification_hash"]
        )
    except (OSError, ValueError, KeyError, TypeError):
        return {"status": "unavailable", "reason": "direct_publication_authority"}
    adapter = _DirectSession(session, publication, registration, qualification)
    return _layout_tick(adapter, registration, defer_append=True, state=state, combined_owners=True)


def publish(session, generation, publication, envelope_hash):
    """Bounded foreground attempt; unfinished work resumes from the durable job.

    Return the unchanged canonical receipt only after a cold registered read.
    Semantic outbox recovery/indexing is a separate bounded background operation.
    """
    ready = session._ready()
    if ready is not None:
        return ready
    frames = _packet(publication)
    generation = Path(generation)
    registration = migration.registration(
        codec.source_descriptor(publication.intent), str(generation)
    )
    for _ in range(frames + 1):  # Actual physical frames plus a separate EOF proof.
        result = step(session, generation, publication, envelope_hash)
        if not isinstance(result, dict) or result.get("status") != "ok":
            return result if isinstance(result, dict) else {"status": "unavailable"}
        try:
            if result["job_id"] != registration["job_id"]:
                raise ValueError("direct publication job")
            migration.cursor(result["staging_cursor"], registration)
            migration.cursor(result["completed_frontier"], registration)
            migration.counts(result["counts"])
            state = result["state"]
            if state not in {"active", "snapshot_export_exhausted"}:
                raise ValueError("direct publication progression")
        except (ValueError, KeyError, TypeError):
            return {"status": "unavailable", "reason": "direct_publication_progress"}
        if state == "snapshot_export_exhausted":
            break
    else:
        return {"status": "in_progress", "reason": "direct_publication_intervals"}
    ready = session._ready()
    if ready is not None:
        return ready
    canonical = publication.canonical
    observed = session.exact(
        generation,
        "layout_read",
        dict(
            operation="receipt",
            arguments=dict(namespace=canonical.namespace, key=canonical.request_id),
        ),
    )
    if not isinstance(observed, dict) or observed.get("status") != "ok":
        return observed if isinstance(observed, dict) else {"status": "unavailable"}
    try:
        if observed["digest"] != canonical.digest:
            raise ValueError("direct receipt request")
        return validate_remember_receipt(canonical, observed["receipt"])
    except (ValueError, KeyError, TypeError):
        return {"status": "conflict", "reason": "direct_publication_receipt"}

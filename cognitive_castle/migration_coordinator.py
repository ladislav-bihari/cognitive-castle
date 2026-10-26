"""Inactive private fixture driver for protected SQLite export coordination.

There is deliberately no public, CLI, scheduler or production mover wiring.
The fixture harness owns creation/sealing/lifetime of source and destination.
An open registration is metadata, never a production capacity/snapshot permit.
Every call uses its caller's one SemanticSession and absolute deadline; bodies
exist only for this tick. A pending interval always regenerates from its start.
"""

from pathlib import Path

from . import migration_codec as codec


def _fixture_tick(session, registration):
    codec.validate_registration(registration)
    if registration["format"] != codec.FORMAT:
        raise ValueError("SQLite fixture registration")
    return _tick(session, registration, native=False)


def _fixture_tick_persisted(session, registration):
    """Private qualified transport; ordinary/v1 workers never select this entry."""
    codec.validate_registration(registration)
    if registration["format"] != codec.FORMAT:
        raise ValueError("SQLite fixture registration")
    return _tick(session, registration, native=False, publish=_publish_persisted)


def _native_fixture_tick(session, registration):
    """The one explicit native fixture entry; never a caller-selected provider."""
    codec.validate_registration(registration)
    if registration["format"] != codec.NATIVE_FORMAT:
        raise ValueError("native fixture registration")
    return _tick(session, registration, native=True)


def _layout_replay(session, generation, arguments, pending, *, defer_append=False):
    from . import lance_layout_codec as layout

    for attempt in range(2):
        phase = "publish" if attempt == 0 else "verify_replay"
        ready = session._ready()
        if ready is not None:
            return None, ready, phase
        result = session.exact(generation, "layout_import_batch", arguments)
        if not isinstance(result, dict) or result.get("status") != "ok":
            return None, result if isinstance(result, dict) else {"status": "unavailable"}, phase
        try:
            if result.keys() != {"status", "format", "outcome", "receipt", "routes"}:
                raise ValueError("layout batch envelope")
            for key in ("records", "published_owners", "entries"):
                codec.bulk.integer(result["receipt"][key])
            if result["format"] != layout.BATCH_RESULT or result[
                "receipt"
            ] != layout.expected_receipt(pending):
                raise ValueError("layout batch receipt")
            if result["outcome"] == "existing_verified":
                return {k: v for k, v in result.items() if k != "status"}, None, None
            if result["outcome"] != "fresh_append" or result["routes"] != [] or attempt:
                raise ValueError("layout existing replay required")
        except (ValueError, KeyError, TypeError):
            return None, {"status": "conflict", "reason": "layout_replay_contract"}, phase
        if defer_append:
            # A later caller regenerates from the durable pending intent and
            # proves actual existing bytes. No proof or source body is saved.
            ready = session._ready()
            return None, ready or {"status": "in_progress", "reason": None}, "verify_replay"
    raise AssertionError("closed replay outcomes")


def _layout_window(context, action, hashes):
    from . import lance_layout_codec as layout

    return dict(
        format=layout.SOURCE_REQUEST,
        action=action,
        generation=context["generation"],
        qualification_hash=context["qualification_hash"],
        allowed_plan_hashes=hashes,
        packing=layout.PACKING,
    )


def _layout_context(state, registration):
    from . import lance_layout_codec as layout

    attached = state["layout"]
    layout._fields(attached, {"format", "envelope_hash", "envelope"}, "layout attachment")
    envelope = layout.validate_envelope(attached["envelope"])
    if attached["format"] != layout.ATTACHMENT or attached["envelope_hash"] != codec.hashed(
        envelope
    ):
        raise ValueError("layout attachment")
    context = state["native_layout"]
    layout._fields(
        context,
        {"format", "generation", "qualification_hash", "epoch", "plan", "target"},
        "native layout context",
    )
    if (
        context["format"] != "lance-layout-native-context-v1"
        or context["generation"] != envelope["generation"]
        or context["generation"]["path"] != registration["destination"]
        or context["qualification_hash"] != envelope["qualification_hash"]
    ):
        raise ValueError("native registered context")
    layout.bulk.sha(context["epoch"])
    if context["plan"] is None:
        if context["target"] is not None:
            raise ValueError("unplanned native target")
    else:
        plan = layout.validate_owner_plan(context["plan"], registration)
        target = layout.validate_target(context["target"])
        if target != layout.target(envelope, target["partition_id"], [plan]):
            raise ValueError("registered owner target")
    return context


def _layout_admit_owner(session, registration, state, context, before, refused):
    from . import lance_layout_codec as layout

    if context["plan"] is not None:
        return context["target"], None, None
    action = "prepare" if registration["format"] == codec.NATIVE_FORMAT else "plan"
    window = _layout_window(context, action, [])
    ready = session._ready()
    if ready is not None:
        return None, refused(ready, "plan"), None
    result = session.layout_source(registration["descriptor"], before, window)
    if not isinstance(result, dict) or result.get("status") != "ok":
        return (
            None,
            refused(result if isinstance(result, dict) else {"status": "unavailable"}, "plan"),
            None,
        )
    try:
        layout.validate_layout_source_result(result, window, registration["descriptor"], before)
    except (ValueError, KeyError, TypeError):
        return None, refused({"status": "conflict", "reason": "layout_owner_plan"}, "plan"), None
    ready = session._ready()
    if ready is not None:
        return None, refused(ready, "bind"), None
    generation = Path(registration["destination"])
    if not result["plans"]:
        terminal = result["export"]
        return (
            None,
            session.store(
                generation,
                "queue_layout_ack",
                dict(
                    job_id=registration["job_id"],
                    intent_id=None,
                    verified_summary=dict(
                        state="snapshot_export_exhausted",
                        cursor_before=before,
                        cursor_after=terminal["cursor_after"],
                        coverage=terminal["coverage"],
                    ),
                ),
            ),
            None,
        )
    bound = session.store(
        generation,
        "queue_layout_bind",
        dict(
            job_id=registration["job_id"],
            plans=result["plans"],
            envelope=state["layout"]["envelope"],
        ),
    )
    if not isinstance(bound, dict) or bound.get("status") != "ok":
        return (
            None,
            refused(bound if isinstance(bound, dict) else {"status": "unavailable"}, "bind"),
            None,
        )
    try:
        if (
            bound.keys() != {"status", "layout", "targets"}
            or bound["layout"] != state["layout"]
            or len(bound["targets"]) != 1
        ):
            raise ValueError("native placement response")
        target = layout.validate_target(bound["targets"][0])
        if target != layout.target(
            state["layout"]["envelope"], target["partition_id"], result["plans"]
        ):
            raise ValueError("native placement target")
    except (ValueError, KeyError, TypeError):
        return None, refused({"status": "conflict", "reason": "layout_owner_target"}, "bind"), None
    return target, None, result if action == "prepare" else None


def _layout_prepared_export(response, context, registration, before, *, prepared):
    """Rebind tick-local source bytes after actual placement, never a cache."""
    from . import lance_layout_codec as layout

    if not prepared:
        return response
    layout.validate_layout_source_result(
        response, _layout_window(context, "prepare", []), registration["descriptor"], before
    )
    result = dict(response)
    del result["preparation_hash"]
    result.update(action="export", cursor_after=response["export"]["cursor_after"])
    return result


def _layout_check_owners(session, generation, pending, exported, version, refused):
    from . import lance_layout_codec as layout

    if not pending["owners"]:
        return [], None
    ready = session._ready()
    if ready is not None:
        return None, refused(ready, "owner")
    observed = session.exact(
        generation, "layout_verify_owners", dict(intent_id=pending["intent_id"], version=version)
    )
    if not isinstance(observed, dict) or observed.get("status") != "ok":
        return None, refused(
            observed if isinstance(observed, dict) else {"status": "unavailable"}, "owner"
        )
    ready = session._ready()
    if ready is not None:
        return None, refused(ready, "owner")
    try:
        return layout.validate_grouped_owner_proof(observed, pending, exported, version), None
    except (ValueError, KeyError, TypeError):
        return None, refused({"status": "conflict", "reason": "layout_owner_contract"}, "owner")


def _layout_fixture_tick(session, registration):
    """Original all-in-one diagnostic; never extends its caller's deadline."""
    return _layout_tick(session, registration, defer_append=False)


def _layout_fixture_step(session, registration):
    """One bounded background step; a fresh append remains unverified pending work."""
    return _layout_tick(session, registration, defer_append=True)


def _layout_tick(session, registration, *, defer_append):
    """Private single-owner layout protocol; authority stays in registered stores."""
    from . import lance_layout_codec as layout

    codec.validate_registration(registration)
    generation = Path(registration["destination"])
    state = session.store(generation, "queue_layout_resume", {"registration": registration})
    if not isinstance(state, dict) or state.get("status") != "ok":
        return state if isinstance(state, dict) else {"status": "unavailable"}
    if state.get("registration") != registration or "layout" not in state:
        return {"status": "unavailable", "reason": "layout_registration", "phase": "open"}
    pending = state["pending"]
    before = state["staging_cursor"]

    def refused(result, phase):
        return dict(
            status=result.get("status", "unavailable"),
            reason=result.get("reason"),
            phase=phase,
            intent_id=pending["intent_id"] if pending else None,
            cursor_before=before,
            cursor_after=before,
            coverage="incomplete",
        )

    try:
        context = _layout_context(state, registration)
        codec.cursor(before, registration)
        if pending is not None:
            codec.validate_pending(pending, registration, state["layout"])
        if pending is not None and pending["cursor_before"] != before:
            raise ValueError("registered pending target")
    except (ValueError, KeyError, TypeError):
        return refused({"status": "conflict", "reason": "layout_pending_contract"}, "open")
    if state["state"] == "snapshot_export_exhausted":
        return state["last_ack"]["result"]
    prepared_source = None
    if pending is None:
        target, failure, prepared_source = _layout_admit_owner(
            session, registration, state, context, before, refused
        )
        if failure is not None:
            return failure
    else:
        target = pending["target"]
    ready = session._ready()
    if ready is not None:
        return refused(ready, "source")
    window = _layout_window(context, "export", target["owner_plan_hashes"])
    response = prepared_source
    if response is None:
        response = session.layout_source(registration["descriptor"], before, window)
    if not isinstance(response, dict) or response.get("status") != "ok":
        return refused(
            response if isinstance(response, dict) else {"status": "unavailable"}, "source"
        )
    try:
        # Tick-local bytes only. Recheck the association after placement,
        # then require the ordinary export contract for the actual target.
        response = _layout_prepared_export(
            response, context, registration, before, prepared=prepared_source is not None
        )
        layout.validate_layout_source_result(response, window, registration["descriptor"], before)
        exported = response["export"]
        regenerated = layout.prepare_layout_interval(registration, exported, target)
        if pending is not None and regenerated != pending:
            raise ValueError("layout regeneration")
    except (ValueError, KeyError, TypeError):
        return refused({"status": "conflict", "reason": "layout_regeneration"}, "source")
    if pending is None:
        ready = session._ready()
        if ready is not None:
            return refused(ready, "prepare")
        prepared = session.store(
            generation,
            "queue_layout_prepare",
            dict(job_id=registration["job_id"], expected_cursor=before, intent=regenerated),
        )
        if prepared != {"status": "ok", "intent_id": regenerated["intent_id"]}:
            return refused(
                prepared
                if isinstance(prepared, dict) and prepared.get("status") != "ok"
                else {"status": "conflict", "reason": "layout_prepare_response"},
                "prepare",
            )
        pending = regenerated
    arguments = dict(
        intent_id=pending["intent_id"],
        batch_id=pending["intent_id"],
        source_binding=registration["source_binding"],
        entries=exported["entries"],
        finalizations=exported["finalizations"],
    )
    batch, failure, phase = _layout_replay(
        session, generation, arguments, pending, defer_append=defer_append
    )
    if failure is not None:
        return refused(failure, phase)
    try:
        # Validate every route before using its version or issuing an owner read.
        for route in batch["routes"]:
            layout.validate_route(route)
        if not batch["routes"]:
            raise ValueError("missing durable batch route")
        version = layout.positive(batch["routes"][-1]["version"])
        # Preflight the complete replay/route contract before another worker.
        # These expected references are not ACK evidence: only the actual
        # grouped observations below supply the final owner_proofs.
        layout.verified_summary(
            pending,
            batch,
            [
                dict(
                    owner=owner["owner"],
                    manifest_hash=owner["manifest_hash"],
                    partition_id=pending["target"]["partition_id"],
                    version=version,
                    live_receipt_ids=owner["live_receipt_ids"],
                )
                for owner in pending["owners"]
            ],
        )
    except (ValueError, KeyError, TypeError, IndexError):
        return refused({"status": "conflict", "reason": "layout_replay_contract"}, "verify_replay")
    proofs, failure = _layout_check_owners(session, generation, pending, exported, version, refused)
    if failure is not None:
        return failure
    try:
        summary = layout.verified_summary(pending, batch, proofs)
    except (ValueError, KeyError, TypeError, IndexError):
        return refused({"status": "conflict", "reason": "layout_replay_contract"}, "verify_replay")
    ready = session._ready()
    if ready is not None:
        return refused(ready, "ack")
    return session.store(
        generation,
        "queue_layout_ack",
        dict(
            job_id=registration["job_id"], intent_id=pending["intent_id"], verified_summary=summary
        ),
    )


def _source(session, registration, cursor, *, native):
    if native:
        return session.native_source(registration["descriptor"], cursor)
    return session.sqlite_source(registration["descriptor"], cursor)


def _publish_v1(session, directory, arguments, expected):
    for phase in ("publish", "verify_replay"):
        receipt = session.exact(directory, "import_batch", arguments)
        if receipt != expected:
            if not isinstance(receipt, dict):
                receipt = dict(status="conflict", reason="migration_batch_response")
            return None, receipt, phase
    return None, None, None


def _publish_persisted(session, directory, arguments, expected):
    import time
    from .persisted_replay import expected_ids, validate_outcome

    identities = expected_ids(arguments["entries"], arguments["finalizations"])
    for phase in ("publish", "verify_replay"):
        result = session.exact(directory, "import_batch_outcome", arguments)
        if not isinstance(result, dict):
            return None, dict(status="conflict", reason="migration_replay_witness"), phase
        if result.get("status") != "ok":
            return None, result, phase
        try:
            version = validate_outcome(
                result,
                expected,
                directory,
                arguments["source_binding"],
                arguments["batch_id"],
                identities,
                until=time.monotonic() + session.remaining,
            )
        except (ValueError, KeyError, TypeError):
            return None, dict(status="conflict", reason="migration_replay_witness"), phase
        if version is not None:
            return version, None, None
    return None, dict(status="conflict", reason="migration_replay_not_persisted"), "verify_replay"


def _owner_version(observed, proof_version):
    version = codec.bulk.integer(observed["version"])
    if version == 0 or (proof_version is not None and version != proof_version):
        raise ValueError("owner proof version")
    return version


def _tick(session, registration, *, native, publish=_publish_v1):
    """Attempt exactly one interval; a refusal preserves the prepared interval.

    V1 always reopens twice. The private publisher may use a worker-established
    complete persisted proof; no remembered proof survives this tick.
    """
    codec.validate_registration(registration)
    generation = Path(registration["destination"])
    job_id = registration["job_id"]
    state = session.store(generation, "queue_migration_open", {"registration": registration})
    if state.get("status") != "ok":
        return {**state, "phase": "open"}
    if state.get("registration") != registration:
        return {"status": "conflict", "reason": "migration_registration", "phase": "open"}
    if state["state"] == "snapshot_export_exhausted":
        return state["last_ack"]["result"]
    pending, before = state["pending"], state["staging_cursor"]

    def refused(result, phase):
        # Recording is another ordinary bounded stage, never a cleanup lease.
        # If budget/preemption is exhausted, the existing pending row is enough
        # to resume; the refusal remains in this compact return value.
        refusal = dict(
            status=result.get("status", "unavailable"),
            reason=result.get("reason"),
            phase=result.get("phase", phase),
            refused_operation=result.get("refused_operation", phase),
            refused_field=result.get("refused_field"),
            orphans=result.get("counts", {}).get("orphans", 0),
        )
        saved = None
        if session._ready() is None:
            saved = session.store(
                generation,
                "queue_migration_refuse",
                dict(job_id=job_id, expected_cursor=before, refusal=refusal),
            )
        return dict(
            status=refusal["status"],
            reason=refusal["reason"],
            phase=phase,
            source_phase=refusal["phase"],
            refused_operation=refusal["refused_operation"],
            refused_field=refusal["refused_field"],
            cursor_before=before,
            cursor_after=before,
            intent_id=pending["intent_id"] if pending else None,
            counts={
                **state["counts"],
                "orphans": max(state["counts"]["orphans"], refusal["orphans"]),
            },
            coverage="incomplete",
            refusal_record=saved,
        )

    source_cursor = pending["cursor_before"] if pending else before
    result = _source(session, registration, source_cursor, native=native)
    try:
        codec.validate_export(result, registration, before)
    except (ValueError, KeyError, TypeError):
        return refused(dict(status="conflict", reason="migration_source_contract"), "source")
    if result["status"] != "ok":
        return refused(result, "source")
    if result["complete"]:
        if pending is not None:
            return refused(
                dict(status="conflict", reason="migration_pending_exhaustion"), "regenerate"
            )
        ready = session._ready()
        if ready is not None:
            return refused(ready, "scan_ack")
        return session.store(
            generation,
            "queue_migration_ack",
            dict(
                job_id=job_id,
                intent_id=None,
                verified_summary=dict(
                    state="snapshot_export_exhausted",
                    cursor_before=before,
                    cursor_after=result["cursor_after"],
                    coverage=result["coverage"],
                ),
            ),
        )
    try:
        regenerated = codec.prepare_interval(registration, result)
    except (ValueError, KeyError, TypeError):
        return refused(dict(status="conflict", reason="migration_interval_contract"), "regenerate")
    if pending is not None and pending != regenerated:
        return refused(
            dict(status="conflict", reason="migration_regeneration_conflict"), "regenerate"
        )
    if pending is None:
        prepared = session.store(
            generation,
            "queue_migration_prepare",
            dict(job_id=job_id, expected_cursor=before, intent=regenerated),
        )
        if prepared != dict(status="ok", intent_id=regenerated["intent_id"]):
            return refused(
                prepared
                if prepared.get("status") != "ok"
                else dict(status="conflict", reason="migration_prepare_response"),
                "prepare",
            )
        pending = regenerated
    arguments = dict(
        batch_id=pending["intent_id"],
        source_binding=registration["source_binding"],
        entries=result["entries"],
        finalizations=result["finalizations"],
    )
    expected = codec.expected_receipt(pending)
    proof_version, failure, phase = publish(session, generation / "exact", arguments, expected)
    if failure is not None:
        return refused(
            failure
            if failure.get("status") != "ok"
            else dict(status="conflict", reason="migration_batch_response"),
            phase,
        )
    versions = []
    for owner in result["finalizations"]:
        owner_arguments = {"owner": owner["owner"]}
        if proof_version is not None:
            owner_arguments["version"] = proof_version
        observed = session.exact(generation / "exact", "bulk_owner", owner_arguments)
        if observed.get("status") != "ok":
            return refused(observed, "owner")
        if observed.get("manifest") != owner:
            return refused(dict(status="conflict", reason="migration_owner_manifest"), "owner")
        try:
            version = _owner_version(observed, proof_version)
        except (ValueError, KeyError, TypeError):
            return refused(dict(status="conflict", reason="migration_owner_version"), "owner")
        versions.append(version)
        live = owner["live"]
        if live is not None:
            # The established receipt API has no pinned-version argument; it
            # independently validates the original namespace/key and records.
            receipt = session.exact(
                generation / "exact",
                "receipt",
                {"namespace": live["namespace"], "key": live["key"]},
            )
            if receipt != dict(status="ok", digest=live["digest"], receipt=live["receipt"]):
                return refused(
                    receipt
                    if receipt.get("status") != "ok"
                    else dict(status="conflict", reason="migration_live_receipt"),
                    "live_receipt",
                )
    summary = codec.verified_summary(pending, versions)
    codec.validate_verified(summary, pending)
    ready = session._ready()
    if ready is not None:
        return refused(ready, "ack")
    return session.store(
        generation,
        "queue_migration_ack",
        dict(job_id=job_id, intent_id=pending["intent_id"], verified_summary=summary),
    )

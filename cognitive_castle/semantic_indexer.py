"""Inactive trusted exact outbox and one-page index driver.

No storage/model runtime runs in this host. All durable operations use fixed
supervised workers under the same admission and absolute session deadline.
"""

from pathlib import Path
import time

from .canonical_records import prepare_remember
from .semantic_intents import prepare_intent, validate_intent, source_exposure_hash, hydrate_group
from .semantic_profile import PROFILE_ID, digest
from .semantic_queue import validate_receipt, validate_references
from .semantic_runtime import SemanticSession
from .semantic_windows import validate_page


def _hydrate(session, directory, group):
    validate_references(group["references"])
    if group["profile"] != PROFILE_ID:
        return {"status": "stale"}
    result = session.exact(
        Path(directory) / "exact",
        "read_sources",
        {"ids": [ref["canonical_id"] for ref in group["references"]]},
    )
    if result.get("status") != "ok":
        return result
    return hydrate_group(group, result["sources"])


def _recover_intent(session, directory, intent):
    validate_intent(intent)
    verifier = getattr(session, "verify_intent", None)
    if verifier is not None:
        proof = verifier(directory, intent)
        if proof.get("status") != "ok":
            return proof
        if (
            set(proof) != {"status", "intent_binding", "digest", "receipt"}
            or proof["intent_binding"] != digest(intent)
            or proof["digest"] != intent["digest"]
            or proof["receipt"] != intent["receipt"]
        ):
            return {"status": "conflict"}
        return session.store(
            directory,
            "queue_intent_complete",
            {"intent_id": intent["intent_id"], "binding": digest(intent)},
        )
    proof = session.exact(
        Path(directory) / "exact",
        "receipt",
        {"namespace": intent["namespace"], "key": intent["key"]},
    )
    if proof.get("status") != "ok":
        return proof
    if proof["digest"] != intent["digest"] or proof["receipt"] != intent["receipt"]:
        return {"status": "conflict"}
    for group in intent["groups"]:
        hydrated = _hydrate(session, directory, group)
        if hydrated["status"] != "ok":
            return hydrated
    return session.store(
        directory,
        "queue_intent_complete",
        {"intent_id": intent["intent_id"], "binding": digest(intent)},
    )


def recover_intents(session, directory):
    """Recover at most two outbox requests; absence never advances a checkpoint."""
    pending = session.store(directory, "queue_intents", {})
    return _recover_pending(session, directory, pending)


def _recover_pending(session, directory, pending):
    if pending.get("status") == "not_initialized":
        return {"status": "ok", "recovered": 0, "pending": []}
    if pending.get("status") != "ok":
        return pending
    try:
        if not isinstance(pending["intents"], list) or len(pending["intents"]) > 2:
            raise ValueError("intent recovery bound")
        for intent in pending["intents"]:
            validate_intent(intent)
    except (ValueError, TypeError, KeyError):
        return {"status": "corrupt", "reason": "invalid_durable_intent"}
    recovered, failures = 0, []
    for intent in pending["intents"]:
        result = _recover_intent(session, directory, intent)
        if result.get("status") == "ok":
            recovered += 1
        else:
            failures.append(
                {
                    "intent_id": intent["intent_id"],
                    "status": result.get("status"),
                    "reason": result.get("reason"),
                }
            )
    return {"status": "ok", "recovered": recovered, "pending": failures}


class ExactPublication(SemanticSession):
    """Internal remember transaction with a fixed three-second total lifetime."""

    def __init__(self, directory):
        super().__init__(Path(__file__), interactive=True)
        self._until = min(self._until, time.monotonic() + 3)
        self.directory = Path(directory).absolute()

    def remember(
        self, request_id, text, provenance=None, *, scopes, namespace="operator", lane="new"
    ):
        prepared = prepare_remember(request_id, text, provenance, namespace=namespace)
        intent = prepare_intent(prepared, scopes, lane)
        registered = self.store(self.directory, "queue_intent_prepare", {"intent": intent})
        if registered.get("status") != "ok":
            return {**registered, "exact_status": "unconfirmed", "semantic_status": "pending"}
        exact = self.exact(
            self.directory / "exact",
            "remember",
            {
                "request_id": request_id,
                "text": text,
                "provenance": provenance,
                "namespace": namespace,
            },
        )
        if exact.get("status") != "ok":
            return {
                **exact,
                "exact_status": "unconfirmed",
                "semantic_status": "pending",
                "intent_id": intent["intent_id"],
            }
        # The append receipt confirms exact success even when later coordination
        # cannot finish. Never collapse an indexing refusal into lost exact data.
        coordinated = _recover_intent(self, self.directory, intent)
        return {
            "status": "ok",
            "exact_status": "committed",
            "receipt": exact,
            "semantic_status": "pending",
            "intent_pending": coordinated.get("status") != "ok",
            "intent_id": intent["intent_id"],
            "reason": coordinated.get(
                "reason", coordinated["status"] if coordinated["status"] != "ok" else None
            ),
        }


def index_one(session, directory):
    """One bounded indexing step; committed pages recover without re-inference."""
    selected = None
    prepare = getattr(session, "prepare_index", None)
    if prepare is None:
        recovery = recover_intents(session, directory)
    else:
        control = prepare(directory)
        if control.get("status") != "ok":
            return {**control, "phase": "intent_recovery", "resources": session.resources}
        malformed = (
            set(control) != {"status", "intents", "selection"}
            or not isinstance(control["intents"], list)
            or len(control["intents"]) > 2
            or control["intents"]
            and control["selection"] is not None
            or not control["intents"]
            and not isinstance(control["selection"], dict)
        )
        if not malformed and not control["intents"] and control["selection"].get("status") == "ok":
            chosen = control["selection"].get("group")
            malformed = (
                not isinstance(chosen, dict)
                or not {
                    "batch_key",
                    "references",
                    "profile",
                    "input_binding",
                    "position",
                    "total_units",
                }
                <= chosen.keys()
            )
        if malformed:
            return dict(
                status="corrupt",
                reason="invalid_index_preparation",
                phase="intent_recovery",
                resources=session.resources,
            )
        recovery = _recover_pending(
            session, directory, dict(status="ok", intents=control["intents"])
        )
        selected = control["selection"]
    if recovery.get("status") != "ok":
        return {**recovery, "phase": "intent_recovery", "resources": session.resources}
    if selected is None:
        selected = session.store(directory, "queue_select", {})
    if selected.get("status") != "ok":
        return {
            **selected,
            "phase": "select",
            "intent_recovery": recovery,
            "resources": session.resources,
        }
    group = selected["group"]
    base = {
        "batch_key": group["batch_key"],
        "position": group["position"],
        "intent_recovery": recovery,
    }

    def failed(result, phase):
        return {**base, **result, "phase": phase, "resources": session.resources}

    prepare = getattr(session, "prepare_group", None)
    if prepare is None:
        hydrated = _hydrate(session, directory, group)
        if hydrated.get("status") != "ok":
            return failed(hydrated, "hydrate")
        receipt = session.store(
            directory,
            "vector_receipt",
            {"batch_key": group["batch_key"], "start_window": group["position"]},
        )
    else:
        validate_references(group["references"])
        if group["profile"] != PROFILE_ID:
            return failed(dict(status="stale"), "hydrate")
        prepared = prepare(directory, group)
        if prepared.get("status") != "ok":
            return failed(prepared, "prepare")
        if set(prepared) != {"status", "sources", "receipt"} or not isinstance(
            prepared["receipt"], dict
        ):
            return failed(dict(status="corrupt", reason="invalid_index_preparation"), "prepare")
        hydrated = hydrate_group(group, prepared["sources"])
        if hydrated.get("status") != "ok":
            return failed(hydrated, "hydrate")
        receipt = prepared["receipt"]
    recovered = receipt.get("status") == "ok"
    if not recovered:
        if receipt.get("status") not in {"not_found", "not_initialized"}:
            return failed(receipt, "receipt_recovery")
        continuation = (
            {"binding": group["input_binding"], "next_window": group["position"]}
            if group["position"]
            else None
        )
        page = session.tokenize(hydrated["sources"], continuation)
        if page.get("status") != "ok":
            return failed(page, "tokenize")
        validate_page(page)
        if (
            page["input_binding"] != group["input_binding"]
            or page["start_window"] != group["position"]
            or group["total_units"] not in (None, page["total_units"])
        ):
            return failed({"status": "stale"}, "tokenize")
        inferred = session.infer(page)
        if inferred.get("status") != "ok":
            return failed(inferred, "infer")
        published = session.store(
            directory,
            "vector_publish",
            {
                "batch_key": group["batch_key"],
                "references": group["references"],
                "page": page,
                "inference": inferred,
            },
        )
        # Even a lost response is recovered only with this session's remaining
        # time. Once spent, the next tick handles it; there is no cleanup budget.
        if published.get("status") not in {"ok", "unavailable"}:
            return failed(published, "publish")
        if getattr(session, "_defer_vector_verification", False) is True:
            if published.get("status") != "ok":
                return failed(published, "publish")
            # Background publication is resumable across separate admitted
            # ticks. A fresh append never substitutes for the next tick's cold
            # proof, and no semantic offset is acknowledged here.
            return {
                **base,
                "status": "in_progress",
                "state": "verification_pending",
                "phase": "receipt_verification",
                "recovered_vector": False,
                "resources": session.resources,
            }
        receipt = session.store(
            directory,
            "vector_receipt",
            {"batch_key": group["batch_key"], "start_window": group["position"]},
        )
    if receipt.get("status") != "ok":
        return failed(receipt, "receipt_verification")
    validate_receipt(receipt)
    if (
        receipt["batch_key"] != group["batch_key"]
        or receipt["profile"] != group["profile"]
        or receipt["input_binding"] != group["input_binding"]
        or receipt["reference_binding"] != digest(group["references"])
        or receipt["start_window"] != group["position"]
        or group["total_units"] not in (None, receipt["total_units"])
    ):
        return failed({"status": "stale"}, "receipt_verification")
    ack = session.store(
        directory, "queue_acknowledge", {"batch_key": group["batch_key"], "receipt": receipt}
    )
    return {
        **base,
        **ack,
        "phase": "acknowledge",
        "recovered_vector": recovered,
        "resources": session.resources,
    }


def query_exact(session, directory, text, scope, *, allowed_source_hashes=None):
    """Internal fixture/helper: embedding, Lance nearest and exact hydration.

    Uses the caller's existing five-second lease, returns at most eight sources,
    and never treats an unavailable semantic stage as successful lexical recall.
    ``allowed_source_hashes`` contains full-source exposure hashes, not text hashes.
    """
    from .semantic_queue import validate_scope

    validate_scope(scope)
    prepared = prepare_remember("semantic-query", text, namespace="internal-query")
    if len(prepared.records) != 1:
        raise ValueError("query must fit one exact chunk")
    source = prepared.records[0].source()
    page = session.tokenize(
        [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    )
    if page.get("status") != "ok":
        return page
    if page["total_units"] != 1 or not page["complete"]:
        return {"status": "unavailable", "reason": "semantic_query_window_limit"}
    inference = session.infer(page)
    if inference.get("status") != "ok":
        return inference
    nearest = session.store(
        directory,
        "vector_nearest",
        {
            "vector": inference["vectors"][0]["vector"],
            "profile": PROFILE_ID,
            "generation": None,
            "scope": scope,
            # Public grants identify the complete original source snapshot,
            # including provenance; equal text alone never transfers a grant.
            "allowed_source_hashes": None,
            "allowed_exposure_hashes": allowed_source_hashes,
            "limit": 8,
        },
    )
    if nearest.get("status") != "ok":
        return nearest
    candidates = nearest["candidates"]
    ids = list(dict.fromkeys(row["canonical_id"] for row in candidates))
    if not ids:
        return {**nearest, "sources": []}
    hydrated = session.exact(Path(directory) / "exact", "read_sources", {"ids": ids})
    if hydrated.get("status") != "ok":
        return hydrated
    sources = hydrated["sources"]
    if [source["id"] for source in sources] != ids:
        return {"status": "stale"}
    by_id = {source["id"]: source for source in sources}
    if any(
        row["source_hash"] != by_id[row["canonical_id"]]["provenance"]["text_hash"]
        or row["exposure_hash"] != source_exposure_hash(by_id[row["canonical_id"]])
        or (
            allowed_source_hashes is not None
            and source_exposure_hash(by_id[row["canonical_id"]]) not in allowed_source_hashes
        )
        for row in candidates
    ):
        return {"status": "stale"}
    if session.remaining <= 0:
        return {"status": "unavailable", "reason": "semantic_deadline"}
    return {**nearest, "sources": sources}

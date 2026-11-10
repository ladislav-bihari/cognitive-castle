"""Pure bounded outbox metadata. Exact bodies remain solely in canonical Lance."""

from .canonical_records import _string
from .semantic_profile import PROFILE_ID, digest
from .semantic_queue import validate_hash, validate_references
from .semantic_windows import input_binding


def source_exposure_hash(source):
    """The established full-source exposure encoding, without storage imports."""
    return digest({key: source[key] for key in ("id", "text", "provenance")})


def hydrate_group(group, sources):
    """Check actual originals against every full-source reference and binding."""
    validate_references(group["references"])
    if group["profile"] != PROFILE_ID:
        return {"status": "stale"}
    if not isinstance(sources, list) or len(sources) != len(group["references"]):
        return {"status": "stale"}
    envelopes = []
    for source, ref in zip(sources, group["references"], strict=True):
        if (
            source["id"] != ref["canonical_id"]
            or source["provenance"]["text_hash"] != ref["source_hash"]
            or source_exposure_hash(source) != ref["exposure_hash"]
        ):
            return {"status": "stale"}
        envelopes.append({"source": source, "expected_hash": ref["source_hash"]})
    if input_binding(envelopes) != group["input_binding"]:
        return {"status": "stale"}
    return {"status": "ok", "sources": envelopes}


def prepare_intent(prepared, scopes, lane):
    if not isinstance(scopes, list) or len(scopes) != len(prepared.records):
        raise ValueError("one trusted scope per canonical record")
    groups = []
    for start in range(0, len(prepared.records), 8):
        records = prepared.records[start : start + 8]
        sources = [record.source() for record in records]
        references = [
            {
                "canonical_id": source["id"],
                "source_hash": source["provenance"]["text_hash"],
                "exposure_hash": source_exposure_hash(source),
                "scope": scope,
            }
            for source, scope in zip(sources, scopes[start : start + 8])
        ]
        groups.append(
            {
                "batch_key": "index:"
                + digest(
                    [
                        prepared.namespace,
                        prepared.request_id,
                        prepared.digest,
                        start,
                        references,
                        PROFILE_ID,
                    ]
                ),
                "references": references,
                "profile": PROFILE_ID,
                "input_binding": input_binding(
                    [
                        {"source": source, "expected_hash": source["provenance"]["text_hash"]}
                        for source in sources
                    ]
                ),
                "lane": lane,
            }
        )
    result = {
        "intent_id": digest([prepared.namespace, prepared.request_id]),
        "namespace": prepared.namespace,
        "key": prepared.request_id,
        "digest": prepared.digest,
        "receipt": prepared.receipt,
        "groups": groups,
    }
    validate_intent(result)
    return result


def validate_intent(intent):
    if not isinstance(intent, dict) or set(intent) != {
        "intent_id",
        "namespace",
        "key",
        "digest",
        "receipt",
        "groups",
    }:
        raise ValueError("intent signature")
    for name in ("namespace", "key"):
        _string(intent[name], 128, name)
    validate_hash(intent["digest"])
    if intent["intent_id"] != digest([intent["namespace"], intent["key"]]):
        raise ValueError("intent identity")
    groups = intent["groups"]
    if not isinstance(groups, list) or not 1 <= len(groups) <= 2:
        raise ValueError("intent groups")
    ids = []
    for group in groups:
        if not isinstance(group, dict) or set(group) != {
            "batch_key",
            "references",
            "profile",
            "input_binding",
            "lane",
        }:
            raise ValueError("intent group")
        validate_references(group["references"])
        validate_hash(group["input_binding"])
        if group["profile"] != PROFILE_ID or group["lane"] not in {"new", "history"}:
            raise ValueError("intent profile or lane")
        expected = "index:" + digest(
            [
                intent["namespace"],
                intent["key"],
                intent["digest"],
                len(ids),
                group["references"],
                PROFILE_ID,
            ]
        )
        if group["batch_key"] != expected:
            raise ValueError("intent group identity")
        ids.extend(ref["canonical_id"] for ref in group["references"])
    receipt = intent["receipt"]
    if (
        len(ids) > 9
        or len(set(ids)) != len(ids)
        or not isinstance(receipt, dict)
        or set(receipt) != {"status", "ids", "stored_text_bytes", "text_sha256"}
        or receipt["status"] != "ok"
        or receipt["ids"] != ids
        or type(receipt["stored_text_bytes"]) is not int
        or not 1 <= receipt["stored_text_bytes"] <= 65536
    ):
        raise ValueError("intent receipt")
    validate_hash(receipt["text_sha256"])
    return intent

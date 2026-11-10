"""Pure body-free recovery references; page hashes are commitments, not grants."""

import re
import json
from .canonical_records import _hash, _json, _string

FORMAT = "capture-recovery-page-v1"


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("recovery hash")
    return value


def prepare_page(source_key, after, references, *, revision=0):
    _sha(source_key)
    if type(revision) is not int or not 0 <= revision < 2**63 - 1:
        raise ValueError("recovery revision")
    if after != "":
        _sha(after)
    if not isinstance(after, str) or not isinstance(references, list) or len(references) > 2:
        raise ValueError("recovery window")
    copied = []
    for reference in references:
        if not isinstance(reference, dict) or reference.keys() != {
            "id",
            "binding",
            "namespace",
            "key",
        }:
            raise ValueError("recovery reference")
        _sha(reference["id"])
        _sha(reference["binding"])
        if reference["namespace"] != "transcript":
            raise ValueError("recovery namespace")
        _string(reference["key"], 128, "recovery request")
        if copied and copied[-1]["id"] >= reference["id"]:
            raise ValueError("recovery order")
        copied.append(dict(reference))
    core = dict(
        format=FORMAT, source_key=source_key, after=after, revision=revision, references=copied
    )
    return {**core, "binding": _hash(_json(core))}


def validate_page(value):
    if not isinstance(value, dict) or value.keys() != {
        "format",
        "source_key",
        "after",
        "references",
        "revision",
        "binding",
    }:
        raise ValueError("recovery page")
    expected = prepare_page(
        value["source_key"], value["after"], value["references"], revision=value["revision"]
    )
    if value != expected:
        raise ValueError("recovery page binding")
    return expected


def validate_proofs(page, proofs):
    """Bound transport fields before hashing; canonical truth is checked in control."""
    page = validate_page(page)
    if not isinstance(proofs, list) or len(proofs) != len(page["references"]):
        raise ValueError("recovery proof count")
    for proof in proofs:
        if not isinstance(proof, dict):
            raise ValueError("recovery proof")
        if proof == {"status": "not_found"}:
            continue
        if proof.keys() != {"status", "digest", "receipt"} or proof["status"] != "ok":
            raise ValueError("recovery proof fields")
        _sha(proof["digest"])
        receipt = proof["receipt"]
        if not isinstance(receipt, dict) or receipt.keys() != {
            "status",
            "ids",
            "stored_text_bytes",
            "text_sha256",
        }:
            raise ValueError("recovery receipt fields")
        if (
            receipt["status"] != "ok"
            or type(receipt["stored_text_bytes"]) is not int
            or not 1 <= receipt["stored_text_bytes"] <= 65536
        ):
            raise ValueError("recovery receipt size")
        _sha(receipt["text_sha256"])
        identities = receipt["ids"]
        if not isinstance(identities, list) or not 1 <= len(identities) <= 9:
            raise ValueError("recovery receipt ids")
        for identity in identities:
            if not isinstance(identity, str) or not re.fullmatch(r"memory:[a-f0-9]{64}", identity):
                raise ValueError("recovery receipt id")
        if len(set(identities)) != len(identities):
            raise ValueError("recovery duplicate ids")
    return json.loads(_json(proofs))

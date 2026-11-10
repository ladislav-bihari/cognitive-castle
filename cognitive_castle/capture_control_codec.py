"""Pure closed capture-worker requests. Authority labels are never exposure grants."""

import json
import re
from .canonical_records import _json
from .capture_recovery_codec import _sha, validate_page, validate_proofs
from .capture_state_codec import _path, validate_authority

FIELDS = {
    "source_queue": ({"path", "allowed_root", "format"}, {"workspace"}),
    "source_page": (set(), {"after", "limit", "pending_only"}),
    "coverage": (set(), set()),
    "source_prepare": ({"source_key"}, {"publication"}),
    "recovery_select": ({"source_key"}, set()),
    "recovery_apply": ({"page", "proofs"}, set()),
}
READS = {"source_page", "coverage", "recovery_select"}


def validate_publication(value):
    if value is None:
        return None
    if not isinstance(value, dict) or value.keys() != {"request_id", "digest", "receipt"}:
        raise ValueError("capture publication fields")
    _sha(value["request_id"])
    _sha(value["digest"])
    receipt = value["receipt"]
    if (
        not isinstance(receipt, dict)
        or receipt.keys() != {"status", "ids", "stored_text_bytes", "text_sha256"}
        or receipt["status"] != "ok"
    ):
        raise ValueError("capture publication receipt")
    if (
        type(receipt["stored_text_bytes"]) is not int
        or not 1 <= receipt["stored_text_bytes"] <= 65536
    ):
        raise ValueError("capture publication bytes")
    _sha(receipt["text_sha256"])
    identities = receipt["ids"]
    if not isinstance(identities, list) or not 1 <= len(identities) <= 9:
        raise ValueError("capture publication ids")
    for identity in identities:
        if not isinstance(identity, str) or not re.fullmatch(r"memory:[a-f0-9]{64}", identity):
            raise ValueError("capture publication id")
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate capture publication ids")
    return json.loads(_json(value))


def validate_operation(operation, args):
    if not isinstance(operation, str) or operation not in FIELDS or not isinstance(args, dict):
        raise ValueError("capture operation")
    required, optional = FIELDS[operation]
    if not required <= args.keys() or args.keys() - required - optional:
        raise ValueError("capture operation arguments")
    if operation == "source_queue":
        _path(args["path"])
        _path(args["allowed_root"])
        if args["format"] not in ("codex", "claude"):
            raise ValueError("capture format")
        if args.get("workspace") is not None:
            _path(args["workspace"])
    elif operation == "source_page":
        if args.get("after", "") != "":
            _sha(args["after"])
        if (
            type(args.get("limit", 8)) is not int
            or not 1 <= args.get("limit", 8) <= 16
            or type(args.get("pending_only", False)) is not bool
        ):
            raise ValueError("capture page bounds")
    elif operation in {"source_prepare", "recovery_select"}:
        _sha(args["source_key"])
        if operation == "source_prepare":
            validate_publication(args.get("publication"))
    elif operation == "recovery_apply":
        validate_page(args["page"])
        validate_proofs(args["page"], args["proofs"])
    return json.loads(_json(args))


def validate_request(request):
    if (
        not isinstance(request, dict)
        or request.keys() != {"authority", "authority_hash", "operation", "arguments", "_host_pid"}
        or type(request["_host_pid"]) is not int
        or request["_host_pid"] <= 0
    ):
        raise ValueError("capture request fields")
    authority = validate_authority(request["authority"], expected_hash=request["authority_hash"])
    args = validate_operation(request["operation"], request["arguments"])
    return {**request, "authority": authority, "arguments": args}

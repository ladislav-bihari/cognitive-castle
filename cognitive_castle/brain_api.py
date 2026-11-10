"""Strict host-assisted brain boundary. No implicit model, provider, or archive access."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat

from .bounded_json import MAX_FRAME_BYTES, strict_loads


def obj(properties, required=()):
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


TEXT = {"type": "string"}
ID = {"type": "string", "maxLength": 128}
VERSION = {"type": "integer", "enum": [1]}
REVISION = {"type": "integer", "minimum": 1}
JSON_OBJECT = {"type": "object"}
SOURCE = obj({"id": ID, "text": TEXT, "provenance": JSON_OBJECT}, ("id", "text", "provenance"))
OBLIGATION = obj(
    {
        "id": ID,
        "text": TEXT,
        "source_id": ID,
        "start": {"type": "integer", "minimum": 0},
        "end": {"type": "integer", "minimum": 1},
    },
    ("id", "text", "source_id", "start", "end"),
)
COMMON = {"protocol_version": VERSION, "session_id": ID}
MUTATION = COMMON | {"request_id": ID, "expected_revision": REVISION}
SPECS = {
    "castle_brain_capabilities": (
        "Inspect model-free capabilities without opening the palace.",
        obj({}),
    ),
    "castle_article_assess": (
        "Bounded article observations, not a truth or aggregate quality score.",
        obj({"text": TEXT, "language": TEXT, "scope": TEXT, "provenance": JSON_OBJECT}, ("text",)),
    ),
    "castle_brain_recall": (
        "Model-free bounded recall across live and registered historical SQLite memory under trusted host policy. match=all (default) requires every literal term; match=any broadens candidates. Examine coverage and previews before exact paging. Platform limits apply.",
        obj(
            {
                "query": TEXT,
                "limit": {"type": "integer", "minimum": 1, "maximum": 8},
                "match": {"type": "string", "enum": ["all", "any"]},
                "workspace": {"type": "string", "maxLength": 2048},
                "wing": {"type": "string", "maxLength": 512},
                "room": {"type": "string", "maxLength": 512},
                "cursor": {"type": "string", "maxLength": 4096},
                "session_id": ID,
                "protocol_version": VERSION,
            },
            ("query",),
        ),
    ),
    "castle_problem_begin": (
        "Start or supersede a bounded SUE examination of your interpretation. Supply original obligations as exact spans; no model is called.",
        obj(
            {
                "protocol_version": VERSION,
                "request_id": ID,
                "problem": TEXT,
                "interpretation": TEXT,
                "sources": {"type": "array", "items": SOURCE, "maxItems": 8},
                "archive_ids": {"type": "array", "items": ID, "maxItems": 8},
                "obligations": {"type": "array", "items": OBLIGATION, "maxItems": 64},
                "parent_id": ID,
                "expected_revision": REVISION,
                "revision_reason": TEXT,
            },
            (
                "protocol_version",
                "request_id",
                "problem",
                "interpretation",
                "sources",
                "obligations",
            ),
        ),
    ),
    "castle_problem_respond": (
        "Submit the active assistant response against the paged SUE schema. Issue dispositions and commitments remain attributed judgments.",
        obj(
            MUTATION
            | {
                "response": JSON_OBJECT,
                "dispositions": {"type": "array", "items": JSON_OBJECT, "maxItems": 128},
                "commitment_map": {"type": "array", "items": JSON_OBJECT, "maxItems": 64},
            },
            (*MUTATION, "response"),
        ),
    ),
    "castle_problem_status": (
        "Resume from current bounded state; consumes the root request budget.",
        obj(COMMON, COMMON),
    ),
    "castle_problem_page": (
        "Read one revision/policy-bound page. Nonzero offset requires the previous cursor.",
        obj(
            COMMON
            | {
                "kind": {
                    "type": "string",
                    "enum": [
                        "sources",
                        "problem",
                        "interpretation",
                        "obligations",
                        "issues",
                        "events",
                        "question",
                        "schema",
                        "source_map",
                        "corrections",
                        "dispositions",
                        "commitments",
                    ],
                },
                "offset": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": 2048},
                "cursor": JSON_OBJECT,
            },
            (*COMMON, "kind"),
        ),
    ),
    "castle_problem_cancel": (
        "Cancel a session; cancellation remains available after the request budget.",
        obj(MUTATION | {"reason": TEXT}, MUTATION),
    ),
    "castle_problem_correct": (
        "Record a correction and invalidate current advice; begin a successor if a slot remains.",
        obj(MUTATION | {"correction": TEXT}, (*MUTATION, "correction")),
    ),
}


SPECS.update(
    {
        "castle_memory_context": (
            "Restore bounded exact workspace evidence and attributed work state.",
            obj(
                {
                    "protocol_version": VERSION,
                    "workspace": TEXT,
                    "limit": {"type": "integer", "minimum": 1, "maximum": 8},
                },
                ("protocol_version", "workspace"),
            ),
        ),
        "castle_memory_context_update": (
            "Record active, completed or cancelled work against exact workspace source references; host-attributed, not independent verification.",
            obj(
                {
                    "protocol_version": VERSION,
                    "workspace": TEXT,
                    "request_id": ID,
                    "expected_revision": {"type": "integer", "minimum": 0},
                    "status": {"type": "string", "enum": ["active", "completed", "cancelled"]},
                    "source_ids": {"type": "array", "items": ID, "maxItems": 8},
                },
                (
                    "protocol_version",
                    "workspace",
                    "request_id",
                    "expected_revision",
                    "status",
                    "source_ids",
                ),
            ),
        ),
        "castle_memory_remember": (
            "Store explicitly requested exact text locally.",
            obj(
                {
                    "protocol_version": VERSION,
                    "request_id": ID,
                    "text": TEXT,
                    "provenance": JSON_OBJECT,
                },
                ("protocol_version", "request_id", "text"),
            ),
        ),
        "castle_memory_read": (
            "Read an exact bounded immutable memory page.",
            obj(
                {
                    "protocol_version": VERSION,
                    "source_id": ID,
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 2048},
                    "expected_hash": ID,
                    "field": {"type": "string", "enum": ["text", "metadata", "columns"]},
                },
                ("protocol_version", "source_id", "expected_hash"),
            ),
        ),
        "castle_memory_status": (
            "Inspect indexed-only memory and paged recorded coverage observations.",
            obj(
                {
                    "protocol_version": VERSION,
                    "after": {"type": "integer", "minimum": 0, "maximum": 2**63 - 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 16},
                },
                ("protocol_version",),
            ),
        ),
    }
)


def validate(value, schema):
    kind = schema.get("type")
    expected = {"object": dict, "array": list, "string": str, "integer": int}
    if kind in expected and type(value) is not expected[kind]:
        raise ValueError("Invalid parameter type")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError("Unsupported parameter value")
    if kind == "object":
        if any(key not in value for key in schema.get("required", [])):
            raise ValueError("Missing required parameter")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            raise ValueError("Unknown parameter")
        for key, item in value.items():
            if key in properties:
                validate(item, properties[key])
    elif kind == "array":
        if len(value) > schema.get("maxItems", 2048):
            raise ValueError("Collection limit exceeded")
        for item in value:
            validate(item, schema.get("items", {}))
    elif kind == "string" and len(value) > schema.get("maxLength", MAX_FRAME_BYTES):
        raise ValueError("Text limit exceeded")
    elif kind == "integer" and not schema.get("minimum", -(2**63)) <= value <= schema.get(
        "maximum", 2**63 - 1
    ):
        raise ValueError("Integer outside range")


def bound_context_response(result, *, budget=8192):
    """Bound the delivered packet, including supervision and caller metadata."""
    while len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > budget:
        sources = result.get("sources")
        if not sources:
            return {"status": "unavailable", "reason": "context_output_limit"}
        item = sources[-1]
        if item["text"]:
            item["text"] = item["text"][: len(item["text"]) // 2]
            item["next_offset"] = item["offset"] + len(item["text"])
            item["truncated"] = True
        else:
            sources.pop()
    return result


def memory_capabilities():
    """Describe the configured reader without opening storage or starting workers."""
    import importlib.util
    import sys
    from .brain_loop import controller_capabilities
    from .memory_runtime import DEADLINES, TOTAL_RSS, WORKER_RSS

    supported = (
        sys.platform == "darwin"
        and importlib.util.find_spec("psutil") is not None
        and controller_capabilities()["status"] == "available"
    )
    return {
        "status": "available" if supported else "unavailable",
        "reason": None if supported else "platform_supervision_unavailable",
        "platform": "darwin",
        "model_required": False,
        "model_free": True,
        "storage_format": "sqlite-fts5-utf8-v1",
        "coverage": "indexed_only",
        "archive_fallback": False,
        "existing_sidecar_required": True,
        "limits": {
            "sources": 8,
            "query_bytes": 1024,
            "query_terms": 16,
            "recall_wire_bytes": 8192,
            "page_codepoints": 2048,
            "recall_deadline_seconds": DEADLINES["recall"],
            "write_deadline_seconds": DEADLINES["remember"],
            "worker_rss_tripwire_bytes": WORKER_RSS,
            "total_rss_tripwire_bytes": TOTAL_RSS,
        },
        "memory_guarantee": "sampled RSS tripwires; not a hard kernel ceiling",
    }


class BrainAPI:
    def __init__(self, palace):
        self.palace = Path(palace).expanduser()
        self.host = os.environ.get("CASTLE_BRAIN_HOST")
        if self.host is not None and not re.fullmatch(r"[a-z0-9_-]{1,32}", self.host):
            raise ValueError("Invalid trusted launch host")
        self.controller_path = self.palace / ".brain"
        if self.host is not None:
            self.controller_path = self.controller_path / "hosts" / self.host
        self.policy_path = self.controller_path / "policy.json"

    def policy(self):
        """Operator-owned, private, bounded file; fail closed and re-read each call."""
        from .brain_loop import controller_capabilities

        denied = {"allow_local_sources": False, "policy_epoch": "missing-or-invalid-v1"}
        if controller_capabilities()["status"] != "available":
            return denied
        try:
            # Reject symlink components, not just the final file.
            if any(p.is_symlink() for p in (self.policy_path, *self.policy_path.parents)):
                return denied
            fd = os.open(self.policy_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid()
                    or info.st_mode & 0o077
                    or info.st_size > 8192
                ):
                    return denied
                raw = stream.read(8193)
            policy = strict_loads(raw, max_bytes=8192)
            if not isinstance(policy, dict) or set(policy) - {
                "allow_local_sources",
                "allowed_source_hashes",
            }:
                return denied
            allow = policy.get("allow_local_sources", False)
            hashes = policy.get("allowed_source_hashes", [])
            if (
                type(allow) is not bool
                or not isinstance(hashes, list)
                or len(hashes) > 16
                or any(
                    not isinstance(h, str) or not re.fullmatch("[a-f0-9]{64}", h) for h in hashes
                )
            ):
                return denied
            return {
                "allow_local_sources": allow,
                "allowed_source_hashes": hashes,
                "policy_epoch": hashlib.sha256(raw).hexdigest(),
            }
        except (OSError, ValueError):
            return denied

    @staticmethod
    def _allowed(source, policy):
        from .brain_loop import source_exposure_hash

        return policy["allow_local_sources"] or source_exposure_hash(source) in policy.get(
            "allowed_source_hashes", []
        )

    def _sources(self, operation, *args, **kwargs):
        from .brain_loop import controller_capabilities

        support = controller_capabilities()
        if support["status"] != "available":
            return support
        if operation == "recall" and self.host is None:
            return {"status": "exposure_blocked", "reason": "Trusted launch host required."}
        policy = self.policy()
        if not policy["allow_local_sources"] and not policy.get("allowed_source_hashes"):
            return {
                "status": "exposure_blocked",
                "reason": "Trusted local source exposure policy is required.",
            }
        from . import brain_sources

        if self.host is None and "match" in kwargs:
            return {"status": "unavailable", "reason": "matching_requires_trusted_memory_host"}

        if self.host is not None or (
            operation == "read_sources"
            and args
            and any(str(i).startswith("memory:") for i in args[0])
        ):
            if self.host is None:
                return {"status": "exposure_blocked", "reason": "Trusted launch host required."}
            if operation == "read_sources":
                if any(not re.fullmatch(r"memory:[a-f0-9]{64}", i) for i in args[0]):
                    raise ValueError("Mixed or invalid memory source IDs")
                parameters = {"ids": args[0]}
            else:
                parameters = dict(kwargs)
                if operation == "recall":
                    parameters["allowed_source_hashes"] = (
                        None
                        if policy["allow_local_sources"]
                        else policy.get("allowed_source_hashes", [])
                    )
                    parameters["policy_epoch"] = policy["policy_epoch"]
            from .memory_runtime import run_memory

            result = run_memory(self.palace, operation, parameters)
        else:
            result = getattr(brain_sources, operation)(str(self.palace), *args, **kwargs)
        if result.get("status") != "ok":
            return result
        # Re-evaluate after potentially slow native work. No stale authorization.
        current = self.policy()
        if current["policy_epoch"] != policy["policy_epoch"]:
            return {"status": "exposure_blocked", "reason": "Exposure policy changed during read."}
        authorized = [
            s
            for s in result["sources"]
            if current["allow_local_sources"]
            or (
                self.host is not None
                and operation == "recall"
                and s.get("exposure_hash") in current.get("allowed_source_hashes", [])
            )
            or self._allowed(s, current)
        ]
        if operation == "read_sources" and len(authorized) != len(result["sources"]):
            return {"status": "exposure_blocked", "reason": "Source snapshot is not authorized."}
        if not current["allow_local_sources"]:
            # Selected snapshot hashes cover ID, exact text and provenance.
            # Registry descriptors are outside that grant and can describe
            # metadata which requires the broader local-source policy.
            authorized = [
                {key: value for key, value in source.items() if key != "metadata"}
                for source in authorized
            ]
        return dict(result, sources=authorized)

    def _memory(self, operation, args):
        from .memory_runtime import run_memory

        policy = self.policy()
        if self.host is None or (
            not policy["allow_local_sources"] and not policy.get("allowed_source_hashes")
        ):
            return {"status": "exposure_blocked", "reason": "Trusted host policy required."}
        args.pop("protocol_version")
        if operation in ("context", "context_update"):
            # A hash-only bundle grant must not disclose other workspace history/state.
            if not policy["allow_local_sources"]:
                return {"status": "exposure_blocked"}
            args["namespace"] = self.host
            operation = "restore_context" if operation == "context" else "update_context"
        if operation == "remember":
            if not policy["allow_local_sources"]:
                return {"status": "exposure_blocked"}
            args["namespace"] = self.host
        if operation == "read":
            if args.get("field", "text") != "text" and not policy["allow_local_sources"]:
                return {
                    "status": "exposure_blocked",
                    "reason": "Metadata requires local source exposure policy.",
                }
            source_id = args["source_id"]
            if not re.fullmatch(r"memory:[a-f0-9]{64}", source_id):
                raise ValueError("Invalid memory source ID")
            loaded = self._sources("read_sources", [source_id])
            if loaded["status"] != "ok":
                return loaded
            operation = "page"
        result = run_memory(self.palace, operation, args)
        if self.policy()["policy_epoch"] != policy["policy_epoch"]:
            return {
                "status": "exposure_blocked",
                "reason": "Exposure policy changed during operation.",
            }
        if operation == "restore_context":
            result = bound_context_response(result)
        if not policy["allow_local_sources"]:
            result.pop("metadata", None)
        if len(json.dumps(result, ensure_ascii=False).encode("utf-8")) > 32768:
            return {"status": "unavailable", "reason": "Response budget exceeded."}
        return result

    @staticmethod
    def _failed_read(loop, parent_id, result):
        # An admitted read that never reaches begin still consumed an automatic
        # attempt. Use the controller's durable counter, without advancing SUE.
        if parent_id is not None:
            current = loop.status(parent_id)
            if current.get("status") != "ok":
                return current
        return result

    def _bind_archive(self, loop, args, arguments, ids):
        fingerprint = hashlib.sha256(
            json.dumps(
                {"operation": "archive_begin", "arguments": arguments},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        replay = loop.replay(args["request_id"], fingerprint)
        if replay is not None:
            return replay
        parent_id = args.get("parent_id")
        preflight = loop.preflight_begin(
            args["problem"], parent_id=parent_id, expected_revision=args.get("expected_revision")
        )
        if preflight is not None:
            return preflight
        try:
            loaded = self._sources("read_sources", ids)
        except (ValueError, OSError):
            self._failed_read(loop, parent_id, {"status": "invalid_request"})
            raise
        if loaded["status"] != "ok":
            return self._failed_read(loop, parent_id, loaded)
        if {s["id"] for s in loaded["sources"]} != set(ids):
            return self._failed_read(
                loop,
                parent_id,
                {"status": "unavailable", "reason": "Not all requested source snapshots exist."},
            )
        args["sources"] = args["sources"] + loaded["sources"]
        args["request_fingerprint"] = fingerprint
        return None

    def call(self, name, arguments):
        if name not in SPECS:
            raise ValueError("Unknown brain operation")
        # Also bound direct Python calls before dispatch, independent of wire framing.
        from .brain_loop import _bounded

        _bounded(arguments, MAX_FRAME_BYTES)
        validate(arguments, SPECS[name][1])
        args = dict(arguments)
        if name == "castle_brain_capabilities":
            from .brain_sources import capabilities
            from .brain_loop import controller_capabilities

            return {
                "status": "ok",
                "protocol_version": 1,
                "model_required": False,
                "article_profile": "article-v1",
                "interpretation_mode": "host_assisted",
                "problem_controller": controller_capabilities(),
                "source_reader": memory_capabilities() if self.host is not None else capabilities(),
                "exposure_policy": "trusted_local_file",
                "limits": {
                    "examinations": 2,
                    "turns_per_examination": 14,
                    "requests_per_root": 64,
                    "wire_input_bytes": MAX_FRAME_BYTES,
                    "wire_output_bytes": 32768,
                },
                "trusted_operator_controls": ["inspect", "resume", "purge_source"],
                "independent_checks": "not_performed",
            }
        if name == "castle_article_assess":
            from .understanding.article_metrics import analyze_article_quality

            return analyze_article_quality(**args)
        if name.startswith("castle_memory_"):
            return self._memory(name.removeprefix("castle_memory_"), args)
        if name == "castle_brain_recall":
            from .brain_loop import source_exposure_hash

            if ("session_id" in args) != ("protocol_version" in args):
                raise ValueError("Scoped recall requires session_id and protocol_version")
            session_id = args.pop("session_id", None)
            args.pop("protocol_version", None)
            if session_id is not None:
                from .brain_loop import BrainLoop

                state = BrainLoop(self.controller_path, exposure_policy=self.policy).status(
                    session_id
                )
                if state["status"] != "ok" or state["execution_state"] != "awaiting_response":
                    return state
            result = self._sources("recall", **args)
            if result["status"] != "ok":
                return result
            response = {
                "status": "ok",
                "sources": [],
                "coverage": "indexed_only" if self.host else "native_bounded",
                "matching": args.get("match", "all") if self.host else "legacy",
                "next_action": "Use archive_ids in problem_begin to bind full snapshots.",
            }
            for key in ("coverage_detail", "next_cursor", "generation", "limitations", "partial"):
                if key in result:
                    response[key] = result[key]
            for source in result["sources"]:
                text = source["text"]
                preview = text[:512]
                item = {
                    "id": source["id"],
                    "hash": source["provenance"].get(
                        "text_hash", hashlib.sha256(text.encode("utf-8")).hexdigest()
                    )
                    if self.host
                    else hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    "exposure_hash": source.get("exposure_hash") or source_exposure_hash(source),
                    "text_bytes": source.get("text_bytes", len(text.encode("utf-8"))),
                    "preview": preview,
                    "preview_offset": 0,
                    "preview_end_offset": len(preview),
                    "preview_truncated": len(text) > len(preview)
                    or source["provenance"].get("preview", False),
                    "attribution": "archive_snapshot",
                    "truth_verified": False,
                }
                if "metadata" in source:
                    item["metadata"] = source["metadata"]
                response["sources"].append(item)
                while len(json.dumps(response, ensure_ascii=False).encode("utf-8")) > 8192:
                    if not item["preview"]:
                        response["sources"].pop()
                        break
                    item["preview"] = item["preview"][: len(item["preview"]) // 2]
                    item["preview_end_offset"] = len(item["preview"])
                    item["preview_truncated"] = True
            return response

        from .brain_loop import BrainLoop

        operation = name.removeprefix("castle_problem_")
        loop = BrainLoop(self.controller_path, exposure_policy=self.policy)
        if operation == "begin":
            ids = args.pop("archive_ids", [])
            if len(ids) + len(args["sources"]) > 8:
                raise ValueError("Source bundle count limit")
            if ids:
                # Bind idempotency to the exact host request, not mutable native
                # table provenance. Lookup is content-free and rechecks current
                # policy/lifecycle before an archive can be reopened.
                failure = self._bind_archive(loop, args, arguments, ids)
                if failure is not None:
                    return failure
        if operation in ("status", "page"):
            args.pop("protocol_version")
        try:
            result = getattr(loop, operation)(**args)
        except (ValueError, OSError):
            if args.get("request_fingerprint"):
                self._failed_read(loop, args.get("parent_id"), {"status": "invalid_request"})
            raise
        if (
            args.get("request_fingerprint")
            and result.get("status") != "ok"
            and "budgets" not in result
        ):
            return self._failed_read(loop, args.get("parent_id"), result)
        return result


def tool_registry(api_factory):
    """Lazy factory keeps discovery free of storage and native initialization."""

    def handler(name):
        return lambda **arguments: api_factory().call(name, arguments)

    return {
        name: {
            "description": description,
            "input_schema": schema,
            "handler": handler(name),
            "strict_arguments": True,
        }
        for name, (description, schema) in SPECS.items()
    }


ACTIONS = {
    "capabilities": "castle_brain_capabilities",
    "article": "castle_article_assess",
    "recall": "castle_brain_recall",
    "memory_remember": "castle_memory_remember",
    "memory_read": "castle_memory_read",
    "memory_status": "castle_memory_status",
    "memory_context": "castle_memory_context",
    "memory_context_update": "castle_memory_context_update",
} | {
    action: f"castle_problem_{action}"
    for action in ("begin", "respond", "status", "page", "cancel", "correct")
}


def configure_parser(sub):
    parser = sub.add_parser("brain", help="Bounded model-free article and problem examination")
    parser.add_argument("brain_action", choices=ACTIONS)
    parser.add_argument("--input", default="-", help="Bounded JSON file, or - for stdin")


def cmd_brain(args):
    import sys
    from .config import CognitiveCastleConfig
    from .bounded_json import FrameReader

    try:
        status_stdin = args.brain_action == "memory_status" and args.input == "-"
        if args.brain_action == "capabilities" and args.input == "-":
            request = {}
        elif status_stdin and sys.stdin.isatty():
            request = {"protocol_version": 1}
        elif args.input == "-":
            # Interactive/native stdin uses the same first-byte deadline as MCP.
            stream = getattr(sys.stdin, "buffer", sys.stdin)
            if hasattr(stream, "getvalue"):
                raw = stream.read(MAX_FRAME_BYTES + 1)
                eof = raw in (b"", "")
            else:
                raw = FrameReader(stream).read()
                eof = raw is None
            if status_stdin:
                request = {"protocol_version": 1} if eof else strict_loads(raw)
            else:
                request = strict_loads(raw or b"{}")
        else:
            from .brain_loop import controller_capabilities

            if controller_capabilities()["status"] != "available":
                raise ValueError("Private file access unavailable; use standard input")
            fd = os.open(args.input, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FRAME_BYTES:
                    raise ValueError("Input must be a bounded regular file")
                request = strict_loads(stream.read(MAX_FRAME_BYTES + 1))
        result = BrainAPI(args.palace or CognitiveCastleConfig().palace_path).call(
            ACTIONS[args.brain_action], request
        )
        raw = json.dumps(result, ensure_ascii=False, allow_nan=False)
        if len(raw.encode("utf-8")) > 32768:
            raise ValueError("Response exceeds limit; request a bounded page")
        print(raw)
    except (ValueError, OSError) as exc:
        print(json.dumps({"status": "invalid_request", "reason": str(exc)}))
        raise SystemExit(2) from None


def _root_registry(palace, registration=None):
    """Operator-only bounded private registry; no tool argument reaches this path."""
    directory = Path(palace).expanduser() / ".memory"
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("Private registry unsupported")
    for component in (directory, *directory.parents):
        if component.is_symlink():
            raise ValueError("Symlink registry ancestor")
    if registration is not None:
        directory.mkdir(mode=0o700, exist_ok=True)
    info = directory.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("Registry directory must be owner-private")
    path = directory / "roots.json"
    roots = {}
    if path.exists() or path.is_symlink():
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or info.st_nlink != 1
                or info.st_size > 16384
            ):
                raise ValueError("Unsafe root registry")
            roots = strict_loads(stream.read(16385), max_bytes=16384)
    if not isinstance(roots, dict) or len(roots) > 8:
        raise ValueError("Invalid root registry")
    for name, entry in roots.items():
        if (
            not re.fullmatch(r"[a-z0-9_-]{1,32}", name)
            or not isinstance(entry, dict)
            or set(entry) != {"path", "format"}
            or entry["format"] not in ("codex", "claude")
            or not isinstance(entry["path"], str)
            or not Path(entry["path"]).is_absolute()
        ):
            raise ValueError("Invalid registered root")
    if registration is not None:
        name, source, source_format = registration
        if not re.fullmatch(r"[a-z0-9_-]{1,32}", name) or source_format not in ("codex", "claude"):
            raise ValueError("Invalid root registration")
        source = Path(source).expanduser().absolute()
        if any(p.is_symlink() for p in (source, *source.parents)) or not source.is_dir():
            raise ValueError("Root must be an original nonsymlink directory")
        roots[name] = {"path": str(source), "format": source_format}
        if len(roots) > 8:
            raise ValueError("At most eight roots")
        raw = json.dumps(roots, ensure_ascii=False).encode()
        if len(raw) > 16384:
            raise ValueError("Registry byte limit")
        import tempfile

        fd, temporary = tempfile.mkstemp(prefix=".roots-", dir=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return roots


def configure_memory_parser(sub):
    parser = sub.add_parser("memory", help="Exact bounded local memory and operator ingestion")
    actions = parser.add_subparsers(dest="memory_action", required=True)
    from .history_cli import configure_history_parser

    configure_history_parser(actions)
    for action in ("remember", "read", "status", "recall", "context", "context-update"):
        child = actions.add_parser(action)
        child.add_argument("--input", default="-")
    register = actions.add_parser(
        "register-root", help="Operator: authorize an original conversation root"
    )
    register.add_argument("--name", required=True)
    register.add_argument("--path", required=True)
    register.add_argument("--format", required=True, choices=("codex", "claude"))
    ingest = actions.add_parser(
        "ingest", help="Operator: ingest one bounded slice from a registered root"
    )
    ingest.add_argument("--root", required=True)
    ingest.add_argument("--path", required=True)
    semantic = actions.add_parser(
        "semantic", help="Operator: explicit bounded semantic diagnostic; no activation"
    )
    semantic_actions = semantic.add_subparsers(dest="semantic_action", required=True)
    tick = semantic_actions.add_parser("tick")
    tick.add_argument("--max-seconds", type=float, default=5)
    tick.add_argument(
        "--diagnostic", action="store_true", help="Opt in to the diagnostic-only model profile"
    )
    capture = actions.add_parser("capture", help="Operator: bounded automatic capture")
    capture_actions = capture.add_subparsers(dest="capture_action", required=True)
    for action in ("enable", "pause", "status", "tick", "reconcile"):
        child = capture_actions.add_parser(action)
        if action in ("tick", "reconcile"):
            child.add_argument("--max-seconds", type=float, default=5)
        if action == "tick":
            child.add_argument("--no-discover", action="store_true")


def cmd_memory(args):
    from .config import CognitiveCastleConfig

    palace = args.palace or CognitiveCastleConfig().palace_path
    if args.memory_action in ("remember", "read", "status", "recall", "context", "context-update"):
        args.brain_action = (
            "recall"
            if args.memory_action == "recall"
            else "memory_" + args.memory_action.replace("-", "_")
        )
        return cmd_brain(args)
    try:
        api = BrainAPI(palace)  # Validate trusted launch configuration before mutation.
        if args.memory_action == "history":
            from .history_cli import cmd_history

            result = cmd_history(api.palace, args)
        elif args.memory_action == "semantic":
            from .semantic_coordinator import index_tick

            result = index_tick(
                api.palace, diagnostic=args.diagnostic, max_seconds=args.max_seconds
            )
        elif args.memory_action == "capture":
            from .memory_coordinator import capture_status, capture_tick, configure_capture

            if args.capture_action in ("enable", "pause"):
                result = configure_capture(api.palace, enabled=args.capture_action == "enable")
            elif args.capture_action == "status":
                result = capture_status(api.palace)
            else:
                result = capture_tick(
                    api.palace,
                    max_seconds=args.max_seconds,
                    discover=not getattr(args, "no_discover", False),
                )
        elif args.memory_action == "register-root":
            roots = _root_registry(api.palace, (args.name, args.path, args.format))
            result = {
                "status": "ok",
                "registered_root": args.name,
                "format": roots[args.name]["format"],
            }
        else:
            from .memory_runtime import run_memory

            roots = _root_registry(api.palace)
            entry = roots.get(args.root)
            if entry is None:
                raise ValueError("Unknown registered root")
            source = Path(args.path).expanduser().absolute()
            root = Path(entry["path"])
            if (
                ".." in source.parts
                or any(p.is_symlink() for p in (source, *source.parents, root, *root.parents))
                or not source.is_relative_to(root)
                or not source.is_file()
                or not root.is_dir()
            ):
                raise ValueError("Ingest path must be an original file within the registered root")
            result = run_memory(
                api.palace,
                "ingest_slice",
                {"path": str(source), "allowed_root": entry["path"], "format": entry["format"]},
            )
        print(json.dumps(result, ensure_ascii=False))
    except (ValueError, OSError) as exc:
        print(json.dumps({"status": "invalid_request", "reason": str(exc)}))
        raise SystemExit(2) from None

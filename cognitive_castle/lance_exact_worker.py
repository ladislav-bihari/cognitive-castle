"""Private fixed-operation exact Lance worker, launched only by the guardian."""

from __future__ import annotations

import importlib.abc
import json
import os
from pathlib import Path
import sys

MAX_FRAME = 512 * 1024
_FIELDS = {
    "layout_import_batch": (
        {"intent_id", "batch_id", "source_binding", "entries", "finalizations"},
        set(),
    ),
    "layout_read": ({"operation", "arguments"}, {"context"}),
    "layout_maintain": ({"partition_id"}, set()),
    "layout_verify_owners": ({"intent_id", "version"}, set()),
    "import_batch": ({"batch_id", "source_binding", "entries", "finalizations"}, set()),
    "build_id_index": (set(), set()),
    "bulk_owner": ({"owner"}, {"version"}),
    "bulk_page": ({"owner", "field", "ordinal"}, {"version"}),
    "bulk_receipt": ({"batch_id", "source_binding"}, {"offset", "version"}),
    "remember": ({"request_id", "text"}, {"provenance", "namespace"}),
    "import_sources": ({"batch_id", "sources"}, {"namespace"}),
    "receipt": ({"namespace", "key"}, set()),
    "read_sources": ({"ids"}, set()),
    "page": ({"source_id"}, {"offset", "limit", "expected_hash"}),
}


class _ForbiddenImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        root = fullname.split(".")[0]
        if root in {
            "torch",
            "transformers",
            "sentence_transformers",
            "tensorflow",
            "mlx",
            "ollama",
            "openai",
            "anthropic",
            "requests",
            "httpx",
            "pandas",
            "lancedb",
            "spacy",
            "Python_sml_ClientInterface",
            "_Python_sml_ClientInterface",
        } or fullname.startswith(
            (
                "cognitive_castle.embedding",
                "cognitive_castle.backends",
                "cognitive_castle.searcher",
                "cognitive_castle.reranker",
                "cognitive_castle.llm",
            )
        ):
            raise ModuleNotFoundError("exact worker forbids models, providers and optional Pandas")
        return None


def _install_guards():
    sys.meta_path.insert(0, _ForbiddenImports())

    def audit(event, args):
        if event.startswith("socket.") or event in {
            "subprocess.Popen",
            "os.system",
            "os.fork",
            "os.forkpty",
            "os.posix_spawn",
            "os.exec",
        }:
            raise PermissionError("exact worker forbids network and child processes")

    sys.addaudithook(audit)


def _admit_storage(request):
    from cognitive_castle.lance_exact_store import _require_supervision, _StoreError

    try:
        if type(request.get("_host_pid")) is not int:
            raise _StoreError("unavailable")
        _require_supervision(request["_host_pid"])
    except _StoreError as exc:
        return {"status": "unavailable", "reason": exc.status}
    return None


def _execute(request):
    from cognitive_castle.bulk_exact_store import BulkExactStore

    if not isinstance(request, dict) or request.keys() != {
        "directory",
        "operation",
        "arguments",
        "_host_pid",
    }:
        raise ValueError("request fields")
    operation, arguments = request["operation"], request["arguments"]
    if (
        not isinstance(operation, str)
        or operation not in _FIELDS
        or not isinstance(arguments, dict)
    ):
        raise ValueError("operation")
    required, optional = _FIELDS[operation]
    if not required <= arguments.keys() or arguments.keys() - required - optional:
        raise ValueError("arguments")
    if not isinstance(request["directory"], str) or not request["directory"]:
        raise ValueError("directory")
    if operation in {
        "layout_import_batch",
        "layout_read",
        "layout_maintain",
        "layout_verify_owners",
    }:
        from cognitive_castle.partitioned_exact_store import PartitionedExactStore

        store = PartitionedExactStore(
            Path(request["directory"]),
            writable=operation in {"layout_import_batch", "layout_maintain"},
        )
        return getattr(store, operation)(**arguments)
    store = BulkExactStore(
        Path(request["directory"]),
        writable=operation in {"remember", "import_sources", "import_batch", "build_id_index"},
    )
    return getattr(store, operation)(**arguments)


def main():
    os.umask(0o077)
    # -I deliberately ignores editable installs/PYTHONPATH/cwd. The trusted
    # executable's own parent selects this checkout before package imports.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    _install_guards()
    try:
        frame = sys.stdin.buffer.read(MAX_FRAME + 1)
        if len(frame) > MAX_FRAME:
            raise ValueError("request limit")
        request = json.loads(frame)
        if not isinstance(request, dict):
            raise ValueError("request")
        result = _admit_storage(request)
        if result is None:
            result = _execute(request)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result = {"status": "unavailable", "reason": "invalid_exact_request"}
    except Exception as exc:
        result = {"status": "unavailable", "reason": "exact_" + type(exc).__name__}
    output = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(output) > MAX_FRAME:
        output = b'{"status":"unavailable","reason":"worker_output_limit"}'
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()

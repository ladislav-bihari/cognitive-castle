"""Fixed queue/vector operations. Internal only; trusted guardian required."""

from __future__ import annotations

import importlib.abc
import json
import os
from pathlib import Path
import sys

MAX_FRAME = 512 * 1024
_FIELDS = {
    "queue_layout_resume": ({"registration"}, set()),
    "queue_layout_maintenance_prepare": ({"partition_id", "version"}, set()),
    "queue_layout_rebuild_prepare": ({"partition_id", "version"}, set()),
    "queue_layout_reconcile": ({"page"}, set()),
    "queue_layout_bind": ({"job_id", "plans", "envelope"}, set()),
    "queue_layout_bind_prefix": ({"job_id", "plans", "envelope"}, set()),
    "queue_layout_prepare": ({"job_id", "expected_cursor", "intent"}, set()),
    "queue_layout_ack": ({"job_id", "intent_id", "verified_summary"}, set()),
    "queue_layout_routes": ({"ids"}, set()),
    "queue_migration_open": ({"registration"}, set()),
    "queue_migration_state": ({"job_id"}, set()),
    "queue_migration_prepare": ({"job_id", "expected_cursor", "intent"}, set()),
    "queue_migration_ack": ({"job_id", "intent_id", "verified_summary"}, set()),
    "queue_migration_refuse": ({"job_id", "expected_cursor", "refusal"}, set()),
    "queue_intent_prepare": ({"intent"}, set()),
    "queue_intents": (set(), set()),
    "queue_intent_complete": ({"intent_id", "binding"}, set()),
    "queue_register": ({"batch_key", "references", "profile", "input_binding", "lane"}, set()),
    "queue_select": (set(), set()),
    "queue_acknowledge": ({"batch_key", "receipt"}, set()),
    "queue_status": (set(), set()),
    "queue_retry": ({"batch_key"}, {"refused"}),
    "vector_publish": ({"batch_key", "references", "page", "inference"}, set()),
    "vector_receipt": ({"batch_key", "start_window"}, set()),
    "vector_nearest": (
        {"vector", "profile", "generation", "scope", "allowed_source_hashes", "limit"},
        set(),
    ),
}
QUEUE_READS = {"status", "migration_state", "layout_routes"}


class _ForbiddenImports(importlib.abc.MetaPathFinder):
    def __init__(self, phase):
        self.phase = phase

    def find_spec(self, fullname, path=None, target=None):
        blocked = {
            "pandas",
            "torch",
            "transformers",
            "sentence_transformers",
            "tensorflow",
            "keras",
            "mlx",
            "ollama",
            "openai",
            "anthropic",
            "requests",
            "httpx",
            "onnx",
            "onnxruntime",
            "tokenizers",
            "lancedb",
            "optimum",
        }
        if self.phase == "queue":
            blocked |= {"lance", "pyarrow", "numpy"}
        if fullname.partition(".")[0] in blocked or fullname.startswith(
            (
                "cognitive_castle.embedding",
                "cognitive_castle.backends",
                "cognitive_castle.searcher",
                "cognitive_castle.reranker",
                "cognitive_castle.llm",
            )
        ):
            raise ModuleNotFoundError("semantic store forbids optional dependency: " + fullname)
        return None


def _install_guards(phase):
    sys.meta_path.insert(0, _ForbiddenImports(phase))

    def audit(event, args):
        if event.startswith("socket.") or event in {
            "subprocess.Popen",
            "os.system",
            "os.exec",
            "os.fork",
            "os.forkpty",
            "os.posix_spawn",
        }:
            raise PermissionError("semantic store forbids network and children")

    sys.addaudithook(audit)


def _admit(request, phase):
    from cognitive_castle.semantic_worker import _admit as admit

    # Reuse the pinned session-chain check, then enforce stricter store limits
    # locally. Do not mutate the staged semantic runtime's budgets.
    reason = admit(request)
    if reason:
        return {"status": "unavailable", "reason": reason}
    import psutil
    from cognitive_castle.brain_reader_guardian import _group_rss

    task_limit, total_limit = (128, 192) if phase == "queue" else (192, 256)
    try:
        task = _group_rss(os.getpgrp())
        if task > task_limit * 1024**2:
            return {"status": "unavailable", "reason": "worker_memory_limit"}
        host_rss = psutil.Process(request["_host_pid"]).memory_info().rss
        if task + host_rss > total_limit * 1024**2:
            return {"status": "unavailable", "reason": "total_memory_limit"}
    except (OSError, psutil.Error):
        return {"status": "unavailable", "reason": "resource_measurement_unavailable"}
    return None


def _execute(request):
    if set(request) != {"directory", "operation", "arguments", "_host_pid"}:
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
    if not isinstance(request["directory"], str) or not Path(request["directory"]).is_absolute():
        raise ValueError("trusted absolute generation required")
    phase, method = operation.split("_", 1)
    if phase == "queue":
        from cognitive_castle.semantic_queue import SemanticQueue
        import sqlite3

        # This process-global setting is restricted to the queue worker.
        with sqlite3.connect(":memory:") as conn:
            row = conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()
            if row is None or not 0 < row[0] <= 16777216:
                raise ValueError("SQLite heap limit unavailable")
        store = SemanticQueue(Path(request["directory"]), writable=method not in QUEUE_READS)
    else:
        from cognitive_castle.lance_vector_store import LanceVectorStore

        store = LanceVectorStore(Path(request["directory"]), writable=method == "publish")
    return getattr(store, method)(**arguments)


def main():
    os.umask(0o077)
    for name in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ[name] = "1"
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.semantic_profile import decode_frame

    try:
        request = decode_frame(sys.stdin.buffer.read(MAX_FRAME + 1))
        operation = request.get("operation")
        if not isinstance(operation, str) or operation not in _FIELDS:
            raise ValueError("operation")
        phase = operation.split("_", 1)[0]
        _install_guards(phase)
        result = _admit(request, phase)
        if result is None:
            result = _execute(request)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, OSError):
        result = {"status": "unavailable", "reason": "invalid_semantic_store_request"}
    except Exception as exc:
        result = {"status": "unavailable", "reason": "semantic_store_" + type(exc).__name__}
    output = json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
        "utf-8"
    )
    if len(output) > MAX_FRAME:
        output = b'{"status":"unavailable","reason":"worker_output_limit"}'
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()

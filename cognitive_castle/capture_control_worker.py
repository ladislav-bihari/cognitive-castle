"""Fixed independent metadata worker. No initialization, canonical engine or model."""

import json
import os
from pathlib import Path
import sys

MAX_FRAME = 512 * 1024


def _store(request):
    from cognitive_castle.capture_control_store import CaptureControlStore

    return CaptureControlStore(request["authority"], expected_hash=request["authority_hash"])


def _execute(request):
    from cognitive_castle.capture_control_codec import validate_request

    request = validate_request(request)
    store = _store(request)
    operation, args = request["operation"], request["arguments"]
    try:
        if operation == "source_prepare":
            from cognitive_castle.capture_control import prepare_source

            return prepare_source(store, **args)
        if operation in {"source_queue", "source_page", "coverage"}:
            from cognitive_castle import memory_capture as capture

            target = {
                "source_queue": capture.queue_source,
                "source_page": capture.source_page,
                "coverage": capture.coverage,
            }[operation]
            return target(store, **args)
        from cognitive_castle.capture_publication import select_recovery_page, apply_recovery_page

        if operation == "recovery_select":
            return select_recovery_page(store, **args)
        return apply_recovery_page(store, **args)
    except Exception as exc:
        from cognitive_castle.fast_memory import _Corrupt, _Quota, _Unavailable
        import sqlite3

        if isinstance(exc, (OSError, sqlite3.Error, _Corrupt, _Quota, _Unavailable)):
            return store._error(exc)
        raise


def main():
    os.umask(0o077)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.capture_control_codec import validate_request

    try:
        request = validate_request(decode_frame(sys.stdin.buffer.read(MAX_FRAME + 1)))
        from cognitive_castle.semantic_store_worker import _install_guards, _admit

        _install_guards("queue")
        result = _admit(request, "queue")
        if result is None:
            import sqlite3

            with sqlite3.connect(":memory:") as conn:
                assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
            result = _execute(request)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, OSError):
        result = dict(status="unavailable", reason="invalid_capture_control_request")
    except Exception as exc:
        result = dict(status="unavailable", reason="capture_control_" + type(exc).__name__)
    output = json.dumps(result, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    if len(output) > MAX_FRAME - 16384:
        output = b'{"status":"unavailable","reason":"worker_output_limit"}'
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()

"""Additional actual-production control-worker proof; wholly synthetic and opt-in."""

import json
import os
from pathlib import Path
import platform
import re
import sys


def _operator():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_store_worker import _install_guards, _admit

    request = decode_frame(sys.stdin.buffer.read(512 * 1024 + 1))
    if (
        request.keys() != {"directory", "operation", "arguments", "_host_pid"}
        or request["operation"] != "initialize"
        or request["arguments"] != {"max_bytes": 3 * 1024**2}
    ):
        raise ValueError("fixed control operator request")
    _install_guards("queue")
    refusal = _admit(request, "queue")
    if refusal is not None:
        return refusal
    import sqlite3

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    from cognitive_castle.capture_control_store import initialize_capture_control

    return initialize_capture_control(Path(request["directory"]), max_bytes=3 * 1024**2)


if __name__ == "__main__":
    os.umask(0o077)
    try:
        if sys.argv[1:] != ["initialize"]:
            raise ValueError("fixed operator invocation")
        result = _operator()
    except Exception as exc:
        result = dict(status="unavailable", reason="fixture_" + type(exc).__name__)
    output = json.dumps(result, ensure_ascii=True, allow_nan=False).encode()
    if len(output) > 512 * 1024:
        output = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - the trusted operator exits before test-host imports.

_GRANT = os.environ.get("CASTLE_CAPTURE_CONTROL_QUALIFICATION_SOURCE", "")
pytestmark = pytest.mark.skipif(
    sys.platform != "darwin"
    or platform.mac_ver()[0] != "26.6.2"
    or platform.machine() != "arm64"
    or platform.python_version() != "3.12.11"
    or re.fullmatch("[a-f0-9]{40}", _GRANT) is None,
    reason="requires explicit frozen actual-production synthetic control qualification",
)
_CALLS = 0


def _stage(session, operation, call, expected):
    global _CALLS
    _CALLS += 1
    number = _CALLS
    assert number <= 8
    print(
        "LAYOUT_NATIVE_START "
        + json.dumps(dict(call=number, operation="queue_capture_" + operation)),
        flush=True,
    )
    before = len(session.resources)
    result = call()
    resources = session.resources[before:]
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=number,
                operation="queue_capture_" + operation,
                status=result.get("status"),
                reason=result.get("reason"),
                resources=resources,
            )
        ),
        flush=True,
    )
    assert result.get("status") == expected and result.get("reason") is None, result
    assert len(resources) == 1
    measured = resources[0]
    assert 0 < measured["peak_task_rss_bytes"] <= 128 * 1024**2
    assert 0 < measured["peak_total_rss_bytes"] <= 192 * 1024**2
    assert 0 < measured["elapsed_seconds"] <= (2 if operation == "source_page" else 3)
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )
    return result


def _session(root):
    from cognitive_castle.semantic_runtime import SemanticSession

    return SemanticSession(root / "unused-profile", interactive=False)


def _initialize(root):
    with _session(root) as session:
        return _stage(
            session,
            "initialize",
            lambda: session._run_stage(
                dict(
                    directory=str(root / "control"),
                    operation="initialize",
                    arguments=dict(max_bytes=3 * 1024**2),
                ),
                worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "initialize"],
                cap=3,
                rss=128 * 1024**2,
                total=192 * 1024**2,
            ),
            "ok",
        )


def _call(root, authority, operation, args, expected="ok"):
    from cognitive_castle.capture_control_runtime import CaptureControlClient

    with _session(root) as session:
        client = CaptureControlClient(
            session, authority["authority"], expected_hash=authority["authority_hash"]
        )
        return _stage(session, operation, lambda: client.call(operation, args), expected)


def _source(root, body):
    path = root / "source.jsonl"
    path.write_bytes(
        json.dumps(
            dict(
                type="response_item",
                payload=dict(
                    type="message", role="user", content=[dict(type="input_text", text=body)]
                ),
            ),
            ensure_ascii=False,
        ).encode()
        + b"\n"
    )
    return path


def _setup(root):
    authority = _initialize(root)
    body = "Guarded exact Ω\x00\r\n  "
    path = _source(root, body)
    queued = _call(
        root,
        authority,
        "source_queue",
        dict(path=str(path), allowed_root=str(root), format="codex", workspace="/project"),
    )
    return authority, path, queued["source_key"], body


def test_control_production_actual_supervision_preserves_original_handoff(tmp_path):
    authority, _, key, body = _setup(tmp_path.resolve())
    first = _call(tmp_path.resolve(), authority, "source_prepare", dict(source_key=key), "prepared")
    assert first["preparation"]["text"] == body and first["publication_consumed"] is False
    page = _call(tmp_path.resolve(), authority, "source_page", dict(pending_only=True))
    assert (
        page["sources"][0]["processed_bytes"] == 0
        and page["sources"][0]["publication_pending"] == 1
    )
    again = _call(tmp_path.resolve(), authority, "source_prepare", dict(source_key=key), "prepared")
    assert again["preparation"] == first["preparation"] and again["scope"] == first["scope"]
    assert _CALLS == 5


def test_control_production_wrong_proof_and_rewrite_remain_unconfirmed(tmp_path):
    from cognitive_castle.canonical_records import prepare_remember

    root = tmp_path.resolve()
    authority, path, key, _ = _setup(root)
    first = _call(root, authority, "source_prepare", dict(source_key=key), "prepared")
    packet = first["preparation"]
    prepared = prepare_remember(
        packet["request_id"], packet["text"], packet["provenance"], namespace="transcript"
    )
    wrong = dict(request_id=prepared.request_id, digest="f" * 64, receipt=prepared.receipt)
    refused = _call(
        root, authority, "source_prepare", dict(source_key=key, publication=wrong), "conflict"
    )
    assert refused["publication_consumed"] is False
    _source(root, "new guarded revision")
    revised = _call(root, authority, "source_prepare", dict(source_key=key))
    assert revised["publication_consumed"] is False
    staged = _call(root, authority, "source_prepare", dict(source_key=key), "prepared")
    assert (
        staged["preparation"]["text"] == "new guarded revision"
        and staged["preparation"]["provenance"]["generation"] == 1
    )
    page = _call(root, authority, "source_page", dict(pending_only=True))
    assert (
        page["sources"][0]["processed_bytes"] == 0
        and page["sources"][0]["publication_pending"] == 2
    )
    assert _CALLS == 7

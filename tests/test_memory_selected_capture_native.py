"""Opt-in actual selected capture. Old SQLite and all words are synthetic."""

import hashlib
import json
import os
from pathlib import Path
import sys
import time


def _old_quota_operator():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_store_worker import _admit, _install_guards

    request = decode_frame(sys.stdin.buffer.read(512 * 1024 + 1))
    if (
        request.keys() != {"directory", "operation", "arguments", "_host_pid"}
        or request["operation"] != "old_quota"
        or request["arguments"] != {}
    ):
        raise ValueError("fixed old-quota operator")
    _install_guards("queue")
    refused = _admit(request, "queue")
    if refused is not None:
        return refused
    import sqlite3
    from cognitive_castle.fast_memory import MemoryStore

    with sqlite3.connect(":memory:") as conn:
        assert 0 < conn.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    old = MemoryStore(Path(request["directory"]))
    assert old.remember("old", "PROTECTED_LEGACY_SQLITE_WORDS")["status"] == "ok"
    with sqlite3.connect(old.db_path) as conn:
        pages = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    # Smaller test-only legacy quota, never a raised production resource budget.
    old._write_budget = lambda: 3 * pages * page_size
    probe = old.remember("cannot-fit", "x" * 65536)
    assert probe["status"] == "quota", probe
    assert old.db_path.stat().st_size == pages * page_size < 1024**2
    return dict(
        status="ok",
        old_bytes=old.db_path.stat().st_size,
        old_hash=hashlib.sha256(old.db_path.read_bytes()).hexdigest(),
        probe_status=probe["status"],
    )


if __name__ == "__main__":
    os.umask(0o077)
    try:
        if sys.argv[1:] != ["old_quota"]:
            raise ValueError("fixed operator invocation")
        result = _old_quota_operator()
    except Exception as exc:
        result = dict(status="unavailable", reason="fixture_" + type(exc).__name__)
    sys.stdout.buffer.write(json.dumps(result, ensure_ascii=True, allow_nan=False).encode())
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - operator must exit before test-host imports.
from test_memory_selected_native import _handoff, pytestmark  # noqa: E402,F401


def test_selected_capture_confirms_actual_native_receipt_and_preserves_quota_full_old_store(
    monkeypatch,
):
    from cognitive_castle import memory_runtime, semantic_runtime
    from cognitive_castle.canonical_records import prepare_remember
    from test_layout_native_integration import _start, _record, _session

    root, seed = _handoff()
    palace = root / "palace"
    text = "CAPTURE_SELECTED_NATIVE Ω\x00\r\n  "
    source = root / "captured-dialogue.jsonl"
    raw = (
        json.dumps(
            dict(
                type="response_item",
                payload=dict(
                    type="message", role="user", content=[dict(type="input_text", text=text)]
                ),
            ),
            ensure_ascii=False,
        ).encode()
        + b"\n"
    )
    with source.open("xb") as output:
        output.write(raw)
    original_stage = semantic_runtime.SemanticSession._run_stage
    prepared = {}
    receipts = {}
    imports = 0
    confirmed = 0

    def observe(self, request, **limits):
        nonlocal imports, confirmed
        operation = request["operation"]
        command = Path(limits["worker_command"][2]).name
        control = command in {
            Path(__file__).name,
            "capture_control_worker.py",
        } or operation.startswith("queue_")
        assert limits["rss"] == (128 if control else 192) * 1024**2
        assert limits["total"] == (192 if control else 256) * 1024**2
        assert limits["cap"] <= 3
        proof = request.get("arguments", {}).get("publication")
        if proof is not None:
            observed = receipts[proof["request_id"]]
            assert (
                observed["session"] == id(self)
                and proof["receipt"] == observed["receipt"]
                and proof["digest"] == observed["digest"]
            )
        label = ("queue_selected_capture_" if control else "selected_native_capture_") + operation
        previous = len(self.resources)
        number = _start(label)
        result = original_stage(self, request, **limits)
        _record(self, label, result, previous, number)
        if control and operation == "source_prepare" and result.get("status") == "prepared":
            canonical = prepare_remember(**result["preparation"])
            assert "".join(record.text for record in canonical.records) == text
            prepared[canonical.request_id] = canonical
        if not control and operation == "layout_import_verified":
            imports += 1
        if (
            not control
            and operation == "layout_read"
            and request["arguments"]["operation"] == "receipt"
            and result.get("status") == "ok"
        ):
            receipts[request["arguments"]["arguments"]["key"]] = dict(
                session=id(self), digest=result["digest"], receipt=result["receipt"]
            )
        if proof is not None and result.get("publication_consumed") is True:
            confirmed += 1
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)
    with _session(root) as session:
        old = session._run_stage(
            dict(directory=str(palace / ".memory"), operation="old_quota", arguments={}),
            worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "old_quota"],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
    assert old["status"] == "ok" and old["probe_status"] == "quota"
    legacy = palace / ".memory/memory.sqlite3"

    def run(operation, arguments):
        before = time.monotonic()
        result = memory_runtime.run_memory(palace, operation, arguments)
        elapsed = time.monotonic() - before
        limit = 2 if operation in {"source_page", "coverage", "read_sources"} else 3
        print(
            "SELECTED_CAPTURE_OPERATION "
            + json.dumps(dict(operation=operation, seconds=elapsed, status=result.get("status"))),
            flush=True,
        )
        assert elapsed <= limit and result.get("status") in {"ok", "in_progress"}, result
        assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
            sys.modules
        )
        return result

    queued = run(
        "queue_source",
        dict(
            path=str(source), allowed_root=str(root), format="codex", workspace="/capture/project"
        ),
    )
    key = queued["source_key"]
    for attempt in range(5):
        result = run("capture_one", dict(key=key))
        row = run("source_page", {})["sources"][0]
        if result.get("publication_consumed") is True:
            assert row["processed_bytes"] == len(raw) and row["publication_pending"] == 0
            break
        assert (
            row["processed_bytes"] == 0
            and row["publication_pending"] == 1
            and row["scoped_chunks"] == 0
        )
    else:
        pytest.fail("capture never confirmed inside five bounded calls")
    assert confirmed == 1 and imports >= 4 and len(prepared) == 1
    assert (
        row["scoped_chunks"] == 1
        and row["unscoped_chunks"] == 0
        and row["scope_accounting_complete"] is True
    )
    before = imports
    again = run("capture_one", dict(key=key))
    assert again.get("publication_consumed") is False and imports == before
    assert run("source_page", {})["sources"][0]["scoped_chunks"] == 1
    assert run("coverage", {})["pending_sources"] == 0
    canonical = next(iter(prepared.values()))
    sources = run("read_sources", dict(ids=[canonical.records[0].id, seed["record"]["id"]]))[
        "sources"
    ]
    assert sources[0]["text"] == text and sources[0]["provenance"]["caller"]["source"] == key
    assert sources[1] == seed["record"] and source.read_bytes() == raw
    assert (
        legacy.stat().st_size == old["old_bytes"]
        and hashlib.sha256(legacy.read_bytes()).hexdigest() == old["old_hash"]
    )
    for directory in (root / "control", root / "generation/coordination"):
        for path in directory.iterdir():
            if path.is_file():
                assert path.stat().st_size <= 4 * 1024**2
                assert b"CAPTURE_SELECTED_NATIVE" not in path.read_bytes()

"""Opt-in actual-production selected exact path, entirely synthetic.

Seed and each cold public read have separate finite outer qualification phases.
The synthetic profile manifest is never used for model inference. No user palace,
provider, model, ordinary production selector or installed service is modified.
"""

import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import time
import pytest

_GRANT = os.environ.get("CASTLE_SELECTED_NATIVE_SOURCE", "")
pytestmark = pytest.mark.skipif(
    sys.platform != "darwin"
    or platform.mac_ver()[0] != "26.6.2"
    or platform.machine() != "arm64"
    or platform.python_version() != "3.12.11"
    or re.fullmatch("[a-f0-9]{40}", _GRANT) is None,
    reason="requires frozen actual-production selected exact qualification grant",
)


def _root():
    path = Path(os.environ["CASTLE_SELECTED_NATIVE_ROOT"])
    assert path.is_absolute() and path.resolve() == path and not path.is_symlink()
    assert path.name == "corpus" and path.parent.name == "fixtures"
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    return path


def _write(path, value):
    from cognitive_castle.canonical_records import _json

    with path.open("x") as output:
        output.write(_json(value))
    path.chmod(0o400)


def _handoff():
    root = _root()
    path = root / "handoff.json"
    assert not path.is_symlink() and path.stat().st_size <= 16384
    assert stat.S_IMODE(path.stat().st_mode) == 0o400
    value = json.loads(path.read_text())
    assert value["source"] == _GRANT
    assert value["record"]["text"] == "Direct Ω\x00\r\n  exact body"
    return root, value


def test_selected_native_seed_real_registered_canonical_corpus():
    from test_lance_live_native import _setup_live, _attempt
    from test_layout_native_integration import _start, _record, _session
    from cognitive_castle.canonical_records import _hash, _json

    root = _root()
    assert not root.exists()
    root.mkdir(mode=0o700)
    generation, publication, binding = _setup_live(root)
    for expected in ("in_progress", "ok", "in_progress", "ok", "ok"):
        result, _ = _attempt(generation, publication, binding)
        assert result["status"] == expected, result
    assert result["state"] == "snapshot_export_exhausted" and result["counts"]["members"] == 1
    with _session(root) as session:
        call = _start("direct_operator_selected_control")
        control = session._run_stage(
            dict(
                directory=str(root / "control"),
                operation="initialize",
                arguments=dict(max_bytes=3 * 1024**2),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("test_capture_control_native.py")),
                "initialize",
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        _record(session, "direct_operator_selected_control", control, 0, call)
    assert control["status"] == "ok", control
    palace = root / "palace"
    palace.mkdir(mode=0o700)
    (palace / ".memory").mkdir(mode=0o700)
    profile = root / "exact-only-fixture-profile.json"
    _write(profile, dict(fixture_only="no model invoked"))
    from test_layout_native_integration import _qualification

    qualification, envelope = _qualification(generation)
    assert _hash(_json(envelope)) == binding
    selected = dict(
        format="castle-canonical-selection-v1",
        backend="lance",
        envelope=envelope,
        control_authority=control["authority"],
        control_hash=control["authority_hash"],
        profile=dict(path=str(profile), hash=_hash(profile.read_text())),
        wing="project",
        room="conversation",
    )
    _write(palace / ".memory/canonical-selection.json", selected)
    _write(
        palace / ".memory/canonical-backend.json",
        dict(
            format="castle-canonical-backend-v1",
            backend="lance",
            selection_hash=_hash(_json(selected)),
        ),
    )
    _write(
        root / "handoff.json", dict(source=_GRANT, record=publication.canonical.records[0].source())
    )
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )


def _ordinary(monkeypatch, operation, arguments):
    from cognitive_castle import semantic_runtime, memory_runtime
    from test_layout_native_integration import _start, _record

    root, value = _handoff()
    original = semantic_runtime.SemanticSession._run_stage
    count = 0

    def observe(self, request, **limits):
        nonlocal count
        count += 1
        assert count == 1
        assert (
            limits["cap"] == 2
            and limits["rss"] == 192 * 1024**2
            and limits["total"] == 256 * 1024**2
        )
        assert request["operation"] == "layout_read" and request["arguments"]["envelope_hash"]
        previous = len(self.resources)
        call = _start("selected_native_" + operation)
        result = original(self, request, **limits)
        _record(self, "selected_native_" + operation, result, previous, call)
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)
    before = time.monotonic()
    result = memory_runtime.run_memory(root / "palace", operation, arguments)
    elapsed = time.monotonic() - before
    print(
        "SELECTED_NATIVE_OPERATION " + json.dumps(dict(operation=operation, seconds=elapsed)),
        flush=True,
    )
    assert elapsed <= 2 and count == 1 and result["status"] == "ok", result
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )
    return result, value["record"]


def test_selected_native_ordinary_read_preserves_exact_source(monkeypatch):
    _, value = _handoff()
    result, original = _ordinary(monkeypatch, "read_sources", dict(ids=[value["record"]["id"]]))
    assert result["sources"] == [original]


def test_selected_native_ordinary_page_preserves_hash_offsets_and_controls(monkeypatch):
    _, value = _handoff()
    record = value["record"]
    result, original = _ordinary(
        monkeypatch,
        "page",
        dict(
            source_id=record["id"],
            field="text",
            offset=1,
            limit=8,
            expected_hash=record["provenance"]["text_hash"],
        ),
    )
    assert result["text"] == "irect Ω\x00" and result["offset"] == 1 and result["next_offset"] == 9
    assert (
        result["text_hash"] == original["provenance"]["text_hash"] and result["complete"] is False
    )


def test_selected_native_status_reads_only_pinned_metadata_and_reports_partial_coverage(
    monkeypatch,
):
    import hashlib
    from cognitive_castle import semantic_runtime, memory_runtime
    from cognitive_castle.memory_selection import load_selection
    from cognitive_castle.migration_codec import hashed
    from test_layout_native_integration import _start, _record

    root, _ = _handoff()

    def snapshots():
        return {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for folder in (root / "control", root / "generation/coordination")
            for path in folder.iterdir()
            if path.is_file()
        }

    before = snapshots()
    original = semantic_runtime.SemanticSession._run_stage
    calls = []

    def observe(self, request, **limits):
        operation = request["operation"]
        assert operation in {"control_status", "queue_status", "coverage"}
        assert (limits["cap"], limits["rss"], limits["total"]) == (2, 128 * 1024**2, 192 * 1024**2)
        if operation == "queue_status":
            assert request["arguments"]["envelope_hash"] == hashed(
                load_selection(root / "palace")["envelope"]
            )
        calls.append(operation)
        previous = len(self.resources)
        number = _start("queue_selected_status_" + operation)
        result = original(self, request, **limits)
        _record(self, "queue_selected_status_" + operation, result, previous, number)
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)
    start = time.monotonic()
    result = memory_runtime.run_memory(root / "palace", "status", dict(after=0, limit=2))
    elapsed = time.monotonic() - start
    print("SELECTED_STATUS_OPERATION " + json.dumps(dict(seconds=elapsed)), flush=True)
    assert elapsed <= 2 and result["status"] == "ok", result
    assert calls == ["control_status", "queue_status", "coverage"]
    assert (
        result["indexed_chunks"] == 1 and result["canonical"]["count_basis"] == "registered_claims"
    )
    assert result["canonical"]["native_rows_rechecked"] is False
    assert result["observations"] == [] and result["next_after"] is None
    assert (
        result["coverage_detail"]["complete"] is False
        and result["semantic"]["coverage_complete"] is False
    )
    assert snapshots() == before
    assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
        sys.modules
    )


def test_selected_control_discovery_reopens_and_finds_new_earlier_names(monkeypatch):
    from cognitive_castle import memory_runtime, semantic_runtime
    from test_layout_native_integration import _start, _record

    root, _ = _handoff()
    originals = root / "discovered-originals"
    originals.mkdir(mode=0o700)
    nested = originals / "nested"
    nested.mkdir(mode=0o700)
    body = "DISCOVERY_SELECTED_NATIVE Ω"
    raw = (
        json.dumps(
            dict(
                type="response_item",
                payload=dict(
                    type="message", role="user", content=[dict(type="input_text", text=body)]
                ),
            ),
            ensure_ascii=False,
        )
        + "\n"
    )
    (originals / "a.jsonl").write_text(raw)
    (nested / "c.jsonl").write_text(raw)
    original_stage = semantic_runtime.SemanticSession._run_stage

    def observe(self, request, **limits):
        operation = request["operation"]
        assert Path(limits["worker_command"][2]).name == "capture_control_worker.py"
        assert limits["rss"] == 128 * 1024**2 and limits["total"] == 192 * 1024**2
        assert limits["cap"] == (3 if operation == "discover_sources" else 2)
        previous = len(self.resources)
        number = _start("queue_selected_discovery_" + operation)
        result = original_stage(self, request, **limits)
        _record(self, "queue_selected_discovery_" + operation, result, previous, number)
        return result

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", observe)

    def run(operation, args):
        before = time.monotonic()
        result = memory_runtime.run_memory(root / "palace", operation, args)
        elapsed = time.monotonic() - before
        print(
            "SELECTED_DISCOVERY_OPERATION "
            + json.dumps(dict(operation=operation, seconds=elapsed)),
            flush=True,
        )
        assert result["status"] == "ok" and elapsed <= (
            3 if operation == "discover_sources" else 2
        ), result
        assert not {"sqlite3", "lance", "lancedb", "pyarrow", "numpy", "torch"}.intersection(
            sys.modules
        )
        return result

    assert run("discovery_status", {})["roots"] == []
    for sweep_number in (1, 2):
        if sweep_number == 2:
            (originals / "0-new.jsonl").write_text(raw)
        for _ in range(12):
            result = run(
                "discover_sources", dict(allowed_root=str(originals), format="codex", max_entries=1)
            )
            if result["discovery"] == "complete_at_last_sweep":
                break
        else:
            pytest.fail("bounded actual discovery sweep never completed")
        status = run("discovery_status", {})
        assert status["roots"][0]["completed_sweeps"] == sweep_number
        assert status["source_currentness"] == "last_observed_only"
    sources = run("source_page", {})["sources"]
    assert {r["path"] for r in sources} == {
        str(originals / "a.jsonl"),
        str(nested / "c.jsonl"),
        str(originals / "0-new.jsonl"),
    }
    assert all(r["processed_bytes"] == 0 for r in sources)
    for path in (root / "control").iterdir():
        if path.is_file():
            assert path.stat().st_size <= 4 * 1024**2
            assert b"DISCOVERY_SELECTED_NATIVE" not in path.read_bytes()

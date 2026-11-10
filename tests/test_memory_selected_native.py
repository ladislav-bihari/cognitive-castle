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

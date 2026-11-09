"""Pure boundary tests; SQLite and native runtimes must stay out of the host."""

import copy
import sys

import pytest


def test_cursor_rejects_rebinding_phase_and_noninteger_offsets():
    from cognitive_castle.sqlite_export_codec import initial_cursor, validate_cursor

    cursor = initial_cursor("a" * 64, "store")
    assert validate_cursor(cursor, "a" * 64, "store") == cursor
    for change in (
        {"source_binding": "b" * 64},
        {"phase": "sql"},
        {"entry": True},
        {"field_byte_offset": -1},
        {"owner": "invented"},
        {"key": ["x"]},
    ):
        bad = {**cursor, **change}
        with pytest.raises(ValueError):
            validate_cursor(bad, "a" * 64, "store")
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)


def test_registered_descriptor_binding_covers_immutable_facts():
    from cognitive_castle.sqlite_export_codec import source_binding, validate_descriptor

    ident = dict(
        device=1, inode=2, size=3, mtime_ns=4, ctime_ns=5, mode=0o100600, uid=1000, links=1
    )
    descriptor = dict(
        format="protected-sqlite-export-v1",
        store_id="store",
        snapshot_id="snap",
        path="/private/protected/source.sqlite3",
        identity=ident,
        manifest_path="/private/protected/manifest.json",
        manifest_identity=ident,
        manifest_hash="a" * 64,
    )
    assert validate_descriptor(descriptor) == descriptor
    original = source_binding(descriptor)
    changed = copy.deepcopy(descriptor)
    changed["identity"]["inode"] = 9
    assert source_binding(changed) != original
    for key in ("path", "identity", "manifest_hash"):
        bad = {**descriptor, key: None}
        with pytest.raises(ValueError):
            validate_descriptor(bad)


def test_ascii_transport_limit_is_checked_beyond_utf8():
    from cognitive_castle.sqlite_export_codec import frame_bytes

    assert len(frame_bytes({"value": "\x00" * 100})) > 600
    with pytest.raises(ValueError):
        frame_bytes({"value": "\x00" * 90000})


def test_export_rejects_unregistered_descriptor_without_loading_storage():
    from cognitive_castle.sqlite_source_runtime import export_slice

    with pytest.raises(ValueError):
        export_slice({"path": "/tmp/arbitrary.sqlite3"}, None)
    assert not {"sqlite3", "lance", "pyarrow"}.intersection(sys.modules)


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_supervisor_refusal_retains_source_cursor_and_fixed_profile(monkeypatch):
    import os
    from cognitive_castle import memory_runtime
    from cognitive_castle.sqlite_source_runtime import export_slice
    from cognitive_castle.sqlite_export_codec import FORMAT, initial_cursor, source_binding

    identity = dict(
        device=1, inode=2, size=3, mtime_ns=4, ctime_ns=5, mode=0o100600, uid=1000, links=1
    )
    descriptor = dict(
        format=FORMAT,
        store_id="store",
        snapshot_id="snap",
        path="/private/snapshot.db",
        identity=identity,
        manifest_path="/private/manifest.json",
        manifest_identity=identity,
        manifest_hash="a" * 64,
    )
    cursor = initial_cursor(source_binding(descriptor), "store")
    monkeypatch.setattr(memory_runtime, "interactive_waiting", lambda: False)
    monkeypatch.setattr(memory_runtime, "_admission", lambda: os.open("/dev/null", os.O_RDONLY))

    def refused(request, admission, **limits):
        assert limits["deadline"] == 2 and limits["rss_limit"] == 128 * 1024**2
        assert limits["total_rss_limit"] == 192 * 1024**2 and limits["background_preempt"]
        return dict(status="unavailable", reason="worker_memory_limit")

    monkeypatch.setattr(memory_runtime, "_supervise", refused)
    response = export_slice(descriptor, cursor)
    assert response["status"] == "unavailable" and response["reason"] == "worker_memory_limit"
    assert response["cursor_before"] == response["cursor_after"] == cursor
    assert response["entries"] == [] and response["finalizations"] == []


@pytest.mark.parametrize("boundary", ["owner", "done"])
@pytest.mark.parametrize("failure", ["refusal", "value", "memory", "sqlite", "os", "post_identity"])
def test_postcheck_refusals_clear_completion_and_progress_without_storage(boundary, failure):
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    from cognitive_castle.sqlite_export_codec import (
        empty_counts,
        initial_cursor,
        validate_cursor,
        frame_bytes,
    )

    # Execute the actual adapter control flow without importing its SQLite-only
    # module. Fake source boundaries inject only the required postcheck failure.
    module = ast.parse(
        (
            Path(__file__).resolve().parents[1] / "cognitive_castle" / "sqlite_source_export.py"
        ).read_text()
    )
    selected = [
        node
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.ClassDef))
        and node.name in {"Refusal", "export_slice", "_normalize_refusal"}
    ]

    class SqliteFailure(Exception):
        sqlite_errorcode = 9

    class Corrupt(Exception):
        pass

    namespace = dict(
        empty_counts=empty_counts,
        initial_cursor=initial_cursor,
        validate_cursor=validate_cursor,
        frame_bytes=frame_bytes,
        sqlite3=SimpleNamespace(Error=SqliteFailure),
        _Corrupt=Corrupt,
        _HistoryError=Corrupt,
    )
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), "actual_export_control_flow", "exec"),
        namespace,
    )
    refusal = namespace["Refusal"]
    errors = dict(
        refusal=refusal("qualification_required", "summary_slice_deadline"),
        value=ValueError(),
        memory=MemoryError(),
        sqlite=SqliteFailure(),
        os=OSError(),
    )

    class Source:
        def __init__(self, descriptor):
            self.binding = "a" * 64
            self.conn = SimpleNamespace(close=lambda: None)
            self.tables = set()
            self.active_field = "descriptor"
            self.active_operation = "postcheck"

        def open(self):
            pass

        def validate_files(self):
            if failure != "post_identity":
                raise errors[failure]

        def validate_identities(self):
            if failure == "post_identity":
                raise refusal("stale", "identity_changed")

    def finalize(source, result, cursor, phase, key):
        result.update(
            phase_complete=True,
            entries=[{"staged": True}],
            finalizations=[{"owner": True}],
            cursor_after={"advanced": True},
        )
        result["counts"].update(live_owners=1, members=9)

    namespace.update(
        Source=Source,
        next_unit=lambda source, cursor: (
            ("done", None) if boundary == "done" else ("live", ["ns", "key"])
        ),
        _export_owner=finalize,
    )
    cursor = initial_cursor("a" * 64, "store")
    result = namespace["export_slice"]({"store_id": "store"}, cursor)
    assert result["status"] != "ok"
    assert result["complete"] is False and result["phase_complete"] is False
    assert result["cursor_before"] == result["cursor_after"] == cursor
    assert result["entries"] == [] and result["finalizations"] == []
    assert result["counts"] == empty_counts()
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)

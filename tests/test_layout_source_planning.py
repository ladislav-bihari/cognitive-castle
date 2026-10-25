"""Pure layout-source contracts and extracted control flow; no engine imports."""

import copy
import importlib
import sys

import pytest


def request_case(backend):
    if backend == "sqlite":
        from test_migration_codec import descriptor
    else:
        from test_native_export_codec import descriptor
    runtime = importlib.import_module("cognitive_castle." + backend + "_source_runtime")
    original = runtime.source_request(descriptor())
    window = dict(
        format="lance-layout-source-request-v1",
        action="plan",
        generation=dict(path="/private/generation", device=1, inode=2),
        qualification_hash="a" * 64,
        allowed_plan_hashes=[],
        packing="bulk-v1-ascii-layout-1",
    )
    return runtime, original, window


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_request_is_explicit_and_preserves_original_cursor(backend):
    runtime, original, window = request_case(backend)
    before = copy.deepcopy(original)
    request = runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    assert request == {**original, "layout": window}
    assert original == before and original["cursor"]["packing"] == "bulk-v1-ascii-1"
    assert runtime.source_request(original["descriptor"], original["cursor"]) == original
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize(
    "change",
    [
        dict(action="other"),
        dict(text="BODY"),
        dict(allowed_plan_hashes=["b" * 64]),
        dict(packing="bulk-v1-ascii-1"),
    ],
)
def test_layout_request_refuses_mixed_or_body_bearing_window(backend, change):
    runtime, original, window = request_case(backend)
    with pytest.raises(ValueError):
        runtime.source_request(
            original["descriptor"], original["cursor"], layout={**window, **change}
        )


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_guard_refusal_keeps_reason_and_clears_all_progress(backend):
    runtime, original, window = request_case(backend)
    request = {**original, "layout": window}
    result = runtime.source_result(request, dict(status="unavailable", reason="worker_deadline"))
    assert result == dict(
        format="lance-layout-source-result-v1",
        status="unavailable",
        reason="worker_deadline",
        action="plan",
        source_binding=original["cursor"]["source_binding"],
        cursor_before=original["cursor"],
        cursor_after=original["cursor"],
        plans=[],
        export=None,
    )
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_guard_normalization_matches_closed_text_limits(backend):
    from cognitive_castle.lance_layout_codec import validate_layout_source_result

    runtime, original, window = request_case(backend)
    request = {**original, "layout": window}
    reason = "r" * 1024
    result = runtime.source_result(request, dict(status="unavailable", reason=reason))
    assert result["reason"] == reason
    validate_layout_source_result(result, window, original["descriptor"], original["cursor"])
    for status in ("", "s" * 65, False):
        result = runtime.source_result(request, dict(status=status, reason="worker_deadline"))
        assert result["status"] == "unavailable" and result["reason"] == "worker_deadline"
        validate_layout_source_result(result, window, original["descriptor"], original["cursor"])
    for reason in ("", "r" * 1025, "\ud800"):
        result = runtime.source_result(request, dict(status="unavailable", reason=reason))
        assert result["reason"] == "source_invalid_response"
        validate_layout_source_result(result, window, original["descriptor"], original["cursor"])


def extracted_source(backend):
    import ast
    import hashlib
    from pathlib import Path
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle import canonical_records as canonical
    from cognitive_castle.historical_export_evidence import pages, member_text
    from cognitive_castle.sqlite_export_codec import record_entry

    path = (
        Path(__file__).resolve().parents[1] / "cognitive_castle" / (backend + "_source_export.py")
    )
    tree = ast.parse(path.read_text())
    selected = [
        n
        for n in tree.body
        if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in {"Source", "Refusal"}
    ]
    namespace = dict(
        bulk=bulk,
        hashlib=hashlib,
        _hash=canonical._hash,
        _json=canonical._json,
        _string=canonical._string,
        pages=pages,
        member_text=member_text,
        record_entry=record_entry,
    )
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["Source"]


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_planning_counts_existing_summary_rows_without_second_scan(backend):
    from cognitive_castle.canonical_records import prepare_remember, _hash

    source_class = extracted_source(backend)
    source = source_class.__new__(source_class)
    prepared = prepare_remember("plan", "Ω\0" * 6000)
    records = prepared.records
    source.binding = "a" * 64
    source.check_time = lambda: None
    source.fields = lambda *args: iter(
        (name, iter((text,)))
        for name, text in (
            ("metadata", "META_SENTINEL"),
            ("columns", "COLUMNS_SENTINEL"),
            ("scopes", "null"),
            ("descriptor", "DESCRIPTOR_SENTINEL"),
        )
    )

    class Counters:
        rows = None

        def __init__(self):
            self.rows = []

        def add_entry(self, row):
            self.rows.append(copy.deepcopy(row))

    counters = Counters()
    if backend == "sqlite":
        source.small = lambda *args: dict(
            namespace=prepared.namespace, request_id=prepared.request_id, digest=prepared.digest
        )
        source.members = lambda phase, key, start_ordinal=0: enumerate(
            records[start_ordinal:], start_ordinal
        )
        key = [prepared.namespace, prepared.request_id]
        manifest = source.owner("live", key, planning=counters)
        expected = [row for row, position in source.entries("live", key, manifest)]
    else:
        source.occurrence = "b" * 64
        source.header = dict(
            text_hash=_hash("".join(r.text for r in records)),
            text_bytes=sum(len(r.text.encode()) for r in records),
            text_characters=sum(len(r.text) for r in records),
        )
        source.records = lambda: iter(records)
        manifest = source.owner(planning=counters)
        expected = [row for row, position in source.entries(manifest)]
    assert counters.rows == expected
    assert (
        len(expected)
        == sum(s["pages"] for s in manifest["fields"].values()) + 2 * manifest["members"]
    )
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


def flow_case(backend, monkeypatch, *, eof=False, post_failure=False):
    import ast
    import os
    from pathlib import Path
    from types import SimpleNamespace
    from cognitive_castle import bulk_exact_codec as bulk, migration_codec as migration
    from cognitive_castle import lance_layout_directory as directory
    from cognitive_castle import sqlite_export_codec as sqlite
    from test_partitioned_exact_store import qualification_case

    runtime, request, window = request_case(backend)
    layout, _, qualification, _ = qualification_case()
    window["qualification_hash"] = migration.hashed(qualification)
    reg = migration.registration(request["descriptor"], window["generation"]["path"])
    if backend == "sqlite":
        from test_migration_codec import exported

        golden = exported(reg)
        golden["coverage"] = "indexed-only"
        following = None
        codec = sqlite
    else:
        from cognitive_castle import native_export_codec as codec

        following = dict(fragment=0, offset=1, row_address=0)
        d = request["descriptor"]
        identity = migration.hashed(
            [d["archive_id"], d["original_snapshot_id"], d["version"], 0, 0]
        )
        entries, manifest = bulk.prepare_owner(
            identity, reg["source_binding"], [], metadata_id=identity
        )
        golden = dict(
            status="ok",
            reason=None,
            source_binding=reg["source_binding"],
            cursor_before=request["cursor"],
            cursor_after={**request["cursor"], "key": [migration._json(following)]},
            entries=entries,
            finalizations=[manifest],
            counts={**migration.empty_counts(), "historical_occurrences": 1},
            phase="history",
            phase_complete=True,
            complete=False,
            coverage="incomplete",
            frame_bytes=0,
        )
    for _ in range(3):
        golden["frame_bytes"] = len(sqlite.frame_bytes(golden))
    path = (
        Path(__file__).resolve().parents[1] / "cognitive_castle" / (backend + "_source_export.py")
    )
    selected = [
        n
        for n in ast.parse(path.read_text()).body
        if isinstance(n, (ast.FunctionDef, ast.ClassDef))
        and n.name in {"Refusal", "export_slice", "_normalize_refusal", "_layout_response"}
    ]

    class SourceError(Exception):
        pass

    ns = dict(
        __package__="cognitive_castle",
        empty_counts=sqlite.empty_counts,
        initial_cursor=codec.initial_cursor,
        validate_cursor=codec.validate_cursor,
        frame_bytes=sqlite.frame_bytes,
        layout_result=sqlite.layout_result,
        _json=migration._json,
        _Corrupt=SourceError,
        _HistoryError=SourceError,
        sqlite3=SimpleNamespace(Error=SourceError),
        os=os,
    )
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), ns)
    events = []

    class Source:
        def __init__(self, descriptor):
            self.binding, self.descriptor = reg["source_binding"], descriptor
            self.opened, self.tables = False, set()
            self.active_operation, self.active_field = "fixture", None
            self.conn = (
                SimpleNamespace(close=lambda: events.append("close"))
                if backend == "sqlite"
                else None
            )
            self.reader = (
                SimpleNamespace(close=lambda: events.append("close"))
                if backend == "native"
                else None
            )
            self.following, self.raw_before = following, request["descriptor"].get("start_cursor")

        def open(self, *args):
            events.append("open")
            self.opened = True
            return not eof

        def owner(self, *args, planning):
            events.append("owner")
            for row in golden["entries"]:
                planning.add_entry(row)
            return golden["finalizations"][0]

        def check_time(self):
            pass

        def validate_files(self):
            events.append("files")
            if post_failure:
                raise ns["Refusal"]("stale", "source_changed_after_summary")

        def validate_identities(self):
            events.append("identities")

    def emit(source, result, cursor, *args, **kwargs):
        events.append("emit")
        assert kwargs.get("manifest", kwargs.get("owner")) == golden["finalizations"][0]
        result.update(copy.deepcopy(golden))

    ns.update(
        Source=Source,
        _export_owner=emit,
        next_unit=lambda source, cursor: (
            ("done", None) if eof else ("history", golden["cursor_after"]["key"])
        ),
    )
    monkeypatch.setattr(
        directory,
        "load_layout_qualification",
        lambda generation, hashed: qualification,
        raising=False,
    )
    return ns["export_slice"], request, window, events, golden, reg, qualification


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_plan_keeps_cursor_and_never_runs_export_loop(backend, monkeypatch):
    run, request, window, events, golden, reg, qualification = flow_case(backend, monkeypatch)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["export"] is None and len(result["plans"]) == 1
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]
    assert events.count("owner") == 1 and "emit" not in events
    assert result["plans"][0]["source_key"] == golden["cursor_after"]["key"]


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_plan_eof_is_original_terminal_without_outer_progress(backend, monkeypatch):
    run, request, window, events, _, reg, _ = flow_case(backend, monkeypatch, eof=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["plans"] == []
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]
    from cognitive_castle import migration_codec as migration

    assert migration.validate_export(result["export"], reg, request["cursor"])["complete"]
    assert "owner" not in events and "emit" not in events


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_layout_export_checks_admitted_plan_before_original_emit(backend, monkeypatch):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration

    run, request, window, events, golden, reg, qualification = flow_case(backend, monkeypatch)
    counters = layout.LayoutPlanningCounters(qualification)
    for row in golden["entries"]:
        counters.add_entry(row)
    expected = counters.finish(
        reg, golden["finalizations"][0], golden["phase"], golden["cursor_after"]["key"]
    )
    window.update(action="export", allowed_plan_hashes=[migration.hashed(expected)])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["plans"] == [expected]
    assert result["export"] == golden and result["cursor_after"] == golden["cursor_after"]
    assert events.count("owner") == events.count("emit") == 1 and events.index(
        "owner"
    ) < events.index("emit")


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_changed_plan_and_postcheck_never_expose_body_or_progress(backend, monkeypatch):
    run, request, window, events, _, _, _ = flow_case(backend, monkeypatch)
    window.update(action="export", allowed_plan_hashes=["f" * 64])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "conflict" and result["reason"] == "layout_owner_plan_mismatch"
    assert result["plans"] == [] and result["export"] is None and "emit" not in events
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]
    run, request, window, events, _, _, _ = flow_case(backend, monkeypatch, post_failure=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "stale" and result["reason"] == "source_changed_after_summary"
    assert result["plans"] == [] and result["export"] is None
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("failure", ["memory", "frame"])
def test_wrapper_failure_drops_tentative_plan_and_preserves_profile(backend, monkeypatch, failure):
    from cognitive_castle.sqlite_export_codec import layout_result

    run, request, window, _, _, _, _ = flow_case(backend, monkeypatch)
    calls = []

    def fail_once(*args):
        calls.append(1)
        if len(calls) == 1:
            raise MemoryError if failure == "memory" else ValueError
        return layout_result(*args)

    run.__globals__["layout_result"] = fail_once
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "qualification_required"
    expected = (
        "layout_source_frame_limit"
        if failure == "frame"
        else "sqlite_summary_limit"
        if backend == "sqlite"
        else "native_decode_summary_limit"
    )
    assert result["reason"] == expected
    assert result["plans"] == [] and result["export"] is None
    assert result["cursor_before"] == result["cursor_after"] == request["cursor"]

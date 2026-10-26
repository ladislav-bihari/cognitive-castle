"""Extracted SQLite prefix flow preserves one protected view and original exports."""

import ast
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_layout_source_planning import flow_case
from test_layout_source_packing import prefix_case


def run_case(
    monkeypatch,
    *,
    count=3,
    eof=False,
    post_failure=False,
    large=None,
    coverage=None,
    identity_failure=False,
    phase_boundary=False,
):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle import bulk_exact_codec as bulk

    run, request, window, events, _, reg, qualification = flow_case(
        "sqlite", monkeypatch, eof=eof, post_failure=post_failure
    )
    _, before, parts, _ = prefix_case("sqlite", count=count)
    assert before == request["cursor"]
    for part in parts:
        counters = layout.LayoutPlanningCounters(qualification)
        for row in part["export"]["entries"]:
            counters.add_entry(row)
        part["plan"] = counters.finish(
            reg,
            part["export"]["finalizations"][0],
            "history",
            part["export"]["cursor_after"]["key"],
        )
    ns = run.__globals__
    path = Path(__file__).resolve().parents[1] / "cognitive_castle/sqlite_source_export.py"
    helpers = [
        n
        for n in ast.parse(path.read_text()).body
        if isinstance(n, ast.FunctionDef)
        and n.name in {"_prefix_payload", "_prefix_response", "_prefix_fits"}
    ]
    ns.update(bulk=bulk)
    exec(compile(ast.Module(body=helpers, type_ignores=[]), str(path), "exec"), ns)
    original = ns["Source"]

    class Source(original):
        def __init__(self, descriptor):
            super().__init__(descriptor)
            self.index = 0
            if coverage is not None:
                self.tables.add(coverage)
                self.conn.execute = self.execute

        def execute(self, sql):
            events.append("coverage:" + sql)
            return SimpleNamespace(fetchone=lambda: (1,))

        def prefix_bounds(self, phase, key):
            events.append("preview:" + str(self.index))
            return dict(rows=257 if self.index == large else 7, decoded=0)

        def owner(self, phase, key, *, planning):
            events.append("owner:" + str(self.index))
            part = parts[self.index]
            assert key == part["plan"]["source_key"]
            for row in part["export"]["entries"]:
                planning.add_entry(row)
            return copy.deepcopy(part["export"]["finalizations"][0])

        def validate_identities(self):
            super().validate_identities()
            if identity_failure:
                raise ns["Refusal"]("stale", "source_post_slice_identity")

    def next_unit(source, cursor, *, same_phase=False):
        index = 0
        if cursor != before:
            index = next(
                i + 1 for i, p in enumerate(parts) if p["export"]["cursor_after"] == cursor
            )
        source.index = index
        events.append("next:" + str(index))
        if eof or index == len(parts):
            return "done", None
        if phase_boundary and index == 1:
            return "audit", ["history_gaps", '["later"]']
        return "history", parts[index]["plan"]["source_key"]

    def emit(source, result, cursor, phase, key, *, manifest):
        events.append("emit:" + str(source.index))
        part = parts[source.index]
        assert cursor == part["export"]["cursor_before"]
        assert phase == "history" and key == part["plan"]["source_key"]
        assert manifest == part["export"]["finalizations"][0]
        coverage = result["coverage"]
        result.update(copy.deepcopy(part["export"]))
        result["coverage"] = coverage

    ns.update(Source=Source, next_unit=next_unit, _export_owner=emit)
    window.update(format=layout.PREFIX_SOURCE_REQUEST, action="prepare")
    return run, request, window, events, parts, migration


@pytest.mark.parametrize("count", [1, 3, 9])
@pytest.mark.parametrize("action", ["prepare", "export"])
def test_sqlite_prefix_uses_one_connection_and_exact_original_boundaries(
    monkeypatch, count, action
):
    run, request, window, events, parts, migration = run_case(monkeypatch, count=count)
    selected = parts[:8]
    if action == "export":
        window.update(
            action=action, allowed_plan_hashes=[migration.hashed(p["plan"]) for p in selected]
        )
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok"
    assert result["prefix"]["components"] == selected
    assert result["terminal"] is result["fallback_plan"] is None
    assert (
        events.count("open")
        == events.count("files")
        == events.count("close")
        == events.count("identities")
        == 1
    )
    assert "next:8" not in events and "owner:8" not in events
    assert result["cursor_after"] == (
        request["cursor"] if action == "prepare" else selected[-1]["export"]["cursor_after"]
    )


@pytest.mark.parametrize("action", ["prepare", "export"])
def test_sqlite_prefix_eof_preserves_original_terminal_contract(monkeypatch, action):
    run, request, window, events, parts, migration = run_case(monkeypatch, eof=True)
    if action == "export":
        window.update(action=action, allowed_plan_hashes=[migration.hashed(parts[0]["plan"])])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["cursor_after"] == request["cursor"]
    assert result["prefix"] is result["fallback_plan"] is None
    assert not any(e.startswith("owner:") for e in events)
    assert (
        (result["status"] == "ok" and result["terminal"]["complete"])
        if action == "prepare"
        else (result["status"] != "ok" and result["terminal"] is None)
    )


def test_sqlite_frozen_plan_is_checked_before_body(monkeypatch):
    run, request, window, events, _, _ = run_case(monkeypatch)
    window.update(action="export", allowed_plan_hashes=["f" * 64])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "conflict" and not any(e.startswith("emit:") for e in events)
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_after"] == request["cursor"]


def test_sqlite_postcheck_discards_whole_prefix(monkeypatch):
    run, request, window, events, _, _ = run_case(monkeypatch, post_failure=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "stale" and "emit:0" in events
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_after"] == request["cursor"]


@pytest.mark.parametrize("index", [0, 1])
def test_sqlite_large_owner_falls_back_or_stops_before_summary(monkeypatch, index):
    run, request, window, events, parts, _ = run_case(monkeypatch, large=index)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["cursor_after"] == request["cursor"]
    if index == 0:
        assert result["fallback_plan"] == parts[0]["plan"] and "emit:0" not in events
    else:
        assert result["prefix"]["components"] == parts[:1] and "owner:1" not in events


@pytest.mark.parametrize("coverage", ["history_gaps", "history_staged_rows"])
def test_sqlite_coverage_is_read_once_and_bound_into_every_original_export(monkeypatch, coverage):
    run, request, window, events, _, _ = run_case(monkeypatch, coverage=coverage)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok"
    assert len([e for e in events if e.startswith("coverage:")]) == 1
    assert all(p["export"]["coverage"] == "incomplete" for p in result["prefix"]["components"])


def test_sqlite_prefix_stops_before_next_phase(monkeypatch):
    run, request, window, events, parts, _ = run_case(monkeypatch, phase_boundary=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["prefix"]["components"] == parts[:1]
    assert "owner:1" not in events


def test_sqlite_close_identity_failure_clears_all_payloads(monkeypatch):
    run, request, window, events, _, _ = run_case(monkeypatch, identity_failure=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "stale" and result["cursor_after"] == request["cursor"]
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert events[-2:] == ["close", "identities"]


def test_same_phase_successor_does_not_start_other_phase_or_orphan_scan():
    path = Path(__file__).resolve().parents[1] / "cognitive_castle/sqlite_source_export.py"
    nodes = [
        n
        for n in ast.parse(path.read_text()).body
        if isinstance(n, ast.FunctionDef) and n.name == "next_unit"
    ]
    ns = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), ns)
    events = []
    source = SimpleNamespace(
        check_time=lambda: events.append("time"),
        tables={"receipts"},
        keys=lambda table, key: events.append(table),
    )
    assert ns["next_unit"](
        source, dict(phase="live", key=["ns", "last"], owner=None), same_phase=True
    ) == ("live", None)
    assert events == ["time", "receipts"]

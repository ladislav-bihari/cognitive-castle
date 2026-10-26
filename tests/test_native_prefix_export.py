"""Extracted native prefix flow; no database, engine or model imports."""

import ast
import copy
from pathlib import Path

import pytest

from test_layout_source_planning import flow_case
from test_layout_source_packing import prefix_case


def run_case(monkeypatch, *, count=3, eof=False, post_failure=False, damage=None):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle import bulk_exact_codec as bulk

    run, request, window, events, _, reg, qualification = flow_case(
        "native", monkeypatch, eof=eof, post_failure=post_failure
    )
    _, before, components, _ = prefix_case("native", count=count)
    assert before == request["cursor"]
    for part in components:
        planning = layout.LayoutPlanningCounters(qualification)
        for row in part["export"]["entries"]:
            planning.add_entry(row)
        part["plan"] = planning.finish(
            reg,
            part["export"]["finalizations"][0],
            "history",
            part["export"]["cursor_after"]["key"],
        )
    ns = run.__globals__
    path = Path(__file__).resolve().parents[1] / "cognitive_castle/native_source_export.py"
    helpers = [
        node
        for node in ast.parse(path.read_text()).body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_prefix_payload", "_prefix_response", "_prefix_fits"}
    ]
    ns.update(
        bulk=bulk, PROJECTION_CHARACTERS=8192, MAX_PROJECTIONS=64, MAX_PROJECTED_BYTES=2 * 1024**2
    )
    exec(compile(ast.Module(body=helpers, type_ignores=[]), str(path), "exec"), ns)
    original = ns["Source"]

    class Source(original):
        def __init__(self, descriptor):
            super().__init__(descriptor)
            self.index = 0
            self.projections = self.projected_bytes = 0
            self.select()

        def select(self):
            self.header = dict(text_characters=0, text_bytes=0)
            self.metadata = self.columns = "null"
            if self.index < len(components):
                self.following = __import__("json").loads(
                    components[self.index]["plan"]["source_key"][0]
                )
            if damage and self.index == damage[0]:
                field, value = damage[1:]
                if field in self.header:
                    self.header[field] = value
                else:
                    setattr(self, field, value)

        def owner(self, *, planning):
            events.append("owner:" + str(self.index))
            for row in components[self.index]["export"]["entries"]:
                planning.add_entry(row)
            return copy.deepcopy(components[self.index]["export"]["finalizations"][0])

        def advance(self, cursor):
            events.append("advance:" + str(self.index))
            assert cursor == components[self.index]["export"]["cursor_after"]
            self.index += 1
            self.select()
            return self.index < len(components)

    def emit(source, result, cursor, *, owner):
        events.append("emit:" + str(source.index))
        part = components[source.index]
        assert cursor == part["export"]["cursor_before"]
        assert owner == part["export"]["finalizations"][0]
        result.update(copy.deepcopy(part["export"]))

    ns.update(Source=Source, _export_owner=emit)
    window.update(format=layout.PREFIX_SOURCE_REQUEST, action="prepare")
    return run, request, window, events, components, migration


@pytest.mark.parametrize("count", [1, 3, 9])
@pytest.mark.parametrize("action", ["prepare", "export"])
def test_one_source_view_preserves_bounded_original_prefix(monkeypatch, count, action):
    run, request, window, events, parts, migration = run_case(monkeypatch, count=count)
    selected = parts[:8]
    if action == "export":
        window.update(
            action=action, allowed_plan_hashes=[migration.hashed(p["plan"]) for p in selected]
        )
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok"
    assert result["prefix"]["components"] == selected
    assert result["fallback_plan"] is result["terminal"] is None
    assert events.count("open") == events.count("files") == events.count("close") == 1
    assert len([e for e in events if e.startswith("emit:")]) == min(count, 8)
    assert "owner:8" not in events and "advance:7" not in events
    assert result["cursor_after"] == (
        request["cursor"] if action == "prepare" else selected[-1]["export"]["cursor_after"]
    )


@pytest.mark.parametrize("action", ["prepare", "export"])
def test_prefix_eof_retains_original_terminal_only_for_prepare(monkeypatch, action):
    run, request, window, events, parts, migration = run_case(monkeypatch, eof=True)
    if action == "export":
        window.update(action=action, allowed_plan_hashes=[migration.hashed(parts[0]["plan"])])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["cursor_after"] == request["cursor"]
    assert result["prefix"] is result["fallback_plan"] is None
    if action == "prepare":
        assert result["status"] == "ok" and result["terminal"]["complete"]
    else:
        assert result["status"] != "ok" and result["terminal"] is None
    assert events == ["open", "files", "close"]


def test_prefix_frozen_plan_disagreement_precedes_body_emission(monkeypatch):
    run, request, window, events, _, _ = run_case(monkeypatch)
    window.update(action="export", allowed_plan_hashes=["f" * 64])
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "conflict"
    assert not any(e.startswith("emit:") for e in events)
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_after"] == request["cursor"]


def test_postcheck_discards_the_entire_transient_prefix(monkeypatch):
    run, request, window, events, _, _ = run_case(monkeypatch, post_failure=True)
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert "emit:0" in events
    assert result["status"] == "stale" and result["reason"] == "source_changed_after_summary"
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_after"] == request["cursor"] and events[-1] == "close"


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize(
    "field,value",
    [("text_characters", 17 * 8192), ("text_bytes", 600000), ("metadata", "x" * 400000)],
)
def test_large_owner_falls_back_or_ends_prefix_without_starving(monkeypatch, index, field, value):
    run, request, window, events, parts, _ = run_case(monkeypatch, damage=(index, field, value))
    result = run(request["descriptor"], request["cursor"], layout=window)
    assert result["status"] == "ok" and result["cursor_after"] == request["cursor"]
    if index == 0:
        assert result["fallback_plan"] == parts[0]["plan"] and result["prefix"] is None
        assert "emit:0" not in events
    else:
        assert result["prefix"]["components"] == parts[:1]
        assert "owner:1" not in events and "emit:1" not in events
    assert events.count("open") == events.count("files") == events.count("close") == 1

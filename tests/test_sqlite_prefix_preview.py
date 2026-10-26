"""Scalar-only bounded SQLite owner preview; no connection is opened here."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_layout_source_planning import extracted_source


def source_case(*, phase="live", scalar=None, ids=None, relational=None, indexed=True):
    cls = extracted_source("sqlite")
    source = cls.__new__(cls)
    source.check_time = lambda: None
    source.tables = {
        "chunks",
        "receipts",
        "history_occurrences",
        "history_occurrence_chunks",
        "history_aliases",
    }
    tree = ast.parse(
        (
            Path(__file__).resolve().parents[1] / "cognitive_castle/sqlite_source_export.py"
        ).read_text()
    )
    tables = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "TABLES" for t in n.targets)
    )
    cls.prefix_bounds.__globals__["TABLES"] = ast.literal_eval(tables.value)
    cls.prefix_bounds.__globals__["json"] = json
    calls = []
    scalar = scalar if scalar is not None else (160, 23)
    ids = (
        ids
        if ids is not None
        else [(b"memory:" + b"a" * 64, "text"), (b"memory:" + b"b" * 64, "text")]
    )
    relational = relational if relational is not None else []

    class Rows:
        def __init__(self, rows):
            self.rows = rows

        def fetchone(self):
            return self.rows[0] if self.rows else None

        def __iter__(self):
            return iter(self.rows)

    def execute(sql, args=()):
        calls.append((sql, tuple(args)))
        assert "count(" not in sql.lower() and "sum(" not in sql.lower()
        if sql.startswith("EXPLAIN"):
            if indexed == "skip_scan":
                return Rows(
                    [
                        (
                            0,
                            0,
                            0,
                            "SEARCH history_aliases USING INDEX alias_idx (ANY(alias) AND identity=?)",
                        )
                    ]
                )
            return Rows(
                [
                    (
                        0,
                        0,
                        0,
                        "SEARCH history_aliases USING INDEX identity_idx (identity=?)"
                        if indexed
                        else "SCAN history_aliases",
                    )
                ]
            )
        if "FROM chunks" in sql:
            assert "length(" in sql and "text,provenance" not in sql
            return Rows([(12, 34)])
        if "json_each" in sql:
            assert "LIMIT 129" in sql
            return Rows(ids)
        if "FROM history_occurrence_chunks" in sql:
            assert "LIMIT 129" in sql and "ORDER BY chunk_id" in sql
            return Rows(relational)
        if "FROM history_aliases" in sql:
            assert "length(" in sql and "LIMIT 129" in sql
            return Rows([(7,), (9,)])
        return Rows([scalar])

    source.conn = SimpleNamespace(execute=execute)
    key = ["ns", "request"] if phase == "live" else ["a" * 64]
    if phase == "audit":
        key = ["history_gaps", '["original"]']
    return source, key, calls


def test_preview_reads_only_bounded_ids_and_scalar_lengths():
    source, key, calls = source_case()
    bounds = source.prefix_bounds("live", key)
    assert bounds == dict(rows=9, decoded=23 + 2 * (12 + 34))
    assert len(calls) == 4
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(__import__("sys").modules)


def test_oversized_original_id_array_stops_before_json_parsing():
    source, key, calls = source_case(scalar=(1000000, 23))
    assert source.prefix_bounds("live", key)["rows"] > 256
    assert len(calls) == 1 and not any("json_each" in sql for sql, _ in calls)


@pytest.mark.parametrize("kind", ["json", "relational"])
def test_129th_original_member_ends_preview_without_full_count(kind):
    ids = [(b"memory:" + b"a" * 64, "text")] * 129
    source, key, calls = source_case(
        phase="history",
        scalar=(2, 23),
        ids=[] if kind == "relational" else ids,
        relational=[(b"memory:" + b"b" * 64,)] * 129 if kind == "relational" else [],
    )
    assert source.prefix_bounds("history", key)["rows"] > 256
    assert not any("FROM chunks" in sql for sql, _ in calls)


def test_unindexed_aliases_fall_back_before_scanning_later_owner():
    source, key, calls = source_case(phase="history", indexed=False)
    assert source.prefix_bounds("history", key)["rows"] > 256
    assert not any(
        "FROM history_aliases" in sql and not sql.startswith("EXPLAIN") for sql, _ in calls
    )


def test_audit_preview_keeps_original_row_without_member_scan():
    source, key, calls = source_case(phase="audit", scalar=(1, 2, 3))
    assert source.prefix_bounds("audit", key) == dict(rows=1, decoded=6)
    assert len(calls) == 1 and calls[0][1] == ("original",)


def test_skip_scan_is_not_a_bounded_identity_lookup():
    source, key, calls = source_case(phase="history", indexed="skip_scan")
    assert source.prefix_bounds("history", key)["rows"] > 256
    assert not any(
        "FROM history_aliases" in sql and not sql.startswith("EXPLAIN") for sql, _ in calls
    )


def test_indexed_alias_preview_does_not_sort_unbounded_matches():
    source, key, calls = source_case(phase="history")
    assert source.prefix_bounds("history", key) == dict(rows=9, decoded=131)
    query = next(
        sql for sql, _ in calls if "FROM history_aliases" in sql and not sql.startswith("EXPLAIN")
    )
    assert "ORDER BY" not in query and "LIMIT 129" in query

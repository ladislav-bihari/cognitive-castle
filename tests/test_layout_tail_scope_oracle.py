"""Independent typed-JSON arithmetic for the unchanged 520-chunk tail source."""

import json
import sys
from types import SimpleNamespace

from test_layout_source_planning import extracted_source


def test_tail_scope_preserves_original_json_text_and_requires_seventeen_pages():
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.historical_export_evidence import historical_scope_header, pages

    # Same original source facts as _short_source_worker; no DB or native access.
    identity = _hash(_json(["tail-archive", "tail-original", 1, 0, 0]))
    caller = _json(dict(archive_id="tail-archive", version=1, metadata_id=identity))
    ids = ["memory:" + _hash(_json([identity, ordinal])) for ordinal in range(520)]
    values = {"columns_json": '{"id":"tail"}', "metadata_json": "{}", "provenance": caller}
    source_class = extracted_source("sqlite")
    source = source_class.__new__(source_class)
    source.scopes.__globals__["historical_scope_header"] = historical_scope_header
    source.tables = set()
    source.member_ids = lambda phase, key: iter(enumerate(ids))
    source.raw = lambda table, field, where, args: iter([values[field]])

    def execute(sql, args):
        if sql.startswith("SELECT json_valid("):
            result = (1, "object")
        elif sql.startswith("SELECT json_type("):
            result = (None,)
        elif sql.startswith("SELECT typeof(("):
            field = sql.split("((", 1)[1].split(")", 1)[0]
            result = ("text", len(values[field].encode()))
        else:
            raise AssertionError(sql)
        return SimpleNamespace(fetchone=lambda: result)

    source.conn = SimpleNamespace(execute=execute)
    actual_pages = list(pages(source.scopes("history", [identity])))
    actual = "".join(actual_pages)

    # Build the meaning independently, retaining SQL TEXT as a JSON string.
    expected = dict(
        format="sqlite-source-scopes-v1",
        precedence=["columns", "metadata", "caller"],
        origin="history",
        columns=values["columns_json"],
        metadata=values["metadata_json"],
        selection={
            key: dict(columns=None, metadata=None, selected_origin="caller")
            for key in ("workspace", "wing", "room")
        },
        chunks=[
            dict(id=chunk, caller=caller, capture_mapping=dict(present=False)) for chunk in ids
        ],
    )
    original_typed_text = json.dumps(expected, separators=(",", ":"), ensure_ascii=False)
    assert actual == original_typed_text
    assert len(actual_pages) == 17
    assert len(actual.encode()) > 16 * 8192
    assert all(len(page.encode()) <= 8192 for page in actual_pages)
    assert json.loads(actual)["chunks"][-1]["caller"] == caller
    assert isinstance(json.loads(actual)["columns"], str)

    # Only a lossy TEXT-to-object conversion could justify the old16-page oracle.
    for chunk in expected["chunks"]:
        chunk["caller"] = json.loads(chunk["caller"])
    changed = json.dumps(expected, separators=(",", ":"), ensure_ascii=False)
    assert len(changed.encode()) <= 16 * 8192
    assert changed != actual
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)

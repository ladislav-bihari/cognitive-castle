"""One source view preserves original closed cursors and cumulative budgets."""

import ast
import copy
import json
from pathlib import Path

import pytest

from test_layout_source_planning import extracted_source
from test_native_export_codec import descriptor


def source_case():
    from cognitive_castle import native_export_codec as codec
    from cognitive_castle.canonical_records import _hash, _json

    Source = extracted_source("native")
    namespace = Source.open.__globals__
    source_file = Path(__file__).parents[1] / "cognitive_castle/native_source_export.py"
    nodes = [
        node
        for node in ast.parse(source_file.read_text()).body
        if isinstance(node, ast.FunctionDef) and node.name in {"_kind", "_invalid_json_constant"}
    ]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source_file), "exec"), namespace)
    namespace.update(json=json, validate_cursor=codec.validate_cursor)
    events, readers = [], []
    headers = [
        dict(
            fragment=0,
            offset=index + 1,
            row_address=index,
            values=dict(
                id=str(index),
                metadata_json=' { "workspace":false,"wing":null,"room":0 }\n',
                workspace=0,
                wing=False,
                room="",
            ),
        )
        for index in range(2)
    ]

    class Reader:
        def __init__(self, path, version, before):
            readers.append(self)
            self.cursor, self.index = dict(before), 0
            self.dataset = object()
            self.advances = 0
            self.header_lookups = 0

        def header_or_none(self):
            self.header_lookups += 1
            return headers[self.index] if self.index < len(headers) else None

        def advance(self):
            self.advances += 1
            self.cursor = {
                name: headers[self.index][name] for name in ("fragment", "offset", "row_address")
            }
            self.index += 1
            return self.header_or_none()

    def stage_header(header, job, before):
        following = {name: header[name] for name in ("fragment", "offset", "row_address")}
        identity = _hash(
            _json(
                [
                    job["archive_id"],
                    job["snapshot_id"],
                    job["version"],
                    header["fragment"],
                    header["row_address"],
                ]
            )
        )
        return following, identity, header["values"]["metadata_json"], _json(header["values"])

    namespace.update(
        NativeProjectedRow=Reader,
        _stage_header=stage_header,
        _stage_caller=lambda job, value: (value["identity"], dict(original=value["metadata_json"])),
    )
    source = Source.__new__(Source)
    source.descriptor = descriptor()
    source.binding = codec.source_binding(source.descriptor)
    source.reader = source.header = None
    source.until, source.projections, source.projected_bytes = 12345, 5, 18
    source.check_time = lambda: events.append("time")
    source.validate_files = lambda: events.append("files")
    source.validate_native = lambda: events.append("native")
    before = codec.initial_cursor(
        source.binding, source.descriptor["store_id"], source.descriptor["start_cursor"]
    )
    assert source.open(before)
    return source, before, events, readers, namespace["Refusal"], _json


def closed_cursor(source, before):
    return dict(before, key=[source.open.__globals__["_json"](source.following)])


def test_source_advances_one_view_without_resetting_cumulative_budgets():
    source, before, _, readers, _, _ = source_case()
    reader = source.reader
    first_following = copy.deepcopy(source.following)
    cursor = closed_cursor(source, before)
    assert source.advance(cursor)
    assert source.reader is reader and len(readers) == 1
    assert source.raw_before == first_following
    assert source.following == dict(fragment=0, offset=2, row_address=1)
    assert source.header["values"]["id"] == "1"
    assert source.metadata == ' { "workspace":false,"wing":null,"room":0 }\n'
    assert source.kinds == dict(
        workspace=("integer", "false"), wing=("false", "null"), room=("text", "integer")
    )
    assert (source.until, source.projections, source.projected_bytes) == (12345, 5, 18)
    assert not source.advance(closed_cursor(source, before))
    assert source.raw_before == dict(fragment=0, offset=2, row_address=1)
    assert len(readers) == 1
    assert reader.header_lookups == 3  # Initial row, next row, and one EOF read.


@pytest.mark.parametrize("damage", ["key", "partial", "phase", "reader"])
def test_source_refuses_nonboundary_cursor_before_native_advance(damage):
    source, before, _, readers, Refusal, _json = source_case()
    cursor = closed_cursor(source, before)
    if damage == "key":
        cursor["key"] = [_json(dict(fragment=2, offset=1, row_address=2 << 32))]
    elif damage == "partial":
        cursor.update(owner=source.occurrence, entry=1, field="record")
    elif damage == "phase":
        cursor["phase"] = "done"
    else:
        source.reader = None
    with pytest.raises((ValueError, Refusal)):
        source.advance(cursor)
    assert readers[0].advances == 0


def test_source_deadline_refuses_before_advancing_or_resetting_budget():
    source, before, _, readers, Refusal, _ = source_case()

    def elapsed():
        raise Refusal("qualification_required", "native_summary_slice_deadline")

    source.check_time = elapsed
    with pytest.raises(Refusal) as exc:
        source.advance(closed_cursor(source, before))
    assert exc.value.reason == "native_summary_slice_deadline"
    assert readers[0].advances == 0
    assert (source.until, source.projections, source.projected_bytes) == (12345, 5, 18)


def test_source_refuses_reader_cursor_that_disagrees_with_original_following():
    source, before, _, readers, Refusal, _ = source_case()
    reader, old_identity = source.reader, source.occurrence
    original = reader.advance

    def changed():
        result = original()
        reader.cursor = dict(fragment=9, offset=1, row_address=9 << 32)
        return result

    reader.advance = changed
    with pytest.raises(Refusal):
        source.advance(closed_cursor(source, before))
    assert len(readers) == 1 and source.occurrence == old_identity

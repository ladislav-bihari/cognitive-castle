"""Engine-free resume regressions; native cases use the existing guarded fixture."""

import ast
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from cognitive_castle import bulk_exact_codec as bulk
from cognitive_castle.canonical_records import CanonicalRecord, _hash, _json, _string
from cognitive_castle.historical_export_evidence import member_text, pages
from cognitive_castle.sqlite_export_codec import empty_counts, frame_bytes, initial_cursor


ROOT = Path(__file__).resolve().parents[1]


def exporter_namespace():
    """Execute the real streaming methods without importing a storage engine."""
    tree = ast.parse((ROOT / "cognitive_castle/sqlite_source_export.py").read_text())
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
        and node.name in {"Source", "Refusal", "_export_owner", "_normalize_refusal"}
    ]
    namespace = dict(
        bulk=bulk,
        CanonicalRecord=CanonicalRecord,
        _hash=_hash,
        _json=_json,
        _string=_string,
        member_text=member_text,
        pages=pages,
        initial_cursor=initial_cursor,
        frame_bytes=frame_bytes,
        empty_counts=empty_counts,
        json=json,
        hashlib=hashlib,
    )
    exec(compile(ast.Module(body=selected, type_ignores=[]), "source_resume", "exec"), namespace)
    return namespace


def sample(namespace, fields, bodies):
    records, offset = [], 0
    for ordinal, body in enumerate(bodies):
        records.append(
            CanonicalRecord.from_source(
                dict(
                    id="memory:" + _hash(str(ordinal)),
                    text=body,
                    provenance=dict(
                        caller={"note": "\x00Ω"},
                        chunk_start=offset,
                        chunk_end=offset + len(body),
                        text_hash=_hash(body),
                    ),
                )
            )
        )
        offset += len(body)
    rows, manifest = bulk.prepare_owner("a" * 64, "b" * 64, records, **fields)

    class MemorySource(namespace["Source"]):
        binding = "b" * 64
        descriptor = {"store_id": "store"}

        def __init__(self):
            self.read_fields = []
            self.hydrated = []
            self.proofs = 0

        def check_time(self):
            pass

        def fields(self, phase, key):
            def pieces(field):
                self.read_fields.append(field)
                text = fields[field]
                # Deliberately split inside both page and Unicode boundaries.
                yield text[:3]
                yield text[3:8193]
                yield text[8193:]

            for field in bulk.FIELDS:
                yield field, pieces(field)

        def members(self, phase, key, start_ordinal=0):
            for ordinal, record in enumerate(records):
                if ordinal >= start_ordinal:
                    self.hydrated.append(ordinal)
                    yield ordinal, record

        def owner(self, phase, key):
            self.proofs += 1
            return manifest

    return MemorySource(), rows, manifest


def position(row):
    header = json.loads(row["receipt"])
    if row["kind"] == "bulk_record":
        return "record", header["ordinal"], 0, 0
    field = header["field"]
    return (
        field,
        header["ordinal"] if field == "membership" else 0,
        header["byte_offset"],
        header["offset"],
    )


def cursor_at(rows, manifest, start):
    cursor = initial_cursor("b" * 64, "store")
    field, member, size, chars = (
        position(rows[start]) if start < len(rows) else ("finalization", manifest["members"], 0, 0)
    )
    cursor.update(
        phase="history",
        key=["a" * 64],
        owner=manifest["owner_key"],
        entry=start,
        field=field,
        member_ordinal=member,
        field_byte_offset=size,
        field_char_offset=chars,
    )
    return cursor


def result_at(cursor):
    return dict(
        entries=[],
        finalizations=[],
        counts=empty_counts(),
        phase_complete=False,
        cursor_before=cursor,
        cursor_after=dict(cursor),
    )


CASES = [
    ({"metadata": "", "columns": "", "scopes": "", "descriptor": ""}, []),
    (
        {
            "metadata": "a" * 8191 + "Ω\x00🙂",
            "columns": "{}",
            "scopes": "Ω" * 5000,
            "descriptor": " descriptor\n",
        },
        ["first\x00Ω", "second🙂"],
    ),
]


@pytest.mark.parametrize("fields,bodies", CASES)
def test_seek_bytes_positions_and_empty_finalization_match_full_stream(fields, bodies):
    namespace = exporter_namespace()
    source, rows, manifest = sample(namespace, fields, bodies)
    assert list(source.entries("history", ["a" * 64], manifest)) == [
        (row, position(row)) for row in rows
    ]
    for start in range(len(rows) + 2):
        actual = list(source.entries("history", ["a" * 64], manifest, start_entry=start))
        assert (
            json.dumps(actual).encode()
            == json.dumps([(r, position(r)) for r in rows[start:]]).encode()
        )
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)


def test_seek_does_not_reread_complete_fields_or_hydrate_member_prefix():
    namespace = exporter_namespace()
    source, rows, manifest = sample(namespace, CASES[1][0], CASES[1][1])
    start = len(rows) - 1  # Resume at the record half of the last pair.
    actual = list(source.entries("history", ["a" * 64], manifest, start_entry=start))
    assert actual == [(rows[-1], ("record", 1, 0, 0))]
    assert source.read_fields == []
    assert source.hydrated == [1]


def test_member_seek_checks_all_identities_but_only_hydrates_selected_suffix():
    namespace = exporter_namespace()
    source, _, _ = sample(namespace, CASES[1][0], CASES[1][1])
    records = list(source.members("history", ["a" * 64]))
    checked, hydrated = [], []

    def identities(phase, key):
        for ordinal, record in records:
            checked.append(record.id)
            yield ordinal, record.id

    def execute(sql, args):
        assert sql == "SELECT bounded_projection FROM chunks WHERE id=?"
        hydrated.append(args[0])
        row = next(record.source() for _, record in records if record.id == args[0])
        return SimpleNamespace(fetchone=lambda: row)

    namespace.update(
        CHUNK_COLUMNS="bounded_projection", MemoryStore=SimpleNamespace(_source=lambda row: row)
    )
    source.member_ids = identities
    source.conn = SimpleNamespace(execute=execute)
    actual = list(namespace["Source"].members(source, "history", ["a" * 64], start_ordinal=1))
    assert actual == records[1:]
    assert checked == [record.id for _, record in records]
    assert hydrated == [records[1][1].id]


def test_scope_serialization_reads_original_provenance_without_text_hydration():
    namespace = exporter_namespace()
    source = namespace["Source"].__new__(namespace["Source"])
    source.tables = set()
    identity = "memory:" + "a" * 64
    source.member_ids = lambda phase, key: iter([(0, identity)])

    def value(table, field, where, args):
        assert (table, field, where, args) == ("chunks", "provenance", "id=?", (identity,))
        yield '" {\\"note\\":\\"\\\\u0000Ω\\"} "'

    def no_hydration(*args, **kwargs):
        pytest.fail("scope serialization hydrated canonical text")

    source.members = no_hydration
    source.value = value
    actual = "".join(source.scopes("live", ["operator", "nine"]))
    assert actual == (
        '{"format":"sqlite-source-scopes-v1","precedence":["columns","metadata","caller"],'
        '"origin":"live","chunks":[{"id":"' + identity + '","caller":'
        '" {\\"note\\":\\"\\\\u0000Ω\\"} ","capture_mapping":{"present":false}}]}'
    )


@pytest.mark.parametrize("shape", ["unicode", "decoded", "count", "split_pair", "empty"])
def test_export_interval_bytes_match_original_packing_at_every_boundary(shape, monkeypatch):
    namespace = exporter_namespace()
    fields = dict.fromkeys(bulk.FIELDS, "")
    bodies = []
    if shape == "unicode":
        fields["metadata"] = "\x00Ω🙂" * 14000
        bodies = ["\x00Ω🙂" * 400] * 3
    elif shape == "decoded":
        fields["metadata"] = "a" * (bulk.MAX_DECODED_BYTES + 8192)
        bodies = ["last"]
    elif shape in {"count", "split_pair"}:
        bodies = ["x"] * 128  # 256 entries require separate owner finalization.
        if shape == "split_pair":
            fields["descriptor"] = "d"  # Packet ends between membership and record.
    source, rows, manifest = sample(namespace, fields, bodies)
    original_batches = bulk.entry_batches
    packing_calls = []

    def counted(entries):
        packing_calls.append(True)
        yield from original_batches(entries)

    monkeypatch.setattr(bulk, "entry_batches", counted)
    # All page/member boundaries for small inputs; every packet boundary for large ones.
    starts = {0, len(rows)}
    consumed = 0
    for batch in original_batches(iter(rows)):
        consumed += len(batch)
        starts.update((max(0, consumed - 1), consumed))
    starts.update(range(max(0, len(rows) - 4), len(rows)))
    for start in sorted(starts):
        cursor = cursor_at(rows, manifest, start)
        result = result_at(cursor)
        selected = next(original_batches(iter(rows[start:])), [])
        expected = result_at(cursor)
        expected["entries"] = selected
        after = initial_cursor(source.binding, "store")
        after.update(phase="history", key=["a" * 64])
        following = start + len(selected)
        if following < len(rows):
            after = cursor_at(rows, manifest, following)
        else:
            try:
                bulk.validate_batch("export", source.binding, selected, [manifest])
                frame_bytes({**expected, "finalizations": [manifest]})
            except ValueError:
                after = cursor_at(rows, manifest, len(rows))
            else:
                expected["finalizations"] = [manifest]
                expected["phase_complete"] = True
                expected["counts"].update(historical_occurrences=1, members=len(bodies))
        expected["cursor_after"] = after
        packing_calls.clear()
        namespace["_export_owner"](source, result, cursor, "history", ["a" * 64])
        assert frame_bytes(result) == frame_bytes(expected)
        assert len(packing_calls) == 1
    assert source.proofs == len(starts)


@pytest.mark.parametrize("bad", ["offset", "past_end"])
def test_invalid_resume_refuses_before_progress(bad):
    namespace = exporter_namespace()
    source, rows, manifest = sample(namespace, CASES[1][0], CASES[1][1])
    cursor = cursor_at(rows, manifest, len(rows) + (bad == "past_end"))
    if bad == "offset":
        cursor["field_char_offset"] = 1
    result = result_at(cursor)
    with pytest.raises(namespace["Refusal"]) as refused:
        namespace["_export_owner"](source, result, cursor, "history", ["a" * 64])
    assert refused.value.reason == (
        "cursor_past_owner" if bad == "past_end" else "cursor_position_mismatch"
    )
    assert result == result_at(cursor)
    assert source.proofs == 1


@pytest.mark.parametrize(
    "mode,phase",
    [
        ("missing", "live"),
        ("digest", "live"),
        ("offset", "live"),
        ("hash", "history"),
        ("dual", "history"),
    ],
)
@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_native_corrupt_skipped_prefix_refuses_without_advancing(tmp_path, mode, phase):
    from test_sqlite_source_export import fixture, invoke
    from cognitive_castle.sqlite_export_codec import source_binding

    data = fixture(tmp_path, mode=mode)
    cursor = initial_cursor(source_binding(data["descriptor"]), "store")
    key = ["operator", "nine"] if phase == "live" else [data["history"][0]["identity"]]
    owner = "LIVE:" + _hash(_json(["live", key])) if phase == "live" else key[0]
    cursor.update(
        phase=phase,
        key=key,
        owner=owner,
        entry=100000,
        field="finalization",
        member_ordinal=9 if phase == "live" else 1,
    )
    result = invoke(data["descriptor"], cursor)
    assert result["status"] == "corrupt", result
    assert result["refused_operation"] == "owner_summary"
    assert result["reason"] not in {"cursor_past_owner", "cursor_position_mismatch"}
    assert result["cursor_before"] == result["cursor_after"] == cursor
    assert result["entries"] == result["finalizations"] == []
    assert result["counts"] == empty_counts()
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)

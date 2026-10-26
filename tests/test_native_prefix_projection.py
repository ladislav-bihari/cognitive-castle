"""Engine-free forward selection on the actual immutable projection adapter."""

import hashlib
from types import SimpleNamespace

import pytest

from test_native_export_codec import _projection_class


def reader_case():
    Reader, Error = _projection_class()
    reader = Reader.__new__(Reader)
    reader.cursor = dict(fragment=0, offset=0)
    reader.dataset = SimpleNamespace(version=7)
    reader._version = 7
    reader.columns = ["id", "metadata_json", "wing"]
    reader.fragments = [SimpleNamespace(fragment_id=0), SimpleNamespace(fragment_id=2)]
    reader._header = reader._fragment = reader._bound_next = None
    calls = []
    records = {
        (0, 0): (0, "first", "a", False),
        (2, 0): (2 << 32, "empty", "", None),
        (2, 1): ((2 << 32) + 1, "last", "Ω\0", 0),
    }

    def scan(columns, fragment, offset):
        calls.append((fragment.fragment_id, offset, columns))
        item = records.get((fragment.fragment_id, offset))
        if item is None:
            return None
        address, alias, text, wing = item
        if columns == reader.columns:
            return dict(_rowaddr=address, id=alias, metadata_json=' { "raw":true }\n', wing=wing)
        if "page" in columns:
            return dict(_rowaddr=address, page=text)
        return dict(
            _rowaddr=address,
            bytes=len(text.encode()),
            chars=len(text),
            hash=hashlib.sha256(text.encode()).digest(),
        )

    reader._scan = scan
    return reader, Error, calls


def test_projection_advance_preserves_dataset_and_exact_original_cursor():
    reader, _, _ = reader_case()
    dataset, fragments = reader.dataset, reader.fragments
    first = reader.header()
    assert first["values"]["id"] == "first" and reader.page(0) == "a"
    second = reader.advance()
    assert reader.cursor == {key: first[key] for key in ("fragment", "offset", "row_address")}
    assert second["values"]["id"] == "empty" and reader.page(0) == ""
    assert second["fragment"] == 2 and second["offset"] == 1
    third = reader.advance()
    assert reader.cursor == {key: second[key] for key in ("fragment", "offset", "row_address")}
    assert third["values"]["id"] == "last" and reader.page(0) == "Ω\0"
    assert reader.advance() is None
    assert reader.cursor == {key: third[key] for key in ("fragment", "offset", "row_address")}
    assert reader.dataset is dataset and reader.fragments is fragments


def test_mutable_public_header_cannot_choose_a_different_next_cursor():
    reader, _, _ = reader_case()
    exposed = reader.header()
    exposed.update(fragment=2, offset=2, row_address=(2 << 32) + 1)
    following = reader.advance()
    assert reader.cursor == dict(fragment=0, offset=1, row_address=0)
    assert following["values"]["id"] == "empty"


@pytest.mark.parametrize("state", ["unread", "closed"])
def test_projection_advance_requires_an_actual_bound_header(state):
    reader, Error, calls = reader_case()
    if state == "closed":
        reader.header()
        reader.close()
    count = len(calls)
    with pytest.raises(Error) as exc:
        reader.advance()
    assert exc.value.status == "corrupt"
    assert len(calls) == count


def test_projection_advance_refuses_changed_version_without_another_scan():
    reader, Error, calls = reader_case()
    reader.header()
    before, count = dict(reader.cursor), len(calls)
    reader.dataset.version = 8
    with pytest.raises(Error) as exc:
        reader.advance()
    assert exc.value.status == "changed"
    assert reader.cursor == before and len(calls) == count


def test_projection_advance_cannot_skip_beyond_proven_eof():
    reader, Error, calls = reader_case()
    reader.header()
    reader.advance()
    reader.advance()
    assert reader.advance() is None
    before, count = dict(reader.cursor), len(calls)
    with pytest.raises(Error) as exc:
        reader.advance()
    assert exc.value.status == "corrupt"
    assert reader.cursor == before and len(calls) == count


def test_projection_advance_still_rejects_mismatched_header_statistics():
    reader, Error, _ = reader_case()
    reader.header()
    original = reader._scan

    def scan(columns, fragment, offset):
        result = original(columns, fragment, offset)
        if result is not None and "hash" in columns:
            result["_rowaddr"] += 1
        return result

    reader._scan = scan
    with pytest.raises(Error) as exc:
        reader.advance()
    assert exc.value.status == "changed"

"""One fixed protected-source audit case; pytest host loads no storage engine."""

import json
import sys

import test_layout_native_integration as native
from test_layout_native_tail import _tree
from test_sqlite_source_export import fixture, invoke

pytestmark = native.pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def stage(operation, result, call):
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation=operation,
                status=result["status"],
                reason=result.get("reason"),
                resources=[result["resources"]],
            )
        ),
        flush=True,
    )


def journal_cursor(descriptor, request_key):
    from cognitive_castle.sqlite_export_codec import initial_cursor, source_binding
    from cognitive_castle.canonical_records import _hash, _json

    cursor = initial_cursor(source_binding(descriptor), descriptor["store_id"])
    position = ["capture_publication_intents", _json([request_key])]
    cursor.update(
        phase="audit",
        key=position,
        owner="AUDIT:" + _hash(_json(["audit", position])),
        field="descriptor",
    )
    return cursor


def test_protected_source_preserves_fair_capture_journal_row(tmp_path):
    start = native._CALLS
    call = native._start("source_fixture")
    source = fixture(tmp_path, capture_journal=True, capture_journal_fair=True)
    stage("source_fixture", source, call)
    original = source["journal_row"]
    assert original["state"] == "applied" and original["generation"] == 2
    assert "Original journal body" not in original["value"]
    before = _tree(tmp_path, hashes=True)
    cursor = journal_cursor(source["descriptor"], original["request_key"])
    call = native._start("layout_source")
    exported = invoke(source["descriptor"], cursor)
    stage("layout_source", exported, call)
    assert exported["status"] == "ok", exported
    descriptors = {}
    for row in exported["entries"]:
        if row["kind"] == "bulk_page":
            header = json.loads(row["receipt"])
            if header["field"] == "descriptor":
                descriptors.setdefault(header["owner"], []).append((header["ordinal"], row["text"]))
    originals = []
    for manifest in exported["finalizations"]:
        descriptor = json.loads("".join(text for _, text in sorted(descriptors[manifest["owner"]])))
        if descriptor.get("original_table") == "capture_publication_intents":
            assert manifest["members"] == 0 and manifest["live"] is None
            assert descriptor["kind"] == "AUDIT" and descriptor["row"] == original
            originals.append(descriptor["row"])
    assert originals == [original]
    assert _tree(tmp_path, hashes=True) == before
    assert native._CALLS - start == 2
    assert not {"sqlite3", "lance", "pyarrow", "numpy", "torch", "onnxruntime"}.intersection(
        sys.modules
    )

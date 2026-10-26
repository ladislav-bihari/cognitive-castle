"""Real guarded SQLite prefix, frozen subset, and unchanged legacy fallback."""

import json
from pathlib import Path
import sys


def _worker():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import sqlite_source_worker

    opens = []

    def audit(event, args):
        if (
            event == "sqlite3.connect"
            and isinstance(args[0], str)
            and "?mode=ro&immutable=1" in args[0]
        ):
            opens.append(1)

    sys.addaudithook(audit)
    original = sys.stdout

    class Output:
        def __init__(self):
            self.buffer = self
            self.writes = 0

        def write(self, raw):
            self.writes += 1
            if self.writes != 1:
                raise ValueError("single source response")
            value = json.loads(raw)
            value["_source_connections"] = len(opens)
            return original.buffer.write(
                json.dumps(value, ensure_ascii=True, allow_nan=False).encode()
            )

        def flush(self):
            original.buffer.flush()

    sys.stdout = Output()
    sqlite_source_worker.main()
    sys.stdout = original


if __name__ == "__main__":
    if sys.argv[1:] != ["source"]:
        raise ValueError("fixed SQLite prefix invocation")
    _worker()
    raise SystemExit

import pytest  # noqa: E402
import test_layout_native_integration as native  # noqa: E402
from test_sqlite_source_export import fixture  # noqa: E402
from test_layout_native_tail import _tree  # noqa: E402

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def test_actual_sqlite_prefix_frozen_subset_and_legacy_fallback_preserve_originals(tmp_path):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.canonical_records import _json
    from cognitive_castle.sqlite_source_runtime import source_request
    from cognitive_castle.lance_layout_packing import pack_source_prefix, validate_prefix_result

    first_call = native._CALLS
    call = native._start("source_fixture")
    source = fixture(tmp_path, prefix_small=True)
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation="source_fixture",
                status=source["status"],
                reason=source.get("reason"),
                resources=[source["resources"]],
            )
        ),
        flush=True,
    )
    assert source["status"] == "ok" and len(source["prefix_receipts"]) == 3
    before_tree = _tree(tmp_path / "source", hashes=True)
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    artifact = generation / "exact/layout-qualification.json"
    artifact.write_text(_json(qualification))
    artifact.chmod(0o400)
    artifact_before = artifact.read_bytes()
    request = source_request(source["descriptor"])
    reg = migration.registration(source["descriptor"], str(generation))
    window = dict(
        format=layout.PREFIX_SOURCE_REQUEST,
        action="prepare",
        generation=envelope["generation"],
        qualification_hash=migration.hashed(qualification),
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    original_sources = {s["id"]: s for s in source["original_sources"]}

    def invoke(cursor, selection):
        with native._session(tmp_path) as session:
            call = native._start("layout_source")
            result = session._run_stage(
                source_request(source["descriptor"], cursor, layout=selection),
                worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "source"],
                cap=2,
                rss=128 * 1024**2,
                total=192 * 1024**2,
            )
            native._record(session, "layout_source", result, 0, call)
        assert result["status"] == "ok", result
        assert result.pop("_source_connections") == 1
        assert _tree(tmp_path / "source", hashes=True) == before_tree
        assert artifact.read_bytes() == artifact_before
        return result

    def check_records(exported):
        rows = [r for r in exported["entries"] if r["kind"] == "bulk_record"]
        for row in rows:
            expected = original_sources[row["id"]]
            assert (row["text"], row["provenance"]) == (
                expected["text"],
                _json(expected["provenance"]),
            )
        assert len(rows) == exported["finalizations"][0]["members"]
        assert exported["coverage"] == "incomplete"

    prepared = invoke(request["cursor"], window)
    validate_prefix_result(prepared, window, request["descriptor"], request["cursor"])
    packet = prepared["prefix"]
    assert len(packet["components"]) >= 3
    assert prepared["cursor_after"] == request["cursor"]
    cursor = request["cursor"]
    for index, part in enumerate(packet["components"]):
        exported = part["export"]
        check_records(exported)
        assert exported["cursor_before"] == cursor
        cursor = exported["cursor_after"]
        if index < 3:
            live = exported["finalizations"][0]["live"]
            assert live["key"] == "aaa-prefix-" + str(index)
            assert live["receipt"] == source["prefix_receipts"][index]["receipt"]
        assert part["plan"]["source_key"] == cursor["key"]
    trimmed = pack_source_prefix(reg, request["cursor"], packet["components"][:2])
    frozen_window = dict(
        window,
        action="export",
        allowed_plan_hashes=[migration.hashed(p["plan"]) for p in trimmed["components"]],
    )
    frozen = invoke(request["cursor"], frozen_window)
    validate_prefix_result(frozen, frozen_window, request["descriptor"], request["cursor"])
    assert frozen["prefix"] == trimmed and frozen["cursor_after"] == trimmed["cursor_after"]
    history_cursor = dict(request["cursor"], phase="history")
    fallback = invoke(history_cursor, window)
    validate_prefix_result(fallback, window, request["descriptor"], history_cursor)
    assert fallback["prefix"] is None and fallback["terminal"] is None
    assert fallback["cursor_after"] == history_cursor
    plan = fallback["fallback_plan"]
    original = next(row for row in source["history"] if row["identity"] == plan["owner_key"])
    legacy_window = dict(
        window,
        format=layout.SOURCE_REQUEST,
        action="export",
        allowed_plan_hashes=[migration.hashed(plan)],
    )
    legacy = invoke(history_cursor, legacy_window)
    layout.validate_layout_source_result(
        legacy, legacy_window, request["descriptor"], history_cursor
    )
    assert legacy["plans"] == [plan]
    exported = legacy["export"]
    check_records(exported)
    owner = exported["finalizations"][0]
    assert owner["owner_key"] == original["identity"]
    assert owner["text_hash"] == __import__("hashlib").sha256(original["text"].encode()).hexdigest()
    for field in ("metadata", "columns"):
        pages = [
            r
            for r in exported["entries"]
            if r["kind"] == "bulk_page" and json.loads(r["receipt"])["field"] == field
        ]
        assert "".join(r["text"] for r in pages) == source[field]
    assert native._CALLS - first_call == 5

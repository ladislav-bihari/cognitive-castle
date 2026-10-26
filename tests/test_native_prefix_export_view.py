"""Actual prefix preparation and frozen export preserve original native rows."""

import json
import os
from pathlib import Path
import sys


def _prefix_worker():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_history_worker import _install_guards
    from cognitive_castle.native_source_worker import _admit_source
    from cognitive_castle.semantic_profile import decode_frame

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(512 * 1024 + 1))
    if request.keys() != {"descriptor", "cursor", "layout", "_host_pid"}:
        raise ValueError("fixed native prefix request")
    _install_guards()

    def no_sqlite(event, args):
        if event == "sqlite3.connect":
            raise PermissionError("native source export forbids SQLite connections")

    sys.addaudithook(no_sqlite)
    refused = _admit_source(request)
    if refused is not None:
        return refused
    from cognitive_castle.native_export_codec import prepare_layout_request
    from cognitive_castle.native_source_export import export_slice
    import lance

    prepare_layout_request(request["descriptor"], request["cursor"], request["layout"])
    opens = []
    original_open = lance.dataset

    def counted_open(*args, **kwargs):
        opens.append(1)
        return original_open(*args, **kwargs)

    lance.dataset = counted_open
    result = export_slice(request["descriptor"], request["cursor"], layout=request["layout"])
    return dict(result, native_opens=len(opens))


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["prefix"]:
            raise ValueError("fixed native prefix invocation")
        output = _prefix_worker()
    except Exception as exc:
        trace = exc.__traceback__
        while trace.tb_next is not None:
            trace = trace.tb_next
        output = dict(
            status="unavailable",
            reason="prefix_fixture_" + type(exc).__name__,
            failure_file=Path(trace.tb_frame.f_code.co_filename).name,
            failure_line=trace.tb_lineno,
        )
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False).encode()
    if len(raw) > 512 * 1024:
        raw = b'{"status":"unavailable","reason":"prefix_fixture_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402
import test_layout_native_integration as native  # noqa: E402
from test_native_source_export import make_fixture  # noqa: E402
from test_layout_native_tail import _tree  # noqa: E402

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def test_actual_prefix_prepare_and_frozen_export_preserve_all_originals(tmp_path):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.canonical_records import _json
    from cognitive_castle.native_source_runtime import source_request
    from cognitive_castle.lance_layout_packing import validate_prefix_result

    first_call = native._CALLS
    call = native._start("source_fixture_native")
    source = make_fixture(tmp_path / "source")
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation="source_fixture_native",
                status=source["status"],
                reason=source.get("reason"),
                resources=[source["resources"]],
            )
        ),
        flush=True,
    )
    assert source["status"] == "ok" and len(source["rows"]) == 3
    before_tree = _tree(tmp_path / "source", hashes=True)
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    artifact = generation / "exact/layout-qualification.json"
    artifact.write_text(_json(qualification))
    artifact.chmod(0o400)
    qualification_before = artifact.read_bytes()
    request = source_request(source["descriptor"])
    window = dict(
        format=layout.PREFIX_SOURCE_REQUEST,
        action="prepare",
        generation=envelope["generation"],
        qualification_hash=migration.hashed(qualification),
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    prepared = None
    for action in ("prepare", "export"):
        if action == "export":
            window = dict(
                window,
                action="export",
                allowed_plan_hashes=[
                    migration.hashed(part["plan"]) for part in prepared["components"]
                ],
            )
        with native._session(tmp_path) as session:
            call = native._start("layout_source")
            result = session._run_stage(
                dict(request, layout=window),
                worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "prefix"],
                cap=2,
                rss=512 * 1024**2,
                total=640 * 1024**2,
            )
            native._record(session, "layout_source", result, 0, call)
        assert result["status"] == "ok", result
        assert result.pop("native_opens") == 1
        result.pop("resources")
        validate_prefix_result(result, window, request["descriptor"], request["cursor"])
        packet = result["prefix"]
        assert len(packet["components"]) == len(source["rows"]) == 3
        cursor = request["cursor"]
        for part, original in zip(packet["components"], source["rows"], strict=True):
            exported, plan = part["export"], part["plan"]
            assert exported["cursor_before"] == cursor
            cursor = dict(cursor, key=[_json(original["following"])])
            assert exported["cursor_after"] == cursor and plan["source_key"] == cursor["key"]
            owner = exported["finalizations"][0]
            assert owner["owner_key"] == original["occurrence"]
            assert owner["text_hash"] == original["text_hash"]
            assert owner["members"] == len(original["records"])
            records = [r for r in exported["entries"] if r["kind"] == "bulk_record"]
            assert [(r["id"], r["text"], r["provenance"]) for r in records] == [
                (r["id"], r["text"], _json(r["provenance"])) for r in original["records"]
            ]
            for field in ("metadata", "columns"):
                pages = [
                    r
                    for r in exported["entries"]
                    if r["kind"] == "bulk_page"
                    and r["id"].startswith("page:")
                    and json.loads(r["receipt"])["field"] == field
                ]
                assert "".join(r["text"] for r in pages) == original[field]
        assert packet["cursor_after"] == cursor
        if action == "prepare":
            assert result["cursor_after"] == request["cursor"]
            prepared = packet
        else:
            assert packet == prepared and result["cursor_after"] == cursor
        assert _tree(tmp_path / "source", hashes=True) == before_tree
        assert artifact.read_bytes() == qualification_before
    assert native._CALLS - first_call == 3

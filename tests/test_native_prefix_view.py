"""Two supervised starts prove one native open across original occurrences."""

import json
import os
from pathlib import Path
import sys


def _reader_worker():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_history_worker import _install_guards
    from cognitive_castle.native_source_worker import _admit_source
    from cognitive_castle.semantic_profile import decode_frame

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(512 * 1024 + 1))
    if request.keys() != {"descriptor", "_host_pid"}:
        raise ValueError("fixed prefix-reader request")
    _install_guards()

    def no_sqlite(event, args):
        if event == "sqlite3.connect":
            raise PermissionError("native source export forbids SQLite connections")

    sys.addaudithook(no_sqlite)
    refused = _admit_source(request)
    if refused is not None:
        return refused
    from cognitive_castle.native_source_export import Source
    from cognitive_castle.native_export_codec import initial_cursor
    from cognitive_castle.memory_history import _stage_header, _stage_caller, _chunks
    from cognitive_castle.fast_memory import MemoryStore
    from cognitive_castle.canonical_records import CanonicalRecord, _hash, _json
    import lance

    opens = []
    original_open = lance.dataset

    def counted_open(*args, **kwargs):
        opens.append(1)
        return original_open(*args, **kwargs)

    lance.dataset = counted_open
    descriptor = request["descriptor"]
    source = Source(descriptor)
    cursor = initial_cursor(source.binding, descriptor["store_id"], descriptor["start_cursor"])
    try:
        assert source.open(cursor)
        reader = source.reader
        dataset = reader.dataset
        rows = []
        for _ in range(3):
            source.check_time()
            before = dict(reader.cursor)
            header = reader.header()
            following, occurrence, metadata, columns = _stage_header(header, source.job, before)
            alias, caller = _stage_caller(
                source.job,
                dict(identity=occurrence, metadata_json=metadata, columns_json=columns),
            )
            text = reader.page(0)
            assert len(text) == header["text_characters"]
            assert len(text.encode()) == header["text_bytes"] and _hash(text) == header["text_hash"]
            records = [
                CanonicalRecord.from_source(
                    dict(
                        id="memory:" + _hash(_json([occurrence, start])),
                        text=chunk,
                        provenance=MemoryStore._provenance(
                            caller, _hash(chunk), start, start + len(chunk)
                        ),
                    )
                ).source()
                for start, chunk in _chunks(text)
            ]
            rows.append(
                dict(
                    before=before,
                    following=following,
                    occurrence=occurrence,
                    alias=alias,
                    metadata=metadata,
                    columns=columns,
                    records=records,
                    text_hash=_hash(text),
                )
            )
            next_header = reader.advance()
            assert reader.dataset is dataset and reader.cursor == following
        assert next_header is None and len(opens) == 1
        source.validate_files()
        return dict(status="ok", rows=rows, native_opens=len(opens), cursor=dict(reader.cursor))
    finally:
        if source.reader is not None:
            source.reader.close()
            source.reader = None
        source.validate_files()


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["reader"]:
            raise ValueError("fixed prefix-reader invocation")
        output = _reader_worker()
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


def test_native_prefix_uses_one_view_and_preserves_every_original_row(tmp_path):
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
    descriptor = source["descriptor"]
    before = _tree(tmp_path / "source", hashes=True)
    with native._session(tmp_path) as session:
        call = native._start("layout_source")
        result = session._run_stage(
            dict(descriptor=descriptor),
            worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "reader"],
            cap=2,
            rss=512 * 1024**2,
            total=640 * 1024**2,
        )
        native._record(session, "layout_source", result, 0, call)
    assert result["status"] == "ok", result
    assert result["native_opens"] == 1
    assert result["rows"] == source["rows"] and len(result["rows"]) == 3
    assert result["cursor"] == source["rows"][-1]["following"]
    assert _tree(tmp_path / "source", hashes=True) == before
    assert native._CALLS - first_call == 2

"""Source fixtures run in a separate fixed supervised SQLite process."""

import hashlib
import json
import os
from pathlib import Path
import sys


def fixture_main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_worker import _install_guards, _admit_storage, _sqlite_heap_limit

    _install_guards()
    os.umask(0o077)
    request = json.load(sys.stdin)
    refusal = _admit_storage(request)
    if refusal:
        print(json.dumps(refusal))
        return
    _sqlite_heap_limit()
    from cognitive_castle.fast_memory import MemoryStore
    from cognitive_castle.memory_history import _schema, _chunks, _cursor_key
    from cognitive_castle.memory_capture import _ensure
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.sqlite_export_codec import FORMAT, file_identity

    if "scale_action" in request:
        scale_fixture(request)
        return
    directory = Path(request["directory"])
    directory.mkdir(mode=0o700)
    store = MemoryStore(directory)
    text = ("a" * 8191 + "€") * 7 + "b" * 8178
    receipt = store.remember("nine", text, {"workspace": "original", "note": "\x00"})
    assert receipt["status"] == "ok" and len(receipt["ids"]) == 9
    assert store.remember("nine", text, {"workspace": "original", "note": "\x00"}) == receipt
    prefix_receipts = []
    if request.get("prefix_small"):
        for ordinal, body in enumerate(("first small Ω\0", "second\nsmall", "third small\t")):
            small = store.remember(
                "aaa-prefix-" + str(ordinal), body, {"workspace": "prefix Ω", "note": "\0"}
            )
            assert small["status"] == "ok"
            prefix_receipts.append(dict(receipt=small, text=body))
    facts = dict(archive_id="archive", snapshot_id="original", version=7)
    metadata = (
        ' { "workspace" : "meta", "n" : "\\u0000", "long" : "'
        + "🙂\x00".replace("\x00", "\\u0000") * request.get("large", 10)
        + '" }\n'
    )
    columns = ' {"id":"alias", "workspace":"' + "Ω" * 200 + '","wing":null,"room":""} '
    expected = []
    with store._connect(True) as conn:
        _schema(conn)
        _ensure(conn)
        for ordinal, body in enumerate(("first\x00\nΩ", "second\t", "")):
            identity = _hash(_json(["archive", "original", 7, 0, ordinal]))
            caller = dict(archive_id="archive", version=7, metadata_id=identity)
            ids = []
            for start, chunk in _chunks(body):
                chunk_id = "memory:" + _hash(_json([identity, start]))
                ids.append(chunk_id)
                conn.execute(
                    "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                    (chunk_id, chunk, _json(caller), _hash(chunk), start, start + len(chunk)),
                )
            cursor = dict(fragment=0, offset=ordinal)
            following = dict(fragment=0, offset=ordinal + 1, row_address=ordinal)
            key = _cursor_key(cursor)
            conn.execute(
                "INSERT INTO history_occurrences VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    identity,
                    key,
                    ordinal,
                    "alias",
                    metadata,
                    columns,
                    _hash(body),
                    _json(ids) if ordinal != 1 else "[]",
                    _hash(metadata),
                    _hash(columns),
                ),
            )
            if ordinal == 1:
                for chunk_id in ids:
                    conn.execute(
                        "INSERT INTO history_occurrence_chunks VALUES (?,?)", (identity, chunk_id)
                    )
            conn.execute("INSERT INTO history_aliases VALUES (?,?)", ("alias", identity))
            conn.execute(
                "INSERT INTO history_attempts VALUES (?,?,?)", (key, _json(following), "indexed")
            )
            expected.append(dict(identity=identity, text=body, ids=ids))
        conn.execute(
            "INSERT INTO capture_generation_scopes VALUES (?,?,?,?,?)",
            ("source", 1, None, "original", "conflict"),
        )
        conn.execute(
            "INSERT INTO capture_chunk_scopes VALUES (?,?,?,?,?,?)",
            (receipt["ids"][0], "source", 1, "Ω" * 6000, "observed", 123),
        )
        conn.execute(
            "INSERT INTO capture_chunk_scopes VALUES (?,?,?,?,?,?)",
            (receipt["ids"][1], "source", 1, "other", "observed", 124),
        )
        conn.execute(
            "INSERT INTO capture_sources VALUES (?,?,?)",
            ("source", "pending", _json({"workspace": "first", "conflict_offset": 12})),
        )
        conn.execute("INSERT INTO checkpoints VALUES (?,?)", ("source", ' {"offset":0} '))
        conn.execute(
            "INSERT INTO coverage_observations(identity,value) VALUES (?,?)",
            ("obs", ' {"omitted":true} '),
        )
        key = _cursor_key(dict(fragment=0, offset=3))
        gap_id = _hash(_json(["archive", "original", 7, 0, 3]))
        conn.execute("INSERT INTO history_gaps VALUES (?,?,?)", (key, gap_id, "row_size"))
        conn.execute(
            "INSERT INTO history_attempts VALUES (?,?,?)",
            (key, _json(dict(fragment=0, offset=4, row_address=3)), "gap"),
        )
        stage = _cursor_key(dict(fragment=0, offset=4))
        conn.execute(
            "INSERT INTO history_staged_rows(cursor_key,identity,snapshot_id,source_cursor,next_cursor,metadata_json,columns_json,text_bytes,text_characters,next_character,chunk_start,carry,processed_bytes,state,retry_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stage,
                "f" * 64,
                "original",
                _json(dict(fragment=0, offset=4)),
                _json(dict(fragment=0, offset=5, row_address=4)),
                "{}",
                "{}",
                10000,
                10000,
                8194,
                8192,
                "pa",
                8194,
                "copying",
                0,
            ),
        )
        pending_chunk_id = "memory:" + _hash(_json(["f" * 64, 0]))
        conn.execute(
            "INSERT INTO history_staged_chunks VALUES (?,?,?,?,?,?)",
            (stage, 0, 8192, pending_chunk_id, "z" * 8192, _hash("z" * 8192)),
        )
        mode = request.get("mode")
        if mode == "missing_attempt":
            conn.execute(
                "DELETE FROM history_attempts WHERE cursor_key=?",
                (_cursor_key(dict(fragment=0, offset=0)),),
            )
        elif mode == "missing_attempt_table":
            conn.execute("DROP TABLE history_attempts")
        elif mode == "missing_gap_table":
            conn.execute("DROP TABLE history_gaps")
        elif mode == "empty_alias":
            columns = columns.replace('"id":"alias"', '"id":""')
            conn.execute(
                "UPDATE history_occurrences SET alias='',columns_json=?,columns_hash=?",
                (columns, _hash(columns)),
            )
            conn.execute("UPDATE history_aliases SET alias=''")
        elif mode == "unsupported":
            conn.execute("CREATE TABLE unknown_source_shape(value TEXT)")
        elif mode == "missing":
            conn.execute("DELETE FROM chunks WHERE id=?", (receipt["ids"][0],))
        elif mode == "digest":
            conn.execute("UPDATE receipts SET digest=?", ("a" * 64,))
        elif mode == "offset":
            conn.execute("UPDATE chunks SET start=start+1 WHERE id=?", (receipt["ids"][0],))
        elif mode == "dual":
            conn.execute(
                "INSERT INTO history_occurrence_chunks VALUES (?,?)",
                (expected[0]["identity"], expected[1]["ids"][0]),
            )
        elif mode == "orphan":
            conn.execute(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                ("memory:" + "a" * 64, "orphan", "{}", _hash("orphan"), 0, 6),
            )
        elif mode == "hash":
            conn.execute("UPDATE history_occurrences SET metadata_hash=?", ("a" * 64,))
        elif mode == "repaired":
            # Preserve a resolved gap's lineage against a verified existing occurrence.
            old = expected[0]
            repaired_key = _cursor_key(dict(fragment=0, offset=0))
            conn.execute(
                "INSERT INTO history_gap_resolutions VALUES (?,?,?,?,?,?,?,?)",
                (
                    repaired_key,
                    old["identity"],
                    "row_size",
                    _json(dict(fragment=0, offset=1, row_address=0)),
                    "original",
                    _hash(old["text"]),
                    _hash(metadata),
                    _hash(columns),
                ),
            )
    if request.get("migration_inventory"):
        prune_migration_inventory(store, receipt, expected, request["migration_inventory"])
    with store._connect() as conn:
        original_sources = (
            [MemoryStore._source(row) for row in conn.execute("SELECT * FROM chunks ORDER BY id")]
            if request.get("mode") not in {"offset"}
            else []
        )
    from cognitive_castle.unified_memory import UnifiedMemoryStore

    original_public = []
    if request.get("mode") is None:
        with store._connect() as conn:
            for source in original_sources:
                public = dict(source)
                reference = UnifiedMemoryStore._metadata_reference(conn, source)
                if reference is not None:
                    public["metadata"] = reference
                original_public.append(public)
    path = store.db_path
    fd = os.open(path, os.O_RDONLY)
    os.fsync(fd)
    os.close(fd)
    identity = file_identity(path.stat())
    import sqlite3

    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as conn:
        schema = [
            list(row)
            for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
    manifest = dict(
        format=FORMAT,
        state="pending" if request.get("mode") == "incomplete" else "complete",
        store_id="store",
        snapshot_id="fixture",
        identity=identity,
        content_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
        schema_hash=_hash(_json(schema)),
        original=facts,
        catalog={"version": 1},
        high_water=123,
        coverage_state="indexed-only",
    )
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(_json(manifest))
    with manifest_path.open("rb") as handle:
        os.fsync(handle.fileno())
    descriptor = dict(
        format=FORMAT,
        store_id="store",
        snapshot_id="fixture",
        path=str(path),
        identity=identity,
        manifest_path=str(manifest_path),
        manifest_identity=file_identity(manifest_path.stat()),
        manifest_hash=_hash(manifest_path.read_text()),
    )
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(directory_fd)
    os.close(directory_fd)
    print(
        json.dumps(
            dict(
                status="ok",
                descriptor=descriptor,
                receipt=receipt,
                history=expected,
                metadata=metadata if request.get("large", 10) <= 10 else None,
                columns=columns,
                original_sources=original_sources,
                original_public=original_public,
                pending_chunk_id=pending_chunk_id,
                **({"prefix_receipts": prefix_receipts} if request.get("prefix_small") else {}),
            )
        )
    )


def prune_migration_inventory(store, receipt, expected, inventory):
    """Prune disposable protocol fixtures only before closure/fsync/sealing."""
    from cognitive_castle.memory_history import _cursor_key
    from cognitive_castle.canonical_records import _hash

    with store._connect(True) as conn:
        for identity in receipt["ids"]:
            conn.execute("DELETE FROM chunks WHERE id=?", (identity,))
        conn.execute("DELETE FROM receipts")
        for table in (
            "capture_chunk_scopes",
            "capture_generation_scopes",
            "capture_sources",
            "checkpoints",
            "coverage_observations",
        ):
            conn.execute(f"DELETE FROM {table}")
        if inventory in {"empty", "orphan"}:
            for table in (
                "history_occurrence_chunks",
                "history_occurrences",
                "history_aliases",
                "history_attempts",
                "history_gaps",
                "history_gap_resolutions",
                "history_staged_rows",
                "history_staged_chunks",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.execute("DELETE FROM chunks")
            if inventory == "orphan":
                conn.execute(
                    "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                    ("memory:" + "a" * 64, "orphan", "{}", _hash("orphan"), 0, 6),
                )
        elif inventory == "staging":
            selected = expected[0]["identity"]
            conn.execute("DELETE FROM history_occurrence_chunks")
            conn.execute("DELETE FROM history_occurrences WHERE identity<>?", (selected,))
            conn.execute("DELETE FROM history_aliases WHERE identity<>?", (selected,))
            conn.execute(
                "DELETE FROM history_attempts WHERE cursor_key<>?",
                (_cursor_key(dict(fragment=0, offset=0)),),
            )
            for table in (
                "history_gaps",
                "history_gap_resolutions",
                "history_staged_rows",
                "history_staged_chunks",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.execute("DELETE FROM chunks WHERE id<>?", (expected[0]["ids"][0],))
        elif inventory != "protocol":
            raise ValueError("unknown migration fixture inventory")


def scale_fixture(request):
    """Bounded fixture transactions; no corpus string/list crosses the host."""
    from cognitive_castle.fast_memory import MemoryStore
    from cognitive_castle.memory_history import _schema, _cursor_key
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.sqlite_export_codec import FORMAT, file_identity

    directory = Path(request["directory"])
    action = request["scale_action"]
    if action in {"member_hash", "export"}:
        from cognitive_castle.sqlite_source_export import Source, export_slice

        manifest_path = directory / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        descriptor = dict(
            format=FORMAT,
            store_id="store",
            snapshot_id="scale",
            path=str(directory / "memory.sqlite3"),
            identity=manifest["identity"],
            manifest_path=str(manifest_path),
            manifest_identity=file_identity(manifest_path.stat()),
            manifest_hash=_hash(_json(manifest)),
        )
        if action == "export":
            from cognitive_castle.sqlite_export_codec import initial_cursor, source_binding

            before = initial_cursor(source_binding(descriptor), "store")
            result = export_slice(descriptor, before)
            print(json.dumps(result))
            return
        source = Source(descriptor)
        source.open()
        digest = hashlib.sha256()
        count = size = 0
        identity = _hash(_json(["archive", "original", 7, 0, 0]))
        try:
            expected = source.small("history_occurrences", "text_hash", "identity=?", (identity,))[
                "text_hash"
            ]
            for ordinal, record in source.members("history", [identity]):
                assert ordinal == count
                count += 1
                size += len(record.text.encode())
                digest.update(record.text.encode())
            source.validate_files()
        finally:
            source.conn.close()
        print(
            json.dumps(
                dict(
                    status="ok",
                    members=count,
                    text_bytes=size,
                    text_hash=digest.hexdigest(),
                    expected_hash=expected,
                )
            )
        )
        return
    if action == "plan":
        import sqlite3
        from cognitive_castle.sqlite_source_export import CHUNK_COLUMNS

        old = (
            "SELECT "
            + CHUNK_COLUMNS
            + " FROM chunks WHERE id IN (SELECT chunk_id FROM history_occurrence_chunks WHERE identity=?) ORDER BY start,id"
        )
        new = "SELECT c.id FROM history_occurrence_chunks m JOIN chunks c ON c.id=m.chunk_id WHERE m.identity=? ORDER BY c.start,c.id"
        with sqlite3.connect(
            (directory / "memory.sqlite3").as_uri() + "?mode=ro&immutable=1", uri=True
        ) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA temp_store=MEMORY")
            plans = {}
            for name, query in (("old", old), ("new", new)):
                plans[name] = dict(
                    query=query,
                    plan=[
                        list(row)
                        for row in conn.execute("EXPLAIN QUERY PLAN " + query, ("0" * 64,))
                    ],
                    sorter=[
                        list(row)
                        for row in conn.execute("EXPLAIN " + query, ("0" * 64,))
                        if row[1] in {"SorterOpen", "SorterInsert", "MakeRecord", "Column"}
                    ],
                )
        print(json.dumps(dict(status="ok", plans=plans)))
        return

    if action == "init":
        directory.mkdir(mode=0o700)
    store = MemoryStore(directory)
    identity = _hash(_json(["archive", "original", 7, 0, 0]))
    caller = _json(dict(archive_id="archive", version=7, metadata_id=identity))
    if action in {"init", "append"}:
        start = request.get("offset", 0)
        count = 256 if action == "append" else 0
        with store._connect(True) as conn:
            if action == "init":
                _schema(conn)
            for ordinal in range(count):
                offset = start + ordinal * 8192
                text = "x" * 8192
                chunk = "memory:" + _hash(_json([identity, offset]))
                conn.execute(
                    "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                    (chunk, text, caller, _hash(text), offset, offset + 8192),
                )
                conn.execute(
                    "INSERT INTO history_occurrence_chunks VALUES (?,?)", (identity, chunk)
                )
        print(json.dumps(dict(status="ok", next_offset=start + count * 8192)))
        return
    if action != "seal":
        raise ValueError("scale fixture action")
    digest = hashlib.sha256()
    with store._connect(True) as conn:
        offset = 0
        for row in conn.execute("SELECT start,text FROM chunks ORDER BY start"):
            assert row[0] == offset
            digest.update(row[1].encode())
            offset += len(row[1])
        assert offset == 52 * 1024**2
        following = _json(dict(fragment=0, offset=1, row_address=0))
        cursor = _cursor_key(dict(fragment=0, offset=0))
        columns = '{"id":"scale","workspace":null}'
        conn.execute(
            "INSERT INTO history_occurrences VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                identity,
                cursor,
                0,
                "scale",
                "{}",
                columns,
                digest.hexdigest(),
                "[]",
                _hash("{}"),
                _hash(columns),
            ),
        )
        conn.execute("INSERT INTO history_aliases VALUES (?,?)", ("scale", identity))
        conn.execute("INSERT INTO history_attempts VALUES (?,?,?)", (cursor, following, "indexed"))
    path = store.db_path
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    identity_file = file_identity(path.stat())
    content = hashlib.sha256()
    with path.open("rb") as handle:
        while piece := handle.read(65536):
            content.update(piece)
    import sqlite3

    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as conn:
        schema = [
            list(row)
            for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
    manifest = dict(
        format=FORMAT,
        state="complete",
        store_id="store",
        snapshot_id="scale",
        identity=identity_file,
        content_hash=content.hexdigest(),
        schema_hash=_hash(_json(schema)),
        original=dict(archive_id="archive", snapshot_id="original", version=7),
        catalog={"version": 1},
        high_water=offset,
        coverage_state="indexed-only",
    )
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(_json(manifest))
    with manifest_path.open("rb") as handle:
        os.fsync(handle.fileno())
    descriptor = dict(
        format=FORMAT,
        store_id="store",
        snapshot_id="scale",
        path=str(path),
        identity=identity_file,
        manifest_path=str(manifest_path),
        manifest_identity=file_identity(manifest_path.stat()),
        manifest_hash=_hash(_json(manifest)),
    )
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    os.fsync(directory_fd)
    os.close(directory_fd)
    print(json.dumps(dict(status="ok", descriptor=descriptor, text_bytes=offset, chunks=6656)))


if __name__ == "__main__":
    try:
        fixture_main()
    except Exception as exc:
        print(
            json.dumps({"status": "fixture_error", "reason": type(exc).__name__ + ":" + str(exc)})
        )
    raise SystemExit(0)

import pytest  # noqa: E402
from cognitive_castle.sqlite_export_codec import source_binding, initial_cursor  # noqa: E402

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="fixed owned-group supervision")
ROOT = Path(__file__).resolve().parents[1]


def test_admission(runtime):
    import time

    until = time.monotonic() + 8
    while True:
        try:
            return runtime._admission()
        except BlockingIOError:
            print("ADMISSION REFUSAL: source fixture global admission busy", flush=True)
            if time.monotonic() >= until:
                pytest.fail("source fixture deferred: admission busy for 8 seconds")
            time.sleep(0.1)


test_admission.__test__ = False


def fixture(tmp_path, **options):
    from cognitive_castle import memory_runtime as runtime

    admission = test_admission(runtime)
    try:
        result = runtime._supervise(
            {"directory": str(tmp_path / "source"), **options},
            admission,
            worker_command=[sys.executable, "-I", str(Path(__file__).resolve())],
            deadline=2,
            rss_limit=128 * 1024**2,
            total_rss_limit=192 * 1024**2,
        )
    finally:
        os.close(admission)
    print(
        "SOURCE_FIXTURE",
        json.dumps({k: v for k, v in result.items() if k in {"status", "reason", "resources"}}),
        flush=True,
    )
    assert result["status"] == "ok" or (
        options.get("scale_action") == "export" and result["status"] == "qualification_required"
    ), result
    return result


def invoke(descriptor, cursor=None):
    from cognitive_castle.sqlite_source_runtime import export_slice

    result = export_slice(descriptor, cursor)
    print(
        "SOURCE_SLICE",
        json.dumps(
            {
                k: v
                for k, v in result.items()
                if k
                in {
                    "status",
                    "reason",
                    "counts",
                    "resources",
                    "frame_bytes",
                    "complete",
                    "phase",
                    "refused_field",
                    "refused_operation",
                }
            }
        ),
        flush=True,
    )
    return result


def collect(descriptor):
    cursor = None
    entries, owners, slices = [], [], []
    for _ in range(100):
        result = invoke(descriptor, cursor)
        assert result["status"] == "ok", result
        assert result["cursor_before"] == (
            cursor or initial_cursor(source_binding(descriptor), "store")
        )
        slices.append(result)
        entries.extend(result["entries"])
        owners.extend(result["finalizations"])
        cursor = result["cursor_after"]
        if result["complete"]:
            break
    else:
        pytest.fail("export failed to finish in bounded slices")
    return entries, owners, slices


def test_nine_chunk_and_history_exact_public_envelopes_and_audit_coverage(tmp_path):
    data = fixture(tmp_path)
    directory = Path(data["descriptor"]["path"]).parent
    inventory = {
        p.name: (
            (p.stat().st_ino, p.stat().st_mtime_ns, p.stat().st_size),
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in directory.iterdir()
    }
    first = invoke(data["descriptor"])
    retry = invoke(data["descriptor"])
    assert {k: v for k, v in first.items() if k != "resources"} == {
        k: v for k, v in retry.items() if k != "resources"
    }
    entries, owners, slices = collect(data["descriptor"])
    live = [o for o in owners if o["live"]]
    history = [o for o in owners if o["metadata_id"]]
    audit = [o for o in owners if not o["metadata_id"] and not o["live"]]
    assert len(live) == 1 and live[0]["live"]["receipt"] == data["receipt"]
    assert live[0]["members"] == 9 and live[0]["text_bytes"] == 65536
    assert len(history) == 3 and sum(o["members"] for o in history) == 2
    assert len(audit) >= 10 and all(o["members"] == 0 for o in audit)
    assert sum(s["counts"]["gaps"] for s in slices) == 1
    assert sum(s["counts"]["pending_stages"] for s in slices) == 1
    assert slices[-1]["coverage"] == "incomplete"
    for owner in history:
        for field, expected in (("metadata", data["metadata"]), ("columns", data["columns"])):
            pages = [
                e
                for e in entries
                if e["kind"] == "bulk_page"
                and json.loads(e["receipt"])["owner"] == owner["owner"]
                and json.loads(e["receipt"])["field"] == field
            ]
            assert "".join(p["text"] for p in pages) == expected

    def field_document(owner, field):
        return json.loads(
            "".join(
                e["text"]
                for e in entries
                if e["kind"] == "bulk_page"
                and json.loads(e["receipt"])["owner"] == owner["owner"]
                and json.loads(e["receipt"])["field"] == field
            )
        )

    scopes = field_document(live[0], "scopes")
    mappings = scopes["chunks"]
    assert [row["id"] for row in mappings] == data["receipt"]["ids"]
    assert mappings[0]["capture_mapping"]["row"]["workspace"] == "Ω" * 6000
    assert mappings[0]["capture_mapping"]["row"]["source_time"] == 123
    assert mappings[0]["capture_mapping"]["generation"]["state"] == "conflict"
    assert mappings[0]["capture_mapping"]["generation"]["workspace"] is None
    assert mappings[1]["capture_mapping"]["row"]["workspace"] == "other"
    assert all(row["capture_mapping"] == {"present": False} for row in mappings[2:])
    assert json.loads(mappings[0]["caller"])["workspace"] == "original"
    historical_scopes = field_document(history[0], "scopes")
    assert (
        historical_scopes["columns"] == data["columns"]
        and historical_scopes["metadata"] == data["metadata"]
    )
    assert historical_scopes["selection"]["workspace"] == {
        "columns": "text",
        "metadata": "text",
        "selected_origin": "columns",
    }
    assert historical_scopes["selection"]["wing"] == {
        "columns": "null",
        "metadata": None,
        "selected_origin": "caller",
    }
    assert historical_scopes["selection"]["room"] == {
        "columns": "text",
        "metadata": None,
        "selected_origin": "columns",
    }
    audit_documents = [field_document(owner, "descriptor") for owner in audit]
    assert all(document["kind"] == "AUDIT" for document in audit_documents)
    pending = [doc for doc in audit_documents if doc.get("pending_stage")]
    assert len(pending) == 1 and pending[0]["recovery"] == "pinned-archive-cursor"
    assert pending[0]["row"]["next_character"] == 8194
    assert "carry" not in pending[0]["row"] and "metadata_json" not in pending[0]["row"]
    checkpoint = next(doc for doc in audit_documents if doc["original_table"] == "checkpoints")
    assert checkpoint["row"]["value"] == ' {"offset":0} '
    observed = next(
        doc for doc in audit_documents if doc["original_table"] == "coverage_observations"
    )
    assert observed["row"]["value"] == ' {"omitted":true} '
    assert data["pending_chunk_id"] not in {entry["id"] for entry in entries}
    exported = [
        {"id": e["id"], "text": e["text"], "provenance": json.loads(e["provenance"])}
        for e in entries
        if e["kind"] == "bulk_record"
    ]
    assert sorted(exported, key=lambda item: item["id"]) == data["original_sources"]
    assert {
        p.name: (
            (p.stat().st_ino, p.stat().st_mtime_ns, p.stat().st_size),
            hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in directory.iterdir()
    } == inventory
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)


@pytest.mark.parametrize(
    "mode,phase",
    [
        ("missing", "live"),
        ("digest", "live"),
        ("offset", "live"),
        ("dual", "history"),
        ("hash", "history"),
    ],
)
def test_corrupt_members_and_original_hashes_refuse_without_advancing(tmp_path, mode, phase):
    data = fixture(tmp_path, mode=mode)
    cursor = None
    for _ in range(50):
        result = invoke(data["descriptor"], cursor)
        if result["status"] != "ok":
            assert result["status"] == "corrupt", result
            assert result["cursor_after"] == result["cursor_before"]
            assert result["phase"] == phase
            assert result["entries"] == [] and result["finalizations"] == []
            break
        cursor = result["cursor_after"]
    else:
        pytest.fail("corrupt source accepted")


def test_large_fields_resume_wire_offsets_and_stale_replacement(tmp_path):
    data = fixture(tmp_path, large=15000)
    entries, owners, slices = collect(data["descriptor"])
    assert len(slices) > 4
    assert max(s["frame_bytes"] for s in slices) <= 512 * 1024
    owner = next(o for o in owners if o["metadata_id"])
    pages = [
        e
        for e in entries
        if e["kind"] == "bulk_page"
        and json.loads(e["receipt"])["owner"] == owner["owner"]
        and json.loads(e["receipt"])["field"] == "metadata"
    ]
    raw = "".join(e["text"] for e in pages)
    assert len(raw.encode()) == owner["fields"]["metadata"]["bytes"]
    assert hashlib.sha256(raw.encode()).hexdigest() == owner["fields"]["metadata"]["hash"]
    assert any(
        s["cursor_after"]["field_byte_offset"] != s["cursor_after"]["field_char_offset"]
        for s in slices
        if s["cursor_after"]["owner"]
    )
    path = Path(data["descriptor"]["path"])
    saved = path.read_bytes()
    path.unlink()
    path.write_bytes(saved)
    path.chmod(0o600)
    refused = invoke(data["descriptor"], slices[0]["cursor_after"])
    assert refused["status"] == "stale" and refused["entries"] == []


@pytest.mark.parametrize("mode", ["orphan", "repaired"])
def test_orphan_refuses_and_repair_lineage_stays_distinct_from_occurrences(tmp_path, mode):
    data = fixture(tmp_path, mode=mode)
    cursor = None
    historical = audits = repairs = 0
    for _ in range(50):
        result = invoke(data["descriptor"], cursor)
        if result["status"] != "ok":
            assert mode == "orphan" and result["status"] == "incomplete"
            assert result["reason"] == "orphan_chunk" and result["counts"]["orphans"] == 1
            assert result["cursor_after"] == result["cursor_before"]
            return
        historical += result["counts"]["historical_occurrences"]
        audits += result["counts"]["audit_owners"]
        repairs += result["counts"]["repaired_gaps"]
        cursor = result["cursor_after"]
        if result["complete"]:
            assert mode == "repaired" and repairs == 1 and historical == 3 and audits >= 10
            return
    pytest.fail("bounded export did not finish")


def destination(directory, operation, arguments):
    from cognitive_castle import memory_runtime as runtime

    admission = test_admission(runtime)
    try:
        result = runtime._supervise(
            {"directory": str(directory), "operation": operation, "arguments": arguments},
            admission,
            worker_command=[
                sys.executable,
                "-I",
                str(ROOT / "cognitive_castle" / "lance_exact_worker.py"),
            ],
            deadline=3 if operation in {"import_batch", "remember"} else 2,
            rss_limit=192 * 1024**2,
            total_rss_limit=256 * 1024**2,
        )
    finally:
        os.close(admission)
    print(
        "DESTINATION_STAGE",
        json.dumps({k: v for k, v in result.items() if k in {"status", "reason", "resources"}}),
        flush=True,
    )
    return result


def test_real_bulk_publication_preserves_original_sources_receipt_and_scope_pages(tmp_path):
    data = fixture(tmp_path)
    descriptor = data["descriptor"]
    entries, owners, slices = collect(descriptor)
    directory = tmp_path / "destination"
    live = next(o for o in owners if o["live"])
    history = next(o for o in owners if o["metadata_id"])
    # Publish the real adapter's bounded batches unchanged, with their original
    # finalization placement. The host never imports either storage runtime.
    for ordinal, result in enumerate(slices):
        if not result["entries"] and not result["finalizations"]:
            continue
        actual = destination(
            directory,
            "import_batch",
            dict(
                batch_id="export-" + str(ordinal),
                source_binding=source_binding(descriptor),
                entries=result["entries"],
                finalizations=result["finalizations"],
            ),
        )
        assert actual["status"] == "ok", actual
    assert (
        destination(directory, "receipt", dict(namespace="operator", key="nine"))["receipt"]
        == data["receipt"]
    )
    by_id = {source["id"]: source for source in data["original_sources"]}
    original_text = "".join(by_id[identity]["text"] for identity in data["receipt"]["ids"])
    retried = destination(
        directory,
        "remember",
        dict(
            request_id="nine",
            text=original_text,
            provenance={"workspace": "original", "note": "\x00"},
            namespace="operator",
        ),
    )
    assert {k: v for k, v in retried.items() if k != "resources"} == data["receipt"]
    ids = [source["id"] for source in data["original_sources"]]
    actual_sources = []
    for start in range(0, len(ids), 8):
        response = destination(directory, "read_sources", dict(ids=ids[start : start + 8]))
        assert response["status"] == "ok", response
        actual_sources.extend(response["sources"])
    assert sorted(actual_sources, key=lambda item: item["id"]) == data["original_public"]
    assert all(s["provenance"]["storage_format"] == "sqlite-fts5-utf8-v1" for s in actual_sources)
    for owner, field in ((history, "metadata"), (live, "scopes"), (history, "descriptor")):
        expected_pages = [
            e
            for e in entries
            if e["kind"] == "bulk_page"
            and json.loads(e["receipt"])["owner"] == owner["owner"]
            and json.loads(e["receipt"])["field"] == field
        ]
        for ordinal, entry in enumerate(expected_pages):
            actual = destination(
                directory, "bulk_page", dict(owner=owner["owner"], field=field, ordinal=ordinal)
            )
            assert actual["status"] == "ok", actual
            assert actual["text"] == entry["text"]
    assert not {"sqlite3", "lance", "pyarrow", "torch"}.intersection(sys.modules)


def test_representative_export_qualification_respects_limits(tmp_path):
    fixture(tmp_path, scale_action="init")
    offset = 0
    for _ in range(26):
        offset = fixture(tmp_path, scale_action="append", offset=offset)["next_offset"]
    assert offset == 52 * 1024**2
    source = fixture(tmp_path, scale_action="seal")
    result = invoke(source["descriptor"])
    if result["status"] != "ok":
        assert result["status"] == "qualification_required", result
        assert result["reason"] in {"summary_slice_deadline", "sqlite_summary_limit"}
        assert result["cursor_before"] == result["cursor_after"]
        assert result["entries"] == [] and result["finalizations"] == []
    else:
        assert result["cursor_after"] != result["cursor_before"]
        assert not result["finalizations"] and result["counts"]["historical_occurrences"] == 0


def test_retained_scale_plan():
    retained = os.environ.get("CASTLE_TEST_RETAINED_SOURCE")
    if not retained:
        pytest.skip("explicit preserved disposable source required")
    result = fixture(Path(retained).parent, scale_action="plan")
    print("SOURCE_QUERY_PLANS", json.dumps(result["plans"]), flush=True)
    assert any("TEMP B-TREE" in row[-1] for row in result["plans"]["old"]["plan"])


def test_retained_scale_member_stream_and_changed_export():
    retained = os.environ.get("CASTLE_TEST_RETAINED_SOURCE")
    if not retained:
        pytest.skip("explicit preserved disposable source required")
    result = fixture(Path(retained).parent, scale_action="member_hash")
    assert result["members"] == 6656 and result["text_bytes"] == 52 * 1024**2
    assert result["text_hash"] == result["expected_hash"]
    # Qualification after the understood sorter defect changed. Either exact
    # bounded progress or an explicit unchanged-cursor gate is evidence; neither
    # implies whole-owner publication or destination semantic completion.
    result = fixture(Path(retained).parent, scale_action="export")
    print(
        "CHANGED_SOURCE_QUALIFICATION",
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"entries", "finalizations", "resources"}
            }
        ),
        flush=True,
    )
    if result["status"] != "ok":
        assert result["status"] == "qualification_required", result
        assert result["cursor_after"] == result["cursor_before"]
        assert result["entries"] == [] and result["finalizations"] == []
    else:
        assert result["cursor_after"] != result["cursor_before"]


@pytest.mark.parametrize(
    "mode,reason",
    [("unsupported", "unknown_source_table"), ("incomplete", "snapshot_not_complete")],
)
def test_unsupported_source_and_incomplete_registration_refuse(tmp_path, mode, reason):
    source = fixture(tmp_path, mode=mode)
    result = invoke(source["descriptor"])
    assert result["status"] == "unsupported" and result["reason"] == reason
    assert result["cursor_before"] == result["cursor_after"]
    assert result["entries"] == [] and result["finalizations"] == []


def test_empty_original_alias_is_preserved_for_each_occurrence(tmp_path):
    from cognitive_castle.canonical_records import _hash, _json

    data = fixture(tmp_path, mode="empty_alias")
    descriptor = data["descriptor"]
    binding = source_binding(descriptor)
    identity = data["history"][0]["identity"]
    cursor = initial_cursor(binding, "store")
    cursor.update(phase="history", key=[identity], owner=identity, field="metadata")
    result = invoke(descriptor, cursor)
    assert result["status"] == "ok", result
    assert result["counts"]["historical_occurrences"] == 1
    fields = {}
    for entry in result["entries"]:
        if entry["kind"] == "bulk_page":
            field = json.loads(entry["receipt"])["field"]
            fields[field] = fields.get(field, "") + entry["text"]
    document = json.loads(fields["descriptor"])
    assert document["row"]["alias"] == "" and document["aliases"] == [""]
    assert fields["columns"] == data["columns"] and json.loads(fields["columns"])["id"] == ""
    key = ["history_aliases", _json(["", identity])]
    cursor = initial_cursor(binding, "store")
    cursor.update(
        phase="audit", key=key, owner="AUDIT:" + _hash(_json(["audit", key])), field="descriptor"
    )
    result = invoke(descriptor, cursor)
    assert result["status"] == "ok", result
    assert result["counts"]["audit_owners"] == 1 and result["counts"]["historical_occurrences"] == 0
    document = json.loads("".join(entry["text"] for entry in result["entries"]))
    assert document["kind"] == "AUDIT" and document["row"] == {"alias": "", "identity": identity}
    assert document["original_key"] == ["", identity]


@pytest.mark.parametrize("mode", ["missing_attempt", "missing_attempt_table", "missing_gap_table"])
def test_history_owner_requires_original_attempt_and_table_set(tmp_path, mode):
    source = fixture(tmp_path, mode=mode)
    descriptor = source["descriptor"]
    identity = source["history"][0]["identity"]
    cursor = initial_cursor(source_binding(descriptor), "store")
    cursor.update(phase="history", key=[identity], owner=identity, field="metadata")
    result = invoke(descriptor, cursor)
    assert result["status"] == "corrupt", result
    assert result["reason"] == (
        "missing_history_attempt" if mode == "missing_attempt" else "missing_history_lineage_tables"
    )
    assert result["cursor_after"] == result["cursor_before"] == cursor
    assert result["entries"] == [] and result["finalizations"] == []
    assert not result["complete"] and not result["phase_complete"]
    assert result["counts"]["historical_occurrences"] == 0


def test_repaired_history_owner_and_resolution_keep_valid_lineage(tmp_path):
    from cognitive_castle.canonical_records import _hash, _json

    source = fixture(tmp_path, mode="repaired")
    descriptor = source["descriptor"]
    binding = source_binding(descriptor)
    identity = source["history"][0]["identity"]
    cursor = initial_cursor(binding, "store")
    cursor.update(phase="history", key=[identity], owner=identity, field="metadata")
    result = invoke(descriptor, cursor)
    assert result["status"] == "ok", result
    assert result["counts"]["historical_occurrences"] == 1 and result["phase_complete"]
    original_cursor = _hash(_json(dict(fragment=0, offset=0)))
    key = ["history_gap_resolutions", _json([original_cursor])]
    cursor = initial_cursor(binding, "store")
    cursor.update(
        phase="audit", key=key, owner="AUDIT:" + _hash(_json(["audit", key])), field="descriptor"
    )
    result = invoke(descriptor, cursor)
    assert result["status"] == "ok", result
    assert result["counts"]["repaired_gaps"] == 1 and result["counts"]["audit_owners"] == 1
    assert result["counts"]["historical_occurrences"] == 0 and result["phase_complete"]
    assert (
        result["finalizations"][0]["metadata_id"] is None
        and result["finalizations"][0]["members"] == 0
    )

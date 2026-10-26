"""Fixed remaining Task3 cases; native execution requires root qualification.

These definitions reuse the original protected SQLite source and the frozen
small-case qualification. They do not activate a production profile. No test
creates an original-source cursor or substitutes metadata for an actual proof.
The tail case has exactly520 original one-character historical chunks. Its
fixed packing has20 field pages +1040 member/record entries, then one owner and
five batch receipts:1066 physical rows. Original SQL TEXT provenance remains a
quoted JSON string in scope evidence, requiring17 scope pages.
"""

import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import sys


def _short_source_worker():
    """One fixed source setup; SQLite and all source bodies remain in child."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_worker import _admit_storage, _install_guards, _sqlite_heap_limit
    from cognitive_castle.semantic_profile import decode_frame

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(512 * 1024 + 1))
    if request.keys() != {"directory", "_host_pid"}:
        raise ValueError("fixed source fields")
    _install_guards()
    refused = _admit_storage(request)
    if refused is not None:
        return refused
    _sqlite_heap_limit()  # Existing32MiB source heap, not exact's16MiB heap.
    from cognitive_castle.fast_memory import MemoryStore
    from cognitive_castle.memory_history import _schema, _cursor_key
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.sqlite_export_codec import FORMAT, file_identity
    import sqlite3

    directory = Path(request["directory"])
    if not directory.is_absolute() or ".." in directory.parts or directory.exists():
        raise ValueError("fresh source path")
    if any(parent.is_symlink() for parent in directory.parents):
        raise ValueError("source links")
    directory.mkdir(mode=0o700)
    store = MemoryStore(directory)
    facts = dict(archive_id="tail-archive", snapshot_id="tail-original", version=1)
    identity = _hash(_json([facts["archive_id"], facts["snapshot_id"], 1, 0, 0]))
    caller = _json(dict(archive_id="tail-archive", version=1, metadata_id=identity))
    metadata, columns = "{}", '{"id":"tail"}'
    cursor = _cursor_key(dict(fragment=0, offset=0))
    first_source = None
    with store._connect(True) as conn:
        _schema(conn)
        for ordinal in range(520):
            chunk = "memory:" + _hash(_json([identity, ordinal]))
            conn.execute(
                "INSERT INTO chunks VALUES (?,?,?,?,?,?)",
                (chunk, "x", caller, _hash("x"), ordinal, ordinal + 1),
            )
            conn.execute("INSERT INTO history_occurrence_chunks VALUES (?,?)", (identity, chunk))
            if ordinal == 0:
                first_source = MemoryStore._source(
                    dict(
                        id=chunk, text="x", provenance=caller, text_hash=_hash("x"), start=0, end=1
                    )
                )
        conn.execute(
            "INSERT INTO history_occurrences VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                identity,
                cursor,
                0,
                "tail",
                metadata,
                columns,
                _hash("x" * 520),
                "[]",
                _hash(metadata),
                _hash(columns),
            ),
        )
        conn.execute("INSERT INTO history_aliases VALUES (?,?)", ("tail", identity))
        conn.execute(
            "INSERT INTO history_attempts VALUES (?,?,?)",
            (cursor, _json(dict(fragment=0, offset=1, row_address=0)), "indexed"),
        )
    path = store.db_path
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    file = file_identity(path.stat())
    with sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("PRAGMA temp_store=MEMORY")
        schema = [
            list(row)
            for row in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
            )
        ]
    manifest = dict(
        format=FORMAT,
        state="complete",
        store_id="tail-store",
        snapshot_id="tail-fixture",
        identity=file,
        content_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
        schema_hash=_hash(_json(schema)),
        original=facts,
        catalog={"version": 1},
        high_water=0,
        coverage_state="indexed-only",
    )
    manifest_path = directory / "manifest.json"
    with manifest_path.open("x") as handle:
        handle.write(_json(manifest))
        handle.flush()
        os.fsync(handle.fileno())
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    descriptor = dict(
        format=FORMAT,
        store_id="tail-store",
        snapshot_id="tail-fixture",
        path=str(path),
        identity=file,
        manifest_path=str(manifest_path),
        manifest_identity=file_identity(manifest_path.stat()),
        manifest_hash=_hash(manifest_path.read_text()),
    )
    return dict(status="ok", descriptor=descriptor, first_source=first_source, owner_key=identity)


if __name__ == "__main__":
    try:
        if sys.argv[1:] != ["source520"]:
            raise ValueError("fixed source invocation")
        output = _short_source_worker()
    except Exception as exc:
        output = dict(status="unavailable", reason="fixture_" + type(exc).__name__)
    raw = json.dumps(output, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
    if len(raw) > 16 * 1024:
        raw = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - child exits before pytest or the root test module.

import test_layout_native_integration as native  # noqa: E402


pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")

# A separate deferred-step qualification reuses every geometry/fidelity assertion.
# The original all-in-one diagnostic keeps its own fixed47-start schedule.
EXACT_TAIL_STARTS = 47

# Proposed complete retained-tree diagnostic ceiling, including source, queue,
# both registered partitions and exact/tmp; no exclusions or cleanup.
MAX_TREE_ENTRIES = 2048
MAX_TREE_BYTES = 128 * 1024**2
MAX_TREE_DEPTH = 10


def _tree(root, *, hashes=False):
    entries = {}
    logical = allocated = 0
    stack = [(Path(root), 0)]
    while stack:
        path, depth = stack.pop()
        info = path.lstat()
        directory = stat.S_ISDIR(info.st_mode)
        assert directory or stat.S_ISREG(info.st_mode), path
        assert info.st_uid == os.getuid() and not info.st_mode & 0o077, path
        assert directory or info.st_nlink == 1, path
        assert depth <= MAX_TREE_DEPTH, path
        logical += info.st_size
        allocated += info.st_blocks * 512
        assert max(logical, allocated) <= MAX_TREE_BYTES
        relative = str(path.relative_to(root))
        entries[relative] = (
            info.st_dev,
            info.st_ino,
            info.st_mode,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest() if hashes and not directory else None,
        )
        assert len(entries) <= MAX_TREE_ENTRIES
        if directory:
            stack.extend((child, depth + 1) for child in path.iterdir())
    return entries


def _exact(generation, operation, arguments):
    with native._session(generation) as session:
        return native.ObservedLayoutSession(session).exact(generation, operation, arguments)


def _maintenance(generation, *, before_version, physical_rows, fragments):
    prepared = native._queue(
        generation,
        "queue_layout_maintenance_prepare",
        dict(partition_id=0, version=before_version),
    )
    assert prepared["status"] == "ok", prepared
    maintained = _exact(generation, "layout_maintain", dict(partition_id=0))
    assert maintained["status"] == "ok", maintained
    pin = maintained["pin"]
    assert (pin["version"], pin["physical_rows"], pin["fragments"]) == (
        before_version + 1,
        physical_rows,
        fragments,
    )
    assert 0 < pin["index"]["version"] <= pin["version"]
    assert {key: value for key, value in pin["index"].items() if key != "version"} == dict(
        segments=1, indexed_fragments=fragments, tail_fragments=0, tail_rows=0
    )
    # A new supervised worker reopens persisted storage, with no live Dataset
    # object inherited from maintenance. The page is index confirmation only;
    # its first32 routes are not represented as a complete enumeration.
    reopened = native._read(
        generation,
        "directory_page",
        dict(partition_id=0, version=before_version + 1, cursor=0),
    )
    assert reopened["status"] == "ok" and reopened["page"]["pin"] == pin, reopened
    assert len(reopened["page"]["rows"]) == min(32, physical_rows)
    assert reopened["page"]["complete"] is (physical_rows <= 32)
    confirmed = native._queue(generation, "queue_layout_reconcile", dict(page=reopened["page"]))
    assert confirmed["status"] == "ok" and confirmed["registry_ready"] is True, confirmed
    return pin


def _same_frontier(before, after):
    for key in (
        "pending",
        "counts",
        "staging_cursor",
        "completed_frontier",
        "last_ack",
        "owner_references",
        "semantic_pending",
    ):
        assert after[key] == before[key], key


def test_native_layout_bootstrap_then_optimize_persists_without_source_progress(tmp_path):
    """Exactly33 starts; actual original LIVE and first HISTORY, no source EOF."""
    first_call = native._CALLS
    _tree(tmp_path)
    source, generation, registration, _ = native._setup(tmp_path)
    published, live = native._tick(registration)
    assert published["status"] == "ok", published
    live_routes = live.results["layout_import_batch:2"]["routes"]
    assert {row["version"] for row in live_routes} == {1}
    live_owner = live.arguments["layout_import_batch:1"][2]["finalizations"][0]
    before = native._queue_state(registration)
    initial = _maintenance(
        generation, before_version=1, physical_rows=len(live_routes), fragments=1
    )
    published, history = native._tick(registration)
    assert published["status"] == "ok", published
    _same_frontier(before, history.results["queue_layout_resume:1"])
    history_routes = history.results["layout_import_batch:2"]["routes"]
    assert {row["version"] for row in history_routes} == {3}
    assert history.arguments["layout_import_batch:1"][2]["finalizations"][0]["live"] is None
    after_history = native._queue_state(registration)
    optimized = _maintenance(
        generation,
        before_version=3,
        physical_rows=len(live_routes) + len(history_routes),
        fragments=2,
    )
    assert initial["index"]["segments"] == optimized["index"]["segments"] == 1
    after = native._queue_state(registration)
    _same_frontier(after_history, after)
    # Only the intervening genuine history import advances the source frontier.
    assert after["counts"]["live_owners"] == before["counts"]["live_owners"] == 1
    assert after["counts"]["historical_occurrences"] == 1
    assert after["pending"] is None
    live_receipt = native._read(generation, "receipt", dict(namespace="operator", key="nine"))
    assert live_receipt == dict(
        status="ok", digest=live_owner["live"]["digest"], receipt=source["receipt"]
    )
    restored = native._read(generation, "read_sources", dict(ids=[source["receipt"]["ids"][0]]))
    assert restored == dict(
        status="ok",
        sources=[
            next(
                row for row in source["original_public"] if row["id"] == source["receipt"]["ids"][0]
            )
        ],
    )
    assert native._CALLS - first_call == 33
    _tree(tmp_path)


class _UnderestimatedPlan(native.ObservedLayoutSession):
    """Test-only planner fault; unchanged real source must reject at export."""

    def layout_source(self, descriptor, cursor, window):
        if window["action"] != "plan":
            previous = len(self.session.resources)
            call = native._start("underestimated_export")
            result = self.session.layout_source(descriptor, cursor, window)
            native._record(
                self.session,
                "underestimated_export",
                result,
                previous,
                call,
                expected_reason="layout_owner_plan_mismatch",
            )
            self.calls.append("underestimated_export")
            self.results["underestimated_export"] = copy.deepcopy(result)
            return result
        result = super().layout_source(descriptor, cursor, window)
        assert result["status"] == "ok" and len(result["plans"]) == 1, result
        changed = copy.deepcopy(result)
        # Original owner identity/bytes/member counts stay intact; only the
        # otherwise well-formed numerical reservation is underestimated.
        changed["plans"][0]["expansion"]["physical_rows"] -= 1
        return changed


def test_native_layout_underestimated_owner_refuses_before_prepare_or_native_write(tmp_path):
    """Exactly8 starts; fixture fault never becomes an accepted source proof."""
    from cognitive_castle.migration_coordinator import _layout_fixture_tick

    first_call = native._CALLS
    _tree(tmp_path)
    _, generation, registration, _ = native._setup(tmp_path)
    before = native._queue_state(registration)
    files = _tree(generation / "exact/partitions", hashes=True)
    with native._session(generation) as session:
        observed = _UnderestimatedPlan(session)
        refused = _layout_fixture_tick(observed, registration)
    assert refused["status"] == "conflict", refused
    assert refused["reason"] == "layout_owner_plan_mismatch" and refused["phase"] == "source"
    assert observed.calls == [
        "queue_layout_resume:1",
        "layout_source:1",
        "queue_layout_bind:1",
        "underestimated_export",
    ]
    raw = observed.results["underestimated_export"]
    assert raw["plans"] == [] and raw["export"] is None
    assert raw["cursor_before"] == raw["cursor_after"]
    assert _tree(generation / "exact/partitions", hashes=True) == files
    after = native._queue_state(registration)
    _same_frontier(before, after)
    assert native._CALLS - first_call == 8
    _tree(tmp_path)


def test_native_layout_retained_temp_admission_preserves_prepared_intent(tmp_path):
    """Exactly10 starts; retained6MiB + required4MiB exceeds the fixed9MiB."""
    first_call = native._CALLS
    _tree(tmp_path)
    _, generation, registration, _ = native._setup(tmp_path)
    before = native._queue_state(registration)
    files = _tree(generation / "exact/partitions", hashes=True)
    retained = generation / "exact/tmp/retained-charge"
    fd = os.open(retained, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        block = b"\0" * 65536
        for _ in range(96):
            position = 0
            while position < len(block):
                written = os.write(fd, block[position:])
                assert written > 0
                position += written
        os.fsync(fd)
    finally:
        os.close(fd)
    retained_before = _tree(generation / "exact/tmp", hashes=True)
    refused, observed = native._tick(registration)
    assert refused["status"] == "unavailable" and refused["phase"] == "publish", refused
    assert refused.get("reason") is None, refused
    assert observed.calls == [
        "queue_layout_resume:1",
        "layout_source:1",
        "queue_layout_bind:1",
        "layout_source:2",
        "queue_layout_prepare:1",
        "layout_import_batch:1",
    ]
    assert observed.results["layout_import_batch:1"] == {"status": "unavailable"}
    assert _tree(generation / "exact/partitions", hashes=True) == files
    assert _tree(generation / "exact/tmp", hashes=True) == retained_before
    assert retained.stat().st_size == 6 * 1024**2
    after = native._queue_state(registration)
    assert after["pending"] == observed.arguments["queue_layout_prepare:1"][2]["intent"]
    assert after["pending"] is not None
    # Keep the real prepared claims/intent as unresolved debt. No source ACK or
    # user-visible owner is inferred from the retained artifact refusal.
    _same_frontier({**before, "pending": after["pending"]}, after)
    assert native._CALLS - first_call == 10
    _tree(tmp_path)


def test_native_layout_foreign_stage_and_changed_provenance_preserve_pending(tmp_path):
    """Fixed real lost append; no altered frame may change native storage."""
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle.canonical_records import CanonicalRecord, _json

    first_call = native._CALLS
    _tree(tmp_path)
    source, generation, registration, envelope = native._setup(tmp_path)
    lost, original = native._tick(registration, loss="layout_import_batch:1")
    assert lost == dict(
        status="unavailable",
        reason="fixture_lost_response",
        phase="publish",
        intent_id=original.arguments["layout_import_batch:1"][2]["intent_id"],
        cursor_before=lost["cursor_before"],
        cursor_after=lost["cursor_before"],
        coverage="incomplete",
    ), lost
    pending = native._queue_state(registration)
    files = _tree(generation / "exact/partitions", hashes=True)
    original_args = original.arguments["layout_import_batch:1"][2]
    changed = copy.deepcopy(original_args)
    row = next(row for row in changed["entries"] if row["kind"] == "bulk_record")
    original_record = CanonicalRecord(row["id"], row["text"], row["provenance"])
    provenance = json.loads(row["provenance"])
    provenance["caller"]["workspace"] = "changed-canonical-provenance"
    row["provenance"] = _json(provenance)
    assert CanonicalRecord(row["id"], row["text"], row["provenance"]) != original_record
    bulk.validate_batch(
        changed["batch_id"], changed["source_binding"], changed["entries"], changed["finalizations"]
    )
    rejected = _exact(generation, "layout_import_batch", changed)
    assert rejected == {"status": "conflict"}, rejected
    assert _tree(generation / "exact/partitions", hashes=True) == files
    shared, foreign_registration = native._attach_shared_source(
        tmp_path / "foreign", generation, envelope
    )
    assert shared["receipt"] == source["receipt"]
    assert foreign_registration["source_binding"] != registration["source_binding"]
    refused, foreign = native._tick(foreign_registration)
    assert refused["status"] == "conflict" and refused["phase"] == "bind", refused
    assert foreign.calls == ["queue_layout_resume:1", "layout_source:1", "queue_layout_bind:1"]
    assert _tree(generation / "exact/partitions", hashes=True) == files
    _same_frontier(pending, native._queue_state(registration))
    foreign_state = native._queue_state(foreign_registration)
    assert foreign_state["pending"] is None and foreign_state["last_ack"] is None
    assert foreign_state["counts"]["live_owners"] == foreign_state["owner_references"] == 0
    resumed, replay = native._tick(registration)
    assert resumed["status"] == "ok", resumed
    assert replay.arguments["layout_import_batch:1"][2] == original_args
    assert replay.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    assert _tree(generation / "exact/partitions", hashes=True) == files
    final = native._queue_state(registration)
    assert final["pending"] is None and final["counts"]["live_owners"] == 1
    assert native._CALLS - first_call == 23
    _tree(tmp_path)


def _short_setup(tmp_path):
    from cognitive_castle import migration_codec as migration

    with native._session(tmp_path) as session:
        call = native._start("source520")
        source = session._run_stage(
            dict(directory=str(tmp_path / "source")),
            worker_command=[sys.executable, "-I", str(Path(__file__).resolve()), "source520"],
            cap=2,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        native._record(session, "source520", source, 0, call)
    assert source["status"] == "ok", source
    generation = tmp_path / "generation"
    for path in (
        generation,
        generation / "exact",
        generation / "exact/partitions",
        generation / "exact/tmp",
    ):
        path.mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    for partition in range(envelope["max_partitions"]):
        (generation / "exact/partitions" / str(partition)).mkdir(mode=0o700)
    registration = migration.registration(source["descriptor"], str(generation))
    with native._session(generation) as session:
        call = native._start("operator_register520")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="register",
                arguments=dict(
                    registration=registration, envelope=envelope, qualification=qualification
                ),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(native.__file__).resolve()),
                "operator",
                envelope["qualification_hash"],
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        native._record(session, "operator_register520", result, 0, call)
    assert result["status"] == "ok", result
    return source, generation, registration


def test_native_layout_exact_tail_refuses_then_bootstraps_and_resumes_original_interval(tmp_path):
    """Exactly47 starts:3 actual stages, fixed fourth refusal, index,2 stages."""
    first_call = native._CALLS
    _tree(tmp_path)
    source, generation, registration = _short_setup(tmp_path)
    original_source_files = _tree(tmp_path / "source", hashes=True)
    for ordinal in range(3):  # Fixed schedule, not a sizing probe or retry loop.
        staged, observed = native._tick(registration)
        assert staged["status"] == "ok", staged
        arguments = observed.arguments["layout_import_batch:1"][2]
        assert len(arguments["entries"]) == 256 and arguments["finalizations"] == []
        routes = observed.results["layout_import_batch:2"]["routes"]
        assert len(routes) == 257 and {row["version"] for row in routes} == {ordinal + 1}
        if ordinal == 0:
            plan = observed.arguments["queue_layout_bind:1"][2]["plans"][0]
            assert plan["members"] == 520 and plan["expansion"]["physical_rows"] == 1066
            assert plan["expansion"]["batches"] == plan["expansion"]["fragments"] == 5
    before = native._queue_state(registration)
    assert before["pending"] is None and before["staging_cursor"]["entry"] == 768
    assert before["counts"]["historical_occurrences"] == before["owner_references"] == 0
    files = _tree(generation / "exact/partitions", hashes=True)
    refused, fourth = native._tick(registration)
    assert refused["status"] == "index_required" and refused["phase"] == "prepare", refused
    assert fourth.calls == ["queue_layout_resume:1", "layout_source:1", "queue_layout_prepare:1"]
    intent = fourth.arguments["queue_layout_prepare:1"][2]["intent"]
    assert intent["cursor_before"] == before["staging_cursor"]
    assert intent["entries"] == 256 and len(intent["routes"]) == 257
    assert _tree(generation / "exact/partitions", hashes=True) == files
    _same_frontier(before, native._queue_state(registration))
    pin = _maintenance(generation, before_version=3, physical_rows=771, fragments=3)
    assert pin["index"]["tail_rows"] == 0
    resumed, retry = native._tick(registration)
    assert resumed["status"] == "ok", resumed
    assert retry.arguments["queue_layout_prepare:1"] == fourth.arguments["queue_layout_prepare:1"]
    assert retry.arguments["layout_source:1"] == fourth.arguments["layout_source:1"]
    assert {row["version"] for row in retry.results["layout_import_batch:2"]["routes"]} == {5}
    published, final = native._tick(registration)
    assert published["status"] == "ok", published
    final_args = final.arguments["layout_import_batch:1"][2]
    assert len(final_args["entries"]) == 36 and len(final_args["finalizations"]) == 1
    owner = final_args["finalizations"][0]
    assert {field: facts["pages"] for field, facts in owner["fields"].items()} == dict(
        metadata=1, columns=1, scopes=17, descriptor=1
    )
    assert owner["members"] == owner["text_bytes"] == owner["text_chars"] == 520
    assert owner["text_hash"] == hashlib.sha256(b"x" * 520).hexdigest() and owner["live"] is None
    assert final.results["layout_import_batch:2"]["outcome"] == "existing_verified"
    native._assert_grouped_proof(
        final, final.arguments["queue_layout_prepare:1"][2]["intent"], owner, 6
    )
    state = native._queue_state(registration)
    assert state["pending"] is None and state["counts"]["historical_occurrences"] == 1
    assert state["counts"]["members"] == 520 and state["owner_references"] == 1
    assert state["last_ack"]["intent_id"] == final_args["intent_id"]
    actual = native._read(generation, "bulk_owner", dict(owner=owner["owner"], version=6))
    assert actual["status"] == "ok" and actual["manifest"] == owner and actual["version"] == 6
    restored = native._read(generation, "read_sources", dict(ids=[source["first_source"]["id"]]))
    assert (
        restored["status"] == "ok" and restored["sources"][0]["id"] == source["first_source"]["id"]
    )
    assert restored["sources"][0]["text"] == source["first_source"]["text"]
    assert restored["sources"][0]["provenance"] == source["first_source"]["provenance"]
    assert native._CALLS - first_call == EXACT_TAIL_STARTS
    assert _tree(tmp_path / "source", hashes=True) == original_source_files
    _tree(tmp_path)

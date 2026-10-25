"""One synthetic metadata recovery fixture; root grants its execution separately."""

import hashlib
import json
import os
from pathlib import Path
import stat
import sys


def _census(generation):
    root = generation.lstat()
    if not stat.S_ISDIR(root.st_mode) or root.st_uid != os.getuid() or root.st_mode & 0o077:
        raise ValueError("private fixture root")
    logical, allocated, entries = root.st_size, root.st_blocks * 512, 1
    pending = [(generation, 0)]
    while pending:
        directory, depth = pending.pop()
        if depth > 6:
            raise ValueError("fixture depth")
        with os.scandir(directory) as scan:
            for entry in scan:
                entries += 1
                if entries > 32:
                    raise ValueError("fixture entry bound")
                info = entry.stat(follow_symlinks=False)
                if info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise ValueError("private fixture")
                logical += info.st_size
                allocation = info.st_blocks * 512
                allocated += allocation
                if logical > 8 * 1024**2 or allocated > 8 * 1024**2:
                    raise ValueError("fixture total bytes")
                if stat.S_ISDIR(info.st_mode):
                    pending.append((Path(entry.path), depth + 1))
                elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                    maximum = (
                        64 * 1024 if entry.name == "layout-qualification.json" else 2 * 1024**2
                    )
                    if info.st_size > maximum or allocation > maximum:
                        raise ValueError("fixture byte bound")
                else:
                    raise ValueError("fixture file type")
    return dict(entries=entries, logical_bytes=logical, allocated_bytes=allocated)


def _bounded_rows(conn, table):
    columns = {
        "layout_claims": (
            "id",
            "claim",
            "job_id",
            "partition_id",
            "gate",
            "intent_id",
            "state",
            "seen_token",
        ),
        "layout_routes": (
            "id",
            "kind",
            "row_hash",
            "partition_id",
            "gate",
            "intent_id",
            "job_id",
            "version",
            "receipt",
            "state",
        ),
    }[table]
    projection = ",".join(f"typeof({c}),length(CAST({c} AS BLOB))" for c in columns)
    sizes = conn.execute(f"SELECT {projection} FROM {table} LIMIT 17").fetchall()
    if len(sizes) > 16 or any(
        row[i] not in {"text", "integer", "null"}
        or row[i] != "null"
        and (row[i + 1] is None or row[i + 1] > 4096)
        for row in sizes
        for i in range(0, len(row), 2)
    ):
        raise ValueError("fixture metadata bound")
    return [list(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY id LIMIT 17")]


def _worker(action, expected_hash):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle import semantic_store_worker as worker
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_directory import _NativeLayoutDirectory
    from cognitive_castle.semantic_profile import decode_frame
    from cognitive_castle.semantic_queue import SemanticQueue

    os.umask(0o077)
    request = decode_frame(sys.stdin.buffer.read(worker.MAX_FRAME + 1))
    if (
        request.keys() != {"directory", "operation", "arguments", "_host_pid"}
        or request["operation"] != action
        or action not in {"snapshot", "crash", "readonly776"}
        or request["arguments"].keys() != {"registration"}
    ):
        raise ValueError("fixed recovery fields")
    worker._install_guards("queue")
    refusal = worker._admit(request, "queue")
    if refusal is not None:
        return refusal
    import sqlite3

    with sqlite3.connect(":memory:") as heap:
        assert 0 < heap.execute("PRAGMA hard_heap_limit=16777216").fetchone()[0] <= 16777216
    registration = migration.validate_registration(request["arguments"]["registration"])
    generation = Path(request["directory"])
    if str(generation) != registration["destination"]:
        raise ValueError("fixed generation")
    store = SemanticQueue(generation, writable=action == "crash")
    _census(generation)
    if action == "readonly776":
        try:
            with store._connection(False) as conn:
                conn.execute("PRAGMA user_version").fetchone()
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", None) == 776:
                return dict(status="ok", sqlite_errorcode=776)
            raise
        raise ValueError("hot journal not established")
    context = store._layout_resume_connection() if action == "crash" else store._connection(False)
    with context as conn:
        authority = _NativeLayoutDirectory(conn)
        if authority.envelope["qualification_hash"] != expected_hash:
            raise ValueError("trusted qualification")
        state = store._layout_state(conn, registration["job_id"], authority)
        if state["registration"] != registration or state["pending"] is None:
            raise ValueError("registered pending fixture")
        row = store._migration_row(conn, registration["job_id"])
        raw = row["pending"].encode("utf-8")
        if len(raw) > 128 * 1024:
            raise ValueError("fixed pending bound")
        if action == "snapshot":
            claims = _bounded_rows(conn, "layout_claims")
            routes = _bounded_rows(conn, "layout_routes")
            if len(claims) != len(state["pending"]["routes"]) or len(routes) != len(claims):
                raise ValueError("fixed pending cardinality")
            return dict(
                status="ok",
                state=state,
                pending_text_bytes=len(raw),
                pending_text_sha256=hashlib.sha256(raw).hexdigest(),
                claims_count=len(claims),
                routes_count=len(routes),
                claims_routes_sha256=migration.hashed(dict(claims=claims, routes=routes)),
            )
        conn.execute("PRAGMA cache_size=2")
        conn.execute("PRAGMA main.cache_spill=2")
        assert conn.execute("PRAGMA cache_size").fetchone()[0] == 2
        assert conn.execute("PRAGMA main.cache_spill").fetchone()[0] == 2
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
        conn.execute(
            "UPDATE migration_jobs SET pending=? WHERE job_id=?",
            (row["pending"] + " " * (64 * 1024), registration["job_id"]),
        )
        journal = generation / "coordination/queue.sqlite3-journal"
        journal_bytes = journal.stat().st_size
        if not 0 < journal_bytes <= 2 * 1024**2:
            raise ValueError("fixed rollback journal")
        _census(generation)
        sys.stdout.write(
            json.dumps(dict(status="ok", crash_exit=True, journal_bytes=journal_bytes))
        )
        sys.stdout.flush()
        os._exit(0)  # Deliberate one-shot crash: no SQLite close, rollback or commit.


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            raise ValueError("fixed recovery argv")
        output = _worker(sys.argv[1], sys.argv[2])
    except Exception as exc:
        output = dict(status="unavailable", reason="fixture_" + type(exc).__name__)
    encoded = json.dumps(output, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > 512 * 1024:
        encoded = b'{"status":"unavailable","reason":"fixture_frame"}'
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()
    raise SystemExit

import pytest  # noqa: E402 - fixed supervised workers exit before pytest imports.


@pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")
def test_native_layout_resume_recovers_one_pending_hot_journal(tmp_path):
    import shutil
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.semantic_runtime import SemanticSession
    from test_layout_native_integration import _qualification
    import test_layout_native_integration as root_fixture
    from test_migration_codec import descriptor, exported

    assert shutil.disk_usage(tmp_path).free >= 2 * 1024**3
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    for relative in (
        "exact",
        "exact/partitions",
        "exact/tmp",
        "exact/partitions/0",
        "exact/partitions/1",
    ):
        (generation / relative).mkdir(mode=0o700)
    qualification, envelope = _qualification(generation)
    registration = migration.registration(descriptor(), str(generation))
    result = exported(registration)
    counters = layout.LayoutPlanningCounters(qualification)
    for row in result["entries"]:
        counters.add_entry(row)
    plan = counters.finish(
        registration, result["finalizations"][0], result["phase"], result["cursor_after"]["key"]
    )
    target = layout.target(envelope, 0, [plan])
    intent = layout.prepare_layout_interval(registration, result, target)
    calls = []

    def invoke(operation, arguments, *, custom=None):
        assert len(calls) < 9
        calls.append(operation)
        print(
            "LAYOUT_RESUME_START " + json.dumps(dict(call=len(calls), operation=operation)),
            flush=True,
        )
        with SemanticSession(generation / "unused-profile", interactive=False) as session:
            if custom is None:
                answer = session.store(generation, operation, arguments)
            else:
                command = [
                    sys.executable,
                    "-I",
                    str(custom),
                    "operator" if operation == "register" else operation,
                    envelope["qualification_hash"],
                ]
                answer = session._run_stage(
                    dict(directory=str(generation), operation=operation, arguments=arguments),
                    worker_command=command,
                    cap=2 if operation in {"snapshot", "readonly776"} else 3,
                    rss=128 * 1024**2,
                    total=192 * 1024**2,
                )
            metrics = session.resources
            diagnostic = dict(
                call=len(calls),
                operation=operation,
                status=answer.get("status"),
                reason=answer.get("reason"),
                resources=metrics,
            )
            print(
                "LAYOUT_RESUME_STAGE " + json.dumps(diagnostic, separators=(",", ":")), flush=True
            )
            assert (
                len(metrics) == 1 and answer.get("reason") is None and answer.get("status") == "ok"
            ), diagnostic
        _census(generation)
        return answer

    invoke(
        "register",
        dict(registration=registration, envelope=envelope, qualification=qualification),
        custom=Path(root_fixture.__file__),
    )
    invoke(
        "queue_layout_bind", dict(job_id=registration["job_id"], plans=[plan], envelope=envelope)
    )
    invoke(
        "queue_layout_prepare",
        dict(job_id=registration["job_id"], expected_cursor=intent["cursor_before"], intent=intent),
    )
    arguments = dict(registration=registration)
    baseline = invoke("snapshot", arguments, custom=Path(__file__).resolve())
    assert invoke("crash", arguments, custom=Path(__file__).resolve())["crash_exit"] is True
    assert (
        invoke("readonly776", arguments, custom=Path(__file__).resolve())["sqlite_errorcode"] == 776
    )
    resumed = invoke("queue_layout_resume", arguments)
    assert resumed == baseline["state"]
    restored = invoke("snapshot", arguments, custom=Path(__file__).resolve())
    assert restored == baseline
    assert invoke("queue_layout_resume", arguments) == baseline["state"]
    assert len(calls) == 9

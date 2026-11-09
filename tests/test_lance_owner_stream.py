"""Hash-bound dedicated fixtures; native imports only in guarded child workers."""

import json
import os
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
# Root fills this literal only after reviewing/finalizing the private packet.
# Configuration does not pin this file inside itself; the outer seal pins both.
FROZEN_LAYOUT52_CONFIG_SHA256 = "e9e2a918daa8327948e89dc3ca0a966dda72b0212e51613a05fe9375bcd74f12"  # Conditional persisted protocol requires root freeze.


def _fixture_support():
    import importlib.util

    path = Path(__file__).with_name("lance_owner_stream_fixture.py")
    spec = importlib.util.spec_from_file_location("layout52_fixture_harness", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ordered_execute(request):
    from cognitive_castle import bulk_exact_codec as codec
    from cognitive_castle.bulk_exact_store import OrderedBulkExactStore, _ReplayOutcomeStore
    from cognitive_castle.bulk_owner_stream import OrderedQualification
    from cognitive_castle.lance_exact_store import LanceExactStore

    if len(sys.argv) not in {3, 6} or request.keys() != {
        "directory",
        "operation",
        "arguments",
        "qualification",
        "qualification_hash",
        "_host_pid",
    }:
        raise ValueError("ordered request")
    reference, digest = sys.argv[1:3]
    if request["qualification"] != reference or request["qualification_hash"] != digest:
        raise ValueError("frozen binding")
    arguments = request["arguments"]
    operation = request["operation"]
    fields = {
        "ordered_import_batch": ({"batch_id", "source_binding", "entries", "finalizations"}, set()),
        "ordered_import_batch_outcome": (
            {"batch_id", "source_binding", "entries", "finalizations"},
            set(),
        ),
        "ordered_receipt": ({"namespace", "key"}, set()),
        "ordered_build_id_index": (set(), set()),
        "ordered_bulk_owner": ({"owner"}, {"version"}),
        "ordered_bulk_receipt": ({"batch_id", "source_binding"}, {"offset", "version"}),
    }
    if operation not in fields or not isinstance(arguments, dict):
        raise ValueError("ordered arguments")
    required, optional = fields[operation]
    if not required <= arguments.keys() or arguments.keys() - required - optional:
        raise ValueError("ordered arguments")
    if operation in {"ordered_import_batch", "ordered_import_batch_outcome"}:
        codec.validate_batch(**arguments)
    # This fixed executable's argv binds the qualification; its request cannot
    # replace the path, digest, destination or ceilings. Read at most 64 KiB.
    path = Path(reference)
    if not path.is_absolute() or any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("qualification path")
    LanceExactStore._private(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if os.fstat(descriptor).st_size > 64 * 1024:
            raise ValueError("qualification bytes")
        raw = os.read(descriptor, 64 * 1024 + 1)
    finally:
        os.close(descriptor)
    qualification = OrderedQualification(raw.decode("utf-8"), digest)
    if request["directory"] != qualification.destination:
        raise ValueError("qualification destination")
    if operation == "ordered_bulk_owner" and arguments["owner"] != qualification.owner:
        raise ValueError("qualification owner")
    if (
        operation == "ordered_bulk_receipt"
        and arguments["source_binding"] != qualification.source_binding
    ):
        raise ValueError("qualification source")
    store_type = (
        _ReplayOutcomeStore
        if operation == "ordered_import_batch_outcome"
        else OrderedBulkExactStore
    )
    store = store_type(
        Path(request["directory"]),
        qualification=qualification,
        writable=operation
        in {"ordered_import_batch", "ordered_import_batch_outcome", "ordered_build_id_index"},
    )

    def invoke_actual():
        if operation == "ordered_receipt":
            return store.receipt(**arguments)
        if operation == "ordered_bulk_owner":
            return store.bulk_owner(**arguments)
        if operation == "ordered_bulk_receipt":
            return store.bulk_receipt(**arguments)
        if operation == "ordered_build_id_index":
            return store.build_id_index()
        if arguments["source_binding"] != qualification.source_binding:
            raise ValueError("qualification source")
        for row in arguments["entries"]:
            store._bound_row(row)
        for owner in arguments["finalizations"]:
            if (owner["owner"], owner["source_binding"]) != (
                qualification.owner,
                qualification.source_binding,
            ):
                raise ValueError("qualification owner")
        return store.import_batch(**arguments)

    if len(sys.argv) == 3:
        temporary = Path(reference).parent / "native-tmp"
        profile = None
        if operation in {"ordered_import_batch_outcome", "ordered_receipt"} or (
            operation == "ordered_bulk_owner" and temporary.exists()
        ):
            profile = _fixture_support().set_exact_environment(temporary)
        result = invoke_actual()
        if profile:
            result["fixture_profile"] = profile
        return result
    configuration, configuration_hash, temporary = sys.argv[3:]
    support = _fixture_support()
    fixed_path = support.CONFIGURATION_PATH
    if configuration_hash != FROZEN_LAYOUT52_CONFIG_SHA256 or Path(configuration) != fixed_path:
        raise ValueError("missing frozen large run binding")
    config = support.load_frozen_config(configuration, configuration_hash)
    return support.exact_worker_call(
        config, qualification, store, operation, temporary, invoke_actual
    )


def _fixture_main():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.lance_exact_worker import (
        MAX_FRAME,
        _admit_storage,
        _execute,
        _install_guards,
    )
    from cognitive_castle.lance_exact_store import _StoreError

    _install_guards()
    os.umask(0o077)
    try:
        frame = sys.stdin.buffer.read(MAX_FRAME + 1)
        if len(frame) > MAX_FRAME:
            raise ValueError("frame")
        request = json.loads(frame)
        if not isinstance(request, dict):
            raise ValueError("request")
        result = _admit_storage(request)
        if result is None:
            if request.get("operation") in {
                "ordered_import_batch",
                "ordered_import_batch_outcome",
                "ordered_receipt",
                "ordered_build_id_index",
                "ordered_bulk_owner",
                "ordered_bulk_receipt",
            }:
                result = _ordered_execute(request)
            elif len(sys.argv) == 1:
                result = _execute(request)
            else:
                raise ValueError("fixture operation")
    except _StoreError as error:
        result = {"status": error.status}
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result = {"status": "unavailable", "reason": "invalid_exact_request"}
    except Exception as error:
        result = {"status": "unavailable", "reason": "fixture_" + type(error).__name__}
    result["fixture_native_imported"] = bool({"lance", "pyarrow"}.intersection(sys.modules))
    output = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(output) > MAX_FRAME:
        output = b'{"status":"unavailable","reason":"worker_output_limit"}'
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    _fixture_main()
    raise SystemExit(0)

import pytest  # noqa: E402
from cognitive_castle import bulk_exact_codec as codec  # noqa: E402
from cognitive_castle.canonical_records import _hash, _json, prepare_remember  # noqa: E402
from test_bulk_owner_stream import qualification_manifest  # noqa: E402

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned-group supervision")
ROOT = Path(__file__).resolve().parents[1]
WORKER = Path(__file__).resolve()


def freeze(directory, owner, version, physical_rows, fragments, **changes):
    manifest = qualification_manifest(directory, owner)
    manifest.update(
        expected_states=[
            {"version": version, "physical_rows": physical_rows, "fragments": fragments}
        ],
        max_physical_rows=256,
        max_fragments=16,
        max_versions=32,
        max_artifact_entries=128,
        max_artifact_depth=6,
        max_logical_bytes=8 * 1024**2,
        max_allocated_bytes=8 * 1024**2,
        max_file_bytes=1024**2,
        max_batch_receipts=16,
        reservations={
            operation: {
                "physical_rows": 32,
                "fragments": 1,
                "versions": 1,
                "artifact_entries": 16,
                "logical_bytes": 1024**2,
                "allocated_bytes": 1024**2,
                "file_bytes": 512 * 1024,
            }
            for operation in ("append", "maintenance")
        },
    )
    manifest.update(changes)
    digest = _hash(_json(manifest))
    reference = directory.parent / (directory.name + "-" + digest + ".qualification.json")
    reference.write_text(_json(manifest))
    reference.chmod(0o600)
    return reference, digest


def invoke(
    directory, arguments, bound=None, *, operation="ordered_import_batch", request_changes=None
):
    from cognitive_castle import memory_runtime as runtime

    request = {"directory": str(directory), "operation": operation, "arguments": arguments}
    command = [sys.executable, "-I", str(WORKER)]
    if bound:
        reference, digest = bound
        command.extend([str(reference), digest])
        request.update(qualification=str(reference), qualification_hash=digest)
    request.update(request_changes or {})
    budget = (
        3
        if operation
        in {
            "ordered_import_batch",
            "ordered_import_batch_outcome",
            "ordered_build_id_index",
            "import_batch",
            "build_id_index",
            "remember",
            "import_sources",
        }
        else 2
    )
    admission = runtime._admission()
    try:
        result = runtime._supervise(
            request,
            admission,
            worker_command=command,
            deadline=budget,
            absolute_deadline=time.monotonic() + budget,
            background_preempt=True,
            rss_limit=192 * 1024**2,
            total_rss_limit=256 * 1024**2,
        )
    finally:
        os.close(admission)
    print(
        "ORDERED_STAGE",
        json.dumps(
            {
                "operation": operation,
                **{key: value for key, value in result.items() if key != "sources"},
            }
        ),
        flush=True,
    )
    return {key: value for key, value in result.items() if key != "resources"}


def arguments(rows, owners=(), batch="batch"):
    return {
        "batch_id": batch,
        "source_binding": "snapshot",
        "entries": rows,
        "finalizations": list(owners),
    }


def canonical_files(directory):
    return {
        str(path.relative_to(directory)): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            _hash(path.read_bytes().hex()),
        )
        for path in directory.rglob("*")
        if path.is_file()
    }


def test_pinned_stream_finalization_and_replay(tmp_path):
    directory = tmp_path / "ordered"
    directory.mkdir(mode=0o700)
    original = "ž😀\x00\r\n" * 2000 + "late"
    prepared = prepare_remember("ordered-native", original)
    rows, owner = codec.prepare_owner(
        "ordered",
        "snapshot",
        prepared.records,
        metadata="ž😀\x00 " * 1500,
        live={
            "namespace": prepared.namespace,
            "key": prepared.request_id,
            "digest": prepared.digest,
            "receipt": prepared.receipt,
        },
    )
    split = (
        next(
            i
            for i, row in enumerate(rows)
            if row["kind"] == "bulk_page" and json.loads(row["receipt"])["field"] == "membership"
        )
        + 1
    )
    staged = arguments(rows[:split], batch="split-membership")
    first = invoke(directory, staged, freeze(directory, owner, 0, 0, 0))
    assert first["status"] == "ok", first
    count = split + 1
    middle = arguments(rows[split:-1], batch="middle")
    assert invoke(directory, middle, freeze(directory, owner, 1, count, 1))["status"] == "ok"
    count += len(rows[split:-1]) + 1
    final_args = arguments(rows[-1:], [owner], "finish")
    final = invoke(directory, final_args, freeze(directory, owner, 2, count, 2))
    assert final["status"] == "ok", final
    count += 4  # Last record, owner marker, original live receipt, batch receipt.
    before = canonical_files(directory)
    assert invoke(directory, final_args, freeze(directory, owner, 3, count, 3)) == final
    assert invoke(directory, staged, freeze(directory, owner, 3, count, 3)) == first
    assert canonical_files(directory) == before
    # Reconciliation admits exactly the paired version/geometry records. An
    # acknowledged retry still runs inherited full batch and owner proofs.
    reconciliation = freeze(
        directory,
        owner,
        3,
        count,
        3,
        expected_states=[
            {"version": 2, "physical_rows": count - 4, "fragments": 2},
            {"version": 3, "physical_rows": count, "fragments": 3},
        ],
    )
    assert invoke(directory, final_args, reconciliation) == final
    assert canonical_files(directory) == before
    for change in (
        {"expected_states": [{"version": 2, "physical_rows": count - 4, "fragments": 2}]},
        {"expected_states": [{"version": 3, "physical_rows": count, "fragments": 4}]},
    ):
        assert (
            invoke(directory, final_args, freeze(directory, owner, 3, count, 3, **change))["status"]
            == "stale"
        )
        assert canonical_files(directory) == before
    read = invoke(
        directory, {"ids": [record.id for record in prepared.records]}, operation="read_sources"
    )
    assert read["status"] == "ok" and "".join(row["text"] for row in read["sources"]) == original
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def test_stream_layout_corruption_refuses(tmp_path):
    prepared = prepare_remember("layout", "unchanged original")
    rows, owner = codec.prepare_owner("layout", "snapshot", prepared.records)
    missing = tmp_path / "missing"
    refusal = invoke(missing, arguments(rows, [owner]))
    assert refusal["status"] == "unavailable" and not refusal["fixture_native_imported"]
    assert not missing.exists()
    directory = tmp_path / "ordered"
    directory.mkdir(mode=0o700)
    bound = freeze(directory, owner, 0, 0, 0)
    refusal = invoke(
        directory,
        arguments(rows, [owner]),
        bound,
        request_changes={"qualification_hash": "0" * 64},
    )
    assert refusal["status"] == "unavailable" and not refusal["fixture_native_imported"]
    assert not (directory / "records.lance").exists()
    changed_reference = directory.parent / "changed.qualification.json"
    changed_manifest = json.loads(bound[0].read_text())
    changed_manifest["source_binding"] = "changed-after-freeze"
    changed_reference.write_text(_json(changed_manifest))
    changed_reference.chmod(0o600)
    refusal = invoke(
        directory,
        arguments(rows, [owner]),
        (changed_reference, bound[1]),
    )
    assert refusal["status"] == "unavailable" and not refusal["fixture_native_imported"]
    assert not (directory / "records.lance").exists()
    # The unchanged legacy store accepts arbitrary valid entry order.
    shuffled = [rows[-1], *rows[:-1]]
    legacy = invoke(directory, arguments(shuffled, [owner]), operation="import_batch")
    assert legacy["status"] == "ok", legacy
    count = len(rows) + 2
    before = canonical_files(directory)
    assert (
        invoke(
            directory,
            arguments([], [owner], "ordered-claim"),
            freeze(directory, owner, 1, count, 1),
        )["status"]
        == "corrupt"
    )
    assert canonical_files(directory) == before

    # A lower frozen physical ceiling refuses the real legacy fixture after
    # bounded native open; it cannot claim the smaller registered geometry.
    reduced = json.loads(bound[0].read_text())
    for reservation in reduced["reservations"].values():
        reservation["physical_rows"] = count - 1
    physical_bound = freeze(
        directory,
        owner,
        1,
        count - 1,
        1,
        max_physical_rows=count - 1,
        reservations=reduced["reservations"],
    )
    refusal = invoke(directory, arguments([], [owner], "row-ceiling"), physical_bound)
    assert refusal["status"] == "unavailable" and refusal["fixture_native_imported"]
    assert canonical_files(directory) == before

    foreign_directory = tmp_path / "foreign"
    foreign_directory.mkdir(mode=0o700)
    foreign_rows, _ = codec.prepare_owner("foreign", "snapshot", prepared.records)
    foreign_stage = invoke(
        foreign_directory,
        arguments(foreign_rows, batch="foreign-stage"),
        operation="import_batch",
    )
    assert foreign_stage["status"] == "ok", foreign_stage
    before = canonical_files(foreign_directory)
    refusal = invoke(
        foreign_directory,
        arguments([], [owner], "foreign-claim"),
        freeze(foreign_directory, owner, 1, len(foreign_rows) + 1, 1),
    )
    assert refusal["status"] == "corrupt" and refusal["fixture_native_imported"]
    assert canonical_files(foreign_directory) == before

    _assert_artifact_refusals(tmp_path, owner)
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def _assert_artifact_refusals(tmp_path, owner):
    for case in ("first-extra-artifact", "depth", "oversize-manifest"):
        directory = tmp_path / case
        directory.mkdir(mode=0o700)
        bound = freeze(directory, owner, 0, 0, 0)
        native = directory / "records.lance"
        native.mkdir(mode=0o700)
        if case == "first-extra-artifact":
            # Root + dataset directory +127 files is the first129th artifact.
            for ordinal in range(127):
                path = native / (str(ordinal) + ".orphan")
                path.write_bytes(b"")
                path.chmod(0o600)
        elif case == "depth":
            nested = native
            for ordinal in range(6):
                nested = nested / str(ordinal)
                nested.mkdir(mode=0o700)
        else:
            versions = native / "_versions"
            versions.mkdir(mode=0o700)
            path = versions / "99.manifest"
            path.write_bytes(b"x" * (1024**2 + 1))
            path.chmod(0o600)
        before = canonical_files(directory)
        refusal = invoke(directory, arguments([], [owner], case), bound)
        assert refusal["status"] == "unavailable", refusal
        assert not refusal["fixture_native_imported"]
        assert canonical_files(directory) == before


def test_pinned_stream_qualified_maintenance(tmp_path):
    directory = tmp_path / "maintenance"
    directory.mkdir(mode=0o700)
    prepared = prepare_remember("maintenance", "original ž😀\x00\r\n")
    rows, owner = codec.prepare_owner("maintenance", "snapshot", prepared.records)
    original = arguments(rows, [owner], "maintenance-owner")
    assert invoke(directory, original, freeze(directory, owner, 0, 0, 0))["status"] == "ok"
    count = len(rows) + 2
    before = canonical_files(directory)
    refused_bound = freeze(directory, owner, 1, count, 1)
    reduced = json.loads(refused_bound[0].read_text())["reservations"]
    reduced["maintenance"]["versions"] = 0
    refused_bound = freeze(directory, owner, 1, count, 1, reservations=reduced)
    refusal = invoke(directory, {}, refused_bound, operation="ordered_build_id_index")
    assert refusal["status"] == "unavailable"
    assert canonical_files(directory) == before
    index_bound = freeze(
        directory,
        owner,
        1,
        count,
        1,
        expected_states=[
            {"version": 1, "physical_rows": count, "fragments": 1},
            {"version": 2, "physical_rows": count, "fragments": 1},
        ],
    )
    indexed = invoke(
        directory,
        {},
        index_bound,
        operation="ordered_build_id_index",
    )
    assert indexed["status"] == "ok", indexed
    assert indexed["lookup"]["version"] == 2
    assert indexed["lookup"]["tail_rows"] == 0
    assert indexed["lookup"]["indexed_fragments"] == 1
    after_index = canonical_files(directory)
    refusal = invoke(directory, {}, index_bound, operation="ordered_build_id_index")
    assert refusal["status"] == "stale", refusal
    assert canonical_files(directory) == after_index
    # A fresh fixed worker proves persisted index visibility, then real replay
    # verifies all original batch and owner bytes under the new exact pin.
    fresh = invoke(directory, {"owner": owner["owner"]}, operation="bulk_owner")
    assert fresh["status"] == "ok" and fresh["manifest"] == owner
    assert fresh["lookup"]["version"] == 2 and fresh["lookup"]["tail_rows"] == 0
    assert fresh["lookup"]["indexed_fragments"] == 1
    replay = invoke(directory, original, freeze(directory, owner, 2, count, 1))
    assert replay["status"] == "ok", replay
    assert canonical_files(directory) == after_index
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


@pytest.mark.parametrize(
    ("operation", "budget"),
    [
        ("read_sources", 2),
        ("bulk_owner", 2),
        ("ordered_import_batch", 3),
        ("ordered_build_id_index", 3),
        ("import_batch", 3),
    ],
)
def test_fixture_supervisor_budget_is_fixed(operation, budget, tmp_path, monkeypatch):
    from cognitive_castle import memory_runtime as runtime

    calls = []
    monkeypatch.setattr(runtime, "_admission", lambda: os.open(os.devnull, os.O_RDONLY))
    monkeypatch.setattr(
        runtime,
        "_supervise",
        lambda *args, **kwargs: calls.append((args, kwargs)) or {"status": "ok"},
    )
    monkeypatch.setattr(time, "monotonic", lambda: 100.0)
    assert invoke(tmp_path / "unused", {}, operation=operation) == {"status": "ok"}
    assert len(calls) == 1
    _, supervision = calls[0]
    assert supervision["deadline"] == budget
    assert supervision["absolute_deadline"] == 100.0 + budget
    assert supervision["background_preempt"] is True
    assert supervision["rss_limit"] == 192 * 1024**2
    assert supervision["total_rss_limit"] == 256 * 1024**2
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def test_pinned_stream_receipt_ceiling_refuses_before_append_and_replays_boundary(tmp_path):
    directory = tmp_path / "receipt-ceiling"
    directory.mkdir(mode=0o700)
    prepared = prepare_remember("receipt-ceiling", "original exact bytes")
    rows, owner = codec.prepare_owner("receipt-ceiling", "snapshot", prepared.records)
    staged = arguments(rows, batch="stage")
    assert (
        invoke(directory, staged, freeze(directory, owner, 0, 0, 0, max_batch_receipts=1))["status"]
        == "ok"
    )
    count = len(rows) + 1
    before = canonical_files(directory)
    for attempt in (arguments([], [owner], "finish"), arguments(rows[:1], batch="extra-stage")):
        refusal = invoke(
            directory, attempt, freeze(directory, owner, 1, count, 1, max_batch_receipts=1)
        )
        assert refusal["status"] == "unavailable", refusal
        assert canonical_files(directory) == before
    final_args = arguments([], [owner], "finish")
    final = invoke(
        directory, final_args, freeze(directory, owner, 1, count, 1, max_batch_receipts=2)
    )
    assert final["status"] == "ok", final
    count += 2
    before = canonical_files(directory)
    # The exact resulting receipt ceiling admits full real final-batch replay.
    assert (
        invoke(directory, final_args, freeze(directory, owner, 2, count, 2, max_batch_receipts=2))
        == final
    )
    assert canonical_files(directory) == before
    refusal = invoke(
        directory,
        {},
        freeze(directory, owner, 2, count, 2, max_batch_receipts=1),
        operation="ordered_build_id_index",
    )
    assert refusal["status"] == "unavailable"
    assert canonical_files(directory) == before


def test_pinned_stream_reconciliation_after_state_is_replay_only(tmp_path):
    directory = tmp_path / "reconciliation"
    directory.mkdir(mode=0o700)
    prepared = prepare_remember("reconciliation", "original exact bytes")
    rows, owner = codec.prepare_owner("reconciliation", "snapshot", prepared.records)
    original = arguments(rows, [owner], "original")
    count = len(rows) + 2
    bound = freeze(
        directory,
        owner,
        0,
        0,
        0,
        expected_states=[
            {"version": 0, "physical_rows": 0, "fragments": 0},
            {"version": 1, "physical_rows": count, "fragments": 1},
        ],
    )
    # The registered before-state still permits the original real mutation.
    first = invoke(directory, original, bound)
    assert first["status"] == "ok", first
    before = canonical_files(directory)
    assert invoke(directory, original, bound) == first
    assert canonical_files(directory) == before
    refusal = invoke(directory, arguments([], [owner], "new-finalization"), bound)
    assert refusal["status"] == "stale", refusal
    assert canonical_files(directory) == before
    refusal = invoke(directory, {}, bound, operation="ordered_build_id_index")
    assert refusal["status"] == "stale", refusal
    assert canonical_files(directory) == before
    fresh = invoke(directory, {"owner": owner["owner"]}, operation="bulk_owner")
    assert fresh["status"] == "ok" and fresh["manifest"] == owner
    assert canonical_files(directory) == before


def test_pinned_stream_published_owner_rejects_late_content_preserves_replay(tmp_path):
    directory = tmp_path / "published"
    directory.mkdir(mode=0o700)
    prepared = prepare_remember("published", "original exact bytes")
    rows, owner = codec.prepare_owner(
        "published", "snapshot", prepared.records, descriptor="", scopes=""
    )
    original = arguments(rows, [owner], "original")
    first = invoke(directory, original, freeze(directory, owner, 0, 0, 0))
    assert first["status"] == "ok", first
    count = len(rows) + 2
    before = canonical_files(directory)
    late = codec.page(owner["owner"], "descriptor", 0, "new valid own-owner page", 0, 0)
    refusal = invoke(
        directory, arguments([late], batch="late-content"), freeze(directory, owner, 1, count, 1)
    )
    assert refusal["status"] == "conflict", refusal
    assert canonical_files(directory) == before
    assert invoke(directory, original, freeze(directory, owner, 1, count, 1)) == first
    assert canonical_files(directory) == before
    # A new receipt for equivalent already-present entries adds no content.
    equivalent = arguments(rows, batch="equivalent")
    assert invoke(directory, equivalent, freeze(directory, owner, 1, count, 1))["status"] == "ok"
    assert invoke(directory, original, freeze(directory, owner, 2, count + 1, 2)) == first


def large_harness():
    import importlib.util

    sys.dont_write_bytecode = True
    path = Path(__file__).with_name("lance_owner_stream_fixture.py")
    assert path.is_file(), "the fixed historical qualification harness is missing"
    spec = importlib.util.spec_from_file_location("layout52_fixture_harness", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_large_history_schedule_preserves_original_final_interval():
    harness = large_harness()
    before = {"version": 0, "physical_rows": 0, "fragments": 0}
    maintenance = []
    for ordinal in range(1, 172):
        if harness.maintenance_before(ordinal):
            maintenance.append(ordinal)
            before = harness.after_maintenance(before)
        # Protocol arithmetic only; runtime sizes come from actual source frames.
        entries = 80 if ordinal <= 21 else 79 if ordinal <= 170 else 73
        before = harness.after_append(before, entries=entries, owners=int(ordinal == 171))
    assert maintenance == list(range(10, 164, 9))
    assert before == {"version": 189, "physical_rows": 13696, "fragments": 171}
    assert harness.physical_rows(entries=73, owners=1) == 75
    with pytest.raises(ValueError):
        harness.physical_rows(entries=102, owners=1)
    with pytest.raises(ValueError):
        harness.maintenance_before(172)


@pytest.mark.parametrize(
    ("operation", "mapped", "cap"),
    [
        ("import_batch", "ordered_import_batch", 3),
        ("build_id_index", "ordered_build_id_index", 3),
        ("bulk_owner", "ordered_bulk_owner", 2),
        ("bulk_receipt", "ordered_bulk_receipt", 2),
    ],
)
def test_large_qualified_read_write_adapter_keeps_fixed_limits(operation, mapped, cap):
    harness = large_harness()
    spec = harness.exact_stage(operation)
    assert spec == {"operation": mapped, "cap": cap, "rss": 192 * 1024**2, "total": 256 * 1024**2}
    with pytest.raises(ValueError):
        harness.exact_stage("caller-selected-operation")
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


@pytest.mark.parametrize("operation", ["bulk_owner", "bulk_receipt"])
def test_large_fixture_read_dispatch_is_qualified_and_readonly(tmp_path, monkeypatch, operation):
    from cognitive_castle import bulk_exact_store

    directory = tmp_path / "qualified-read"
    directory.mkdir(mode=0o700)
    _, owner = codec.prepare_owner("qualified-read", "snapshot", ())
    reference, digest = freeze(directory, owner, 0, 0, 0)
    calls = []

    class ReadOnlyStore:
        def __init__(self, path, *, qualification, writable):
            assert path == directory and qualification.manifest_hash == digest
            assert writable is False

        def bulk_owner(self, **arguments):
            calls.append(("bulk_owner", arguments))
            return {"status": "ok", "fixture": "actual-qualified-owner-method"}

        def bulk_receipt(self, **arguments):
            calls.append(("bulk_receipt", arguments))
            return {"status": "ok", "fixture": "actual-qualified-receipt-method"}

    monkeypatch.setattr(bulk_exact_store, "OrderedBulkExactStore", ReadOnlyStore)
    monkeypatch.setattr(sys, "argv", [str(WORKER), str(reference), digest])
    args = (
        {"owner": owner["owner"]}
        if operation == "bulk_owner"
        else {"batch_id": "real-ack-intent", "source_binding": "snapshot"}
    )
    request = {
        "directory": str(directory),
        "operation": "ordered_" + operation,
        "arguments": args,
        "qualification": str(reference),
        "qualification_hash": digest,
        "_host_pid": os.getpid(),
    }
    try:
        actual = _ordered_execute(request)
    except ValueError:
        actual = {"status": "missing_qualified_read_dispatch"}
    assert actual == {
        "status": "ok",
        "fixture": "actual-qualified-"
        + ("owner" if operation == "bulk_owner" else "receipt")
        + "-method",
    }
    assert calls == [(operation, args)]
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def test_large_qualification_keeps_exact_states_and_frozen_growth():
    harness = large_harness()
    assert hasattr(harness, "qualification_for"), (
        "fixed per-intent qualification construction is missing"
    )
    binding = {
        "destination": "/private/fixed-history/generation/exact",
        "destination_identity": [1, 2],
        "parent_identity": [1, 3],
    }
    owner = codec.owner_id("history", "snapshot")
    before = {"version": 188, "physical_rows": 13621, "fragments": 170}
    after = {"version": 189, "physical_rows": 13696, "fragments": 171}
    manifest = harness.qualification_for(
        binding, owner, "snapshot", before, after, operation="append"
    )
    assert manifest["expected_states"] == [before, after]
    assert manifest["max_physical_rows"] == 16384
    assert manifest["max_fragments"] == manifest["max_batch_receipts"] == 171
    assert manifest["max_versions"] == 189
    assert manifest["reservations"]["append"]["physical_rows"] == 75
    assert manifest["reservations"]["append"]["logical_bytes"] == 1024**2
    assert manifest["reservations"]["maintenance"]["logical_bytes"] == 2 * 1024**2
    assert manifest["reservations"]["append"]["file_bytes"] == 2 * 1024**2
    assert manifest["reservations"]["maintenance"]["physical_rows"] == 0
    from cognitive_castle.bulk_owner_stream import OrderedQualification

    assert (
        OrderedQualification.from_manifest(manifest, expected_digest=_hash(_json(manifest))).owner
        == owner
    )
    with pytest.raises(ValueError):
        harness.qualification_for(
            binding,
            owner,
            "snapshot",
            before,
            {**after, "physical_rows": 13725},
            operation="append",
        )


def test_large_missing_freeze_refuses_before_any_path_mutation(monkeypatch):
    harness = large_harness()
    assert hasattr(harness, "load_frozen_config"), "root-frozen configuration gate is missing"
    monkeypatch.setattr(
        Path, "mkdir", lambda *args, **kwargs: pytest.fail("unfrozen fixture created paths")
    )
    monkeypatch.setattr(
        os, "scandir", lambda *args, **kwargs: pytest.fail("unfrozen fixture enumerated artifacts")
    )
    with pytest.raises(ValueError):
        harness.load_frozen_config(None, None)


def test_large_disk_census_counts_all_retained_artifacts_and_first_extra(tmp_path):
    harness = large_harness()
    assert hasattr(harness, "census"), "bounded complete retained-artifact census is missing"
    root = tmp_path / "retained"
    root.mkdir(mode=0o700)
    nested = root / "orphan"
    nested.mkdir(mode=0o700)
    leaf = nested / "old.manifest"
    leaf.write_bytes(b"old")
    leaf.chmod(0o600)
    counts = harness.census([root], max_entries=3, max_depth=2)
    assert counts["entries"] == 3 and counts["files"] == 1 and counts["directories"] == 2
    assert counts["logical_bytes"] == sum(path.lstat().st_size for path in (root, nested, leaf))
    assert counts["allocated_bytes"] == sum(
        path.lstat().st_blocks * 512 for path in (root, nested, leaf)
    )
    extra = nested / "first-extra.tmp"
    extra.write_bytes(b"retained")
    extra.chmod(0o600)
    with pytest.raises(ValueError):
        harness.census([root], max_entries=3, max_depth=2)
    assert extra.read_bytes() == b"retained"


def test_large_disk_forecast_precharges_complete_queue_and_temp_families():
    harness = large_harness()
    assert hasattr(harness, "reserve_stage"), "fixed complete-run reservation is missing"
    snapshot = {
        "source": 60 * 1024**2,
        "destination": 100 * 1024**2,
        "harness": 14 * 1024**2,
        "temp": 2 * 1024**2,
        "queue": 1024**2,
        "free": 4 * 1024**3,
    }
    reserved = harness.reserve_stage(snapshot, "queue")
    assert reserved["queue_resulting_bytes"] == 8 * 1024**2
    assert reserved["working_temp_bytes"] == 8 * 1024**2
    assert reserved["harness_resulting_bytes"] == 21 * 1024**2 + 65536
    exact = harness.reserve_stage(snapshot, "append")
    assert exact["destination_resulting_bytes"] == 101 * 1024**2
    assert exact["working_temp_bytes"] == 64 * 1024**2
    with pytest.raises(ValueError):
        harness.reserve_stage({**snapshot, "free": 2 * 1024**3}, "append")
    with pytest.raises(ValueError):
        harness.reserve_stage({**snapshot, "temp": 17 * 1024**2}, "append")


def test_large_exact_environment_is_fixed_before_native_import(tmp_path, monkeypatch):
    harness = large_harness()
    assert hasattr(harness, "set_exact_environment"), "fixed private native environment is missing"
    temporary = tmp_path / "private-exact-temp"
    temporary.mkdir(mode=0o700)
    monkeypatch.setenv("LANCE_BYPASS_SPILLING", "false")
    monkeypatch.setenv("LANCE_MEM_POOL_SIZE", "unexpected")
    monkeypatch.setenv("LANCE_MAX_TEMP_DIRECTORY_SIZE", "unexpected")
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    # The helper changes only child-local fixed native configuration, no engine.
    configured = harness.set_exact_environment(temporary)
    assert configured == {
        "pool_bytes": 16 * 1024**2,
        "tracked_spill_bytes": 64 * 1024**2,
        "bypass_present": False,
        "tmpdir": str(temporary),
    }
    assert os.environ["LANCE_MEM_POOL_SIZE"] == "16777216"
    assert os.environ["LANCE_MAX_TEMP_DIRECTORY_SIZE"] == "67108864"
    assert "LANCE_BYPASS_SPILLING" not in os.environ
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def test_large_session_preflight_consumes_existing_stage_and_session_clock(monkeypatch):
    harness = large_harness()
    assert hasattr(harness, "FixedSession"), "fixed real-session adapter is missing"
    from cognitive_castle.semantic_runtime import SemanticSession

    clock = [100.0]
    calls = []
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])

    class Ledger:
        def before(self, kind, until):
            assert kind in {"source", "queue"} and until <= 105.0
            clock[0] += 0.25

        def after(self, kind, result, until):
            clock[0] += 0.1
            assert clock[0] < until

    def actual_stage(self, request, **arguments):
        calls.append(arguments)
        clock[0] += 0.5
        return {"status": "ok"}

    monkeypatch.setattr(SemanticSession, "_run_stage", actual_stage)
    session = harness.FixedSession({}, Ledger(), None)
    until = session._until
    for request, cap in (
        ({"operation": "queue_migration_open"}, 3),
        ({"descriptor": {}, "cursor": {}}, 2),
    ):
        assert session._run_stage(request, worker_command=["fixed"], cap=cap, rss=1, total=2) == {
            "status": "ok"
        }
    assert [call["cap"] for call in calls] == [2.75, 1.75]
    assert session._until == until == 105.0


def test_large_pending_qualification_is_body_free_and_uses_real_queue_cursor(tmp_path):
    harness = large_harness()
    assert hasattr(harness, "QualificationBook"), (
        "real pending-intent qualification binding is missing"
    )
    from cognitive_castle import migration_codec
    from cognitive_castle.sqlite_export_codec import empty_counts, frame_bytes
    from test_migration_codec import descriptor, exported

    run = tmp_path / "run"
    for path in (run, run / "generation", run / "generation/exact", run / "qualifications"):
        path.mkdir(mode=0o700)
    reg = migration_codec.registration(descriptor(), str(run / "generation"))
    frame = exported(reg)
    owner = frame["finalizations"][0]
    frame["entries"] = frame["entries"][:1]
    frame["finalizations"] = []
    frame["counts"] = empty_counts()
    frame["phase_complete"] = False
    frame["cursor_after"] = {
        **frame["cursor_before"],
        "phase": "history",
        "key": ["owner"],
        "owner": "owner",
        "entry": 1,
        "field": "metadata",
        "field_char_offset": 17,
        "field_byte_offset": 17,
    }
    for _ in range(3):
        frame["frame_bytes"] = len(frame_bytes(frame))
    intent = migration_codec.prepare_interval(reg, frame)
    config = {
        "run_root": str(run),
        "source_descriptor": descriptor(),
        "source_binding": reg["source_binding"],
        "owner_manifest": owner,
    }
    binding = {
        "destination": str(run / "generation/exact"),
        "destination_identity": [1, 2],
        "parent_identity": [1, 3],
    }
    state = {
        "status": "ok",
        "registration": reg,
        "staging_cursor": intent["cursor_before"],
        "pending": intent,
        "last_ack": None,
    }
    first = harness.QualificationBook(config, binding)
    first.observe_state(state)
    reference, digest = first.for_call(
        "import_batch",
        {
            "batch_id": intent["intent_id"],
            "source_binding": reg["source_binding"],
            "entries": frame["entries"],
            "finalizations": [],
        },
    )
    assert first.ordinal == 1 and first.active == intent
    raw = Path(reference).read_text()
    assert _hash(raw) == digest
    assert (
        "BODY_SENTINEL" not in raw
        and "METADATA_SENTINEL" not in raw
        and "SCOPE_SENTINEL" not in raw
    )
    restarted = harness.QualificationBook(config, binding)
    restarted.observe_state(state)
    assert (
        restarted.active == intent and restarted.active["cursor_before"] == state["staging_cursor"]
    )
    assert restarted.reference == (reference, digest)
    assert state["staging_cursor"] == intent["cursor_before"]  # No seeded advanced cursor.
    assert hasattr(first, "acknowledged"), "actual ACK geometry update is missing"
    ack = {
        "status": "ok",
        "intent_id": intent["intent_id"],
        "staging_cursor": intent["cursor_after"],
    }
    first.acknowledged(ack)
    assert first.ordinal == 2 and first.active is None
    after_ack = {
        **state,
        "staging_cursor": intent["cursor_after"],
        "pending": None,
        "last_ack": {"intent_id": intent["intent_id"], "result": ack},
    }
    resumed = harness.QualificationBook(config, binding)
    resumed.observe_state(after_ack)
    assert resumed.ordinal == 2 and resumed.geometry == {
        "version": 1,
        "physical_rows": 2,
        "fragments": 1,
    }


def test_large_maintenance_resume_uses_existing_read_coverage(tmp_path):
    harness = large_harness()
    assert hasattr(harness.QualificationBook, "start_maintenance"), (
        "fixed maintenance reconciliation binding is missing"
    )
    from cognitive_castle import migration_codec
    from test_migration_codec import descriptor

    run = tmp_path / "run"
    for path in (run, run / "generation", run / "generation/exact", run / "qualifications"):
        path.mkdir(mode=0o700)
    reg = migration_codec.registration(descriptor(), str(run / "generation"))
    _, owner = codec.prepare_owner("owner", reg["source_binding"], ())
    config = {
        "run_root": str(run),
        "source_descriptor": descriptor(),
        "source_binding": reg["source_binding"],
        "owner_manifest": owner,
    }
    binding = {
        "destination": str(run / "generation/exact"),
        "destination_identity": [1, 2],
        "parent_identity": [1, 3],
    }
    book = harness.QualificationBook(config, binding)
    book.ordinal, book.geometry, book.last_ack = (
        10,
        {"version": 9, "physical_rows": 18, "fragments": 9},
        "a" * 64,
    )
    reference = book.start_maintenance()
    assert book.for_call("build_id_index", {}) == reference
    assert (
        book.for_call(
            "bulk_receipt", {"batch_id": book.last_ack, "source_binding": reg["source_binding"]}
        )
        == reference
    )
    assert book.maintenance_state({"status": "ok", "lookup": {"version": 9}}) == "before"
    covered = {
        "status": "ok",
        "lookup": {"version": 10, "indexed_fragments": 9, "tail_rows": 0, "tail_fragments": 0},
    }
    assert book.maintenance_state(covered) == "after"
    book.finish_maintenance(covered)
    assert book.geometry == {"version": 10, "physical_rows": 18, "fragments": 9}
    assert book.ordinal == 10 and book.maintenance is None
    # The next real queue-open repeats ACK9, which predates the independently
    # read/verified maintenance. It must not erase that current index version.
    ack_pin = harness.qualification_for(
        binding,
        owner["owner"],
        reg["source_binding"],
        {"version": 8, "physical_rows": 16, "fragments": 8},
        {"version": 9, "physical_rows": 18, "fragments": 9},
        operation="append",
    )
    book._write(run / "qualifications" / ("append-009-" + "a" * 64 + ".json"), ack_pin, "append")
    from cognitive_castle.migration_codec import initial_source_cursor

    cursor = {
        **initial_source_cursor(reg),
        "phase": "history",
        "key": ["owner"],
        "owner": "owner",
        "entry": 9,
        "field": "metadata",
    }
    book.observe_state(
        {
            "status": "ok",
            "registration": reg,
            "staging_cursor": cursor,
            "pending": None,
            "last_ack": {"intent_id": "a" * 64, "result": {"staging_cursor": cursor}},
        }
    )
    assert book.geometry == {"version": 10, "physical_rows": 18, "fragments": 9}


def test_large_queue_journal_reservation_refuses_before_real_stage(tmp_path, monkeypatch):
    harness = large_harness()
    assert hasattr(harness, "DiskLedger"), "complete fixed-family ledger is missing"
    run = tmp_path / "run"
    for path in (
        run,
        run / "generation",
        run / "generation/exact",
        run / "generation/coordination",
        run / "qualifications",
        run / "evidence",
        run / "tmp",
    ):
        path.mkdir(mode=0o700)
    packet = tmp_path / "packet"
    packet.mkdir(mode=0o700)
    sources = []
    for n in range(8):
        source = tmp_path / str(n)
        source.mkdir(mode=0o700)
        sources.append(str(source))
    journal = run / "generation/coordination/queue.sqlite3-journal"
    journal.write_bytes(b"x" * (8 * 1024**2 + 1))
    journal.chmod(0o600)
    ledger = harness.DiskLedger(
        {"run_root": str(run), "packet_root": str(packet), "source_roots": sources, "code_pins": []}
    )
    with pytest.raises(ValueError):
        ledger.before("queue", time.monotonic() + 2)
    assert journal.exists()


def test_large_exact_sampler_counts_own_unlinked_file_bytes():
    import tempfile

    harness = large_harness()
    assert hasattr(harness, "own_fd_metrics"), "exact worker own-fd accounting is missing"
    with tempfile.TemporaryFile() as temporary:
        temporary.write(b"fixture-unlinked-bytes")
        temporary.flush()
        measured = harness.own_fd_metrics()
        assert 1 <= measured["worker_fd_slots"] <= 512
        assert measured["open_unlinked_logical_bytes"] >= len(b"fixture-unlinked-bytes")
        assert measured["open_unlinked_allocated_bytes"] >= 0


def test_52mib_conditional_persisted_replay_and_ack(request):
    """One root-frozen operator gate; clean checkouts have no private source."""
    if FROZEN_LAYOUT52_CONFIG_SHA256 is None:
        pytest.skip("root-frozen52MiB operator configuration is absent")
    support = _fixture_support()
    configuration = support.CONFIGURATION_PATH
    if not configuration.is_file():
        pytest.skip("private root-frozen52MiB operator inputs are absent")
    config = support.load_frozen_config(configuration, FROZEN_LAYOUT52_CONFIG_SHA256)
    basetemp = request.config.getoption("basetemp")
    assert basetemp is not None and Path(basetemp).absolute() == Path(config["pytest_basetemp"])
    # No tmp_path fixture: pytest never deletes a pre-existing private basetemp.
    # The admitted setup itself checks absence before creating the empty root.
    assert not os.path.lexists(config["pytest_basetemp"])
    try:
        result = support.run_fixture(config)
    except Exception as error:
        if Path(config["run_root"]).exists():
            try:
                support.DiskLedger(config).refusal("host", error)
            except Exception:
                pass
        raise
    print("LAYOUT52_RESULT", json.dumps(result, sort_keys=True), flush=True)
    assert result.get("status") == "ok", result
    assert result["geometry"] == {"version": 189, "physical_rows": 13696, "fragments": 171}
    assert result["cold"]["session_elapsed"] < 5
    assert not {"sqlite3", "lance", "pyarrow", "numpy", "torch"}.intersection(sys.modules)


def private_tiny_retained_census(directory, *, until):
    snapshot = large_harness().census([directory], max_entries=256, max_depth=8, until=until)
    if max(snapshot["logical_bytes"], snapshot["allocated_bytes"]) > 80 * 1024**2:
        raise ValueError("tiny retained byte ceiling")
    return snapshot


@pytest.mark.parametrize("unsafe", ["symlink", "logical_bytes", "entries"])
def test_private_tiny_retained_census_refuses_unsafe_tree(tmp_path, unsafe):
    if unsafe == "symlink":
        (tmp_path / "link").symlink_to(tmp_path / "absent")
    elif unsafe == "logical_bytes":
        with (tmp_path / "large").open("wb") as stream:
            stream.truncate(80 * 1024**2 + 1)
        (tmp_path / "large").chmod(0o600)
    else:
        for ordinal in range(256):
            path = tmp_path / str(ordinal)
            path.touch(mode=0o600)
    with pytest.raises(ValueError):
        private_tiny_retained_census(tmp_path, until=time.monotonic() + 2)


def test_private_outcome_fresh_append_then_persisted_reopen(tmp_path):
    """One fixed tiny LIVE owner; four real processes share one5s admission."""
    from cognitive_castle.semantic_runtime import SemanticSession
    from cognitive_castle.persisted_replay import expected_ids, validate_outcome

    directory = tmp_path / "private-ordered"
    directory.mkdir(mode=0o700)
    temporary = tmp_path / "native-tmp"
    temporary.mkdir(mode=0o700)
    original = "a" * 16384 + "Ω\x00tail"
    prepared = prepare_remember("private-live", original)
    assert len(prepared.records) == 3
    live = dict(
        namespace=prepared.namespace,
        key=prepared.request_id,
        digest=prepared.digest,
        receipt=prepared.receipt,
    )
    rows, owner = codec.prepare_owner(
        "private-tiny",
        "snapshot",
        prepared.records,
        metadata="{}",
        columns="",
        scopes="[]",
        descriptor="tiny",
        live=live,
    )
    assert len(rows) == 9
    args = arguments(rows, [owner], "private-tiny-batch")
    reference, digest = freeze(
        directory,
        owner,
        0,
        0,
        0,
        expected_states=[
            {"version": 0, "physical_rows": 0, "fragments": 0},
            {"version": 1, "physical_rows": 12, "fragments": 1},
        ],
    )
    expected = dict(
        status="ok",
        state="committed_batch",
        receipt_id=codec.key("batch", args["batch_id"]),
        digest=codec.validate_batch(**args),
        records=3,
        published_owners=1,
        entries=9,
    )
    audit = []

    class Session(SemanticSession):
        def exact(self, operation, values):
            assert temporary.is_dir()
            before_tree = private_tiny_retained_census(tmp_path, until=self._until)
            resource_count = len(self.resources)
            result = self._run_stage(
                dict(
                    directory=str(directory),
                    operation=operation,
                    arguments=values,
                    qualification=str(reference),
                    qualification_hash=digest,
                ),
                worker_command=[sys.executable, "-I", str(WORKER), str(reference), digest],
                cap=3 if operation == "ordered_import_batch_outcome" else 2,
                rss=192 * 1024**2,
                total=256 * 1024**2,
            )
            audit.append(
                dict(
                    operation=operation,
                    status=result.get("status"),
                    reason=result.get("reason"),
                    outcome=result.get("outcome"),
                    native_imported=result.get("fixture_native_imported"),
                    resources=self.resources[-1] if len(self.resources) > resource_count else None,
                    retained_before=before_tree,
                    profile=result.get("fixture_profile"),
                )
            )
            try:
                audit[-1]["retained_after"] = private_tiny_retained_census(
                    tmp_path, until=self._until
                )
            finally:
                print("PRIVATE_TINY_STAGE", json.dumps(audit[-1], sort_keys=True), flush=True)
            return {key: value for key, value in result.items() if not key.startswith("fixture_")}

    started = time.monotonic()
    with Session(ROOT / "unused-private-tiny", interactive=False) as session:
        first = session.exact("ordered_import_batch_outcome", args)
        assert first == {"status": "ok", "outcome": "FreshAppend", "receipt": expected}, first
        before = canonical_files(directory)
        second = session.exact("ordered_import_batch_outcome", args)
        version = validate_outcome(
            second,
            expected,
            directory,
            "snapshot",
            args["batch_id"],
            expected_ids(rows, [owner]),
            until=session._until,
        )
        assert version == 1 and canonical_files(directory) == before
        observed = session.exact(
            "ordered_bulk_owner", {"owner": owner["owner"], "version": version}
        )
        assert (
            observed["status"] == "ok"
            and observed["version"] == version
            and observed["manifest"] == owner
        )
        receipt = session.exact(
            "ordered_receipt", {"namespace": prepared.namespace, "key": prepared.request_id}
        )
        assert receipt == {"status": "ok", "digest": prepared.digest, "receipt": prepared.receipt}
        assert canonical_files(directory) == before and session.remaining > 0
    elapsed = time.monotonic() - started
    assert elapsed < 5
    assert all(
        item["profile"]
        == {
            "pool_bytes": 16 * 1024**2,
            "tracked_spill_bytes": 64 * 1024**2,
            "bypass_present": False,
            "tmpdir": str(temporary),
        }
        for item in audit
    )
    print(
        "PRIVATE_TINY_QUALIFICATION",
        json.dumps({"elapsed": elapsed, "stages": audit}, sort_keys=True),
        flush=True,
    )
    assert not {"lance", "pyarrow", "sqlite3"}.intersection(sys.modules)


def test_large_cold_refusal_does_not_create_uncharged_result(tmp_path, monkeypatch):
    from types import SimpleNamespace

    harness = large_harness()
    run = tmp_path / "run"
    for path in (run, run / "evidence"):
        path.mkdir(mode=0o700)
    refusal = run / "evidence/first-refusal.json"
    refusal.write_bytes(b" " * 4096)
    refusal.chmod(0o600)
    config = {
        "run_root": str(run),
        "_config_hash": "a" * 64,
        "owner_manifest": {"text_hash": "b" * 64},
    }
    monkeypatch.setattr(harness, "install_host_guard", lambda: None)
    monkeypatch.setattr(harness, "prepare_run", lambda *args, **kwargs: {})
    monkeypatch.setattr(harness, "host_temp", lambda *args: None)
    monkeypatch.setattr(harness, "QualificationBook", lambda *args: SimpleNamespace(geometry={}))
    observed = SimpleNamespace(counts={}, session=SimpleNamespace(actual_exact_metrics=[]))
    monkeypatch.setattr(
        harness,
        "actual_tick",
        lambda *args, **kwargs: (
            {"status": "unavailable", "reason": "fixture_disk_guard"},
            observed,
            0.01,
        ),
    )
    result = harness.cold_run(config)
    assert result["reason"] == "fixture_disk_guard"
    assert json.loads(refusal.read_text())["reason"] == "fixture_disk_guard"
    assert not harness.result_path(config, "cold-result").exists()


def test_large_external_seal_binds_raw_worker_and_rejects_changed_seal(tmp_path, monkeypatch):
    import hashlib

    harness = large_harness()
    packet = tmp_path / "packet"
    packet.mkdir(mode=0o700)
    configuration = tmp_path / "configuration.json"
    configuration.write_text('{"fixed":"configuration"}')
    configuration.chmod(0o600)
    digest = hashlib.sha256(configuration.read_bytes()).hexdigest()
    monkeypatch.setattr(harness, "TRUSTED_PACKET_ROOT", packet)
    monkeypatch.setattr(harness, "CONFIGURATION_PATH", configuration)
    config = {
        "packet_root": str(packet),
        "_config_path": str(configuration),
        "_config_hash": digest,
        "code_pins": [],
    }

    def record(path):
        return {
            "path": str(path),
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    seal = {
        "format": "ordered-history52-final-seal-v1",
        "configuration": record(configuration),
        "worker": record(harness.WORKER_PATH),
        "helper": record(harness.HELPER_PATH),
    }
    harness.write_metadata(packet / harness.SEAL_NAME, seal)
    harness.write_metadata(
        packet / harness.PIN_NAME,
        {
            "format": "ordered-history52-launch-pin-v1",
            "configuration_hash": digest,
            "seal_hash": hashlib.sha256((packet / harness.SEAL_NAME).read_bytes()).hexdigest(),
        },
    )
    for path in (packet / harness.SEAL_NAME, packet / harness.PIN_NAME):
        path.chmod(0o400)
    actual = harness.verify_trusted_seal(config)
    assert any(
        item["path"] == str(harness.WORKER_PATH) and item["sha256"] == seal["worker"]["sha256"]
        for item in actual
    )
    assert seal["worker"]["sha256"] not in configuration.read_text()
    path = packet / harness.SEAL_NAME
    path.chmod(0o600)
    path.write_bytes(path.read_bytes() + b" ")
    path.chmod(0o400)
    with pytest.raises(ValueError):
        harness.verify_trusted_seal(config)
    monkeypatch.setattr(harness, "TRUSTED_PACKET_ROOT", tmp_path / "missing-trusted-packet")
    with pytest.raises(ValueError):
        harness.verify_trusted_seal(config)  # CLI config cannot select another pin.


def _persisted_gate_logs(harness, tmp_path):
    import hashlib

    run = tmp_path / "run"
    evidence = run / "evidence"
    evidence.mkdir(parents=True)
    rows = []
    for stage in range(1, 123):
        rows.extend(
            [
                {"phase": "start", "stage": stage},
                {
                    "phase": "end",
                    "stage": stage,
                    "status": "ok" if stage < 122 else "unavailable",
                    "reason": None if stage < 122 else "background_preempted",
                },
            ]
        )
    paths = {
        "events.jsonl": "".join(json.dumps(row) + "\n" for row in rows).encode(),
        "peaks.jsonl": b"{}\n" * 122,
        "first-refusal.json": json.dumps(
            {"stage": 122, "kind": "queue", "reason": "background_preempted"}
        )
        .encode()
        .ljust(4096, b" "),
    }
    second = []
    for stage in range(123, 226):
        second.extend(
            [
                {"phase": "start", "stage": stage},
                {
                    "phase": "end",
                    "stage": stage,
                    "status": "ok" if stage < 225 else "unavailable",
                    "reason": None if stage < 225 else "background_preempted",
                },
            ]
        )
    paths.update(
        {
            "events-continuation1.jsonl": "".join(
                json.dumps(row) + "\n" for row in second
            ).encode(),
            "peaks-continuation1.jsonl": b"{}\n" * 103,
            "first-refusal-continuation1.json": json.dumps(
                {"stage": 225, "kind": "append", "reason": "background_preempted"}
            )
            .encode()
            .ljust(4096, b" "),
        }
    )
    third = []
    for stage in range(226, 1076):
        third.append({"phase": "start", "stage": stage})
        if stage < 1075:
            third.append({"phase": "end", "stage": stage, "status": "ok", "reason": None})
    paths.update(
        {
            "events-continuation2.jsonl": "".join(json.dumps(row) + "\n" for row in third).encode(),
            "peaks-continuation2.jsonl": b"{}\n" * 849,
            "first-refusal-continuation2.json": json.dumps(
                {"stage": 1075, "kind": "append", "reason": "fixture census deadline"}
            )
            .encode()
            .ljust(4096, b" "),
        }
    )
    fourth = []
    for stage in range(1076, 1081):
        fourth.append({"phase": "start", "stage": stage})
        if stage < 1080:
            fourth.append({"phase": "end", "stage": stage, "status": "ok", "reason": None})
    cells = []
    for stage in range(1081, 1084):
        cells.extend(
            [
                {"phase": "start", "stage": stage},
                {"phase": "end", "stage": stage, "status": "ok", "reason": None},
                {"stage": stage},
            ]
        )
    slot = (
        b" " * 4096
        + b"".join(json.dumps(cell).encode().ljust(3072, b" ") for cell in cells)
        + b" " * 1024
    )
    paths.update(
        {
            "events-cold.jsonl": "".join(json.dumps(row) + "\n" for row in fourth).encode(),
            "peaks-cold.jsonl": b"{}\n" * 4,
            "first-refusal-cold.json": json.dumps(
                {"stage": 1080, "kind": "append", "reason": "fixture census deadline"}
            )
            .encode()
            .ljust(4096, b" "),
            "phase-cost-events.bin": slot,
            "phase-cost-result.json": json.dumps(
                {"calls": 1083, "diagnostic_only": True, "receipt_matches": True}
            )
            .encode()
            .ljust(4096, b" "),
        }
    )
    records = []
    for name, raw in paths.items():
        path = evidence / name
        path.write_bytes(raw)
        records.append(
            {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        )
    return {
        "run_root": str(run),
        "continuation": {
            "prior_starts": 1083,
            "max_new_starts": 7,
            "max_cumulative_starts": 1090,
            "prior_files": records,
        },
    }


def test_large_persisted_gate_counts_three_streams_and_unpaired_start_without_reset(
    tmp_path, monkeypatch
):
    harness = large_harness()
    config = _persisted_gate_logs(harness, tmp_path)
    ledger = harness.DiskLedger(config)
    assert ledger.calls == 1083
    assert ledger.events.name == "events-persisted.jsonl"
    monkeypatch.setattr(ledger, "snapshot", lambda until: {})
    monkeypatch.setattr(harness, "reserve_stage", lambda *args: {})
    ledger.before("queue", float("inf"))
    assert ledger.calls == 1084
    assert harness.DiskLedger(config).calls == 1084
    original = Path(config["continuation"]["prior_files"][0]["path"]).read_bytes()
    ledger.calls = 1090
    with pytest.raises(ValueError, match="call ceiling"):
        ledger.before("queue", float("inf"))
    assert Path(config["continuation"]["prior_files"][0]["path"]).read_bytes() == original
    assert len(ledger.events.read_text().splitlines()) == 1


@pytest.mark.parametrize("stream", [0, 3, 6, 9])
@pytest.mark.parametrize("tamper", ["digest", "count"])
def test_large_persisted_gate_rejects_changed_or_truncated_prior_log(tmp_path, tamper, stream):
    import hashlib

    harness = large_harness()
    config = _persisted_gate_logs(harness, tmp_path)
    record = config["continuation"]["prior_files"][stream]
    path = Path(record["path"])
    if tamper == "digest":
        path.write_bytes(path.read_bytes() + b" ")
    else:
        path.write_text("\n".join(path.read_text().splitlines()[:-2]) + "\n")
        record.update(
            bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).hexdigest()
        )
    with pytest.raises(ValueError):
        harness.DiskLedger(config)


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "missing_pending",
        "wrong_intent",
        "wrong_geometry",
        "wrong_pin",
        "wrong_loss",
        "wrong_ack",
    ],
)
def test_large_persisted_gate_requires_actual_ack170_and_pending171(monkeypatch, mutation):
    from types import SimpleNamespace

    harness = large_harness()
    before = {"version": 188, "physical_rows": 13621, "fragments": 170}
    after = {"version": 189, "physical_rows": 13696, "fragments": 171}
    intent = {
        "intent_id": "fb5a90aaf74540004785b61f1066d34e81185cb28f3a35889b653afc408ab992",
        "owners": [{}],
        "digest": "13c27d5255967cadf6ff47631ad23c2233564a19a753f3ac6f603f4cfadfe782",
        "records": 37,
        "entries": 73,
    }
    loss = {
        "intent_id": intent["intent_id"],
        "phase": "publish",
        "reason": "fixture_lost_response",
        "actual_receipt": {
            "status": "ok",
            "state": "committed_batch",
            "receipt_id": "receipt:55a2fe9838f63cac92cc4e51b2d3829f9c223338d87153cd183895166ba8edec",
            "digest": intent["digest"],
            "records": 37,
            "published_owners": 1,
            "entries": 73,
        },
    }
    book = SimpleNamespace(
        ordinal=171,
        geometry=dict(before),
        active=intent,
        last_ack="4187eef5b0897f2b8f4424823e5b229fcfa447b3d981ff413c73e137aa4c20b6",
        reference=("/fixed/append-171.json", "a" * 64),
    )
    states = [before, after]
    book._read = lambda *args: ({"expected_states": states}, book.reference)
    state = {"pending": intent}
    if mutation == "missing_pending":
        state["pending"] = None
    elif mutation == "wrong_intent":
        book.active = state["pending"] = {"intent_id": "b" * 64}
    elif mutation == "wrong_geometry":
        book.geometry = after
    elif mutation == "wrong_pin":
        states[1] = {**after, "physical_rows": 13697}
    elif mutation == "wrong_loss":
        loss["actual_receipt"]["entries"] = 74
    elif mutation == "wrong_ack":
        book.last_ack = "c" * 64
    if mutation is None:
        harness.require_resume_boundary(book, state, loss)
        assert not harness.maintenance_before(book.ordinal)
    else:
        with pytest.raises(ValueError):
            harness.require_resume_boundary(book, state, loss)


@pytest.mark.parametrize(
    "geometry",
    [
        {"version": 189, "physical_rows": 13696, "fragments": 171},
        {"version": 188, "physical_rows": 13621, "fragments": 170},
        {"version": 189, "physical_rows": 13695, "fragments": 171},
    ],
)
def test_large_persisted_gate_geometry_refuses_before_import_continues(geometry):
    harness = large_harness()
    metrics, proceeded = {}, []

    def invoke():
        harness.observe_geometry({"mode": "persisted"}, lambda: geometry, metrics)
        proceeded.append("import may continue")

    if geometry == {"version": 189, "physical_rows": 13696, "fragments": 171}:
        invoke()
        assert metrics["geometry"] == geometry and len(proceeded) == 1
    else:
        from cognitive_castle.lance_exact_store import _StoreError

        with pytest.raises(_StoreError) as error:
            invoke()
        assert error.value.status == "stale" and not proceeded and not metrics


def test_large_persisted_gate_private_import_keeps_write_budget_and_temp_entry_reservation():
    h = large_harness()
    assert h.exact_stage("import_batch_outcome") == {
        "operation": "ordered_import_batch_outcome",
        "cap": 3,
        "rss": 192 * 1024**2,
        "total": 256 * 1024**2,
    }
    for retained, unlinked, workspace in [(1008, 0, 1), (1007, 2, 0)]:
        with pytest.raises(ValueError):
            h.check_temp_entries(retained, unlinked=unlinked, workspace=workspace)
    h.check_temp_entries(1007, unlinked=1)


def test_large_persisted_gate_observed_clock_reaches_private_publisher(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from cognitive_castle import migration_coordinator as coordinator, persisted_replay as replay

    h = large_harness()
    directory = tmp_path / "generation" / "exact"
    directory.parent.mkdir(mode=0o700)
    directory.mkdir(mode=0o700)
    identity = "memory:" + "a" * 64
    args = {
        "batch_id": "clock",
        "source_binding": "snapshot",
        "entries": [{"id": identity}],
        "finalizations": [],
    }
    expected = {
        "status": "ok",
        "state": "stage_batch",
        "receipt_id": codec.key("batch", "clock"),
        "digest": "b" * 64,
        "records": 0,
        "published_owners": 0,
        "entries": 1,
    }
    outcome = replay.existing_outcome(expected, directory, "snapshot", "clock", {identity}, 1)

    class Session:
        remaining = 5
        resources = []

        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def _ready(self):
            return None

        def exact(self, *args):
            return outcome

    def tick(observed, registration):
        version, refusal, phase = coordinator._publish_persisted(
            observed, directory, args, expected
        )
        assert (version, refusal, phase) == (1, None, None)
        return {"status": "ok"}

    monkeypatch.setattr(h, "FixedSession", Session)
    monkeypatch.setattr(coordinator, "_fixture_tick_persisted", tick)
    result, observed, elapsed = h.actual_tick({}, None, SimpleNamespace(registration={}), cold=True)
    assert result["status"] == "ok" and observed.counts == {"import_batch_outcome": 1}


def test_large_persisted_gate_keeps_old_cold_config_and_basetemp_charged(tmp_path, monkeypatch):
    h = large_harness()
    run = tmp_path / "run"
    for path in (
        run,
        run / "generation",
        run / "generation/exact",
        run / "generation/coordination",
        run / "tmp",
        tmp_path / "source",
        tmp_path / "packet",
        tmp_path / "old-packet",
        tmp_path / "new-base",
    ):
        path.mkdir(mode=0o700)
    monkeypatch.setattr(h, "RETIRED_PACKET_ROOT", tmp_path / "old-packet")
    for name in (
        "RETIRED_CONFIGURATION_PATH",
        "PREVIOUS_CONFIGURATION_PATH",
        "PUBLISHED_CONFIGURATION_PATH",
        "COLD_CONFIGURATION_PATH",
    ):
        path = tmp_path / name
        path.write_text("retained configuration")
        path.chmod(0o600)
        monkeypatch.setattr(h, name, path)
    for name in ("RETIRED_BASETEMP", "PREVIOUS_BASETEMP", "PUBLISHED_BASETEMP", "COLD_BASETEMP"):
        path = tmp_path / name
        path.mkdir(mode=0o700)
        monkeypatch.setattr(h, name, path)
    marker = h.COLD_BASETEMP / "retained"
    marker.write_text("retained body-free pytest evidence")
    marker.chmod(0o600)
    current = tmp_path / "current-config"
    current.write_text("new configuration")
    current.chmod(0o600)
    ledger = object.__new__(h.DiskLedger)
    ledger.run = run
    ledger.config = {
        "run_root": str(run),
        "packet_root": str(tmp_path / "packet"),
        "source_roots": [str(tmp_path / "source")],
        "_config_path": str(current),
        "pytest_basetemp": str(tmp_path / "new-base"),
        "continuation": {"retained": True},
    }
    seen = []
    census = h.census

    def measured(roots, **kwargs):
        seen.append((list(roots), kwargs["max_entries"]))
        return census(roots, **kwargs)

    monkeypatch.setattr(h, "census", measured)
    snapshot = ledger.snapshot()
    assert snapshot["groups"]["harness"]["files"] == 6
    assert any(h.COLD_CONFIGURATION_PATH in roots and h.COLD_BASETEMP in roots for roots, _ in seen)
    assert any(roots == [run / "tmp"] and cap == 1008 for roots, cap in seen)

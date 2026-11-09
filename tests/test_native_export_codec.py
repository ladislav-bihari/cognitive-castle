"""Engine-free contracts for an inactive registered native source."""

import copy
import hashlib
import json
import sys

import pytest


def descriptor():
    identity = dict(
        device=1, inode=2, size=3, mtime_ns=4, ctime_ns=5, mode=0o100400, uid=501, links=1
    )
    return dict(
        format="pinned-native-export-v1",
        store_id="native-fixture",
        snapshot_path="/private/protected/archive.lance",
        snapshot_identity={**identity, "mode": 0o40500, "links": 3},
        manifest_path="/private/protected/native.json",
        manifest_identity=identity,
        manifest_hash="a" * 64,
        archive_id="original-archive",
        original_snapshot_id="original-snapshot",
        version=7,
        start_cursor={"fragment": 0, "offset": 0},
    )


def test_descriptor_binds_original_identity_and_accepts_directory_link_count():
    from cognitive_castle import native_export_codec as codec

    value = descriptor()
    assert codec.validate_descriptor(value) == value
    encoded = json.dumps(
        ["pinned-native-export-v1", "registered-source", value],
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert codec.source_binding(value) == hashlib.sha256(encoded.encode()).hexdigest()
    for key in ("version", "archive_id", "original_snapshot_id", "start_cursor"):
        other = copy.deepcopy(value)
        other[key] = {
            "version": 8,
            "archive_id": "other",
            "original_snapshot_id": "other",
            "start_cursor": {"fragment": 0, "offset": 1},
        }[key]
        assert codec.source_binding(other) != codec.source_binding(value)
    assert not {"sqlite3", "lance", "pyarrow", "numpy", "torch"}.intersection(sys.modules)


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 0},
        {"version": 2**63},
        {"snapshot_path": "/private/../archive"},
        {"manifest_path": "relative"},
        {"start_cursor": {"fragment": 0, "offset": False}},
        {"start_cursor": {"fragment": 2, "offset": 1, "row_address": 0}},
        {"format": "protected-sqlite-export-v1"},
        {"extra": "no"},
    ],
)
def test_descriptor_rejects_wrong_or_ambiguous_registration(change):
    from cognitive_castle.native_export_codec import validate_descriptor

    with pytest.raises(ValueError):
        validate_descriptor({**descriptor(), **change})


@pytest.mark.parametrize(
    "field,change",
    [
        ("snapshot_identity", {"mode": 0o100400}),
        ("snapshot_identity", {"links": True}),
        ("manifest_identity", {"mode": 0o100600}),
        ("manifest_identity", {"links": 2}),
        ("manifest_identity", {"mode": 0o40500}),
    ],
)
def test_identity_validation_is_type_specific(field, change):
    from cognitive_castle.native_export_codec import validate_descriptor

    value = descriptor()
    value[field].update(change)
    with pytest.raises(ValueError):
        validate_descriptor(value)


def test_cursor_preserves_original_pre_row_json_and_strict_active_positions():
    from cognitive_castle import native_export_codec as codec

    d = descriptor()
    binding = codec.source_binding(d)
    before = codec.initial_cursor(binding, d["store_id"], d["start_cursor"])
    assert before["key"] == ['{"fragment":0,"offset":0}']
    assert before["phase"] == "history" and before["owner"] is None
    active = {**before, "owner": "b" * 64, "entry": 2, "field": "record", "member_ordinal": 0}
    assert codec.validate_cursor(active, binding, d["store_id"], d["start_cursor"]) == active
    for change in (
        {"entry": True},
        {"packing": "other"},
        {"phase": "live"},
        {"key": ['{ "fragment":0,"offset":0}']},
        {"key": []},
        {"field": None},
        {"field_byte_offset": 1},
        {"entry": 0},
        {"owner": None},
        {"source_binding": "c" * 64},
    ):
        with pytest.raises(ValueError):
            codec.validate_cursor({**active, **change}, binding, d["store_id"], d["start_cursor"])
    later = {**before, "key": ['{"fragment":0,"offset":1,"row_address":0}']}
    assert codec.validate_cursor(later, binding, d["store_id"], d["start_cursor"]) == later
    done = {**before, "phase": "done", "key": None}
    assert codec.validate_cursor(done, binding, d["store_id"], d["start_cursor"]) == done
    with pytest.raises(ValueError):
        codec.validate_cursor(before, binding, d["store_id"], {"fragment": 0, "offset": 1})


def test_native_registration_uses_separate_domain_and_strict_source_cursor():
    from cognitive_castle import migration_codec as migration

    value = migration.registration(descriptor(), "/private/destination")
    assert value["format"] == "native-migration-v1"
    assert migration.validate_registration(value) == value
    before = migration.initial_source_cursor(value)
    assert before["format"] == "pinned-native-export-v1" and before["phase"] == "history"
    assert migration.cursor(before, value) == before
    with pytest.raises(ValueError):
        migration.validate_registration({**value, "format": "sqlite-migration-v1"})
    from test_migration_codec import descriptor as sqlite_descriptor

    sqlite = migration.registration(sqlite_descriptor(), "/private/destination")
    assert sqlite["format"] == "sqlite-migration-v1"
    assert migration.initial_source_cursor(sqlite)["phase"] == "live"
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize(
    "reason",
    [
        "worker_memory_limit",
        "total_memory_limit",
        "worker_deadline",
        "background_preempted",
        "system_memory_pressure",
    ],
)
@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_native_session_refusal_uses_history_profile_and_original_cursor(monkeypatch, reason):
    import os
    from cognitive_castle import memory_runtime
    from cognitive_castle.semantic_runtime import SemanticSession

    seen = []
    monkeypatch.setattr(memory_runtime, "interactive_waiting", lambda: False)
    monkeypatch.setattr(memory_runtime, "_admission", lambda: os.open("/dev/null", os.O_RDONLY))

    def refuse(request, admission, **limits):
        seen.append((request, limits))
        return {"status": "unavailable", "reason": reason}

    monkeypatch.setattr(memory_runtime, "_supervise", refuse)
    with SemanticSession("/private/profile", interactive=False) as session:
        response = session.native_source(descriptor())
    assert response["status"] == "unavailable" and response["reason"] == reason
    assert response["cursor_after"] == response["cursor_before"]
    assert response["entries"] == [] and response["finalizations"] == []
    request, limits = seen[0]
    assert limits["deadline"] <= 2 and limits["rss_limit"] == 512 * 1024**2
    assert limits["total_rss_limit"] == 640 * 1024**2 and limits["background_preempt"]
    assert limits["absolute_deadline"] == session._until
    assert limits["worker_command"][-1].endswith("/native_source_worker.py")
    assert set(request) == {"descriptor", "cursor"}
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)


@pytest.mark.parametrize(
    "fault",
    [
        "command",
        "group",
        "parent",
        "worker_session",
        "guardian_session",
        "guardian_uid",
        "host_uid",
        None,
    ],
)
def test_native_worker_requires_actual_guardian_chain_before_source_access(monkeypatch, fault):
    import os
    from pathlib import Path
    from types import SimpleNamespace
    import psutil
    from cognitive_castle import memory_history_worker, native_source_worker

    monkeypatch.setattr(memory_history_worker, "_admit_storage", lambda request: None)
    command = [
        "python",
        "-I",
        str(Path(native_source_worker.__file__).with_name("brain_reader_guardian.py")),
    ]
    if fault == "command":
        command[-1] = "/wrong-guardian"
    guardian_pid, host_pid, uid = 200, 42, os.getuid()
    guardian = SimpleNamespace(
        cmdline=lambda: command,
        pid=guardian_pid,
        ppid=lambda: host_pid + int(fault == "parent"),
        uids=lambda: SimpleNamespace(effective=uid + int(fault == "guardian_uid")),
    )
    host = SimpleNamespace(
        pid=host_pid, uids=lambda: SimpleNamespace(effective=uid + int(fault == "host_uid"))
    )
    monkeypatch.setattr(os, "getpgrp", lambda: guardian_pid + int(fault == "group"))
    monkeypatch.setattr(
        os,
        "getsid",
        lambda pid: (
            guardian_pid
            + int(
                (fault == "worker_session" and pid == 0)
                or (fault == "guardian_session" and pid == guardian_pid)
            )
        ),
    )
    monkeypatch.setattr(
        psutil,
        "Process",
        lambda pid: host if pid == host_pid else guardian,
    )
    result = native_source_worker._admit_source({"_host_pid": host_pid})
    assert result == (
        None
        if fault is None
        else {"status": "unavailable", "reason": "native_source_supervision_required"}
    )


def test_shared_historical_evidence_preserves_raw_json_and_candidate_types():
    from cognitive_castle.historical_export_evidence import historical_scope_header, quoted_pieces

    columns = ' {"workspace":"", "wing":null,"room":false} '
    metadata = '{"workspace":"other","wing":3}'
    kinds = {"workspace": ("text", "text"), "wing": ("null", "integer"), "room": ("false", None)}
    prefix = "".join(
        historical_scope_header(quoted_pieces([columns]), quoted_pieces([metadata]), kinds)
    )
    result = json.loads(prefix + "]}")
    assert result["columns"] == columns and result["metadata"] == metadata
    assert result["selection"] == {
        "workspace": {"columns": "text", "metadata": "text", "selected_origin": "columns"},
        "wing": {"columns": "null", "metadata": "integer", "selected_origin": "metadata"},
        "room": {"columns": "false", "metadata": None, "selected_origin": "columns"},
    }


def test_native_fixture_entry_uses_fixed_source_and_preserves_refusal_cursor():
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.migration_coordinator import _native_fixture_tick
    from cognitive_castle.sqlite_source_runtime import source_refusal

    reg = migration.registration(descriptor(), "/private/generation")
    before = migration.initial_source_cursor(reg)

    class Session:
        def _ready(self):
            return None

        def store(self, path, operation, arguments):
            if operation == "queue_migration_open":
                return dict(
                    status="ok",
                    registration=reg,
                    state="active",
                    pending=None,
                    staging_cursor=before,
                    counts=migration.empty_counts(),
                )
            assert operation == "queue_migration_refuse"
            return {"status": "ok"}

        def native_source(self, d, cursor):
            assert d == reg["descriptor"] and cursor == before
            return source_refusal(
                {"cursor": cursor}, dict(status="unavailable", reason="memory_busy")
            )

        def sqlite_source(self, *args):
            pytest.fail("native registration selected SQLite")

    result = _native_fixture_tick(Session(), reg)
    assert result["reason"] == "memory_busy" and result["cursor_after"] == before
    assert result["intent_id"] is None and not any(result["counts"].values())
    from test_migration_codec import descriptor as sqlite_descriptor

    with pytest.raises(ValueError):
        _native_fixture_tick(
            Session(), migration.registration(sqlite_descriptor(), "/private/generation")
        )


def test_native_immutable_interval_binds_original_owner_and_following_cursor():
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.bulk_exact_codec import prepare_owner

    reg = migration.registration(descriptor(), "/private/generation")
    d = reg["descriptor"]
    following = {"fragment": 0, "offset": 1, "row_address": 0}
    occurrence = hashlib.sha256(
        json.dumps(
            [d["archive_id"], d["original_snapshot_id"], d["version"], 0, 0], separators=(",", ":")
        ).encode()
    ).hexdigest()
    entries, owner = prepare_owner(occurrence, reg["source_binding"], [], metadata_id=occurrence)
    before = migration.initial_source_cursor(reg)
    after = {**before, "key": [json.dumps(following, sort_keys=True, separators=(",", ":"))]}
    result = dict(
        status="ok",
        reason=None,
        source_binding=reg["source_binding"],
        cursor_before=before,
        cursor_after=after,
        entries=entries,
        finalizations=[owner],
        counts={**migration.empty_counts(), "historical_occurrences": 1},
        phase="history",
        phase_complete=True,
        complete=False,
        coverage="incomplete",
        frame_bytes=0,
    )
    for _ in range(3):
        result["frame_bytes"] = len(migration.frame_bytes(result))
    interval = migration.prepare_interval(reg, result)
    assert interval["format"] == "native-migration-v1"
    assert interval["owners"][0]["classification"] == "history"
    assert interval["owners"][0]["semantic_disposition"] == "no_members"
    assert interval == migration.prepare_interval(reg, copy.deepcopy(result))
    for change in ({"owner_key": "b" * 64}, {"metadata_id": "b" * 64}):
        bad = {**result, "finalizations": [{**owner, **change}]}
        for _ in range(3):
            bad["frame_bytes"] = len(migration.frame_bytes(bad))
        with pytest.raises(ValueError):
            migration.validate_export(bad, reg, before)
    bad_reference = {
        **interval["owners"][0],
        "source_key": ['{"fragment":0,"offset":2,"row_address":1}'],
    }
    with pytest.raises(ValueError):
        migration.validate_owner_reference(bad_reference, reg)
    # A partial stage keeps the original pre-row position even on the first
    # interval, before an owner was present in the incoming cursor.
    page = migration.bulk.page(
        migration.bulk.owner_id(occurrence, reg["source_binding"]), "metadata", 0, "{}", 0, 0
    )
    bad = {
        **result,
        "entries": [page],
        "finalizations": [],
        "phase_complete": False,
        "counts": migration.empty_counts(),
        "cursor_after": {**after, "owner": occurrence, "entry": 1, "field": "columns"},
    }
    for _ in range(3):
        bad["frame_bytes"] = len(migration.frame_bytes(bad))
    with pytest.raises(ValueError):
        migration.validate_export(bad, reg, before)


def manifest(d):
    return dict(
        format=d["format"],
        state="complete",
        store_id=d["store_id"],
        snapshot_identity=d["snapshot_identity"],
        original=dict(
            archive_id=d["archive_id"], snapshot_id=d["original_snapshot_id"], version=d["version"]
        ),
        start_cursor=d["start_cursor"],
        schema=dict(hash="a" * 64, bytes=123),
        fragments=[{"id": 0}],
        inventory=[
            dict(
                path="data/file.lance",
                snapshot_identity=d["manifest_identity"],
                content_hash="c" * 64,
            )
        ],
        lineage={"coverage": "pending-reconciliation"},
    )


def test_native_manifest_bounds_shape_and_original_types():
    from cognitive_castle.native_export_codec import validate_manifest

    d = {**descriptor(), "version": 1}
    good = manifest(d)
    assert validate_manifest(good, d) == good
    for change in (
        {"state": "pending"},
        {"original": {**good["original"], "version": True}},
        {"start_cursor": {"fragment": False, "offset": 0}},
        {"snapshot_identity": {**good["snapshot_identity"], "device": True}},
        {"schema": {"hash": "a" * 64, "bytes": 65537}},
        {"fragments": [{"id": True}]},
        {"fragments": [{"id": 1}, {"id": 0}]},
        {"inventory": good["inventory"] * 4097},
        {"lineage": {"large": "x" * 524288}},
        {"extra": "no"},
    ):
        with pytest.raises(ValueError):
            validate_manifest({**good, **change}, d)


def _projection_class():
    import ast
    import re
    from pathlib import Path

    class HistoryError(Exception):
        def __init__(self, status):
            self.status = status

    tree = ast.parse(
        (Path(__file__).parents[1] / "cognitive_castle/history_projection.py").read_text()
    )
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "NativeProjectedRow"
    ]
    namespace = dict(
        re=re,
        _HistoryError=HistoryError,
        PAGE_CHARACTERS=131072,
        _BYTES="bytes",
        _CHARACTERS="chars",
        _HASH="hash",
        _PAGE="page",
    )
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), "real_native_projection", "exec"),
        namespace,
    )
    return namespace["NativeProjectedRow"], HistoryError


def test_header_or_none_distinguishes_exhaustion_from_bad_statistics():
    from types import SimpleNamespace

    Reader, HistoryError = _projection_class()
    row = Reader.__new__(Reader)
    row._header = None
    row.cursor = {"fragment": 0, "offset": 0}
    row.fragments = [SimpleNamespace(fragment_id=0), SimpleNamespace(fragment_id=2)]
    row.columns = ["id", "metadata_json"]
    row._scan = lambda columns, fragment, offset: None
    assert row.header_or_none() is None
    with pytest.raises(HistoryError) as exc:
        row.header()
    assert exc.value.status == "corrupt"
    row._scan = lambda columns, fragment, offset: (
        {"_rowaddr": 0, "id": "a", "metadata_json": "{}"} if columns == row.columns else None
    )
    with pytest.raises(HistoryError) as exc:
        row.header_or_none()
    assert exc.value.status == "changed"


def test_projection_count_and_summary_refusals_are_nonprogress_and_keep_original_reason():
    import ast
    import time
    from pathlib import Path
    from types import SimpleNamespace
    from cognitive_castle import canonical_records as canonical
    from cognitive_castle import native_export_codec as codec
    from cognitive_castle.historical_export_evidence import pages

    tree = ast.parse(
        (Path(__file__).parents[1] / "cognitive_castle/native_source_export.py").read_text()
    )
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
        and node.name in {"Source", "Refusal", "export_slice", "_normalize_refusal"}
    ]
    namespace = dict(
        time=time,
        hashlib=hashlib,
        pages=pages,
        CanonicalRecord=canonical.CanonicalRecord,
        _hash=canonical._hash,
        _json=canonical._json,
        PROJECTION_CHARACTERS=8192,
        MAX_PROJECTIONS=64,
        MAX_PROJECTED_BYTES=2097152,
        MemoryStore=SimpleNamespace(
            _provenance=lambda c, h, s, e: dict(caller=c, text_hash=h, chunk_start=s, chunk_end=e)
        ),
        empty_counts=codec.empty_counts,
        source_binding=codec.source_binding,
        validate_cursor=codec.validate_cursor,
        initial_cursor=codec.initial_cursor,
        frame_bytes=codec.frame_bytes,
    )

    class HistoryError(Exception):
        pass

    namespace["_HistoryError"] = HistoryError
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), "real_native_source_control", "exec"),
        namespace,
    )
    Source, Refusal = namespace["Source"], namespace["Refusal"]
    source = Source.__new__(Source)
    source.until = time.monotonic() + 10
    source.header = dict(text_characters=8192 * 65, text_bytes=8192 * 65, text_hash="a" * 64)
    source.reader = SimpleNamespace(page=lambda start, limit: "x" * limit)
    source.projections = source.projected_bytes = 0
    source.occurrence, source.caller = "b" * 64, {}
    with pytest.raises(Refusal) as exc:
        for _ in source.records():
            pass
    assert exc.value.reason == "native_projection_count" and source.projections == 64

    class Boundary:
        def __init__(self, d):
            self.binding, self.opened, self.reader = codec.source_binding(d), True, None
            self.active_field, self.active_operation = "text", "summary"

        def open(self, cursor):
            raise Refusal("qualification_required", "native_projection_count")

        def validate_files(self):
            raise Refusal("qualification_required", "native_summary_slice_deadline")

    namespace["Source"] = Boundary
    d = descriptor()
    before = codec.initial_cursor(codec.source_binding(d), d["store_id"], d["start_cursor"])
    result = namespace["export_slice"](d, before)
    assert (
        result["status"] == "qualification_required"
        and result["reason"] == "native_projection_count"
    )
    assert (
        result["cursor_after"] == before and not result["entries"] and not result["finalizations"]
    )
    assert not result["complete"] and not result["phase_complete"]


@pytest.mark.parametrize("boundary", ["owner", "exhaustion"])
@pytest.mark.parametrize("failure", ["stale", "deadline", "memory", "value", "os"])
def test_native_postcheck_never_retains_tentative_owner_or_scan_progress(boundary, failure):
    import ast
    from pathlib import Path
    from cognitive_castle import native_export_codec as codec

    tree = ast.parse(
        (Path(__file__).parents[1] / "cognitive_castle/native_source_export.py").read_text()
    )
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
        and node.name in {"Refusal", "export_slice", "_normalize_refusal"}
    ]

    class HistoryError(Exception):
        pass

    namespace = dict(
        _HistoryError=HistoryError,
        validate_cursor=codec.validate_cursor,
        initial_cursor=codec.initial_cursor,
        empty_counts=codec.empty_counts,
        frame_bytes=codec.frame_bytes,
    )
    exec(
        compile(ast.Module(body=selected, type_ignores=[]), "real_native_postcheck", "exec"),
        namespace,
    )
    Refusal = namespace["Refusal"]
    errors = dict(
        stale=Refusal("stale", "native_source_identity_changed"),
        deadline=Refusal("qualification_required", "native_summary_slice_deadline"),
        memory=MemoryError(),
        value=ValueError(),
        os=OSError(),
    )

    class Source:
        def __init__(self, d):
            self.binding, self.opened, self.reader = codec.source_binding(d), True, None
            self.active_field, self.active_operation = "descriptor", "postcheck"

        def open(self, cursor):
            return boundary == "owner"

        def validate_files(self):
            raise errors[failure]

    def finalize(source, result, cursor):
        result.update(
            entries=[{"tentative": True}],
            finalizations=[{"tentative": True}],
            phase_complete=True,
            cursor_after={"tentative": True},
        )
        result["counts"].update(historical_occurrences=1, members=3)

    namespace.update(Source=Source, _export_owner=finalize)
    d = descriptor()
    before = codec.initial_cursor(codec.source_binding(d), d["store_id"], d["start_cursor"])
    result = namespace["export_slice"](d, before)
    assert result["status"] != "ok" and result["cursor_after"] == before
    assert result["entries"] == [] and result["finalizations"] == []
    assert not result["complete"] and not result["phase_complete"]
    assert result["counts"] == codec.empty_counts()

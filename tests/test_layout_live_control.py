"""Real temporary control transactions, not native/OS admission qualification."""

import os

import pytest

from cognitive_castle import lance_layout_directory as directory
from cognitive_castle import migration_codec as migration
from cognitive_castle import semantic_queue as queue_module
from cognitive_castle import live_publication_codec as live
from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.lance_exact_store import _StoreError
from test_migration_codec import descriptor
from test_partitioned_exact_store import qualification_case


@pytest.fixture
def control(tmp_path, monkeypatch):
    root = tmp_path.resolve() / "generation"
    root.mkdir(mode=0o700)
    for path in ("coordination", "exact", "exact/tmp", "exact/partitions"):
        (root / path).mkdir(mode=0o700)
    _, _, qualification, envelope = qualification_case()
    for pid in range(envelope["max_partitions"]):
        (root / "exact/partitions" / str(pid)).mkdir(mode=0o700)
    info = root.stat()
    identity = dict(path=str(root), device=info.st_dev, inode=info.st_ino)
    qualification["generation"] = envelope["generation"] = identity
    envelope["qualification_hash"] = migration.hashed(qualification)
    path = root / "coordination/queue.sqlite3"
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    store = queue_module.SemanticQueue(root, writable=True)
    # Exercise actual schema/transactions without claiming OS worker admission.
    # Native/model imports and the installed palace are never involved here.
    monkeypatch.setattr(queue_module, "_require_supervision", lambda: None)
    monkeypatch.setattr(store, "_paths", lambda create: None)
    with store._connection(True) as conn:
        conn.execute("PRAGMA hard_heap_limit=16777216")
    return store, qualification, envelope


def prepare(control):
    store, qualification, envelope = control
    with store._connection(True) as conn:
        return directory._prepare_layout_authority(
            conn, envelope, qualification, expected_hash=envelope["qualification_hash"]
        )


def test_operator_can_prepare_registered_authority_with_no_source_jobs(control):
    store, _, envelope = control
    result = prepare(control)
    assert result["status"] == "ok"
    with store._connection(False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
        assert conn.execute("SELECT count(*) FROM migration_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM layout_jobs").fetchone()[0] == 0
        registered = directory._NativeLayoutDirectory(conn)
        assert registered.envelope == envelope
        assert registered.epoch == result["epoch"]
        assert registered._ready_registry()
        assert (
            conn.execute("SELECT count(*) FROM layout_registry").fetchone()[0]
            == envelope["max_partitions"]
        )


def test_operator_replay_keeps_same_authority_and_qualification_artifact(control):
    store, _, _ = control
    first = prepare(control)
    artifact = store.generation / "exact/layout-qualification.json"
    before = artifact.stat()
    assert prepare(control) == first
    after = artifact.stat()
    assert (before.st_ino, before.st_mtime_ns, before.st_size) == (
        after.st_ino,
        after.st_mtime_ns,
        after.st_size,
    )


def test_operator_rejects_progressed_control_before_creating_authority(control):
    store, _, _ = control
    registration = migration.registration(descriptor(), str(store.generation))
    assert store.migration_open(registration)["status"] == "ok"
    with store._connection(True) as conn:
        conn.execute("UPDATE migration_jobs SET state='snapshot_export_exhausted'")
    with pytest.raises(_StoreError):
        prepare(control)
    assert not (store.generation / "exact/layout-qualification.json").exists()
    with store._connection(False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3


def test_operator_does_not_overwrite_orphan_qualification_artifact(control):
    store, _, _ = control
    artifact = store.generation / "exact/layout-qualification.json"
    artifact.write_bytes(b"protected interrupted setup")
    artifact.chmod(0o400)
    before = artifact.read_bytes()
    with pytest.raises(_StoreError):
        prepare(control)
    assert artifact.read_bytes() == before


def test_sql_rollback_retains_artifact_and_refuses_unqualified_restart(control):
    store, qualification, envelope = control
    with pytest.raises(RuntimeError, match="lost transaction"):
        with store._connection(True) as conn:
            directory._prepare_layout_authority(
                conn, envelope, qualification, expected_hash=envelope["qualification_hash"]
            )
            raise RuntimeError("lost transaction")
    with store._connection(False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert (
            conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'layout_%'"
            ).fetchone()[0]
            == 0
        )
    assert (store.generation / "exact/layout-qualification.json").exists()
    with pytest.raises(_StoreError):
        prepare(control)


def test_existing_job_operator_registration_remains_compatible(control):
    store, qualification, envelope = control
    registration = migration.registration(descriptor(), str(store.generation))
    assert store.migration_open(registration)["status"] == "ok"
    with store._connection(True) as conn:
        result = directory._register_layout_authority(
            conn,
            registration["job_id"],
            envelope,
            qualification,
            expected_hash=envelope["qualification_hash"],
        )
    assert result["status"] == "ok"
    with store._connection(False) as conn:
        attachment = directory._NativeLayoutDirectory(conn).attachment(registration["job_id"])
        assert attachment["envelope"] == envelope


def live_intent(text="PRIVATE_BODY Ω\x00", workspace="/project"):
    canonical = prepare_remember("request", text, {"note": "PRIVATE_CALLER"}, namespace="operator")
    scopes = [
        dict(workspace=workspace, wing="project", room="conversation") for _ in canonical.records
    ]
    return live.prepare_publication(canonical, scopes).intent


def test_live_open_atomically_registers_real_job_attachment_and_body_free_outbox(control):
    store, _, envelope = control
    prepare(control)
    intent = live_intent()
    opened = store.live_open(intent, migration.hashed(envelope))
    assert opened["status"] == "ok"
    assert opened["registration"]["format"] == migration.LIVE_FORMAT
    assert opened["pending"] is None
    assert opened["layout"]["envelope"] == envelope
    with store._connection(False) as conn:
        rows = conn.execute("SELECT registration FROM migration_jobs").fetchall()
        assert len(rows) == 1
        assert conn.execute("SELECT count(*) FROM layout_jobs").fetchone()[0] == 1
        outbox = conn.execute("SELECT binding,intent_json,complete FROM intents").fetchall()
        assert len(outbox) == 1 and outbox[0]["complete"] == 0
        assert outbox[0]["binding"] == queue_module.digest(intent["semantic"])
        raw = rows[0][0] + outbox[0]["intent_json"]
        assert "PRIVATE_BODY" not in raw and "PRIVATE_CALLER" not in raw
    assert store.live_open(intent, migration.hashed(envelope)) == opened


@pytest.mark.parametrize("change", ["body", "scope"])
def test_changed_request_cannot_mutate_existing_job_or_outbox(control, change):
    store, _, envelope = control
    prepare(control)
    binding = migration.hashed(envelope)
    original = live_intent()
    first = store.live_open(original, binding)
    assert first["status"] == "ok"
    changed = live_intent("different") if change == "body" else live_intent(workspace="/other")
    assert store.live_open(changed, binding) == {"status": "conflict"}
    assert store.live_open(original, binding) == first
    with store._connection(False) as conn:
        assert conn.execute("SELECT count(*) FROM migration_jobs").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM intents").fetchone()[0] == 1


def test_conflicting_outbox_does_not_leave_job_or_attachment(control):
    store, _, envelope = control
    prepare(control)
    assert store.intent_prepare(live_intent("other")["semantic"])["status"] == "ok"
    assert store.live_open(live_intent(), migration.hashed(envelope)) == {"status": "conflict"}
    with store._connection(False) as conn:
        assert conn.execute("SELECT count(*) FROM migration_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM layout_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM intents").fetchone()[0] == 1


def test_response_construction_failure_rolls_back_all_registration_rows(control, monkeypatch):
    store, _, envelope = control
    prepare(control)

    def unavailable_context(*args):
        raise _StoreError("unavailable")

    monkeypatch.setattr(directory._NativeLayoutDirectory, "context", unavailable_context)
    assert store.live_open(live_intent(), migration.hashed(envelope)) == {"status": "unavailable"}
    with store._connection(False) as conn:
        for table in ("migration_jobs", "layout_jobs", "intents"):
            assert conn.execute("SELECT count(*) FROM " + table).fetchone()[0] == 0


def test_live_open_never_bootstraps_authority_or_accepts_wrong_operator_binding(control):
    store, _, envelope = control
    intent = live_intent()
    assert store.live_open(intent, migration.hashed(envelope)) == {"status": "unavailable"}
    with store._connection(False) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    assert not (store.generation / "exact/layout-qualification.json").exists()
    prepare(control)
    assert store.live_open(intent, "0" * 64) == {"status": "conflict"}
    with store._connection(False) as conn:
        assert conn.execute("SELECT count(*) FROM migration_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM intents").fetchone()[0] == 0

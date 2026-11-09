"""Real temporary control transactions, not native/OS admission qualification."""

import os

import pytest

from cognitive_castle import lance_layout_directory as directory
from cognitive_castle import migration_codec as migration
from cognitive_castle import semantic_queue as queue_module
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

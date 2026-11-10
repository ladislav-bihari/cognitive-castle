"""Private disposable control state; no native storage/user palace qualification."""

import hashlib
import sqlite3
import pytest
from cognitive_castle.fast_memory import _Unavailable, _Corrupt


def api():
    from cognitive_castle import capture_control_store

    return capture_control_store


def create(tmp_path):
    result = api().initialize_capture_control(tmp_path / "control", max_bytes=3 * 1024**2)
    assert result["status"] == "ok"
    store = api().CaptureControlStore(result["authority"], expected_hash=result["authority_hash"])
    return store, result


def checkpoint():
    return dict(
        generation=0,
        offset=0,
        identity=[1, 2, 0],
        gaps=0,
        unsupported=0,
        skipping=False,
        anchor="a" * 64,
        observed_size=20,
        observed_mtime_ns=10,
    )


def test_control_operator_creates_only_private_metadata_and_reopens(tmp_path):
    store, result = create(tmp_path)
    value = checkpoint()
    store.save_checkpoint("a" * 64, value)
    reopened = api().CaptureControlStore(
        result["authority"], expected_hash=result["authority_hash"]
    )
    assert reopened.load_checkpoint("a" * 64) == value
    with reopened._connect() as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {
        "capture_control_authority",
        "checkpoints",
        "coverage_observations",
        "sqlite_sequence",
        "capture_sources",
        "capture_generation_scopes",
        "capture_chunk_scopes",
        "capture_publication_intents",
        "capture_verified_chunks",
        "capture_count_baselines",
    }
    assert not any(
        hasattr(store, name) for name in ("remember", "receipt", "recall", "read_sources")
    )
    assert store.directory.stat().st_mode & 0o077 == 0
    assert all(p.stat().st_mode & 0o077 == 0 for p in store.directory.iterdir())


def test_control_operator_refuses_existing_canonical_directory_without_change(tmp_path):
    from cognitive_castle.fast_memory import MemoryStore

    old = MemoryStore(tmp_path / "old")
    assert old.remember("old", "protected original")["status"] == "ok"
    before = old.db_path.read_bytes()
    with pytest.raises((ValueError, OSError)):
        api().initialize_capture_control(old.directory, max_bytes=3 * 1024**2)
    assert old.db_path.read_bytes() == before


def test_control_ordinary_operations_never_create_a_missing_database(tmp_path):
    store, _ = create(tmp_path)
    preserved = store.db_path.with_suffix(".preserved")
    store.db_path.rename(preserved)
    with pytest.raises((_Unavailable, _Corrupt)):
        store.save_checkpoint("a" * 64, checkpoint())
    assert not store.db_path.exists() and preserved.exists()


@pytest.mark.parametrize("field", ["text", "pending", "accounting", "counter", "identity"])
def test_control_rejects_body_or_invalid_checkpoint_without_writing(tmp_path, field):
    store, _ = create(tmp_path)
    bad = checkpoint()
    if field == "text":
        bad["text"] = "BODY_MUST_NOT_PERSIST"
    elif field == "pending":
        bad["pending"] = {"hash": "b" * 64, "cursor": [0, 1], "text": "BODY_MUST_NOT_PERSIST"}
    elif field == "accounting":
        bad["accounting"] = {"text": "BODY_MUST_NOT_PERSIST"}
    elif field == "counter":
        bad["offset"] = True
    else:
        bad["identity"] = [1, True, 0]
    before = store.db_path.read_bytes()
    with pytest.raises(ValueError):
        store.save_checkpoint("a" * 64, bad)
    assert store.db_path.read_bytes() == before


def test_control_rejects_wrong_authority_hash_before_mutation(tmp_path):
    store, result = create(tmp_path)
    before = store.db_path.read_bytes()
    with pytest.raises(ValueError):
        api().CaptureControlStore(result["authority"], expected_hash="f" * 64)
    assert store.db_path.read_bytes() == before


def test_control_rejects_added_canonical_table_without_mutating_it(tmp_path):
    store, _ = create(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("CREATE TABLE chunks (text TEXT)")
    before = store.db_path.read_bytes()
    with pytest.raises((_Unavailable, _Corrupt)):
        store.save_checkpoint("a" * 64, checkpoint())
    assert store.db_path.read_bytes() == before


def test_control_rejects_db_identity_replacement_before_mutation(tmp_path):
    store, _ = create(tmp_path)
    preserved = store.db_path.with_suffix(".preserved")
    store.db_path.rename(preserved)
    store.db_path.write_bytes(preserved.read_bytes())
    store.db_path.chmod(0o600)
    before = store.db_path.read_bytes()
    with pytest.raises((_Unavailable, _Corrupt)):
        store.save_checkpoint("a" * 64, checkpoint())
    assert store.db_path.read_bytes() == before and preserved.exists()


def test_control_prepares_real_source_while_legacy_database_is_genuinely_quota_full(
    tmp_path, monkeypatch
):
    from cognitive_castle.fast_memory import MemoryStore
    from cognitive_castle import memory_capture as capture
    from cognitive_castle.capture_publication import CapturePublication
    from cognitive_castle.memory_ingest import ingest_slice
    from test_capture_handoff import transcript, stage

    old = MemoryStore(tmp_path / "old")
    assert old.remember("old", "protected original")["status"] == "ok"
    with sqlite3.connect(old.db_path) as conn:
        pages = conn.execute("PRAGMA page_count").fetchone()[0]
        size = conn.execute("PRAGMA page_size").fetchone()[0]
        assert conn.execute(f"PRAGMA max_page_count={pages}").fetchone()[0] == pages
    monkeypatch.setattr(old, "_write_budget", lambda: 3 * pages * size)
    assert old.remember("cannot-fit", "x" * 65536)["status"] == "quota"
    before = hashlib.sha256(old.db_path.read_bytes()).hexdigest()
    store, _ = create(tmp_path)
    text = "CONTROL_BODY_MUST_NOT_PERSIST Ω\x00\r\n  "
    path = transcript(tmp_path, text)
    key = capture.queue_source(
        store, path, allowed_root=tmp_path, format="codex", workspace="/project"
    )["source_key"]
    source = capture._prepare_scope(store, key, None)
    journal = CapturePublication(store, key, source["scope_attempt"])
    result = ingest_slice(
        store,
        path,
        allowed_root=tmp_path,
        format="codex",
        publisher=stage,
        on_prepared=journal.prepare,
        on_published=journal.confirm,
    )
    assert result["status"] == "prepared" and result["preparation"]["text"] == text
    assert store.load_checkpoint(key)["offset"] == 0
    assert hashlib.sha256(old.db_path.read_bytes()).hexdigest() == before
    assert not any(
        text.encode() in p.read_bytes() for p in store.directory.iterdir() if p.is_file()
    )
    with store._connect() as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM capture_publication_intents WHERE state='pending'"
            ).fetchone()[0]
            == 1
        )


def test_control_recovers_its_actual_hot_journal_under_existing_authority(tmp_path):
    import subprocess
    import sys

    store, _ = create(tmp_path)
    original = checkpoint()
    store.save_checkpoint("a" * 64, original)
    script = "import sqlite3,os,sys; c=sqlite3.connect(sys.argv[1]); c.execute('PRAGMA cache_size=1'); c.execute('BEGIN IMMEDIATE'); c.execute('UPDATE checkpoints SET value=?',('x'*65536,)); os._exit(0)"
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(store.db_path)], timeout=3, check=False
    )
    assert result.returncode == 0
    journal = store.db_path.with_name(store.db_path.name + "-journal")
    assert journal.exists() and journal.stat().st_size > 512
    (tmp_path / "pre-recovery-journal.bin").write_bytes(journal.read_bytes())
    store.save_checkpoint("b" * 64, checkpoint())
    assert store.load_checkpoint("a" * 64) == original
    assert store.load_checkpoint("b" * 64) == original


@pytest.mark.parametrize("invalid", ["long", "unicode"])
def test_control_operator_validates_path_before_any_creation(tmp_path, monkeypatch, invalid):
    from pathlib import Path

    path = tmp_path / ("x" * 2100 if invalid == "long" else "\ud800")

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid authority path reached directory creation")

    monkeypatch.setattr(Path, "mkdir", forbidden)
    with pytest.raises(ValueError):
        api().initialize_capture_control(path, max_bytes=3 * 1024**2)


def test_control_checkpoint_accepts_unconfirmed_record_reservation():
    from cognitive_castle.capture_state_codec import validate_checkpoint

    value = checkpoint()
    value["pending"] = dict(hash="b" * 64, cursor=[-1, 0])
    assert validate_checkpoint(value) == value


@pytest.mark.parametrize("cursor", [[-1, 1], [-2, 0], [True, 0], [0, True], [-1.0, 0], [0, 1.0]])
def test_control_checkpoint_rejects_invalid_reservation_cursors(cursor):
    from cognitive_castle.capture_state_codec import validate_checkpoint

    value = checkpoint()
    value["pending"] = dict(hash="b" * 64, cursor=cursor)
    with pytest.raises(ValueError):
        validate_checkpoint(value)

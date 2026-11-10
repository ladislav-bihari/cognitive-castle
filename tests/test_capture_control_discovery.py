"""Real metadata-only discovery in independent control storage; synthetic files."""

import hashlib
import sqlite3
import pytest
from test_capture_control_store import create
from test_capture_control_worker import worker, request


def call(authority, operation, args=None):
    return worker()._execute(request(authority, operation, args or {}))


def discover(authority, root, limit=1):
    return call(
        authority,
        "discover_sources",
        dict(allowed_root=str(root), format="codex", max_entries=limit),
    )


def sweep(authority, root):
    for _ in range(12):
        result = discover(authority, root)
        assert result["status"] == "ok", result
        if result["discovery"] == "complete_at_last_sweep":
            return result
    pytest.fail("finite discovery sweep did not complete")


def test_control_discovery_pages_reopen_and_resweep_names_behind_cursor(tmp_path):
    store, authority = create(tmp_path)
    root = tmp_path / "originals"
    root.mkdir()
    (root / "a.jsonl").write_text("DISCOVERY_BODY_MUST_STAY_IN_ORIGINAL\n")
    folder = root / "folder"
    folder.mkdir()
    (folder / "c.jsonl").write_text("another original\n")
    first = discover(authority, root)
    assert first["status"] == "ok" and first["discovery"] == "incomplete"
    assert call(authority, "discovery_status")["roots"][0]["completed_sweeps"] == 0
    sweep(authority, root)
    status = call(authority, "discovery_status")
    assert status["roots"][0]["completed_sweeps"] == 1
    assert status["source_currentness"] == "last_observed_only"
    (root / "0-new.jsonl").write_text("new name behind old cursor\n")
    sweep(authority, root)
    assert call(authority, "discovery_status")["roots"][0]["completed_sweeps"] == 2
    sources = call(authority, "source_page")["sources"]
    assert {row["path"] for row in sources} == {
        str(root / "a.jsonl"),
        str(folder / "c.jsonl"),
        str(root / "0-new.jsonl"),
    }
    assert all(row["processed_bytes"] == 0 for row in sources)
    assert b"DISCOVERY_BODY_MUST_STAY_IN_ORIGINAL" not in store.db_path.read_bytes()


def test_control_discovery_does_not_follow_file_or_root_symlinks(tmp_path):
    store, authority = create(tmp_path)
    root = tmp_path / "originals"
    root.mkdir()
    outside = tmp_path / "outside.jsonl"
    outside.write_text("outside root\n")
    (root / "escape.jsonl").symlink_to(outside)
    assert sweep(authority, root)["queued_sources"] == 0
    assert call(authority, "source_page")["sources"] == []
    alias = tmp_path / "root-alias"
    alias.symlink_to(root, target_is_directory=True)
    before = hashlib.sha256(store.db_path.read_bytes()).hexdigest()
    assert discover(authority, alias)["status"] == "unavailable"
    assert hashlib.sha256(store.db_path.read_bytes()).hexdigest() == before


def test_control_incomplete_scan_never_advances_cursor_or_claims_sweep(tmp_path, monkeypatch):
    from cognitive_castle import memory_discovery

    store, authority = create(tmp_path)
    root = tmp_path / "originals"
    root.mkdir()
    for name in ("a", "b"):
        (root / (name + ".jsonl")).write_text("original\n")
    monkeypatch.setattr(memory_discovery, "MAX_SCAN_ENTRIES", 1)
    result = discover(authority, root)
    assert (
        result["status"] == "ok"
        and result["discovery"] == "incomplete"
        and result["reason"] == "discovery_entry_limit"
    )
    with store._connect() as conn:
        assert conn.execute("SELECT cursor FROM discovery_dirs").fetchone()[0] == ""
    status = call(authority, "discovery_status")["roots"][0]
    assert status["completed_sweeps"] == 0 and status["discovery"] == "incomplete"
    monkeypatch.setattr(memory_discovery, "MAX_SCAN_ENTRIES", 8192)
    sweep(authority, root)
    assert len(call(authority, "source_page")["sources"]) == 2


def test_old_control_schema_is_refused_without_implicit_upgrade(tmp_path):
    store, authority = create(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("PRAGMA user_version=1")
    before = store.db_path.read_bytes()
    assert call(authority, "discovery_status")["status"] == "corrupt"
    assert store.db_path.read_bytes() == before


@pytest.mark.parametrize(
    "args",
    [
        dict(allowed_root="/", format="codex", max_entries=True),
        dict(allowed_root="/", format="codex", max_entries=65),
        dict(allowed_root="/", format="wrong"),
        dict(allowed_root="/", format="codex", initialize=True),
    ],
)
def test_discovery_request_bounds_are_checked_before_storage(tmp_path, monkeypatch, args):
    _, authority = create(tmp_path)

    def forbidden(*a, **kw):
        raise AssertionError("invalid discovery arguments reached storage")

    monkeypatch.setattr(worker(), "_store", forbidden)
    with pytest.raises(ValueError):
        call(authority, "discover_sources", args)

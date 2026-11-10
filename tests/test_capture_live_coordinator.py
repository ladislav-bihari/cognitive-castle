"""Real SQL handoff/controller with an explicit SQL-only publication fixture.

These tests verify orchestration and confirmation, never native Lance admission.
"""

import json
from pathlib import Path
import subprocess
import sys
import pytest
from cognitive_castle import live_publication
from cognitive_castle.fast_memory import MemoryStore
from test_capture_control_worker import source_case, request, worker


def api():
    from cognitive_castle.capture_live_coordinator import advance

    return advance


class Session:
    expired = False

    def _ready(self):
        return dict(status="unavailable", reason="semantic_deadline") if self.expired else None


class Control:
    def __init__(self, authority):
        self.authority = authority
        self.session = Session()

    def call(self, operation, arguments):
        return worker()._execute(request(self.authority, operation, arguments))


def run(control, key):
    return api()(control, key, "/generation", "a" * 64, wing="projects", room="conversations")


def publisher_fixture(tmp_path, monkeypatch, control, *, wrong=False, expire=False):
    destination = MemoryStore(tmp_path / "SQL-PUBLICATION-FIXTURE-ONLY")
    publications = []

    def publish(session, generation, publication, envelope_hash):
        assert session is control.session
        publications.append(publication)
        canonical = publication.canonical
        result = destination.remember(
            canonical.request_id,
            "".join(record.text for record in canonical.records),
            canonical.records[0].source()["provenance"]["caller"],
            namespace=canonical.namespace,
        )
        assert result["status"] == "ok"
        observed = destination.receipt(namespace=canonical.namespace, key=canonical.request_id)
        receipt = observed["receipt"]
        if wrong:
            receipt["stored_text_bytes"] += 1
        control.session.expired = expire
        return receipt

    monkeypatch.setattr(live_publication, "publish", publish)
    return destination, publications


def test_actual_handoff_text_and_scope_are_published_before_source_confirmation(
    tmp_path, monkeypatch
):
    store, authority, path, key, text = source_case(tmp_path)
    control = Control(authority)
    destination, publications = publisher_fixture(tmp_path, monkeypatch, control)
    result = run(control, key)
    assert result["status"] == "ok" and result["publication_consumed"] is True
    assert store.load_checkpoint(key)["offset"] == path.stat().st_size
    assert len(publications) == 1
    canonical = publications[0].canonical
    assert "".join(record.text for record in canonical.records) == "WORKER_BODY Ω\x00\r\n  "
    scopes = publications[0].intent["semantic"]["groups"][0]["references"]
    assert scopes[0]["scope"] == dict(workspace="/project", wing="projects", room="conversations")
    read = destination.read_sources([canonical.records[0].id])
    assert read["sources"][0]["text"] == text


def test_unverified_native_progress_leaves_sql_cursor_and_verified_ids_untouched(
    tmp_path, monkeypatch
):
    store, authority, _, key, _ = source_case(tmp_path)
    pending = dict(status="in_progress", reason="direct_publication_intervals")
    monkeypatch.setattr(live_publication, "publish", lambda *args: pending)
    assert run(Control(authority), key) == pending
    assert store.load_checkpoint(key)["offset"] == 0
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0


@pytest.mark.parametrize("wrong,expire", [(True, False), (False, True)])
def test_wrong_or_late_receipt_cannot_confirm_source(tmp_path, monkeypatch, wrong, expire):
    store, authority, _, key, _ = source_case(tmp_path)
    control = Control(authority)
    publisher_fixture(tmp_path, monkeypatch, control, wrong=wrong, expire=expire)
    result = run(control, key)
    assert result["status"] in {"unavailable", "conflict"}
    assert store.load_checkpoint(key)["offset"] == 0
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0


def test_actual_unscoped_capture_commits_without_inventing_workspace(tmp_path, monkeypatch):
    from test_capture_control_store import create
    from test_capture_handoff import transcript

    store, authority = create(tmp_path)
    path = transcript(tmp_path, "unscoped original Ω\x00\r\n")
    queued = worker()._execute(
        request(
            authority,
            "source_queue",
            dict(path=str(path), allowed_root=str(tmp_path), format="codex"),
        )
    )
    assert queued["status"] == "ok"
    key = queued["source_key"]
    control = Control(authority)
    _, publications = publisher_fixture(tmp_path, monkeypatch, control)
    result = run(control, key)
    assert result["status"] == "ok" and result["publication_consumed"] is True
    assert store.load_checkpoint(key)["offset"] == path.stat().st_size
    reference = publications[0].intent["semantic"]["groups"][0]["references"][0]
    assert reference["scope"] == dict(workspace=None, wing="projects", room="conversations")
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM capture_chunk_scopes").fetchone()[0] == 0
    assert control.call("coverage", {})["unscoped_chunks"] == 1


def test_source_rewrite_during_publication_cannot_confirm_changed_words(tmp_path, monkeypatch):
    store, authority, path, key, _ = source_case(tmp_path)
    control = Control(authority)
    publisher_fixture(tmp_path, monkeypatch, control)
    publish = live_publication.publish

    def rewrite(*args):
        receipt = publish(*args)
        record = json.loads(path.read_bytes())
        record["payload"]["content"][0]["text"] = "replacement source words"
        path.write_bytes(json.dumps(record).encode() + b"\n")
        return receipt

    monkeypatch.setattr(live_publication, "publish", rewrite)
    result = run(control, key)
    assert result.get("publication_consumed") is not True
    checkpoint = store.load_checkpoint(key)
    assert checkpoint["offset"] == 0 and checkpoint["generation"] == 1
    with store._connect() as conn:
        assert conn.execute("SELECT count(*) FROM capture_verified_chunks").fetchone()[0] == 0


def test_lost_confirmation_response_retries_without_publishing_again(tmp_path, monkeypatch):
    store, authority, path, key, _ = source_case(tmp_path)
    control = Control(authority)
    _, publications = publisher_fixture(tmp_path, monkeypatch, control)
    original = control.call

    def lose_response(operation, arguments):
        result = original(operation, arguments)
        if "publication" in arguments:
            assert result["publication_consumed"] is True
            return dict(status="unavailable", reason="test_lost_response")
        return result

    control.call = lose_response
    assert run(control, key) == dict(status="unavailable", reason="test_lost_response")
    assert store.load_checkpoint(key)["offset"] == path.stat().st_size
    control.call = original
    assert run(control, key)["status"] == "ok"
    assert len(publications) == 1


def test_coordinator_import_keeps_host_free_of_storage_and_model_engines():
    root = Path(__file__).resolve().parents[1]
    code = f"import sys;sys.path.insert(0,{str(root)!r});import cognitive_castle.capture_live_coordinator;assert not {{'sqlite3','lance','lancedb','pyarrow','numpy','torch'}}.intersection(sys.modules)"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=3
    )
    assert result.returncode == 0, result.stderr

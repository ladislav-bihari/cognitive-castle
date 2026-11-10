"""Real SQL handoff/controller with an explicit SQL-only publication fixture.

These tests verify orchestration and confirmation, never native Lance admission.
"""

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


def test_actual_handoff_text_and_scope_are_published_before_source_confirmation(tmp_path, monkeypatch):
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


def test_unverified_native_progress_leaves_sql_cursor_and_verified_ids_untouched(tmp_path, monkeypatch):
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


def test_unscoped_source_is_pending_without_inventing_workspace_or_publishing(tmp_path, monkeypatch):
    store, authority, _, key, _ = source_case(tmp_path)
    control = Control(authority)
    original = control.call

    def unscoped(operation, arguments):
        result = original(operation, arguments)
        if result.get("status") == "prepared":
            result["scope"] = dict(workspace=None, attribution="unknown", map=False)
        return result

    control.call = unscoped

    def forbidden(*args):
        raise AssertionError("unscoped words received an invented publication scope")

    monkeypatch.setattr(live_publication, "publish", forbidden)
    result = run(control, key)
    assert result == dict(status="in_progress", reason="capture_scope_unresolved")
    assert store.load_checkpoint(key)["offset"] == 0

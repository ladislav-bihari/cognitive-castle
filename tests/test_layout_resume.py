"""Engine-free checks of the fixed existing-layout crash-recovery entry."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


def case(monkeypatch):
    from cognitive_castle import semantic_queue as queue
    from test_lance_layout_codec import fixture

    _, _, registration, *_ = fixture()
    store = queue.SemanticQueue(Path(registration["destination"]), writable=True)
    monkeypatch.setattr(queue, "_require_supervision", lambda: None)
    return queue, store, registration


def test_layout_resume_has_closed_write_protocol(monkeypatch):
    from cognitive_castle.semantic_queue import SemanticQueue
    from cognitive_castle.semantic_store_worker import _FIELDS, QUEUE_READS
    from cognitive_castle.semantic_runtime import SemanticSession

    assert _FIELDS["queue_layout_resume"] == ({"registration"}, set())
    assert "layout_resume" not in QUEUE_READS
    assert callable(SemanticQueue.layout_resume)
    session = SemanticSession(Path("/private/unused"), interactive=False)
    stages = []
    monkeypatch.setattr(session, "_run_stage", lambda request, **limits: stages.append(limits))
    session.store(Path("/private/generation"), "queue_layout_resume", {"registration": {}})
    assert stages[0]["cap"] == 3
    assert stages[0]["rss"] == 128 * 1024**2 and stages[0]["total"] == 192 * 1024**2


def test_layout_resume_existing_only_connection_never_initializes(monkeypatch):
    from cognitive_castle.lance_exact_store import _StoreError

    queue, store, _ = case(monkeypatch)
    calls = []

    class SQLError(Exception):
        pass

    class Connection:
        def execute(self, sql):
            calls.append(sql)

        def commit(self):
            calls.append("COMMIT")

        def close(self):
            calls.append("CLOSE")

    connection = Connection()

    def connect(database, **kwargs):
        assert database.endswith("/coordination/queue.sqlite3?mode=rw")
        assert kwargs == dict(uri=True, timeout=0)
        calls.append("CONNECT_RW")
        return connection

    monkeypatch.setattr(store, "_paths", lambda create: calls.append(("paths", create)))
    monkeypatch.setitem(sys.modules, "sqlite3", SimpleNamespace(connect=connect, Row=object))
    with store._layout_resume_connection() as actual:
        assert actual is connection
    assert calls == [
        ("paths", False),
        "CONNECT_RW",
        "PRAGMA trusted_schema=OFF",
        "PRAGMA cache_size=-512",
        "PRAGMA mmap_size=0",
        "PRAGMA synchronous=FULL",
        "BEGIN IMMEDIATE",
        "COMMIT",
        "CLOSE",
    ]

    def missing(database, **kwargs):
        assert database.endswith("?mode=rw")
        raise SQLError("missing")

    monkeypatch.setitem(sys.modules, "sqlite3", SimpleNamespace(connect=missing))
    with pytest.raises(SQLError):
        with store._layout_resume_connection():
            pytest.fail("missing database cannot initialize")
    store.writable = False
    before = list(calls)
    with pytest.raises(_StoreError):
        with store._layout_resume_connection():
            pytest.fail("read-only instance cannot recover")
    assert calls == before


def test_layout_resume_requires_authority_and_exact_registration(monkeypatch):
    from cognitive_castle import lance_layout_directory as directory
    from cognitive_castle.lance_exact_store import _StoreError

    queue, store, registration = case(monkeypatch)
    transactions = []

    class DBError(Exception):
        pass

    monkeypatch.setitem(
        sys.modules, "sqlite3", SimpleNamespace(OperationalError=DBError, DatabaseError=DBError)
    )

    @contextmanager
    def connection():
        transactions.append("BEGIN")
        try:
            yield object()
        except Exception:
            transactions.append("ROLLBACK")
            raise
        else:
            transactions.append("COMMIT")

    monkeypatch.setattr(store, "_layout_resume_connection", connection)
    state = dict(status="ok", registration=registration, layout={}, native_layout={}, pending=None)

    class Authority:
        present = True
        valid_schema = True

        def __init__(self, conn):
            transactions.append("AUTHORITY")
            if not self.valid_schema:
                raise _StoreError("unavailable")

        def attachment(self, job):
            assert job == registration["job_id"]
            return {} if self.present else None

    monkeypatch.setattr(directory, "_NativeLayoutDirectory", Authority)
    monkeypatch.setattr(queue.SemanticQueue, "_layout_state", staticmethod(lambda *args: state))
    assert store.layout_resume(registration) == state
    assert transactions == ["BEGIN", "AUTHORITY", "COMMIT"]
    Authority.present = False
    transactions.clear()
    assert store.layout_resume(registration)["status"] == "unavailable"
    assert transactions == ["BEGIN", "AUTHORITY", "ROLLBACK"]
    Authority.present = True
    state["registration"] = {**registration, "destination": "/private/other"}
    transactions.clear()
    assert store.layout_resume(registration)["status"] == "conflict"
    assert transactions == ["BEGIN", "AUTHORITY", "ROLLBACK"]
    Authority.valid_schema = False
    transactions.clear()
    assert store.layout_resume(registration)["status"] == "unavailable"
    assert transactions == ["BEGIN", "AUTHORITY", "ROLLBACK"]

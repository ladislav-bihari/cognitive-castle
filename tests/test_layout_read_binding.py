"""Selected authority must bind reads inside the worker, before native access.

Pure dispatch tests supply metadata only; they do not qualify native reads.
"""

from types import SimpleNamespace
import pytest
from cognitive_castle import migration_codec
from cognitive_castle.partitioned_exact_store import PartitionedExactStore
from test_partitioned_exact_store import qualification_case


def store(monkeypatch):
    _, _, _, envelope = qualification_case()
    target = PartitionedExactStore(envelope["generation"]["path"])
    authority = SimpleNamespace(envelope=envelope)
    monkeypatch.setattr(target, "_run", lambda operation: operation(authority))
    return target, envelope


def test_wrong_selected_authority_refuses_before_native_read(monkeypatch):
    target, _ = store(monkeypatch)

    def forbidden(*args):
        raise AssertionError("wrong generation reached native read")

    monkeypatch.setattr(target, "_read", forbidden)
    result = target.layout_read(
        "read_sources", dict(ids=["memory:" + "a" * 64]), envelope_hash="f" * 64
    )
    assert result == dict(status="conflict", reason="selected_layout_authority")


def test_matching_selected_authority_keeps_existing_read_response(monkeypatch):
    target, envelope = store(monkeypatch)
    result = dict(status="unavailable", reason="fixture_native_not_run")

    def read(authority, operation, arguments, identities, context):
        assert authority.envelope == envelope and operation == "read_sources"
        assert arguments == dict(ids=["memory:" + "a" * 64])
        assert context is None
        return result

    monkeypatch.setattr(target, "_read", read)
    assert (
        target.layout_read(
            "read_sources",
            dict(ids=["memory:" + "a" * 64]),
            envelope_hash=migration_codec.hashed(envelope),
        )
        == result
    )


def test_malformed_selected_authority_fails_before_control_or_native_access(monkeypatch):
    target, _ = store(monkeypatch)

    def forbidden(*args):
        raise AssertionError("invalid authority reached storage")

    monkeypatch.setattr(target, "_run", forbidden)
    with pytest.raises(ValueError):
        target.layout_read(
            "read_sources", dict(ids=["memory:" + "a" * 64]), envelope_hash="invalid"
        )


def test_fixed_exact_worker_accepts_and_enforces_selected_read_authority(monkeypatch):
    from cognitive_castle.lance_exact_worker import _execute

    _, _, _, envelope = qualification_case()

    def run(self, operation):
        return operation(SimpleNamespace(envelope=envelope))

    monkeypatch.setattr(PartitionedExactStore, "_run", run)
    result = _execute(
        dict(
            directory=envelope["generation"]["path"],
            operation="layout_read",
            arguments=dict(
                operation="read_sources",
                arguments=dict(ids=["memory:" + "a" * 64]),
                envelope_hash="f" * 64,
            ),
            _host_pid=123,
        )
    )
    assert result == dict(status="conflict", reason="selected_layout_authority")

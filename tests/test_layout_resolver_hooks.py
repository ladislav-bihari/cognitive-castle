"""Pure lookup indirection and namespace refusal; no native/storage engines."""

from contextlib import contextmanager
import os
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from cognitive_castle import bulk_exact_codec as codec
from cognitive_castle import bulk_exact_store as bulk
from cognitive_castle import lance_exact_store as exact
from cognitive_castle.canonical_records import _hash, _json, prepare_remember


def test_default_hooks_preserve_module_lookup_and_coverage(monkeypatch):
    calls = []
    dataset = object()
    ids = ["memory:" + "a" * 64]
    monkeypatch.setattr(
        bulk,
        "lookup_many",
        lambda ds, values, checked=False: calls.append((ds, values, checked)) or {},
    )
    monkeypatch.setattr(
        bulk, "coverage", lambda ds: calls.append(("coverage", ds)) or {"version": 7}
    )
    store = bulk.BulkExactStore(Path("/private/legacy"))
    assert store._lookup_rows(dataset, ids, checked=True) == {}
    assert store._coverage(dataset) == {"version": 7}
    assert exact.LanceExactStore._find(dataset, ids[0]) is None
    assert calls == [(dataset, ids, True), ("coverage", dataset), (dataset, ids, False)]


def routed_case(monkeypatch):
    prepared = prepare_remember("routed", "original Ω\0")
    entries, owner = codec.prepare_owner("owner", "binding", prepared.records, metadata="m")
    rows = {r["id"]: r for r in entries}
    rows[owner["owner"]] = codec.receipt_row(owner, owner["owner"])
    legacy_id = "receipt:" + _hash(_json([prepared.namespace, prepared.request_id]))
    rows[legacy_id] = dict(
        id=legacy_id,
        kind="receipt",
        text="",
        provenance="",
        digest=prepared.digest,
        receipt=_json(prepared.receipt),
    )
    manifest = dict(
        format=codec.FORMAT,
        state="committed_batch",
        batch_id="old",
        source_binding="binding",
        digest=codec.validate_batch("old", "binding", entries, [owner]),
        proof=[[r["id"], _hash(_json(r))] for r in [*entries, rows[owner["owner"]]]],
        records=len(prepared.records),
        published_owners=1,
        entries=len(entries),
    )
    receipt_id = codec.key("batch", "old")
    rows[receipt_id] = codec.receipt_row(manifest, receipt_id)
    calls = []
    dataset = SimpleNamespace(version=7)

    class Routed(bulk.BulkExactStore):
        def _lookup_rows(self, ds, ids, *, checked=False):
            assert ds is dataset
            values = list(ids)
            calls.append(("lookup", values, checked))
            return {identity: rows[identity] for identity in values if identity in rows}

        def _coverage(self, ds):
            assert ds is dataset
            calls.append(("coverage",))
            return {"version": 7}

    def bypass(*args, **kwargs):
        raise AssertionError("instance routing hook bypassed")

    monkeypatch.setattr(bulk, "lookup_many", bypass)
    monkeypatch.setattr(bulk, "coverage", bypass)
    store = Routed(Path("/private/legacy"), writable=True)
    store._paths = lambda: None
    store._open = lambda: dataset

    @contextmanager
    def writer():
        yield dataset

    store._writer = writer
    return store, dataset, entries, owner, prepared, manifest, receipt_id, calls


def test_batch_proof_owner_reads_and_visibility_use_instance_hooks(monkeypatch):
    store, dataset, entries, owner, prepared, manifest, receipt_id, calls = routed_case(monkeypatch)
    assert store.import_batch("old", "binding", entries, [owner]) == store._compact(
        manifest, receipt_id
    )
    assert store.bulk_owner(owner["owner"])["manifest"] == owner
    assert store.bulk_page(owner["owner"], "membership", 0)["status"] == "ok"
    assert store.bulk_receipt("old", "binding")["status"] == "ok"
    record = next(r for r in entries if r["kind"] == "bulk_record")
    assert bulk.visible_source(store, dataset, record) == prepared.records[0].source()
    assert any(c[0] == "coverage" for c in calls)


def test_candidate_existence_uses_hook_before_native_append(monkeypatch):
    store, _, entries, _, _, _, _, calls = routed_case(monkeypatch)
    observed = []

    def before_append(dataset, rows):
        observed.extend(rows)
        assert len(rows) == 1 and rows[0]["id"] == codec.key("batch", "new")
        raise exact._StoreError("fixture_stop")

    store._before_append = before_append
    assert store.import_batch("new", "binding", entries, []) == {"status": "fixture_stop"}
    assert observed and any(c[0] == "lookup" and len(c[1]) == len(entries) for c in calls)
    assert not {"lance", "pyarrow", "numpy", "sqlite3"}.intersection(sys.modules)


def test_inherited_receipt_remember_and_hydration_use_instance_hooks(monkeypatch):
    store, _, _, _, prepared, _, _, _ = routed_case(monkeypatch)
    result = store.receipt(prepared.namespace, prepared.request_id)
    assert result == dict(status="ok", digest=prepared.digest, receipt=prepared.receipt)
    assert store.remember(prepared.request_id, "original Ω\0") == prepared.receipt
    assert store.read_sources([prepared.records[0].id]) == dict(
        status="ok", sources=[prepared.records[0].source()]
    )
    assert store.page(prepared.records[0].id)["status"] == "ok"


@pytest.mark.parametrize("marker", ["qualification", "queue", "broken_qualification"])
def test_registered_namespace_refuses_before_custom_paths_or_creation(
    tmp_path, monkeypatch, marker
):
    generation = tmp_path.resolve() / "generation"
    generation.mkdir(mode=0o700)
    if marker == "queue":
        (generation / "coordination").mkdir(mode=0o700)
        (generation / "coordination/queue.sqlite3").touch(mode=0o600)
    else:
        (generation / "exact").mkdir(mode=0o700)
        artifact = generation / "exact/layout-qualification.json"
        if marker == "qualification":
            artifact.touch(mode=0o400)
        else:
            artifact.symlink_to(generation / "missing")
    destination = generation / "exact/partitions/0"
    store = bulk.BulkExactStore(destination, writable=True)
    monkeypatch.setattr(exact, "_require_supervision", lambda: None)
    store._paths = lambda **kwargs: pytest.fail("registered mutation reached paths")
    with pytest.raises(exact._StoreError) as error, store._writer():
        pytest.fail("registered mutation reached native open")
    assert error.value.status == "unavailable" and not destination.exists()
    direct = exact.LanceExactStore(destination, writable=True)
    with pytest.raises(exact._StoreError):
        direct._paths(create=True)
    assert not destination.exists()


def test_writer_rechecks_namespace_after_lock_and_closes_descriptor(tmp_path, monkeypatch):
    generation = tmp_path.resolve() / "generation"
    destination = generation / "exact/partitions/0"
    destination.mkdir(parents=True, mode=0o700)
    (generation / "coordination").mkdir(mode=0o700)
    store = exact.LanceExactStore(destination, writable=True)
    events, descriptors = [], []
    monkeypatch.setattr(exact, "_require_supervision", lambda: None)
    store._paths = lambda **kwargs: events.append("paths")
    store._open = lambda: pytest.fail("late marker reached native open")

    def locked(descriptor, mode):
        descriptors.append(descriptor)
        events.append("lock")
        (generation / "coordination/queue.sqlite3").touch(mode=0o600)

    monkeypatch.setattr(exact.fcntl, "flock", locked)
    with pytest.raises(exact._StoreError), store._writer():
        pytest.fail("late marker admitted")
    assert events == ["paths", "lock"]
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_private_override_is_called_before_paths_and_under_lock(tmp_path, monkeypatch):
    events = []
    destination = tmp_path.resolve() / "legacy"
    destination.mkdir(mode=0o700)
    monkeypatch.setattr(exact, "_require_supervision", lambda: events.append("supervision"))

    class Qualified(exact.LanceExactStore):
        def _check_mutation_namespace(self):
            events.append("authority")

        def _paths(self, **kwargs):
            events.append("paths")

        def _open(self):
            events.append("open")
            return "dataset"

    monkeypatch.setattr(exact.fcntl, "flock", lambda *args: events.append("lock"))
    with Qualified(destination, writable=True)._writer() as dataset:
        assert dataset == "dataset"
    assert events == ["supervision", "authority", "paths", "lock", "authority", "open"]


@pytest.mark.parametrize("marker", ["qualification", "queue"])
def test_dotdot_registered_alias_refuses_before_paths(tmp_path, monkeypatch, marker):
    generation = tmp_path.resolve() / "generation"
    home = generation / "exact/partitions/0"
    home.mkdir(parents=True, mode=0o700)
    path = generation / (
        "exact/layout-qualification.json"
        if marker == "qualification"
        else "coordination/queue.sqlite3"
    )
    path.parent.mkdir(exist_ok=True, mode=0o700)
    path.touch(mode=0o400)
    store = exact.LanceExactStore(home / ".." / "0", writable=True)
    monkeypatch.setattr(exact, "_require_supervision", lambda: None)
    store._paths = lambda **kwargs: pytest.fail("dotdot alias reached paths")
    with pytest.raises(exact._StoreError), store._writer():
        pytest.fail("dotdot alias reached native open")


@pytest.mark.parametrize("same_identity", [True, False])
def test_case_alias_requires_actual_parent_identity(tmp_path, monkeypatch, same_identity):
    generation = tmp_path.resolve() / "generation"
    parent = generation / "exact/partitions"
    parent.mkdir(parents=True, mode=0o700)
    (generation / "exact/layout-qualification.json").touch(mode=0o400)
    alias = generation / "EXACT/PARTITIONS/0"
    original = Path.lstat

    def lstat(path, *args, **kwargs):
        relative = path.relative_to(generation) if path.is_relative_to(generation) else None
        if relative is not None and relative.parts and relative.parts[0] == "EXACT":
            mapped = generation.joinpath(
                *(p.lower() if p in {"EXACT", "PARTITIONS"} else p for p in relative.parts)
            )
            info = original(mapped, *args, **kwargs)
            if not same_identity and path == alias.parent:
                values = list(info)
                values[1] += 1
                return os.stat_result(values)
            return info
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(exact, "_require_supervision", lambda: None)
    store = exact.LanceExactStore(alias, writable=True)
    store._paths = lambda **kwargs: pytest.fail("case alias reached paths")
    if same_identity:
        with pytest.raises(exact._StoreError), store._writer():
            pytest.fail("case alias reached native open")
    else:
        # A different case-sensitive directory is outside the registered namespace.
        assert store._check_mutation_namespace() is None


@pytest.mark.parametrize("operation", ["bulk_owner", "read_sources", "receipt"])
def test_registered_read_cannot_expose_unacked_home_before_open(tmp_path, monkeypatch, operation):
    generation = tmp_path.resolve() / "generation"
    home = generation / "exact/partitions/0"
    home.mkdir(parents=True, mode=0o700)
    (generation / "coordination").mkdir(mode=0o700)
    (generation / "coordination/queue.sqlite3").touch(mode=0o600)
    monkeypatch.setattr(exact, "_require_supervision", lambda: None)
    store = bulk.BulkExactStore(home)
    store._open = lambda: pytest.fail("unacknowledged registered read reached open")
    arguments = {
        "bulk_owner": ("receipt:" + "a" * 64,),
        "read_sources": (["memory:" + "b" * 64],),
        "receipt": ("operator", "key"),
    }
    assert getattr(store, operation)(*arguments[operation]) == {"status": "unavailable"}

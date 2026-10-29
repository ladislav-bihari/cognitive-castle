"""Published reads pin the registered snapshot; private proof reads stay current.

Fake engine factory only: real schema/geometry/index/pin checks run, without
importing a native engine. Path/supervision and actual Lance versions require
separate owned-host qualification.
"""

import copy
import sys
from types import SimpleNamespace as NS

import pytest


def case(tmp_path, monkeypatch, *, kind="append", mode="public", fault=None):
    from test_layout_authority import authority_case
    from cognitive_castle import lance_exact_store as exact, partitioned_exact_store as module
    from cognitive_castle.bulk_exact_store import INDEX_NAME

    directory, layout, migration, policy, envelope, pin = authority_case()
    pin = copy.deepcopy(pin)
    pin.update(version=7, physical_rows=40, fragments=1)
    pin["index"].update(tail_rows=40, tail_fragments=1)
    if fault == "zero_pin":
        pin.update(version=0, physical_rows=0, fragments=0)
        pin["index"].update(tail_rows=0, tail_fragments=0)
    before = {k: pin[k] for k in ("version", "physical_rows", "fragments")}
    after = dict(
        version=pin["version"] + 1,
        physical_rows=pin["physical_rows"] + (4 if kind == "append" else 0),
        fragments=pin["fragments"] + int(kind == "append"),
    )
    mutation = (
        None
        if kind is None
        else dict(
            kind=kind,
            intent_id="a" * 64 if kind == "append" else None,
            before=copy.deepcopy(pin),
            after=after,
            new_ids=["memory:" + format(i, "064x") for i in range(4)] if kind == "append" else [],
        )
    )
    store = object.__new__(module._PartitionStore)
    store.directory = tmp_path
    (tmp_path / exact.DATASET).mkdir()
    store.pid = 0
    store.intent_id = "a" * 64 if mode in {"write", "proof"} else None
    store.proof_version = after["version"] if mode == "proof" else None
    store.writable = mode == "write"
    store.maintenance = mode == "maintenance"
    store.enumerating = mode == "enumerating"
    store.registry = dict(pin=pin, mutation=mutation)
    states = [before, after] if mode != "public" else [before]
    owner = "receipt:" + "b" * 64
    store.qualification = NS(
        owner=owner,
        storage_mode="2.0",
        max_fragments=64,
        max_physical_rows=4096,
        max_versions=128,
        expected_states=states,
    )
    calls = []
    store._paths = lambda **kwargs: calls.append(("paths",))

    def resolve(ids):
        assert ids == [owner]
        calls.append(("publication_gate",))
        return dict(status="unavailable" if fault == "unpublished" else "ok", absent=[])

    store.authority = NS(
        envelope=envelope,
        qualification=policy,
        partition=lambda pid: copy.deepcopy(store.registry),
        resolve=resolve,
    )
    monkeypatch.setattr(
        "cognitive_castle.lance_layout_directory.load_layout_qualification", lambda *args: policy
    )
    store._context_batch = lambda dataset: calls.append(("actual_batch_context", dataset.version))
    schema = object()
    monkeypatch.setattr(exact, "_schema", lambda: schema)

    def dataset(version):
        geometry = before if version == pin["version"] else after
        rows = geometry["physical_rows"] + int(fault == "geometry")
        fragments = []
        for ordinal in range(geometry["fragments"]):
            count = rows if ordinal == 0 else 0
            fragments.append(
                NS(
                    fragment_id=ordinal,
                    physical_rows=count,
                    metadata=NS(
                        deletion_file=None,
                        row_id_meta=None,
                        created_at_version_meta=None,
                        last_updated_at_version_meta=None,
                    ),
                )
            )

        def indices():
            if (kind == "maintenance" and version == after["version"]) or fault == "index":
                return [
                    NS(
                        name=INDEX_NAME,
                        field_names=["id"],
                        index_type="btree",
                        segments=[
                            NS(
                                fragment_ids=list(range(geometry["fragments"])),
                                dataset_version_at_last_update=version,
                            )
                        ],
                    )
                ]
            return []

        return NS(
            version=version,
            data_storage_version="2.0",
            get_fragments=lambda: fragments,
            describe_indices=indices,
            schema=NS(
                equals=lambda expected, check_metadata: (
                    expected is schema and check_metadata and fault != "schema"
                )
            ),
        )

    def open_dataset(path, **kwargs):
        calls.append(("open", path, kwargs))
        if fault == "missing":
            raise OSError("requested snapshot unavailable")
        version = kwargs.get("version", pin["version"] if kind is None else after["version"])
        if fault == "version":
            version = after["version"]
        return dataset(version)

    monkeypatch.setitem(sys.modules, "lance", NS(dataset=open_dataset))
    return store, calls, pin, after


@pytest.mark.parametrize("kind", ["append", "maintenance"])
def test_published_read_uses_exact_before_snapshot_while_later_mutation_waits(
    tmp_path, monkeypatch, kind
):
    store, calls, pin, _ = case(tmp_path, monkeypatch, kind=kind)
    result = store._open()
    assert result.version == pin["version"]
    opened = [c for c in calls if c[0] == "open"]
    assert len(opened) == 1 and opened[0][2]["version"] == pin["version"]
    assert ("publication_gate",) in calls
    assert not any(c[0] == "actual_batch_context" for c in calls)
    assert (
        opened[0][2]["metadata_cache_size_bytes"]
        == opened[0][2]["index_cache_size_bytes"]
        == 1024**2
    )
    assert opened[0][2]["read_params"] == dict(
        cache_repetition_index=False, validate_on_decode=True
    )


@pytest.mark.parametrize("mode", ["write", "proof", "enumerating", "maintenance"])
def test_private_mutation_and_proof_modes_keep_latest_version_and_existing_guards(
    tmp_path, monkeypatch, mode
):
    store, calls, _, after = case(
        tmp_path, monkeypatch, kind="maintenance" if mode == "maintenance" else "append", mode=mode
    )
    result = store._open()
    assert result.version == after["version"]
    assert len([c for c in calls if c[0] == "open"]) == 1
    assert "version" not in next(c[2] for c in calls if c[0] == "open")
    assert ("publication_gate",) not in calls
    assert any(c[0] == "actual_batch_context" for c in calls) == (mode == "proof")


def test_without_pending_mutation_public_read_still_opens_current_and_validates_pin(
    tmp_path, monkeypatch
):
    store, calls, pin, _ = case(tmp_path, monkeypatch, kind=None)
    assert store._open().version == pin["version"]
    assert "version" not in next(c[2] for c in calls if c[0] == "open")


@pytest.mark.parametrize(
    "fault", ["missing", "schema", "version", "geometry", "index", "unpublished", "zero_pin"]
)
def test_snapshot_choice_never_falls_back_or_bypasses_existing_validation(
    tmp_path, monkeypatch, fault
):
    from cognitive_castle.lance_exact_store import _StoreError

    store, calls, _, _ = case(tmp_path, monkeypatch, fault=fault)
    with pytest.raises((OSError, _StoreError)):
        store._open()
    opened = [c for c in calls if c[0] == "open"]
    assert len(opened) == (0 if fault in {"unpublished", "zero_pin"} else 1)
    assert all(c[2].get("version") == 7 for c in opened)


def test_unregistered_latest_commit_cannot_be_hidden_by_forcing_an_old_snapshot(
    tmp_path, monkeypatch
):
    from cognitive_castle.lance_exact_store import _StoreError

    store, calls, _, _ = case(tmp_path, monkeypatch, kind=None, fault="version")
    with pytest.raises(_StoreError):
        store._open()
    opened = [c for c in calls if c[0] == "open"]
    assert len(opened) == 1 and "version" not in opened[0][2]

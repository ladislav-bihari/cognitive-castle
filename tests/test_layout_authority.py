"""Engine-free authority boundaries; SQLite qualification remains separately gated."""

import pytest
from test_partitioned_exact_store import qualification_case


def authority_case():
    from cognitive_castle.lance_layout_directory import _NativeLayoutDirectory

    layout, migration, qualification, envelope = qualification_case()
    directory = object.__new__(_NativeLayoutDirectory)
    directory.qualification, directory.envelope, directory.epoch = qualification, envelope, "a" * 64
    pin = dict(
        format=layout.PARTITION_PIN,
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        partition_id=0,
        parent_identity=[1, 2],
        directory_identity=[1, 3],
        version=0,
        physical_rows=0,
        fragments=0,
        index=dict(version=None, segments=0, indexed_fragments=0, tail_fragments=0, tail_rows=0),
    )
    return directory, layout, migration, qualification, envelope, pin


def test_partition_protocol_private_registration_rejects_unpinned_policy_before_io():
    from cognitive_castle.lance_layout_directory import _register_layout_authority
    from cognitive_castle.semantic_store_worker import _FIELDS

    _, _, q, envelope = qualification_case()

    class NoSQL:
        def execute(self, *args):
            raise AssertionError("unqualified registration must not reach SQL")

    with pytest.raises(ValueError):
        _register_layout_authority(NoSQL(), "a" * 64, envelope, q, expected_hash="b" * 64)
    assert not any(
        "qualification" in operation or "authority" in operation for operation in _FIELDS
    )


def test_partition_protocol_claim_hit_never_becomes_global_absence(monkeypatch):
    from cognitive_castle.lance_layout_directory import _NativeLayoutDirectory

    identity = "memory:" + "a" * 64
    other = "memory:" + "b" * 64
    directory = object.__new__(_NativeLayoutDirectory)
    claim = dict(
        id=identity,
        kind="bulk_record",
        partition_id=0,
        gate="receipt:" + "c" * 64,
        intent_id="d" * 64,
        state="confirmed",
        version=1,
        receipt="receipt:" + "e" * 64,
        row_hash="f" * 64,
    )
    monkeypatch.setattr(directory, "_claim", lambda value: claim if value == identity else None)
    monkeypatch.setattr(directory, "_route_for_id", lambda value: None)
    monkeypatch.setattr(directory, "_route_row", lambda value: None)
    monkeypatch.setattr(directory, "_ready_registry", lambda: True)
    assert directory.resolve([identity]) == dict(
        status="unavailable", routes=[], absent=[], unavailable=[identity], claims=[]
    )
    assert directory.resolve([other]) == dict(
        status="ok", routes=[], absent=[other], unavailable=[], claims=[]
    )
    monkeypatch.setattr(directory, "_ready_registry", lambda: False)
    assert directory.resolve([other])["unavailable"] == [other]


def test_partition_protocol_orphan_route_never_becomes_global_absence(monkeypatch):
    directory, *_ = authority_case()
    identity = "memory:" + "a" * 64
    monkeypatch.setattr(directory, "_claim", lambda value: None)
    monkeypatch.setattr(directory, "_ready_registry", lambda: True)
    monkeypatch.setattr(directory, "_route_row", lambda value: object())
    assert directory.resolve([identity]) == dict(
        status="unavailable", routes=[], absent=[], unavailable=[identity], claims=[]
    )
    monkeypatch.setattr(directory, "_route_row", lambda value: None)
    assert directory.resolve([identity]) == dict(
        status="ok", routes=[], absent=[identity], unavailable=[], claims=[]
    )


def test_claim_schema_rejects_unknown_body_and_unconfirmed_version():
    from cognitive_castle.lance_layout_directory import _claim_shape
    from cognitive_castle.lance_exact_store import _StoreError

    value = dict(
        id="memory:" + "a" * 64,
        kind="bulk_record",
        partition_id=0,
        gate="receipt:" + "b" * 64,
        intent_id="c" * 64,
        state="reserved",
        version=None,
        receipt="receipt:" + "d" * 64,
        row_hash="e" * 64,
    )
    assert _claim_shape(value) == value
    for change in (
        dict(text="body"),
        dict(version=True),
        dict(row_hash="not a hash"),
        dict(state="published"),
    ):
        with pytest.raises((ValueError, _StoreError)):
            _claim_shape({**value, **change})


def test_reserved_claim_requires_exact_current_intent_candidate(monkeypatch):
    directory, layout, migration, _, envelope, _ = authority_case()
    from test_lance_layout_codec import fixture

    _, _, reg, result, plan, _, _ = fixture()
    intent = layout.prepare_layout_interval(reg, result, layout.target(envelope, 0, [plan]))
    candidate = intent["routes"][0]
    claim = dict(
        **candidate,
        partition_id=0,
        intent_id=intent["intent_id"],
        state="reserved",
        version=None,
        receipt=layout.batch_receipt_id(intent),
    )
    monkeypatch.setattr(directory, "pending", lambda value: intent)
    monkeypatch.setattr(directory, "_claim", lambda value: claim)
    assert directory.resolve([claim["id"]], intent_id=intent["intent_id"])["claims"] == [claim]
    assert directory.resolve([claim["id"]])["unavailable"] == [claim["id"]]
    claim["row_hash"] = "f" * 64
    assert directory.resolve([claim["id"]], intent_id=intent["intent_id"])["unavailable"] == [
        claim["id"]
    ]


def test_staged_route_is_visible_only_to_same_pending_owner(monkeypatch):
    directory, layout, migration, _, envelope, _ = authority_case()
    from test_lance_layout_codec import fixture

    _, _, reg, result, plan, _, _ = fixture()
    intent = layout.prepare_layout_interval(reg, result, layout.target(envelope, 0, [plan]))
    candidate = intent["routes"][0]
    claim = dict(
        **candidate,
        partition_id=0,
        intent_id="b" * 64,
        state="confirmed",
        version=1,
        receipt="receipt:" + "c" * 64,
    )
    route = dict(
        format=layout.ROUTE,
        **candidate,
        partition_id=0,
        version=1,
        receipt=claim["receipt"],
        state="staged",
    )
    monkeypatch.setattr(directory, "pending", lambda value: intent)
    monkeypatch.setattr(directory, "_claim", lambda value: claim)
    monkeypatch.setattr(directory, "_route_for_id", lambda value: route)
    monkeypatch.setattr(directory, "partition", lambda value: None)
    assert directory.resolve([claim["id"]])["unavailable"] == [claim["id"]]
    assert directory.resolve([claim["id"]], intent_id=intent["intent_id"])["routes"] == [route]
    route["gate"] = claim["gate"] = "receipt:" + "f" * 64
    assert directory.resolve([claim["id"]], intent_id=intent["intent_id"])["unavailable"] == [
        claim["id"]
    ]


def test_dirty_registry_and_tail_limit_refuse_before_metadata_writes(monkeypatch):
    from cognitive_castle.lance_exact_store import _StoreError
    from test_lance_layout_codec import fixture

    directory, layout, _, _, envelope, pin = authority_case()
    _, _, reg, result, plan, _, _ = fixture()
    intent = layout.prepare_layout_interval(reg, result, layout.target(envelope, 0, [plan]))

    class NoSQL:
        def execute(self, *args):
            pytest.fail("refused admission reached mutation")

    directory.conn = NoSQL()
    monkeypatch.setattr(directory, "_state", lambda value: dict(registration=reg))
    monkeypatch.setattr(directory, "context", lambda value: {})
    registered = dict(pin=pin, mutation=None, active_owner=intent["routes"][0]["gate"])
    monkeypatch.setattr(directory, "partition", lambda value: registered)
    monkeypatch.setattr(directory, "_ready_registry", lambda: False)
    with pytest.raises(_StoreError):
        directory.prepare(reg["job_id"], intent["cursor_before"], intent)
    monkeypatch.setattr(directory, "_ready_registry", lambda: True)
    monkeypatch.setattr(directory, "_claim", lambda value: None)
    monkeypatch.setattr(directory, "_route_row", lambda value: None)
    pin.update(version=1, physical_rows=1024, fragments=1)
    pin["index"].update(tail_rows=1024, tail_fragments=1)
    with pytest.raises(_StoreError) as error:
        directory.prepare(reg["job_id"], intent["cursor_before"], intent)
    assert error.value.status == "index_required"


def test_maintenance_prepare_replay_keeps_same_mutation_token(monkeypatch):
    directory, _, _, _, _, pin = authority_case()
    pin.update(version=1, physical_rows=1, fragments=1)
    pin["index"].update(tail_rows=1, tail_fragments=1)
    mutation = dict(
        kind="maintenance",
        intent_id=None,
        before=pin,
        after=dict(version=2, physical_rows=1, fragments=1),
        new_ids=[],
    )
    monkeypatch.setattr(
        directory,
        "partition",
        lambda value: dict(pin=pin, mutation=mutation, rebuild_token="b" * 64),
    )
    assert directory.maintenance_prepare(0, 1) == dict(status="ok", rebuild_token="b" * 64)


def test_wrong_rebuild_epoch_or_token_cannot_mutate(monkeypatch):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, _, _, _, envelope, pin = authority_case()
    registered = dict(pin=pin, mutation=None, rebuild_token="b" * 64)
    monkeypatch.setattr(directory, "partition", lambda value: registered)
    page = dict(
        format="lance-layout-directory-page-v1",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        epoch=directory.epoch,
        rebuild_token="c" * 64,
        partition_id=0,
        version=0,
        cursor_before=0,
        cursor_after=0,
        pin=pin,
        rows=[],
        complete=True,
    )
    with pytest.raises(_StoreError):
        directory.reconcile(page)
    page.update(rebuild_token="b" * 64, epoch="d" * 64)
    with pytest.raises(_StoreError):
        directory.reconcile(page)


def test_dirty_append_rebuild_never_confirms_claim_or_acknowledges_source(monkeypatch):
    directory, layout, _, _, envelope, pin = authority_case()
    identity, owner, intent = "receipt:" + "b" * 64, "receipt:" + "c" * 64, "d" * 64
    claim = dict(
        id=identity,
        kind="bulk_receipt",
        partition_id=0,
        gate=owner,
        intent_id=intent,
        state="reserved",
        version=None,
        receipt=identity,
        row_hash=None,
    )
    mutation = dict(
        kind="append",
        intent_id=intent,
        before=pin,
        after=dict(version=1, physical_rows=1, fragments=1),
        new_ids=[identity],
    )
    registered = dict(
        pin=pin,
        mutation=mutation,
        rebuild_token="e" * 64,
        cursor=0,
        rebuild_version=0,
        last_page=None,
    )
    actual = directory._expected_pin(registered)
    observed = dict(
        format=layout.ROUTE,
        id=identity,
        kind="bulk_receipt",
        partition_id=0,
        gate=owner,
        version=1,
        receipt=identity,
        row_hash="f" * 64,
        state="staged",
    )
    page = dict(
        format="lance-layout-directory-page-v1",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        epoch=directory.epoch,
        rebuild_token=registered["rebuild_token"],
        partition_id=0,
        version=1,
        cursor_before=0,
        cursor_after=1,
        pin=actual,
        rows=[observed],
        complete=True,
    )

    class Recorder:
        writes = []

        def execute(self, sql, arguments):
            if sql.startswith("UPDATE") or sql.startswith("INSERT"):
                self.writes.append(sql)
            return self

        def fetchone(self):
            return (None, 0)

    directory.conn = Recorder()
    monkeypatch.setattr(directory, "partition", lambda value: registered)
    monkeypatch.setattr(directory, "_claim", lambda value: claim)
    monkeypatch.setattr(directory, "_refresh_ready", lambda: None)
    monkeypatch.setattr(directory, "_ready_registry", lambda: False)
    assert directory.reconcile(page) == dict(status="ok", next_cursor=1, registry_ready=False)
    assert claim["state"] == "reserved" and claim["version"] is None
    assert directory.conn.writes and all(
        "layout_claims SET seen_token" in sql or "layout_registry SET rebuild_cursor" in sql
        for sql in directory.conn.writes
    )


def test_private_setup_accepts_only_bounded_empty_precreated_containers(tmp_path):
    from cognitive_castle.lance_layout_directory import _empty_pins
    from cognitive_castle.lance_exact_store import _StoreError

    _, _, _, _, envelope, _ = authority_case()
    root = tmp_path.resolve() / "generation"
    root.mkdir(mode=0o700)
    (root / "exact").mkdir(mode=0o700)
    (root / "exact/tmp").mkdir(mode=0o700)
    (root / "exact/partitions").mkdir(mode=0o700)
    for pid in range(envelope["max_partitions"]):
        (root / "exact/partitions" / str(pid)).mkdir(mode=0o700)
    info = root.stat()
    envelope = {
        **envelope,
        "generation": dict(path=str(root), device=info.st_dev, inode=info.st_ino),
    }
    pins = _empty_pins(envelope)
    assert set(pins) == {str(pid) for pid in range(envelope["max_partitions"])}
    assert all(p["version"] == p["physical_rows"] == p["fragments"] == 0 for p in pins.values())
    # An interrupted artifact without authority cannot be treated as fresh setup.
    (root / "exact/layout-qualification.json").touch(mode=0o400)
    with pytest.raises(_StoreError):
        _empty_pins(envelope)


def test_progressed_metadata_schema_cannot_implicitly_acquire_native_authority():
    from cognitive_castle.lance_layout_directory import _register_layout_authority
    from cognitive_castle.lance_exact_store import _StoreError

    _, migration, qualification, envelope = qualification_case()

    class MetadataOnly:
        in_transaction = True

        def execute(self, sql):
            assert sql == "PRAGMA user_version"
            return self

        def fetchone(self):
            return (4,)

    with pytest.raises(_StoreError) as error:
        _register_layout_authority(
            MetadataOnly(),
            "a" * 64,
            envelope,
            qualification,
            expected_hash=migration.hashed(qualification),
        )
    assert error.value.status == "unavailable"


@pytest.mark.parametrize("cursor,complete", [(3, False), (10, True)])
def test_ack_extends_only_certified_prefix_without_erasing_rebuild_debt(
    monkeypatch, cursor, complete
):
    from cognitive_castle.lance_layout_directory import LayoutDirectory
    from test_lance_layout_codec import fixture

    directory, layout, migration, _, envelope, pin = authority_case()
    _, _, reg, exported, plan, _, _ = fixture()
    intent = layout.prepare_layout_interval(reg, exported, layout.target(envelope, 0, [plan]))
    pin.update(version=1, physical_rows=10, fragments=1)
    pin["index"].update(tail_rows=10, tail_fragments=1)
    mutation = dict(
        kind="append",
        intent_id=intent["intent_id"],
        before=pin,
        after=dict(version=2, physical_rows=10 + len(intent["routes"]), fragments=2),
        new_ids=[r["id"] for r in intent["routes"]],
    )
    registered = dict(
        pin=pin,
        mutation=mutation,
        active_owner=plan["owner"],
        cursor=cursor,
        rebuild_token="c" * 64,
    )
    observations = [
        {
            **row,
            "format": layout.ROUTE,
            "row_hash": row["row_hash"] or "d" * 64,
            "partition_id": 0,
            "version": 2,
            "receipt": layout.batch_receipt_id(intent),
            "state": "staged",
        }
        for row in intent["routes"]
    ]
    summary = layout.verified_summary(
        intent,
        dict(
            format=layout.BATCH_RESULT,
            outcome="existing_verified",
            receipt=layout.expected_receipt(intent),
            routes=observations,
        ),
        [
            dict(
                owner=plan["owner"],
                manifest_hash=plan["manifest_hash"],
                partition_id=0,
                version=2,
                live_receipt_ids=plan["live_receipt_ids"],
            )
        ],
    )
    claims = {
        row["id"]: dict(
            **row,
            partition_id=0,
            intent_id=intent["intent_id"],
            state="reserved",
            version=None,
            receipt=layout.batch_receipt_id(intent),
        )
        for row in intent["routes"]
    }

    class Recorder:
        next_cursor = None

        def execute(self, sql, arguments):
            if sql.startswith("UPDATE layout_registry SET pin="):
                self.next_cursor = arguments[1]

    directory.conn = Recorder()
    monkeypatch.setattr(directory, "_state", lambda job: dict(last_ack=None))
    monkeypatch.setattr(directory, "pending", lambda identity: intent)
    monkeypatch.setattr(directory, "partition", lambda pid: registered)
    monkeypatch.setattr(directory, "_claim", lambda identity: claims[identity])
    monkeypatch.setattr(directory, "_refresh_ready", lambda: None)
    monkeypatch.setattr(LayoutDirectory, "ack", lambda *args: dict(status="ok"))
    assert directory.ack(reg["job_id"], intent["intent_id"], summary) == dict(status="ok")
    assert (directory.conn.next_cursor == mutation["after"]["physical_rows"]) is complete


def test_index_confirmation_preserves_intervening_rebuild_debt(monkeypatch):
    directory, layout, _, _, envelope, pin = authority_case()
    pin.update(version=1, physical_rows=10, fragments=1)
    pin["index"].update(tail_rows=10, tail_fragments=1)
    mutation = dict(
        kind="maintenance",
        intent_id=None,
        before=pin,
        after=dict(version=2, physical_rows=10, fragments=1),
        new_ids=[],
    )
    registered = dict(pin=pin, mutation=mutation, cursor=2, last_page=None, rebuild_token="c" * 64)
    actual = {
        **pin,
        "version": 2,
        "index": dict(version=2, segments=1, indexed_fragments=1, tail_fragments=0, tail_rows=0),
    }
    row = dict(
        format=layout.ROUTE,
        id="receipt:" + "d" * 64,
        kind="bulk_receipt",
        row_hash="e" * 64,
        partition_id=0,
        gate="receipt:" + "f" * 64,
        version=1,
        receipt="receipt:" + "a" * 64,
        state="published",
    )
    page = dict(
        format="lance-layout-directory-page-v1",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        epoch=directory.epoch,
        rebuild_token=registered["rebuild_token"],
        partition_id=0,
        version=2,
        cursor_before=0,
        cursor_after=1,
        pin=actual,
        rows=[row],
        complete=False,
    )

    class Recorder:
        cursor = None

        def execute(self, sql, arguments):
            self.cursor = arguments[1]

    directory.conn = Recorder()
    monkeypatch.setattr(directory, "partition", lambda pid: registered)
    monkeypatch.setattr(directory, "claim_route", lambda identity: row)
    monkeypatch.setattr(directory, "_refresh_ready", lambda: None)
    monkeypatch.setattr(directory, "_ready_registry", lambda: False)
    assert directory.reconcile(page)["registry_ready"] is False
    assert directory.conn.cursor == 2


def test_partition_protocol_missing_registry_cannot_certify_absence():
    directory, _, _, _, envelope, _ = authority_case()
    maximum = envelope["max_partitions"]

    class Rows:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Connection:
        geometry = (maximum, 0, maximum - 1)

        def execute(self, sql, args=()):
            assert "LIMIT 257" in sql
            return Rows(self.geometry)

    directory.conn = Connection()
    assert directory._registry_complete()
    for geometry in ((maximum - 1, 0, maximum - 2), (maximum, 1, maximum), (257, 0, 256)):
        directory.conn.geometry = geometry
        assert not directory._registry_complete()


@pytest.mark.parametrize(
    "field", ["id", "kind", "partition_id", "gate", "row_hash", "version", "receipt", "state"]
)
def test_ack_reused_claim_mismatch_refuses_before_writes(monkeypatch, field):
    from cognitive_castle import lance_layout_directory as metadata
    from cognitive_castle.lance_exact_store import _StoreError

    directory, layout, _, _, _, _ = authority_case()
    identity = "memory:" + "a" * 64
    observed = dict(
        format=layout.ROUTE,
        id=identity,
        kind="bulk_record",
        partition_id=0,
        gate="receipt:" + "b" * 64,
        row_hash="c" * 64,
        version=1,
        receipt="receipt:" + "d" * 64,
        state="published",
    )
    claim = {k: v for k, v in observed.items() if k != "format"}
    claim.update(state="confirmed", intent_id="e" * 64)
    actual_route = dict(observed)
    replacements = dict(
        id="memory:" + "f" * 64,
        kind="bulk_field_page",
        partition_id=1,
        gate="receipt:" + "f" * 64,
        row_hash="f" * 64,
        version=2,
        receipt="receipt:" + "f" * 64,
        state="staged",
    )
    if field == "state":
        actual_route[field] = replacements[field]
    else:
        claim[field] = replacements[field]
    intent_id, job_id = "e" * 64, "f" * 64
    intent = dict(job_id=job_id, target=dict(partition_id=0))
    monkeypatch.setattr(directory, "_state", lambda job: dict(last_ack=None))
    monkeypatch.setattr(directory, "pending", lambda identity: intent)
    monkeypatch.setattr(
        directory, "partition", lambda pid: dict(mutation=dict(intent_id=intent_id, new_ids=[]))
    )
    monkeypatch.setattr(directory, "_expected_pin", lambda registered: dict(version=2))
    monkeypatch.setattr(directory, "_claim", lambda identity: claim)
    monkeypatch.setattr(directory, "claim_route", lambda identity: actual_route)
    monkeypatch.setattr(layout, "validate_layout_verified", lambda *args: None)
    monkeypatch.setattr(
        metadata.LayoutDirectory,
        "ack",
        lambda *args: pytest.fail("mismatched authority reached writes"),
    )
    with pytest.raises(_StoreError):
        directory.ack(job_id, intent_id, dict(batch=dict(routes=[observed])))

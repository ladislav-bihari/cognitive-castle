"""Engine-free transaction control oracle; native/SQLite qualification is separate.

The connection models SQL writes, not SQLite isolation. Qualified pin validation is
covered independently; this oracle isolates prepare, replay, gates and common ACK
progression without importing a native engine or SQLite in the test host.
"""

import copy
import json

import pytest


def case(monkeypatch, *, owners=2):
    from test_layout_native_prefix_binding import authority, Result
    from test_layout_authority import authority_case
    from test_layout_source_packing import prefix_case
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_packing import pack_source_prefix
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval

    directory, job, plans, envelope, store, events, state = authority(monkeypatch)
    target = directory.bind_prefix(job, plans[:owners], envelope)["target"]
    reg, before, parts, _ = prefix_case()
    intent = prepare_prefix_interval(reg, pack_source_prefix(reg, before, parts[:owners]), target)
    store.update(routes={}, claims={}, migration_owners={}, ready=1)
    from cognitive_castle import lance_layout_codec as layout

    state["layout"].update(format=layout.ATTACHMENT, envelope_hash=migration.hashed(envelope))
    state.update(
        counts=migration.empty_counts(),
        completed_frontier=copy.deepcopy(before),
        last_ack=None,
        last_refusal=None,
        owner_references=0,
        semantic_pending=0,
        coverage="indexed-only",
    )
    pin = authority_case()[-1]
    for pid, registry in store["registry"].items():
        registry.update(
            pin=dict(copy.deepcopy(pin), partition_id=pid),
            cursor=0,
            claim_count=0,
            rebuild_token="a" * 64,
        )
    raw_conn = directory.conn

    class Conn:
        def execute(self, sql, args=()):
            events.append((sql, args))
            if sql.startswith("INSERT INTO layout_routes"):
                store["routes"][args[0]] = dict(
                    zip(
                        ["id", "kind", "row_hash", "partition_id", "gate", "intent_id", "job_id"],
                        args,
                    ),
                    version=None,
                    receipt=None,
                    state="reserved",
                )
            elif sql.startswith("UPDATE migration_jobs SET pending="):
                state.update(pending=json.loads(args[0]), state="active", last_refusal=None)
            elif sql.startswith("INSERT INTO layout_claims"):
                store["claims"][args[0]] = json.loads(args[1])
            elif sql.startswith("UPDATE layout_registry SET mutation="):
                store["registry"][args[1]]["mutation"] = json.loads(args[0])
            elif sql == "UPDATE layout_authority SET ready=0 WHERE id=1":
                store["ready"] = 0
            elif "FROM layout_claims WHERE intent_id=" in sql:
                return Result(
                    [(job,)]
                    if any(c["intent_id"] == args[0] for c in store["claims"].values())
                    else []
                )
            elif sql.startswith("SELECT partition_id FROM layout_registry WHERE active_owner="):
                return Result(
                    [
                        (pid,)
                        for pid, r in store["registry"].items()
                        if r["active_owner"] == args[0]
                    ][:1]
                )
            elif sql.startswith("SELECT partition_id FROM layout_homes WHERE owner="):
                return Result([(store["homes"][args[0]]["pid"],)])
            elif sql.startswith("SELECT 1 FROM migration_owners"):
                return Result([(1,)] if args[1] in store["migration_owners"] else [])
            elif sql.startswith("UPDATE layout_routes SET row_hash="):
                store["routes"][args[4]].update(
                    row_hash=args[0], version=args[1], receipt=args[2], state=args[3]
                )
            elif sql.startswith("UPDATE layout_homes SET published=1"):
                store["homes"][args[0]]["published"] = 1
            elif sql.startswith("INSERT INTO migration_owners"):
                store["migration_owners"][args[1]] = json.loads(args[2])
            elif sql.startswith("UPDATE migration_jobs SET staging_cursor="):
                state.update(
                    staging_cursor=json.loads(args[0]),
                    completed_frontier=json.loads(args[1]),
                    counts=json.loads(args[2]),
                    coverage=args[3],
                    last_ack=json.loads(args[4]),
                    pending=None,
                    state="active",
                    last_refusal=None,
                    owner_references=state["owner_references"] + args[5],
                    semantic_pending=state["semantic_pending"] + args[6],
                )
            elif sql.startswith("UPDATE layout_claims SET claim="):
                store["claims"][args[1]] = json.loads(args[0])
            elif sql.startswith("UPDATE layout_registry SET pin="):
                store["registry"][args[6]].update(
                    pin=json.loads(args[0]),
                    mutation=None,
                    state="writable",
                    cursor=args[1],
                    claim_count=args[4],
                    active_owner=args[5],
                )
            elif sql.startswith("UPDATE layout_spans SET first_row="):
                store["spans"][args[4]] = tuple(args[:4])
            elif sql.startswith("UPDATE layout_spans SET next_row="):
                old = store["spans"][args[2]]
                store["spans"][args[2]] = (old[0], args[0], old[2], args[1])
            elif sql.startswith("SELECT 1 FROM layout_registry WHERE mutation IS NOT NULL"):
                return Result(
                    [(1,)]
                    if any(
                        r["mutation"] is not None or r["cursor"] != r["claim_count"]
                        for r in store["registry"].values()
                    )
                    else []
                )
            elif sql.startswith("UPDATE layout_authority SET ready=?"):
                store["ready"] = args[0]
            else:
                return raw_conn.execute(sql, args)
            return Result([])

    directory.conn = Conn()
    directory._claim = lambda identity: copy.deepcopy(store["claims"].get(identity))
    directory._route_row = lambda identity: copy.deepcopy(store["routes"].get(identity))
    directory._ready_registry = lambda: bool(store["ready"])
    directory.qualification = dict(reservations=dict(append=dict(physical_rows=256)))

    def expected(registered):
        # Qualified shape/limits are a separate gate. Retain exact production
        # append arithmetic and tail debt here, rather than fake native evidence.
        result = copy.deepcopy(registered["pin"])
        mutation = registered["mutation"]
        if mutation is not None:
            result.update(mutation["after"])
            result["index"]["tail_rows"] += len(mutation["new_ids"])
            result["index"]["tail_fragments"] += 1
        return result

    directory._expected_pin = expected
    events.clear()
    return directory, job, intent, store, state, parts[:owners]


def observed(intent, parts):
    from cognitive_castle import bulk_exact_codec as bulk, migration_codec as migration
    from cognitive_castle import lance_layout_codec as layout

    entries = [r for part in parts for r in part["export"]["entries"]]
    manifests = [part["export"]["finalizations"][0] for part in parts]
    rows = entries + [bulk.receipt_row(m, m["owner"]) for m in manifests]
    batch = dict(
        format=bulk.FORMAT,
        state="committed_batch",
        batch_id=intent["intent_id"],
        source_binding=intent["source_binding"],
        digest=intent["digest"],
        proof=[[r["id"], migration.hashed(r)] for r in rows],
        records=intent["records"],
        entries=intent["entries"],
        published_owners=len(manifests),
    )
    rows.append(bulk.receipt_row(batch, layout.batch_receipt_id(intent)))
    by_id = {r["id"]: r for r in rows}
    routes = [
        dict(
            r,
            format=layout.ROUTE,
            row_hash=migration.hashed(by_id[r["id"]]),
            partition_id=intent["target"]["partition_id"],
            version=1,
            receipt=layout.batch_receipt_id(intent),
            state="staged",
        )
        for r in intent["routes"]
    ]
    return dict(
        format=layout.VERIFIED,
        intent_hash=migration.hashed(intent),
        proof_version=migration.PROOF_VERSION,
        batch=dict(
            format=layout.BATCH_RESULT,
            outcome="existing_verified",
            receipt=layout.expected_receipt(intent),
            routes=routes,
        ),
        owner_proofs=[
            dict(
                owner=o["owner"],
                manifest_hash=o["manifest_hash"],
                partition_id=intent["target"]["partition_id"],
                version=1,
                live_receipt_ids=o["live_receipt_ids"],
            )
            for o in intent["owners"]
        ],
    )


def prepared(monkeypatch):
    directory, job, intent, store, state, parts = case(monkeypatch)
    result = directory.prepare(job, intent["cursor_before"], intent)
    assert result == dict(status="ok", intent_id=intent["intent_id"])
    return directory, job, intent, store, state, parts


def test_packed_prepare_replay_and_atomic_ack_progress_all_original_owners(monkeypatch):
    from cognitive_castle.lance_layout_packed_rows import owner_spans

    directory, job, intent, store, state, parts = prepared(monkeypatch)
    saved = copy.deepcopy((store, state))
    assert directory.prepare(job, intent["cursor_before"], intent)["status"] == "ok"
    assert (store, state) == saved
    assert directory.pending(intent["intent_id"]) == intent
    ids = [r["id"] for r in intent["routes"]]
    assert directory.resolve(ids)["unavailable"] == ids
    resolved = directory.resolve(ids, intent_id=intent["intent_id"])
    assert resolved["status"] == "ok" and [r["id"] for r in resolved["claims"]] == ids
    spans = owner_spans(intent, store["registry"][0]["mutation"])
    assert all(directory.span(o) == span for o, span in spans.items())
    assert not store["migration_owners"] and not any(
        h["published"] for h in store["homes"].values()
    )
    assert state["completed_frontier"] == intent["cursor_before"]
    proof = observed(intent, parts)
    result = directory.ack(job, intent["intent_id"], proof)
    assert result["staging_cursor"] == result["completed_frontier"] == intent["cursor_after"]
    assert result["counts"] == intent["counts"]
    assert (
        state["pending"] is None
        and state["owner_references"] == 2
        and state["semantic_pending"] == 2
    )
    assert set(store["migration_owners"]) == {o["owner"] for o in intent["owners"]}
    assert all(h["published"] for h in store["homes"].values()) and store["ready"] == 1
    assert all(c["state"] == "confirmed" for c in store["claims"].values())
    assert all(r["state"] == "published" for r in store["routes"].values())
    assert store["registry"][0]["mutation"] is None
    assert store["spans"] == {
        o: (s["first_row"], s["next_row"], s["first_fragment"], s["next_fragment"])
        for o, s in spans.items()
    }
    saved = copy.deepcopy((store, state))
    assert directory.ack(job, intent["intent_id"], proof) == result and (store, state) == saved


@pytest.mark.parametrize(
    "damage",
    ["later_owner_version", "missing_owner", "later_row_hash", "later_gate", "missing_batch"],
)
def test_bad_later_proof_never_writes_any_part_of_the_ack(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, intent, store, state, parts = prepared(monkeypatch)
    proof = observed(intent, parts)
    if damage == "later_owner_version":
        proof["owner_proofs"][-1]["version"] += 1
    elif damage == "missing_owner":
        proof["owner_proofs"].pop()
    elif damage == "later_row_hash":
        proof["batch"]["routes"][-2]["row_hash"] = "e" * 64
    elif damage == "later_gate":
        proof["batch"]["routes"][-2]["gate"] = intent["owners"][0]["owner"]
    else:
        proof["batch"]["routes"].pop()
    saved = copy.deepcopy((store, state))
    with pytest.raises((ValueError, _StoreError)):
        directory.ack(job, intent["intent_id"], proof)
    assert (store, state) == saved


def test_pending_dispatch_requires_the_explicit_attached_envelope(monkeypatch):
    from cognitive_castle import migration_codec as migration

    _, _, intent, _, state, _ = case(monkeypatch)
    assert migration.validate_pending(intent, state["registration"], state["layout"]) == intent
    with pytest.raises(ValueError):
        migration.validate_pending(intent, state["registration"])


def test_legacy_directory_cannot_prepare_packed_even_when_attached(monkeypatch):
    from cognitive_castle.lance_layout_directory import LayoutDirectory
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, intent, store, state, _ = case(monkeypatch)
    legacy = object.__new__(LayoutDirectory)
    legacy.conn, legacy._state = directory.conn, directory._state
    saved = copy.deepcopy((store, state))
    with pytest.raises((ValueError, _StoreError)):
        legacy.prepare(job, intent["cursor_before"], intent)
    assert (store, state) == saved


@pytest.mark.parametrize("native", [True, False])
def test_public_prefix_binding_requires_native_authority(monkeypatch, native):
    from cognitive_castle import lance_layout_directory as module
    from cognitive_castle.semantic_queue import SemanticQueue
    from cognitive_castle.lance_exact_store import _StoreError
    from test_layout_source_packing import prefix_case

    directory, job, intent, store, state, _ = case(monkeypatch)
    actual = directory if native else object.__new__(module.LayoutDirectory)
    monkeypatch.setattr(module, "_directory", lambda conn: actual)
    queue = object.__new__(SemanticQueue)
    calls = []

    def run(write, operation):
        calls.append(write)
        return operation(directory.conn)

    queue._run = run
    saved = copy.deepcopy((store, state))
    plans = [part["plan"] for part in prefix_case()[2]]
    if native:
        result = queue.layout_bind_prefix(job, plans, directory.envelope)
        assert result["status"] == "ok" and result["target"] == intent["target"]
    else:
        with pytest.raises(_StoreError):
            queue.layout_bind_prefix(job, plans, directory.envelope)
    assert calls == [True] and (store, state) == saved


def test_prefix_worker_operation_has_closed_original_fields():
    from cognitive_castle.semantic_store_worker import _FIELDS, QUEUE_READS

    assert _FIELDS["queue_layout_bind_prefix"] == ({"job_id", "plans", "envelope"}, set())
    assert "layout_bind_prefix" not in QUEUE_READS


@pytest.mark.parametrize(
    "damage", ["staged_record", "nonrecord", "missing_route", "changed_route", "reserved_claim"]
)
def test_packed_prepare_cannot_reuse_staged_or_foreign_claims(monkeypatch, damage):
    from cognitive_castle import lance_layout_codec as layout
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, intent, store, state, _ = case(monkeypatch)
    candidate = next(
        r
        for r in intent["routes"]
        if r["kind"] == ("bulk_page" if damage == "nonrecord" else "bulk_record")
    )
    claim = dict(
        candidate,
        partition_id=0,
        intent_id="e" * 64,
        state="confirmed",
        version=1,
        receipt="receipt:" + "e" * 64,
    )
    route = dict(
        candidate,
        format=layout.ROUTE,
        partition_id=0,
        version=1,
        receipt=claim["receipt"],
        state="published",
    )
    if damage == "staged_record":
        route["state"] = "staged"
    elif damage == "missing_route":
        route = None
    elif damage == "changed_route":
        route["row_hash"] = "e" * 64
    elif damage == "reserved_claim":
        claim.update(state="reserved", version=None)
    store["claims"][candidate["id"]] = claim
    directory._route_for_id = lambda identity: (
        copy.deepcopy(route) if identity == candidate["id"] else None
    )
    saved = copy.deepcopy((store, state))
    with pytest.raises(_StoreError):
        directory.prepare(job, intent["cursor_before"], intent)
    assert (store, state) == saved


@pytest.mark.parametrize("damage", ["cursor", "intent"])
def test_prepared_prefix_cannot_be_replaced_by_stale_replay(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, intent, store, state, _ = prepared(monkeypatch)
    saved = copy.deepcopy((store, state))
    proposed = copy.deepcopy(intent)
    expected = intent["cursor_after"] if damage == "cursor" else intent["cursor_before"]
    if damage == "intent":
        proposed["intent_id"] = "e" * 64
    with pytest.raises((ValueError, _StoreError)):
        directory.prepare(job, expected, proposed)
    assert (store, state) == saved


def test_published_owner_span_remains_available_during_next_prefix_on_same_home(monkeypatch):
    from test_layout_source_packing import prefix_case
    from cognitive_castle.lance_layout_packing import pack_source_prefix
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval

    directory, job, intent, store, state, parts = case(monkeypatch, owners=1)
    directory.prepare(job, intent["cursor_before"], intent)
    directory.ack(job, intent["intent_id"], observed(intent, parts))
    old_owner = intent["owners"][0]["owner"]
    old_span = directory.span(old_owner)
    second = prefix_case()[2][1]
    target = directory.bind_prefix(job, [second["plan"]], directory.envelope)["target"]
    assert target["partition_id"] == intent["target"]["partition_id"] == 0
    packet = pack_source_prefix(state["registration"], state["staging_cursor"], [second])
    following = prepare_prefix_interval(state["registration"], packet, target)
    directory.prepare(job, state["staging_cursor"], following)
    assert directory.span(old_owner) == old_span
    assert store["homes"][old_owner]["published"] == 1
    new_span = directory.span(following["owners"][0]["owner"])
    assert new_span["first_row"] == store["registry"][0]["pin"]["physical_rows"]
    assert old_span["next_row"] < new_span["first_row"]

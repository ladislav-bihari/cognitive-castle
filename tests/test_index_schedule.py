"""Real disposable scheduling SQL; no native/model or production qualification."""

import pytest

from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.semantic_intents import prepare_intent
from cognitive_castle.semantic_profile import digest
from test_layout_live_control import control as control, prepare
from test_semantic_queue import registration, SCOPE

FORMAT = "castle-index-schedule-v1"


def setup(control):
    prepare(control)
    store, _, envelope = control
    return store, digest(envelope)


def add(store, key, lane="new"):
    prepared = prepare_remember(key, "original exact text " + key)
    intent = prepare_intent(prepared, [SCOPE] * len(prepared.records), lane)
    assert store.intent_prepare(intent) == dict(status="ok")
    return intent


def snapshot(store):
    with store._connection(False) as conn:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        rows = [tuple(r) for r in conn.execute("SELECT * FROM intents ORDER BY seq")]
        names = [
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        ]
    return version, rows, names


def test_explicit_schedule_initialization_upgrades_only_on_operator_call(control):
    store, pin = setup(control)
    before = snapshot(store)
    assert before[0] == 5
    assert store.initialize_index_schedule(envelope_hash=pin) == dict(
        status="ok", format=FORMAT, processed=0, backfill_cursor=0, ready=True
    )
    after = snapshot(store)
    assert after[0] == 6 and after[1] == before[1]
    assert set(after[2]) - set(before[2]) == {"index_schedule", "index_intents"}
    assert store.initialize_index_schedule(envelope_hash=pin) == dict(
        status="ok", format=FORMAT, processed=0, backfill_cursor=0, ready=True
    )


def test_selected_fairness_requirement_does_not_implicitly_upgrade(control):
    store, pin = setup(control)
    add(store, "pending")
    before = snapshot(store)
    assert store.prepare_index(envelope_hash=pin, require_fairness=True) == dict(
        status="unavailable", reason="index_schedule_not_ready"
    )
    assert snapshot(store) == before


def test_bounded_backfill_preserves_all_original_intent_bytes_and_bindings(control):
    store, pin = setup(control)
    for n in range(9):
        add(store, str(n), "new" if n % 2 else "history")
    before = snapshot(store)
    assert store.initialize_index_schedule(envelope_hash=pin) == dict(
        status="in_progress", format=FORMAT, processed=8, backfill_cursor=8, ready=False
    )
    assert store.prepare_index(envelope_hash=pin, require_fairness=True) == dict(
        status="unavailable", reason="index_schedule_not_ready"
    )
    assert store.initialize_index_schedule(envelope_hash=pin) == dict(
        status="ok", format=FORMAT, processed=1, backfill_cursor=9, ready=True
    )
    assert snapshot(store)[1] == before[1]
    with store._connection(False) as conn:
        lanes = [
            tuple(r)
            for r in conn.execute("SELECT seq,lane,complete FROM index_intents ORDER BY seq")
        ]
    assert lanes == [(n + 1, "new" if n % 2 else "history", 0) for n in range(9)]


def test_wrong_authority_never_creates_schema_or_advances_backfill(control):
    store, pin = setup(control)
    add(store, "one")
    before = snapshot(store)
    assert store.initialize_index_schedule(envelope_hash="f" * 64) == dict(status="conflict")
    assert snapshot(store) == before
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    before = snapshot(store)
    assert store.initialize_index_schedule(envelope_hash="f" * 64) == dict(status="conflict")
    assert snapshot(store) == before


def test_ready_groups_and_outbox_each_get_a_turn_across_reopened_connections(control):
    store, pin = setup(control)
    intents = [
        add(store, key, lane)
        for key, lane in [("n1", "new"), ("n2", "new"), ("n3", "new"), ("h1", "history")]
    ]
    assert store.register(**registration("ready"))["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    first = store.prepare_index(envelope_hash=pin, require_fairness=True)
    second = store.prepare_index(envelope_hash=pin, require_fairness=True)
    third = store.prepare_index(envelope_hash=pin, require_fairness=True)
    fourth = store.prepare_index(envelope_hash=pin, require_fairness=True)
    assert first == dict(status="ok", intents=intents[:2], selection=None)
    assert third == dict(status="ok", intents=intents[2:], selection=None)
    for chosen in (second, fourth):
        assert chosen["intents"] == []
        assert chosen["selection"]["group"]["batch_key"] == "ready"
        assert chosen["selection"]["group"]["position"] == 0  # no invented ACK


def test_new_intent_gets_priority_over_existing_history_backlog(control):
    store, pin = setup(control)
    for n in range(17):
        add(store, "old" + str(n), "history")
    while not store.initialize_index_schedule(envelope_hash=pin)["ready"]:
        pass
    fresh = add(store, "fresh")
    result = store.prepare_index(envelope_hash=pin, require_fairness=True)
    assert result["intents"][0] == fresh and len(result["intents"]) == 2
    assert result["intents"][1]["groups"][0]["lane"] == "history"


def test_one_pending_intent_is_not_returned_twice_and_never_blocks_ready_work(control):
    store, pin = setup(control)
    pending = add(store, "refused-forever")
    assert store.register(**registration("ready"))["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    kinds = []
    for _ in range(6):
        result = store.prepare_index(envelope_hash=pin, require_fairness=True)
        if result["intents"]:
            assert result["intents"] == [pending]
            kinds.append("recover")
        else:
            assert result["selection"]["group"]["batch_key"] == "ready"
            kinds.append("index")
    assert kinds == ["recover", "index", "recover", "index", "recover", "index"]


def test_intent_completion_updates_projection_atomically(control):
    store, pin = setup(control)
    intent = add(store, "ready-after-proof")
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    assert store.intent_complete(intent["intent_id"], digest(intent)) == dict(status="ok")
    with store._connection(False) as conn:
        assert conn.execute("SELECT complete FROM index_intents").fetchone()[0] == 1
    result = store.prepare_index(envelope_hash=pin, require_fairness=True)
    assert (
        result["intents"] == []
        and result["selection"]["group"]["batch_key"] == intent["groups"][0]["batch_key"]
    )


def test_group_fairness_is_preserved_when_outbox_is_empty(control):
    store, pin = setup(control)
    for key, lane in [("new", "new"), ("history", "history")]:
        assert store.register(**registration(key, lane))["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    assert [
        store.prepare_index(envelope_hash=pin)["selection"]["group"]["batch_key"] for _ in range(5)
    ] == ["new", "new", "new", "history", "new"]


def test_corrupt_projected_lane_cannot_grant_wrong_priority(control):
    store, pin = setup(control)
    add(store, "history", "history")
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    with store._connection(True) as conn:
        conn.execute("UPDATE index_intents SET lane='new'")
    assert store.prepare_index(envelope_hash=pin, require_fairness=True)["status"] == "corrupt"


@pytest.mark.parametrize("value", [None, 1, "yes"])
def test_untyped_fairness_flag_is_rejected_before_transaction(control, monkeypatch, value):
    store, pin = setup(control)

    def forbidden(*args):
        pytest.fail("invalid scheduling flag entered a transaction")

    monkeypatch.setattr(store, "_run", forbidden)
    with pytest.raises(ValueError):
        store.prepare_index(envelope_hash=pin, require_fairness=value)


def test_corrupt_backfill_rolls_back_schema_creation(control):
    store, pin = setup(control)
    add(store, "bad")
    with store._connection(True) as conn:
        conn.execute("UPDATE intents SET binding=?", ("f" * 64,))
    before = snapshot(store)
    assert store.initialize_index_schedule(envelope_hash=pin)["status"] == "corrupt"
    assert snapshot(store) == before


def test_failed_group_claim_rolls_back_fair_turn_and_group_state(control, monkeypatch):
    store, pin = setup(control)
    add(store, "pending")
    assert store.register(**registration("ready"))["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    assert store.prepare_index(envelope_hash=pin)["intents"]
    with store._connection(False) as conn:
        before = tuple(conn.execute("SELECT * FROM index_schedule").fetchone())
    original = store._select_group

    def interrupted(conn):
        original(conn)
        raise ValueError("interrupted actual group claim")

    monkeypatch.setattr(store, "_select_group", interrupted)
    assert store.prepare_index(envelope_hash=pin)["status"] == "unavailable"
    with store._connection(False) as conn:
        assert tuple(conn.execute("SELECT * FROM index_schedule").fetchone()) == before
        assert conn.execute("SELECT state FROM groups").fetchone()[0] == "pending"


def test_wrong_authority_does_not_consume_a_fair_turn(control):
    store, pin = setup(control)
    add(store, "pending")
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    with store._connection(False) as conn:
        before = tuple(conn.execute("SELECT * FROM index_schedule").fetchone())
    assert store.prepare_index(envelope_hash="f" * 64)["status"] == "conflict"
    with store._connection(False) as conn:
        assert tuple(conn.execute("SELECT * FROM index_schedule").fetchone()) == before


def test_intents_added_and_completed_during_backfill_keep_atomic_projection(control):
    store, pin = setup(control)
    for n in range(9):
        add(store, str(n))
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is False
    late = add(store, "late")
    assert store.intent_complete(late["intent_id"], digest(late))["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin) == dict(
        status="ok", format=FORMAT, processed=2, backfill_cursor=10, ready=True
    )
    with store._connection(False) as conn:
        row = conn.execute(
            "SELECT seq,lane,complete FROM index_intents WHERE intent_id=?", (late["intent_id"],)
        ).fetchone()
        assert tuple(row) == (10, "new", 1)


def test_mixed_lane_intent_gets_new_priority_without_changing_original(control):
    store, pin = setup(control)
    add(store, "old", "history")
    prepared = prepare_remember("mixed", "€" * 21845)
    intent = prepare_intent(prepared, [SCOPE] * len(prepared.records), "new")
    assert len(intent["groups"]) == 2
    intent["groups"][1]["lane"] = "history"
    assert store.intent_prepare(intent)["status"] == "ok"
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    assert store.prepare_index(envelope_hash=pin)["intents"][0] == intent


def test_schema6_rejects_unknown_objects_with_sqlite_like_names(control):
    store, pin = setup(control)
    assert store.initialize_index_schedule(envelope_hash=pin)["ready"] is True
    with store._connection(True) as conn:
        conn.execute("CREATE TABLE sqliteXhidden(value TEXT)")
    assert store.prepare_index(envelope_hash=pin)["status"] == "unavailable"

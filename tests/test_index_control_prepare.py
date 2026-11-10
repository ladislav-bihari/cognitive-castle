"""Actual disposable SQL transactions; no native/model qualification."""

import pytest

from cognitive_castle.semantic_profile import digest
from cognitive_castle.semantic_intents import prepare_intent
from cognitive_castle.canonical_records import prepare_remember
from test_layout_live_control import control, prepare  # noqa: F401
from test_semantic_queue import registration, SCOPE


def setup(control):
    prepare(control)
    store, _, envelope = control
    return store, digest(envelope)


def schedule(store):
    with store._connection(False) as conn:
        row = conn.execute("SELECT new_pages,intent_cursor FROM schedule WHERE id=1").fetchone()
        groups = conn.execute("SELECT batch_key,state,position FROM groups ORDER BY seq").fetchall()
        return tuple(row), [tuple(g) for g in groups]


def test_index_control_preparation_idle_preserves_schedule(control):
    store, pin = setup(control)
    before = schedule(store)
    assert store.prepare_index(envelope_hash=pin) == dict(
        status="ok", intents=[], selection=dict(status="idle")
    )
    assert schedule(store) == before


def test_index_control_preparation_claims_without_advancing_position(control):
    store, pin = setup(control)
    assert store.register(**registration("ready"))["status"] == "ok"
    result = store.prepare_index(envelope_hash=pin)
    assert result["status"] == "ok" and result["intents"] == []
    assert result["selection"]["status"] == "ok"
    group = result["selection"]["group"]
    assert group["batch_key"] == "ready" and group["state"] == "inflight" and group["position"] == 0
    assert schedule(store) == ((1, 0), [("ready", "inflight", 0)])


def test_index_control_preparation_preserves_lane_fairness_and_inflight_priority(control):
    store, pin = setup(control)
    for key, lane in [("first", "new"), ("second", "new"), ("history", "history")]:
        assert store.register(**registration(key, lane))["status"] == "ok"
    selected = [
        store.prepare_index(envelope_hash=pin)["selection"]["group"]["batch_key"] for _ in range(5)
    ]
    assert selected == ["first", "first", "first", "history", "first"]
    _, groups = schedule(store)
    assert all(position == 0 for _, _, position in groups)
    assert groups[1] == ("second", "pending", 0)


def pending(store):
    values = []
    for index in range(3):
        prepared = prepare_remember(str(index), "exact original")
        intent = prepare_intent(prepared, [SCOPE] * len(prepared.records), "new")
        assert store.intent_prepare(intent)["status"] == "ok"
        values.append(intent)
    return values


def test_index_control_preparation_rotates_two_pending_intents_without_claiming_group(control):
    store, pin = setup(control)
    assert store.register(**registration("ready"))["status"] == "ok"
    intents = pending(store)
    first = store.prepare_index(envelope_hash=pin)
    second = store.prepare_index(envelope_hash=pin)
    assert first == dict(status="ok", intents=intents[:2], selection=None)
    assert second == dict(status="ok", intents=[intents[2], intents[0]], selection=None)
    assert schedule(store) == ((0, 1), [("ready", "pending", 0)])


def test_index_control_preparation_wrong_pin_leaves_intent_cursor_and_group_unchanged(control):
    store, _ = setup(control)
    assert store.register(**registration("ready"))["status"] == "ok"
    pending(store)
    before = schedule(store)
    assert store.prepare_index(envelope_hash="f" * 64) == dict(status="conflict")
    assert schedule(store) == before


def test_index_control_preparation_rolls_back_claim_and_fairness_together(control, monkeypatch):
    store, pin = setup(control)
    assert store.register(**registration("ready"))["status"] == "ok"
    before = schedule(store)
    original = store._select_group

    def interrupted(conn):
        original(conn)
        raise RuntimeError("after real selection updates")

    monkeypatch.setattr(store, "_select_group", interrupted)
    assert store.prepare_index(envelope_hash=pin)["status"] == "unavailable"
    assert schedule(store) == before


@pytest.mark.parametrize("pin", ["", True, "A" * 64])
def test_index_control_preparation_invalid_pin_stops_before_transaction(control, monkeypatch, pin):
    store, _ = setup(control)
    monkeypatch.setattr(
        store, "_run", lambda *a: pytest.fail("invalid pin reached SQL transaction")
    )
    with pytest.raises(ValueError):
        store.prepare_index(envelope_hash=pin)

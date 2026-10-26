"""A fresh append yields; only a new invocation's actual proof permits ACK."""

import copy

import pytest

from test_layout_coordinator import PendingSession


def test_fresh_step_yields_without_replay_visibility_or_ack():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    session = PendingSession(new_owner=True, fresh=1)
    before = copy.deepcopy(session.intent["cursor_before"])
    result = _layout_fixture_step(session, session.reg)
    assert result == dict(
        status="in_progress",
        reason=None,
        phase="verify_replay",
        intent_id=session.intent["intent_id"],
        cursor_before=before,
        cursor_after=before,
        coverage="incomplete",
    )
    assert [op for op, _ in session.calls] == [
        "queue_layout_resume",
        "layout_source",
        "queue_layout_bind",
        "layout_source",
        "queue_layout_prepare",
        "layout_import_batch",
    ]


def test_later_step_regenerates_same_request_and_uses_actual_existing_proof():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    fresh = PendingSession(new_owner=True, fresh=1)
    assert _layout_fixture_step(fresh, fresh.reg)["status"] == "in_progress"
    later = PendingSession()
    assert later.intent == fresh.intent
    assert _layout_fixture_step(later, later.reg) == dict(
        status="ok", intent_id=later.intent["intent_id"]
    )
    assert [op for op, _ in later.calls] == [
        "queue_layout_resume",
        "layout_source",
        "layout_import_batch",
        "layout_verify_owners",
        "queue_layout_ack",
    ]
    first = next(args for op, args in fresh.calls if op == "layout_import_batch")
    second = next(args for op, args in later.calls if op == "layout_import_batch")
    assert first == second
    assert not any("verified" in key for key in later.state)


def test_late_fresh_success_is_refusal_not_progress():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    session = PendingSession(fresh=1)
    original = session.exact

    def exact(*args):
        result = original(*args)
        session.remaining = 0
        return result

    session.exact = exact
    result = _layout_fixture_step(session, session.reg)
    assert result["status"] == "unavailable" and result["reason"] == "semantic_deadline"
    assert result["cursor_after"] == session.intent["cursor_before"]
    assert not any(op == "queue_layout_ack" for op, _ in session.calls)


@pytest.mark.parametrize("damage", ["receipt", "routes", "outcome", "extra"])
def test_invalid_fresh_reply_cannot_yield_accepted_progress(damage):
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    session = PendingSession(fresh=1)
    original = session.exact

    def exact(*args):
        result = copy.deepcopy(original(*args))
        if damage == "receipt":
            result["receipt"]["digest"] = "0" * 64
        elif damage == "routes":
            result["routes"] = [{"untrusted": True}]
        elif damage == "outcome":
            result["outcome"] = "verified_from_cache"
        else:
            result["verified"] = True
        return result

    session.exact = exact
    result = _layout_fixture_step(session, session.reg)
    assert result["status"] == "conflict" and result["reason"] == "layout_replay_contract"
    assert len([op for op, _ in session.calls if op == "layout_import_batch"]) == 1
    assert not any(op in {"layout_verify_owners", "queue_layout_ack"} for op, _ in session.calls)


def test_later_step_never_uses_prior_body_when_source_changed():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    fresh = PendingSession(fresh=1)
    assert _layout_fixture_step(fresh, fresh.reg)["status"] == "in_progress"
    later = PendingSession(changed_source=True)
    result = _layout_fixture_step(later, later.reg)
    assert result["status"] == "conflict" and result["phase"] == "source"
    assert not any(op == "layout_import_batch" for op, _ in later.calls)


def test_late_owner_check_in_later_step_never_acknowledges():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    session = PendingSession(late_owner=True)
    result = _layout_fixture_step(session, session.reg)
    assert result["status"] == "unavailable"
    assert not any(op == "queue_layout_ack" for op, _ in session.calls)


def test_repeated_fresh_steps_keep_unverified_state_and_original_request():
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    first, later = PendingSession(fresh=1), PendingSession(fresh=1)
    for session in (first, later):
        assert _layout_fixture_step(session, session.reg)["status"] == "in_progress"
        assert len([op for op, _ in session.calls if op == "layout_import_batch"]) == 1
        assert not any(op == "queue_layout_ack" for op, _ in session.calls)
    assert first.calls == later.calls


@pytest.mark.parametrize("pending", [False, True])
def test_native_step_keeps_preparation_and_cold_recovery(monkeypatch, pending):
    from cognitive_castle.migration_coordinator import _layout_fixture_step
    from test_layout_source_preparation import native_session

    session, events = native_session(monkeypatch, pending=pending, admitted=pending)
    result = _layout_fixture_step(session, session.reg)
    assert result["status"] == ("ok" if pending else "in_progress")
    assert events.count("open") == events.count("owner") == events.count("emit") == 1
    assert session.calls[1][1]["action"] == ("export" if pending else "prepare")
    assert len([op for op, _ in session.calls if op == "layout_import_batch"]) == 1
    assert any(op == "queue_layout_ack" for op, _ in session.calls) == pending

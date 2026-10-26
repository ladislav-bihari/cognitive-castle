"""Explicit prefix request/reply versions preserve refusal and source boundaries."""

import copy

import pytest

from test_layout_source_planning import request_case
from test_layout_source_packing import prefix_case

PREFIX_REQUEST = "lance-layout-source-prefix-request-v1"


def case(backend, action="prepare"):
    from cognitive_castle import migration_codec as migration
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    runtime, original, window = request_case(backend)
    window.update(format=PREFIX_REQUEST, action=action)
    reg, before, components, _ = prefix_case(backend)
    assert before == original["cursor"] and reg["descriptor"] == original["descriptor"]
    packet = pack_source_prefix(reg, before, components)
    if action == "export":
        window["allowed_plan_hashes"] = [migration.hashed(part["plan"]) for part in components]
    return runtime, original, window, reg, packet


def terminal(reg, before):
    from cognitive_castle import migration_codec as migration

    after = dict(before, phase="done", key=None)
    result = dict(
        status="ok",
        reason=None,
        source_binding=reg["source_binding"],
        cursor_before=before,
        cursor_after=after,
        entries=[],
        finalizations=[],
        counts=migration.empty_counts(),
        phase="done",
        phase_complete=True,
        complete=True,
        coverage="incomplete",
        frame_bytes=0,
    )
    for _ in range(3):
        result["frame_bytes"] = len(migration.frame_bytes(result))
    migration.validate_export(result, reg, before)
    return result


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("action", ["prepare", "export"])
def test_prefix_request_has_explicit_version_and_keeps_original_cursor(backend, action):
    from cognitive_castle import migration_codec as migration

    runtime, original, window = request_case(backend)
    window.update(format=PREFIX_REQUEST, action=action)
    if action == "export":
        window["allowed_plan_hashes"] = ["a" * 64, "b" * 64]
    saved = copy.deepcopy(original)
    request = runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    assert request == {**original, "layout": window}
    assert original == saved
    migration.frame_bytes(request)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize(
    "damage",
    ["format", "action", "unknown", "prepare_hash", "hash", "duplicate", "empty_export", "nine"],
)
def test_prefix_window_refuses_unknown_or_unbounded_selection(backend, damage):
    runtime, original, window = request_case(backend)
    window.update(format=PREFIX_REQUEST, action="prepare")
    if damage == "format":
        window["format"] = "unknown"
    elif damage == "action":
        window["action"] = "provider"
    elif damage == "unknown":
        window["rss_mib"] = 4096
    elif damage == "prepare_hash":
        window["allowed_plan_hashes"] = ["a" * 64]
    elif damage == "hash":
        window.update(action="export", allowed_plan_hashes=["bad"])
    elif damage == "duplicate":
        window.update(action="export", allowed_plan_hashes=["a" * 64] * 2)
    elif damage == "empty_export":
        window["action"] = "export"
    else:
        window.update(action="export", allowed_plan_hashes=[format(i, "064x") for i in range(9)])
    with pytest.raises(ValueError):
        runtime.source_request(original["descriptor"], original["cursor"], layout=window)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("action", ["prepare", "export"])
def test_prefix_reply_keeps_source_evidence_separate_from_publication(backend, action):
    from cognitive_castle.lance_layout_packing import prefix_result, validate_prefix_result
    from cognitive_castle.lance_layout_codec import validate_layout_source_result

    _, original, window, _, packet = case(backend, action)
    result = prefix_result(original["descriptor"], original["cursor"], window, prefix=packet)
    assert result["status"] == "ok" and result["prefix"] == packet
    assert result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_before"] == original["cursor"]
    assert result["cursor_after"] == (
        original["cursor"] if action == "prepare" else packet["cursor_after"]
    )
    assert (
        validate_prefix_result(result, window, original["descriptor"], original["cursor"]) == result
    )
    assert (
        validate_layout_source_result(result, window, original["descriptor"], original["cursor"])
        == result
    )


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize(
    "reason", ["worker_deadline", "worker_memory_limit", "background_preempted"]
)
def test_prefix_guard_refusal_clears_every_transient_payload(backend, reason):
    from cognitive_castle.lance_layout_packing import validate_prefix_result

    runtime, original, window, _, packet = case(backend)
    request = runtime.source_request(original["descriptor"], original["cursor"], layout=window)
    result = runtime.source_result(
        request, dict(status="unavailable", reason=reason, prefix=packet)
    )
    assert result["status"] == "unavailable" and result["reason"] == reason
    assert result["prefix"] is result["fallback_plan"] is result["terminal"] is None
    assert result["cursor_before"] == result["cursor_after"] == original["cursor"]
    validate_prefix_result(result, window, original["descriptor"], original["cursor"])


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "binding",
        "before",
        "after",
        "action",
        "refusal_body",
        "reason",
        "empty",
        "multiple",
        "prefix_hash",
    ],
)
def test_prefix_reply_refuses_changed_binding_or_ambiguous_payload(backend, damage):
    from cognitive_castle.lance_layout_packing import prefix_result, validate_prefix_result

    _, original, window, _, packet = case(backend)
    result = copy.deepcopy(
        prefix_result(original["descriptor"], original["cursor"], window, prefix=packet)
    )
    if damage == "unknown":
        result["verified"] = True
    elif damage == "binding":
        result["source_binding"] = "b" * 64
    elif damage == "before":
        result["cursor_before"] = packet["cursor_after"]
    elif damage == "after":
        result["cursor_after"] = packet["cursor_after"]
    elif damage == "action":
        result["action"] = "export"
    elif damage == "refusal_body":
        result["status"] = "unavailable"
    elif damage == "reason":
        result["reason"] = "not_success"
    elif damage == "empty":
        result["prefix"] = None
    elif damage == "multiple":
        result["fallback_plan"] = packet["components"][0]["plan"]
    else:
        result["prefix"]["prefix_hash"] = "0" * 64
    with pytest.raises(ValueError):
        validate_prefix_result(result, window, original["descriptor"], original["cursor"])


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_prepare_retains_original_body_free_fallback_and_terminal_evidence(backend):
    from cognitive_castle.lance_layout_packing import prefix_result, validate_prefix_result

    _, original, window, reg, packet = case(backend)
    for payload in (
        dict(fallback_plan=packet["components"][0]["plan"]),
        dict(terminal=terminal(reg, original["cursor"])),
    ):
        result = prefix_result(original["descriptor"], original["cursor"], window, **payload)
        assert result["prefix"] is None
        assert result["cursor_before"] == result["cursor_after"] == original["cursor"]
        validate_prefix_result(result, window, original["descriptor"], original["cursor"])


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("payload", ["fallback", "terminal"])
def test_frozen_export_cannot_replace_its_owner_prefix_with_fallback_or_eof(backend, payload):
    from cognitive_castle.lance_layout_packing import prefix_result

    _, original, window, reg, packet = case(backend, "export")
    supplied = (
        dict(fallback_plan=packet["components"][0]["plan"])
        if payload == "fallback"
        else dict(terminal=terminal(reg, original["cursor"]))
    )
    with pytest.raises(ValueError):
        prefix_result(original["descriptor"], original["cursor"], window, **supplied)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
def test_prefix_request_cannot_resume_a_partial_owner(backend):
    runtime, original, window = request_case(backend)
    reg, _, components, _ = prefix_case(backend)
    cursor = copy.deepcopy(components[1]["export"]["cursor_before"])
    cursor.update(owner=components[1]["plan"]["owner_key"], entry=1, field="record")
    if backend == "sqlite":
        cursor["key"] = components[1]["plan"]["source_key"]
    runtime.source_request(
        original["descriptor"], cursor
    )  # Original streaming cursor remains valid.
    window.update(format=PREFIX_REQUEST, action="prepare")
    with pytest.raises(ValueError):
        runtime.source_request(reg["descriptor"], cursor, layout=window)


@pytest.mark.parametrize("backend", ["sqlite", "native"])
@pytest.mark.parametrize("change", ["reordered", "shorter", "longer"])
def test_export_reply_requires_exact_frozen_ordered_owner_selection(backend, change):
    from cognitive_castle.lance_layout_packing import prefix_result, validate_prefix_result

    _, original, window, _, packet = case(backend, "export")
    result = prefix_result(original["descriptor"], original["cursor"], window, prefix=packet)
    changed = copy.deepcopy(window)
    hashes = changed["allowed_plan_hashes"]
    changed["allowed_plan_hashes"] = (
        list(reversed(hashes))
        if change == "reordered"
        else hashes[:-1]
        if change == "shorter"
        else [*hashes, "f" * 64]
    )
    with pytest.raises(ValueError):
        validate_prefix_result(result, changed, original["descriptor"], original["cursor"])

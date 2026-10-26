"""Fixed58-start deferred lifecycle; original tail evidence is preserved."""

import copy
import json
from pathlib import Path
from types import SimpleNamespace
import time

import test_layout_native_integration as native
import test_layout_native_tail as tail

pytestmark = tail.pytestmark


def _one_step(registration):
    from cognitive_castle.migration_coordinator import _layout_fixture_step

    generation = Path(registration["destination"])
    started = time.monotonic()
    with native._session(generation) as session:
        observed = native.ObservedLayoutSession(session)
        result = _layout_fixture_step(observed, registration)
        elapsed = time.monotonic() - started
        print(
            "LAYOUT_NATIVE_TICK "
            + json.dumps(
                dict(
                    status=result.get("status"),
                    reason=result.get("reason"),
                    elapsed=elapsed,
                    calls=observed.calls,
                )
            ),
            flush=True,
        )
        assert elapsed <= 5, result
    return result, observed


def _logical_interval(registration):
    first, staged = _one_step(registration)
    if first["status"] != "in_progress":
        return first, staged
    assert first["reason"] is None and first["phase"] == "verify_replay"
    assert first["cursor_before"] == first["cursor_after"]
    assert not any(
        name.startswith(("queue_layout_ack:", "layout_verify_owners:")) for name in staged.calls
    )
    assert staged.results["layout_import_batch:1"]["outcome"] == "fresh_append"
    before = staged.results["queue_layout_resume:1"]
    original = staged.arguments["layout_import_batch:1"][2]
    if original["finalizations"]:
        assert len(original["finalizations"]) == 1
        owner = original["finalizations"][0]["owner"]
        hidden = native._read(Path(registration["destination"]), "bulk_owner", dict(owner=owner))
        assert hidden == {"status": "unavailable"}, hidden
    verified, checked = _one_step(registration)
    assert verified["status"] == "ok", verified
    resumed = checked.results["queue_layout_resume:1"]
    assert resumed["staging_cursor"] == first["cursor_before"]
    assert resumed["completed_frontier"] == before["completed_frontier"]
    assert resumed["counts"] == before["counts"]
    assert resumed["last_ack"] == before["last_ack"]
    assert resumed["pending"] == staged.arguments["queue_layout_prepare:1"][2]["intent"]
    assert checked.arguments["layout_import_batch:1"][2] == original
    assert checked.results["layout_import_batch:1"]["outcome"] == "existing_verified"

    # Present the same logical import1/import2 observations to the original
    # assertions, while the emitted resource events retain both actual steps.
    combined = SimpleNamespace(calls=[], arguments={}, results={})
    counts = {}
    for observed in (staged, checked):
        for point in observed.calls:
            operation = point.split(":", 1)[0]
            counts[operation] = counts.get(operation, 0) + 1
            target = operation + ":" + str(counts[operation])
            combined.calls.append(target)
            combined.arguments[target] = copy.deepcopy(observed.arguments[point])
            combined.results[target] = copy.deepcopy(observed.results[point])
    return verified, combined


def test_deferred_tail_keeps_original_geometry_visibility_and_atomic_ack(tmp_path, monkeypatch):
    monkeypatch.setattr(native, "_tick", _logical_interval)
    monkeypatch.setattr(tail, "EXACT_TAIL_STARTS", 58)
    tail.test_native_layout_exact_tail_refuses_then_bootstraps_and_resumes_original_interval(
        tmp_path
    )

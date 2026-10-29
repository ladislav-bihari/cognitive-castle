"""Cold real-engine qualification of packed publication and published snapshots.

The pytest host never imports a storage engine. Every storage operation uses
the original supervised profile. These synthetic cases do not qualify capacity
for the real palace or activate the ordinary memory routes.
"""

import copy
import json
from pathlib import Path
import sys
import time

import test_layout_native_integration as native
from test_layout_native_tail import _tree
from test_sqlite_source_export import fixture

pytestmark = native.pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def _setup(tmp_path):
    from cognitive_castle import migration_codec as migration

    call = native._start("source_fixture")
    source = fixture(tmp_path, prefix_small=True)
    print(
        "LAYOUT_NATIVE_STAGE "
        + json.dumps(
            dict(
                call=call,
                operation="source_fixture",
                status=source["status"],
                reason=source.get("reason"),
                resources=[source["resources"]],
            )
        ),
        flush=True,
    )
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    for path in (generation / "exact", generation / "exact/partitions", generation / "exact/tmp"):
        path.mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    for pid in range(envelope["max_partitions"]):
        (generation / "exact/partitions" / str(pid)).mkdir(mode=0o700)
    reg = migration.registration(source["descriptor"], str(generation))
    with native._session(generation) as session:
        call = native._start("operator_register")
        result = session._run_stage(
            dict(
                directory=str(generation),
                operation="register",
                arguments=dict(registration=reg, envelope=envelope, qualification=qualification),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(native.__file__).resolve()),
                "operator",
                migration.hashed(qualification),
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        native._record(session, "operator_register", result, 0, call)
    assert result["status"] == "ok", result
    return source, generation, reg, envelope


def _step(reg, *, loss=None):
    from cognitive_castle.migration_coordinator import _layout_prefix_step

    generation = Path(reg["destination"])
    started = time.monotonic()
    with native._session(generation) as session:
        observed = native.ObservedLayoutSession(session, loss=loss)
        result = _layout_prefix_step(observed, reg)
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


def _assert_originals(source, generation, members):
    ids = [identity for manifest in members for identity in manifest["live"]["receipt"]["ids"]]
    assert 1 <= len(ids) <= 8
    actual = native._read(generation, "read_sources", dict(ids=ids))
    expected = {s["id"]: s for s in source["original_public"]}
    assert actual == dict(status="ok", sources=[expected[i] for i in ids]), actual


def _assert_frozen(tmp_path, before_source, artifact, before_artifact):
    assert _tree(tmp_path / "source", hashes=True) == before_source
    assert artifact.read_bytes() == before_artifact
    assert not {"sqlite3", "lance", "pyarrow", "numpy", "torch"}.intersection(sys.modules)


def test_real_packed_prefix_publishes_all_owners_and_recovers_lost_ack(tmp_path):
    from cognitive_castle.lance_layout_packed import validate_grouped_owner_proof

    start = native._CALLS
    source, generation, reg, _ = _setup(tmp_path)
    before_source = _tree(tmp_path / "source", hashes=True)
    artifact = generation / "exact/layout-qualification.json"
    before_artifact = artifact.read_bytes()
    fresh, first = _step(reg)
    assert fresh["status"] == "in_progress" and fresh["phase"] == "verify_replay", fresh
    pending = first.arguments["queue_layout_prepare:1"][2]["intent"]
    arguments = first.arguments["layout_import_batch:1"][2]
    manifests = arguments["finalizations"]
    assert len(manifests) == len(pending["owners"]) == 2
    assert [m["live"]["key"] for m in manifests] == ["aaa-prefix-0", "aaa-prefix-1"]
    assert first.results["layout_import_batch:1"]["outcome"] == "fresh_append"
    assert first.calls == [
        "queue_layout_resume:1",
        "layout_source:1",
        "queue_layout_bind_prefix:1",
        "queue_layout_prepare:1",
        "layout_import_batch:1",
    ]
    for manifest in manifests:
        assert native._read(generation, "bulk_owner", dict(owner=manifest["owner"])) == {
            "status": "unavailable"
        }
    lost, checked = _step(reg, loss="queue_layout_ack:1")
    assert lost["status"] == "unavailable" and lost["reason"] == "fixture_lost_response", lost
    assert checked.calls == [
        "queue_layout_resume:1",
        "layout_source:1",
        "layout_import_batch:1",
        "layout_verify_owners:1",
        "queue_layout_ack:1",
    ]
    assert checked.arguments["layout_import_batch:1"][2] == arguments
    assert checked.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    resumed = checked.results["queue_layout_resume:1"]
    initial = first.results["queue_layout_resume:1"]
    for key in ("staging_cursor", "completed_frontier", "counts", "last_ack"):
        assert resumed[key] == initial[key]
    assert resumed["pending"] == pending
    version = checked.results["layout_import_batch:1"]["routes"][-1]["version"]
    proofs = validate_grouped_owner_proof(
        checked.results["layout_verify_owners:1"], pending, dict(finalizations=manifests), version
    )
    assert [p["owner"] for p in proofs] == [m["owner"] for m in manifests]
    ack = checked.arguments["queue_layout_ack:1"][2]
    repeated = native._queue(generation, "queue_layout_ack", copy.deepcopy(ack))
    assert repeated == checked.results["queue_layout_ack:1"] and repeated["status"] == "ok"
    state = native._queue_state(reg)
    assert state["status"] == "ok" and state["pending"] is None, state
    assert state["staging_cursor"] == state["completed_frontier"] == pending["cursor_after"]
    assert (
        state["counts"]["live_owners"]
        == state["owner_references"]
        == state["semantic_pending"]
        == 2
    )
    _assert_originals(source, generation, manifests)
    for manifest in manifests:
        owner = native._read(generation, "bulk_owner", dict(owner=manifest["owner"]))
        assert owner["status"] == "ok" and owner["manifest"] == manifest, owner
        live = manifest["live"]
        receipt = native._read(
            generation, "receipt", dict(namespace=live["namespace"], key=live["key"])
        )
        assert receipt == dict(status="ok", digest=live["digest"], receipt=live["receipt"])
    _assert_frozen(tmp_path, before_source, artifact, before_artifact)
    assert native._CALLS - start == 21


def test_real_old_snapshot_stays_readable_during_next_same_home_append(tmp_path):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.lance_layout_packed import prepare_prefix_interval
    from cognitive_castle.lance_layout_packing import pack_source_prefix

    start = native._CALLS
    source, generation, reg, envelope = _setup(tmp_path)
    before_source = _tree(tmp_path / "source", hashes=True)
    artifact = generation / "exact/layout-qualification.json"
    before_artifact = artifact.read_bytes()
    before = migration.initial_source_cursor(reg)
    window = dict(
        format=layout.PREFIX_SOURCE_REQUEST,
        action="prepare",
        generation=envelope["generation"],
        qualification_hash=envelope["qualification_hash"],
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    with native._session(generation) as session:
        call = native._start("layout_source")
        prepared = session.layout_source(source["descriptor"], before, window)
        native._record(session, "layout_source", prepared, 0, call)
    assert prepared["status"] == "ok", prepared
    # Establish one real published owner, leaving room for the next owner in
    # the SAME home under the original two-owner reservation envelope.
    packet = pack_source_prefix(reg, before, prepared["prefix"]["components"][:1])
    binding = native._queue(
        generation,
        "queue_layout_bind_prefix",
        dict(job_id=reg["job_id"], plans=[packet["components"][0]["plan"]], envelope=envelope),
    )
    assert binding["status"] == "ok", binding
    pending = prepare_prefix_interval(reg, packet, binding["target"])
    result = native._queue(
        generation,
        "queue_layout_prepare",
        dict(job_id=reg["job_id"], expected_cursor=before, intent=pending),
    )
    assert result == dict(status="ok", intent_id=pending["intent_id"])
    fresh, first = _step(reg)
    assert fresh["status"] == "in_progress", fresh
    done, checked = _step(reg)
    assert done["status"] == "ok", done
    old = first.arguments["layout_import_batch:1"][2]["finalizations"][0]
    next_fresh, staged = _step(reg)
    assert next_fresh["status"] == "in_progress", next_fresh
    next_pending = staged.arguments["queue_layout_prepare:1"][2]["intent"]
    assert next_pending["target"]["partition_id"] == pending["target"]["partition_id"]
    new = staged.arguments["layout_import_batch:1"][2]["finalizations"]
    assert len(new) == 1 and new[0]["live"]["key"] == "aaa-prefix-1"
    assert checked.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    _assert_originals(source, generation, [old])
    live = old["live"]
    assert native._read(
        generation, "receipt", dict(namespace=live["namespace"], key=live["key"])
    ) == dict(status="ok", digest=live["digest"], receipt=live["receipt"])
    old_owner = native._read(generation, "bulk_owner", dict(owner=old["owner"]))
    assert old_owner["status"] == "ok" and old_owner["manifest"] == old, old_owner
    assert native._read(generation, "bulk_owner", dict(owner=new[0]["owner"])) == {
        "status": "unavailable"
    }
    done, second_checked = _step(reg)
    assert done["status"] == "ok", done
    assert second_checked.results["layout_import_batch:1"]["outcome"] == "existing_verified"
    state = native._queue_state(reg)
    assert state["pending"] is None and state["counts"]["live_owners"] == 2
    _assert_originals(source, generation, [old])
    _assert_originals(source, generation, new)
    _assert_frozen(tmp_path, before_source, artifact, before_artifact)
    assert native._CALLS - start == 30

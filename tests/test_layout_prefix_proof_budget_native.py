"""Actual SQLite source cuts a body-fitting cohort before proof overflow."""

import json
import sys

import pytest
import test_layout_native_integration as native
from test_sqlite_source_export import fixture
from test_layout_native_tail import _tree

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def test_actual_source_prefix_cuts_before_unconfirmable_group_reply(tmp_path):
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration
    from cognitive_castle.canonical_records import _json
    from cognitive_castle.lance_layout_packed import (
        prepare_prefix_interval,
        validate_grouped_owner_proof,
    )

    first_call = native._CALLS
    call = native._start("source_fixture")
    source = fixture(tmp_path, mode="proof_budget")
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
    assert len(source["proof_receipts"]) == 3 and all(
        len(r["ids"]) == 9 for r in source["proof_receipts"]
    )
    assert len(source["receipt"]["ids"]) == 9 and source["original_public"] == []
    before_source = _tree(tmp_path / "source", hashes=True)
    originals = {row["id"]: row for row in source["original_sources"]}
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    qualification, envelope = native._qualification(generation)
    artifact = generation / "exact/layout-qualification.json"
    artifact.write_text(_json(qualification))
    artifact.chmod(0o400)
    artifact_before = artifact.read_bytes()
    reg = migration.registration(source["descriptor"], str(generation))
    before = migration.initial_source_cursor(reg)
    window = dict(
        format=layout.PREFIX_SOURCE_REQUEST,
        action="prepare",
        generation=envelope["generation"],
        qualification_hash=migration.hashed(qualification),
        allowed_plan_hashes=[],
        packing=layout.PACKING,
    )
    packet = None
    for action in ("prepare", "export"):
        if action == "export":
            window = dict(
                window,
                action=action,
                allowed_plan_hashes=[migration.hashed(p["plan"]) for p in packet["components"]],
            )
        with native._session(generation) as session:
            call = native._start("layout_source")
            result = session.layout_source(source["descriptor"], before, window)
            native._record(session, "layout_source", result, 0, call)
        assert result["status"] == "ok", result
        actual = result["prefix"]
        assert len(actual["components"]) == 3
        if packet is not None:
            assert actual == packet
        packet = actual
        assert result["cursor_after"] == (before if action == "prepare" else packet["cursor_after"])
        assert packet["cursor_after"]["key"][1] == "aaa-proof-2"
        for index, part in enumerate(packet["components"]):
            exported = part["export"]
            owner = exported["finalizations"][0]
            assert owner["live"]["receipt"] == source["proof_receipts"][index]
            rows = [r for r in exported["entries"] if r["kind"] == "bulk_record"]
            assert len(rows) == owner["members"] == 9
            for row in rows:
                original = originals[row["id"]]
                assert (row["text"], row["provenance"]) == (
                    original["text"],
                    _json(original["provenance"]),
                )
        maximum = 2**63 - 1
        manifests = [p["export"]["finalizations"][0] for p in packet["components"]]
        prototype = dict(
            status="ok",
            format=layout.GROUPED_OWNER_PROOF,
            intent_hash="f" * 64,
            owners=[layout.owner_observation(m, maximum, maximum) for m in manifests],
        )
        assert max(len(json.dumps(prototype).encode()), len(_json(prototype).encode())) <= 4096
        target = layout.target(envelope, 0, [p["plan"] for p in packet["components"]])
        intent = prepare_prefix_interval(reg, packet, target)
        observed_shape = dict(
            status="ok",
            format=layout.GROUPED_OWNER_PROOF,
            intent_hash=migration.hashed(intent),
            owners=[layout.owner_observation(m, 0, 1) for m in manifests],
        )
        # This validates the transport shape against original manifests only;
        # no actual canonical/native observation or publication is claimed.
        assert (
            len(
                validate_grouped_owner_proof(
                    observed_shape, intent, dict(finalizations=manifests), 1
                )
            )
            == 3
        )
        assert _tree(tmp_path / "source", hashes=True) == before_source
        assert artifact.read_bytes() == artifact_before
    assert native._CALLS - first_call == 3

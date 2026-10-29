"""Exercise the real inherited append guard, without a native storage engine."""

import copy
from types import SimpleNamespace as NS

import pytest


def case():
    from test_layout_packed_facade import home_case
    from cognitive_castle import bulk_exact_codec as bulk, migration_codec as migration

    home, packet, intent, mutation, rows, _ = home_case()
    rows = copy.deepcopy(rows)
    entries = [row for part in packet["components"] for row in part["export"]["entries"]]
    proof = [[r["id"], migration.hashed(r)] for r in rows[:-1]]
    manifest = dict(
        format=bulk.FORMAT,
        state="committed_batch",
        batch_id=intent["intent_id"],
        source_binding=intent["source_binding"],
        digest=intent["digest"],
        proof=proof,
        records=len([r for r in entries if r["kind"] == "bulk_record"]),
        published_owners=len(packet["components"]),
        entries=len(entries),
    )
    rows[-1] = bulk.receipt_row(manifest, intent["routes"][-1]["id"])
    before = {k: mutation["before"][k] for k in mutation["after"]}
    home._mutation_geometry = lambda dataset: before
    home._coverage = lambda dataset: dict(tail_rows=0, tail_fragments=0)
    home._artifacts = dict(
        artifact_entries=0, logical_bytes=0, allocated_bytes=0, versions=0, file_bytes=0
    )
    reserve = dict(
        physical_rows=len(rows),
        fragments=1,
        versions=1,
        file_bytes=4 * 1024**2,
        artifact_entries=16,
        logical_bytes=2 * 1024**2,
        allocated_bytes=2 * 1024**2,
    )
    home.qualification = NS(
        owner=intent["owners"][0]["owner"],
        source_binding=intent["source_binding"],
        reservations=dict(append=reserve),
        max_physical_rows=4096,
        max_fragments=64,
        max_versions=128,
        max_file_bytes=4 * 1024**2,
        max_artifact_entries=512,
        max_logical_bytes=32 * 1024**2,
        max_allocated_bytes=32 * 1024**2,
        max_batch_receipts=64,
    )
    return home, rows, intent


def test_packed_append_runs_the_real_parent_guard_for_all_registered_owners():
    home, rows, _ = case()
    home._before_append(None, rows)
    assert home._charged_append[0] == dict(version=7, physical_rows=40, fragments=3)
    assert home._charged_append[3] == 1


@pytest.mark.parametrize(
    "damage",
    ["foreign_owner", "changed_text", "batch_binding", "batch_digest", "batch_key", "batch_count"],
)
def test_packed_append_keeps_strict_original_row_and_batch_bindings(damage):
    from cognitive_castle import bulk_exact_codec as bulk
    from cognitive_castle.lance_exact_store import _StoreError
    import json

    home, rows, _ = case()
    if damage == "foreign_owner":
        rows[0]["digest"] = "receipt:" + "e" * 64
    elif damage == "changed_text":
        rows[0]["text"] += "foreign"
    else:
        manifest = json.loads(rows[-1]["receipt"])
        field = {
            "batch_binding": "source_binding",
            "batch_digest": "digest",
            "batch_key": "batch_id",
            "batch_count": "published_owners",
        }[damage]
        manifest[field] = 1 if damage == "batch_count" else "e" * 64
        rows[-1] = bulk.receipt_row(manifest, rows[-1]["id"])
    with pytest.raises((_StoreError, ValueError)):
        home._before_append(None, rows)
    assert not hasattr(home, "_charged_append")


def test_dedicated_single_owner_row_guard_still_refuses_foreign_owner():
    from cognitive_castle.bulk_exact_store import OrderedBulkExactStore
    from cognitive_castle.lance_exact_store import _StoreError

    home, rows, intent = case()
    foreign = next(
        r
        for r in rows
        if r["kind"] == "bulk_record" and r["digest"] != intent["owners"][0]["owner"]
    )
    with pytest.raises(_StoreError, match="corrupt"):
        OrderedBulkExactStore._bound_row(home, foreign)

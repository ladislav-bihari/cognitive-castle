"""Ordered placement cuts before another home without hiding foreign metadata."""

import copy

import pytest

from test_layout_source_packing import prefix_case


def placement(*, last=False, maximum=2, usage=0, old=None):
    from cognitive_castle.lance_layout_directory import LayoutDirectory
    from cognitive_castle.canonical_records import _json
    from cognitive_castle import lance_layout_codec as layout, migration_codec as migration

    reg, _, parts, envelope = prefix_case(count=3)
    envelope = copy.deepcopy(envelope)
    envelope.update(max_partitions=maximum, limits=dict.fromkeys(layout.EXPANSION, 250000))
    plans = [p["plan"] for p in parts]
    calls = []
    reservations = {
        0: dict.fromkeys(layout.EXPANSION, usage),
        1: dict.fromkeys(layout.EXPANSION, 100000),
    }

    class Result:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class Conn:
        def execute(self, sql, args=()):
            calls.append((sql, args))
            if "FROM layout_partitions ORDER BY" in sql:
                return Result(
                    (0, migration.hashed(envelope), _json(reservations[0])) if last else None
                )
            if "FROM layout_homes WHERE owner=" in sql:
                if old is not None and args[0] == plans[0]["owner"]:
                    return Result(old(reg, plans[0], migration, _json))
                return Result(None)
            raise AssertionError("unexpected placement query: " + sql)

    directory = object.__new__(LayoutDirectory)
    directory.conn = Conn()
    directory._partition = lambda pid, env: copy.deepcopy(reservations[pid])
    return directory, reg["job_id"], plans, envelope, calls


def test_default_placement_preserves_full_original_rollover():
    directory, job, plans, envelope, calls = placement()
    updates, homes, targets = directory._placement(job, plans, envelope)
    assert [p["partition_id"] for p in targets] == [0, 0, 1]
    assert len(homes) == 3 and set(updates) == {0, 1}
    assert all(sql.startswith("SELECT") for sql, _ in calls)


def test_prefix_placement_stops_before_other_home_without_new_reservation():
    directory, job, plans, envelope, calls = placement()
    updates, homes, targets = directory._placement(job, plans, envelope, stop_partition=0)
    assert [p["partition_id"] for p in targets] == [0, 0]
    assert [p for p, _ in homes] == plans[:2] and set(updates) == {0}
    assert all(value == 200000 for value in updates[0].values())
    assert all(sql.startswith("SELECT") for sql, _ in calls)


def test_default_exhausted_capacity_still_refuses():
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, _ = placement(last=True, maximum=1, usage=200000)
    with pytest.raises(_StoreError):
        directory._placement(job, plans[:1], envelope)


def test_prefix_exhausted_capacity_returns_empty_cutoff_without_masking_first_failure():
    directory, job, plans, envelope, _ = placement(last=True, maximum=1, usage=200000)
    assert directory._placement(job, plans[:1], envelope, stop_partition=0) == ({}, [], [])


@pytest.mark.parametrize("prefix", [False, True])
def test_oversized_first_owner_refuses_but_later_owner_is_a_prefix_boundary(prefix):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, _ = placement()
    plans[0]["expansion"] = dict.fromkeys(plans[0]["expansion"], 300000)
    if prefix:
        assert directory._placement(job, plans[:1], envelope, stop_partition=0) == ({}, [], [])
    else:
        with pytest.raises(_StoreError):
            directory._placement(job, plans[:1], envelope)


def test_existing_valid_home_is_checked_then_stops_before_other_home():
    def old(reg, plan, codec, dump):
        return (reg["job_id"], dump(plan), 1, codec.hashed(plan))

    directory, job, plans, envelope, calls = placement(old=old)
    assert directory._placement(job, plans[:1], envelope, stop_partition=0) == ({}, [], [])
    assert len(calls) == 2


@pytest.mark.parametrize("damage", ["job", "plan", "hash"])
def test_foreign_existing_metadata_refuses_instead_of_becoming_cutoff(damage):
    from cognitive_castle.lance_exact_store import _StoreError

    def old(reg, plan, codec, dump):
        values = [reg["job_id"], dump(plan), 1, codec.hashed(plan)]
        values[{"job": 0, "plan": 1, "hash": 3}[damage]] = "foreign"
        return tuple(values)

    directory, job, plans, envelope, _ = placement(old=old)
    with pytest.raises(_StoreError):
        directory._placement(job, plans[:1], envelope, stop_partition=0)


@pytest.mark.parametrize("stop", [-1, True, 2])
def test_unqualified_stop_partition_refuses_before_sql(stop):
    directory, job, plans, envelope, calls = placement()
    with pytest.raises(ValueError):
        directory._placement(job, plans, envelope, stop_partition=stop)
    assert calls == []


def test_complete_same_home_prefix_keeps_every_original_plan():
    directory, job, plans, envelope, _ = placement()
    envelope["limits"] = dict.fromkeys(envelope["limits"], 500000)
    _, homes, targets = directory._placement(job, plans, envelope, stop_partition=0)
    assert [p for p, _ in homes] == plans
    assert len(targets) == 3 and {p["partition_id"] for p in targets} == {0}

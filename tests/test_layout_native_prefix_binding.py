"""Engine-free transaction control; actual SQLite qualification is separate."""

import copy

import pytest

from test_layout_source_packing import prefix_case


class Row:
    """SQLite rows iterate/index normally but do not equal or order as tuples."""

    def __init__(self, values):
        self.values = tuple(values)

    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        return iter(self.values)

    def __len__(self):
        return len(self.values)


class Result:
    def __init__(self, rows):
        self.rows = [Row(row) for row in rows]

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class Conn:
    def __init__(self, store, events, savepoints, envelope, migration, dump):
        self.scope = store, events, savepoints, envelope, migration, dump

    def execute(self, sql, args=()):
        store, events, savepoints, envelope, migration, _json = self.scope
        events.append((sql, args))
        if sql.startswith("SAVEPOINT "):
            savepoints[sql.split()[-1]] = copy.deepcopy(store)
        elif sql.startswith("ROLLBACK TO "):
            name = sql.split()[-1]
            store.clear()
            store.update(copy.deepcopy(savepoints[name]))
        elif sql.startswith("RELEASE "):
            savepoints.pop(sql.split()[-1])
        elif "FROM layout_partitions ORDER BY" in sql:
            if not store["reservations"]:
                return Result([])
            pid = max(store["reservations"])
            return Result([(pid, migration.hashed(envelope), _json(store["reservations"][pid]))])
        elif "FROM layout_homes WHERE job_id=" in sql:
            rows = [
                (owner, h["pid"])
                for owner, h in store["homes"].items()
                if h["job"] == args[0] and not h["published"]
            ]
            return Result(rows[:9])
        elif "FROM layout_registry WHERE active_owner IS NOT NULL" in sql:
            rows = [
                (r["active_owner"], pid)
                for pid, r in store["registry"].items()
                if r["active_owner"] is not None
            ]
            if "LIMIT 1" in sql:
                return Result([(r[0],) for r in rows[:1]])
            return Result(rows[:2])
        elif "FROM layout_homes WHERE owner=" in sql:
            home = store["homes"].get(args[0])
            if home is None:
                return Result([])
            if sql.startswith("SELECT published"):
                return Result([(home["published"],)])
            return Result(
                [
                    (
                        home["job"],
                        _json(home["plan"]),
                        home["pid"],
                        migration.hashed(home["plan"]),
                    )
                ]
            )
        elif "FROM layout_spans WHERE owner=" in sql:
            span = store["spans"].get(args[0])
            return Result([span] if span is not None else [])
        elif sql.startswith("INSERT INTO layout_spans"):
            store["spans"][args[0]] = tuple(args[1:])
        elif sql.startswith("UPDATE layout_registry SET state='sealed'"):
            for pid, row in store["registry"].items():
                if pid < args[0] and row["state"] == "writable":
                    row["state"] = "sealed"
        elif sql.startswith("UPDATE layout_registry SET active_owner="):
            store["registry"][args[1]]["active_owner"] = args[0]
        else:
            raise AssertionError("unexpected prefix SQL: " + sql)
        return Result([])


def authority(monkeypatch, *, maximum=2, fault=None):
    from cognitive_castle import lance_layout_codec as codec, migration_codec as migration
    from cognitive_castle.lance_layout_directory import LayoutDirectory, _NativeLayoutDirectory
    from cognitive_castle.canonical_records import _json
    from cognitive_castle.lance_exact_store import _StoreError

    reg, before, parts, envelope = prefix_case(count=3)
    plans = [p["plan"] for p in parts]
    envelope = copy.deepcopy(envelope)
    envelope.update(max_partitions=maximum, limits=dict.fromkeys(codec.EXPANSION, 250000))
    store = dict(
        homes={},
        reservations={},
        spans={},
        registry={
            i: dict(
                state="empty",
                mutation=None,
                active_owner=None,
                pin=dict(physical_rows=0, fragments=0),
            )
            for i in range(maximum)
        },
    )
    events, savepoints = [], {}
    state = dict(
        registration=reg,
        layout=dict(envelope=envelope),
        staging_cursor=before,
        pending=None,
        state="registered",
    )

    directory = object.__new__(_NativeLayoutDirectory)
    directory.conn, directory.envelope, directory.epoch = (
        Conn(store, events, savepoints, envelope, migration, _json),
        envelope,
        "a" * 64,
    )
    directory._state = lambda job: copy.deepcopy(state)
    directory.attachment = lambda job: state["layout"]
    directory._partition = lambda pid, env: copy.deepcopy(
        store["reservations"].get(pid, dict.fromkeys(codec.EXPANSION, 0))
    )
    directory.partition = lambda pid: copy.deepcopy(store["registry"][pid])
    directory.owner_plan = lambda owner: copy.deepcopy(store["homes"][owner]["plan"])

    def base_bind(self, job, selected, env):
        assert len(selected) == 1
        plan = selected[0]
        updates, homes, targets = self._placement(job, selected, env)
        for pid, value in updates.items():
            store["reservations"][pid] = value
        for original, pid in homes:
            store["homes"][original["owner"]] = dict(
                job=job, plan=copy.deepcopy(original), pid=pid, published=0
            )
        if plan["owner"] == plans[1]["owner"] and fault == "exception":
            raise _StoreError("unavailable")
        if plan["owner"] == plans[1]["owner"] and fault == "cross":
            home = store["homes"][plan["owner"]]
            home["pid"] = 1
            store["reservations"][1] = dict(plan["expansion"])
            targets = [codec.target(env, 1, [plan])]
        return dict(status="ok", targets=targets)

    monkeypatch.setattr(LayoutDirectory, "bind", base_bind)
    return directory, reg["job_id"], plans, envelope, store, events, state


def call(directory, job, plans, envelope):
    return directory.bind_prefix(job, plans, envelope)


def test_prefix_bind_reserves_only_one_home_and_no_ghost_next_owner(monkeypatch):
    directory, job, plans, envelope, store, _, _ = authority(monkeypatch)
    result = call(directory, job, plans, envelope)
    assert result["format"] == "lance-layout-prefix-binding-v1" and result["status"] == "ok"
    assert result["target"]["partition_id"] == 0
    assert set(store["homes"]) == set(store["spans"]) == {p["owner"] for p in plans[:2]}
    assert set(store["reservations"]) == {0}
    assert store["registry"][0]["active_owner"] == plans[0]["owner"]


@pytest.mark.parametrize("initial", [1, 3])
def test_repeated_or_extended_original_prefix_is_idempotent(monkeypatch, initial):
    directory, job, plans, envelope, store, _, _ = authority(monkeypatch)
    call(directory, job, plans[:initial], envelope)
    result = call(directory, job, plans, envelope)
    before = copy.deepcopy(store)
    assert call(directory, job, plans, envelope) == result and store == before
    assert len(store["homes"]) == 2


def test_shorter_than_already_bound_prefix_refuses_before_mutation(monkeypatch):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, store, _, _ = authority(monkeypatch)
    call(directory, job, plans, envelope)
    before = copy.deepcopy(store)
    with pytest.raises(_StoreError):
        call(directory, job, plans[:1], envelope)
    assert store == before


@pytest.mark.parametrize("damage", ["pending", "partial", "done"])
def test_prefix_cannot_replace_pending_partial_or_completed_source(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, store, events, state = authority(monkeypatch)
    if damage == "pending":
        state["pending"] = dict(intent_id="a" * 64)
    elif damage == "partial":
        state["staging_cursor"]["owner"] = "a" * 64
    else:
        state["state"] = "snapshot_export_exhausted"
    with pytest.raises(_StoreError):
        call(directory, job, plans, envelope)
    assert store["homes"] == {} and not any(q.startswith("SAVEPOINT") for q, _ in events)


@pytest.mark.parametrize("damage", ["empty", "nine", "duplicate", "phase"])
def test_invalid_prefix_input_refuses_before_sql(monkeypatch, damage):
    directory, job, plans, envelope, _, events, _ = authority(monkeypatch)
    if damage == "empty":
        plans = []
    elif damage == "nine":
        plans = plans * 3
    elif damage == "duplicate":
        plans = plans[:1] * 2
    else:
        plans[1]["source_phase"] = "live"
    with pytest.raises(ValueError):
        call(directory, job, plans, envelope)
    assert events == []


@pytest.mark.parametrize("damage", ["sealed", "mutation"])
def test_unwritable_home_rolls_back_complete_binding(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, store, _, _ = authority(monkeypatch)
    store["registry"][0]["state" if damage == "sealed" else "mutation"] = (
        "sealed" if damage == "sealed" else {"kind": "append"}
    )
    before = copy.deepcopy(store)
    with pytest.raises(_StoreError):
        call(directory, job, plans, envelope)
    assert store == before


def test_cross_home_reply_rolls_back_just_unselected_owner(monkeypatch):
    directory, job, plans, envelope, store, events, _ = authority(monkeypatch, fault="cross")
    result = call(directory, job, plans, envelope)
    assert len(result["target"]["owner_plan_hashes"]) == 1
    assert set(store["homes"]) == set(store["spans"]) == {plans[0]["owner"]}
    assert set(store["reservations"]) == {0}
    assert any(q.startswith("ROLLBACK TO") for q, _ in events)


def test_later_exception_rolls_back_all_earlier_owner_bindings(monkeypatch):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, store, _, _ = authority(monkeypatch, fault="exception")
    before = copy.deepcopy(store)
    with pytest.raises(_StoreError):
        call(directory, job, plans, envelope)
    assert store == before


@pytest.mark.parametrize(
    "damage", ["foreign_anchor", "missing_anchor", "mixed_home", "bad_span", "published"]
)
def test_existing_bound_metadata_must_be_consistent(monkeypatch, damage):
    from cognitive_castle.lance_exact_store import _StoreError

    directory, job, plans, envelope, store, _, _ = authority(monkeypatch)
    call(directory, job, plans, envelope)
    if damage == "foreign_anchor":
        store["registry"][0]["active_owner"] = "receipt:" + "f" * 64
    elif damage == "missing_anchor":
        store["registry"][0]["active_owner"] = None
    elif damage == "mixed_home":
        store["homes"][plans[1]["owner"]]["pid"] = 1
    elif damage == "bad_span":
        store["spans"][plans[1]["owner"]] = (0, 8, 0, 1)
    else:
        store["homes"][plans[0]["owner"]]["published"] = 1
    before = copy.deepcopy(store)
    with pytest.raises(_StoreError):
        call(directory, job, plans, envelope)
    assert store == before


@pytest.mark.parametrize("count", [0, 1, 2])
def test_context_keeps_v1_for_single_owner_and_explicit_unordered_v2_for_prefix(monkeypatch, count):
    directory, job, plans, envelope, _, _, _ = authority(monkeypatch)
    if count:
        call(directory, job, plans[:count], envelope)
    result = directory.context(job)
    assert result["format"] == "lance-layout-native-context-v" + ("1" if count < 2 else "2")
    if count < 2:
        assert result["plan"] == (plans[0] if count else None)
    else:
        assert {p["owner"] for p in result["bound_plans"]} == {p["owner"] for p in plans[:2]}
        assert result["first_owner"] == plans[0]["owner"] and result["target"] is None


def test_final_available_partition_does_not_discard_valid_prefix(monkeypatch):
    directory, job, plans, envelope, store, _, _ = authority(monkeypatch, maximum=1)
    assert len(call(directory, job, plans, envelope)["target"]["owner_plan_hashes"]) == 2
    assert set(store["reservations"]) == {0}

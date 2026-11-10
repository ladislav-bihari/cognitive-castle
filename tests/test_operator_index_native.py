"""Opt-in real operator ticks; append one synthetic fact, preserve the prior eight."""

import hashlib
import json
import os
from pathlib import Path
import sys
import time

from test_memory_selected_native import _root, _write, pytestmark  # noqa: F401
from test_registered_model_native import _fact, _pure_host, _session, SCOPE
from test_index_background_native import _observe

DATA_SOURCE = "ac40671daf96735d5341117abeac1563dee63540"
TEXT = "The observatory's copper cabinet holds the spare amber lens."


def _case():
    root = _root()
    value = json.loads((root / "model-handoff.json").read_text())
    assert value["source"] == DATA_SOURCE == os.environ["CASTLE_INDEX_DATA_SOURCE"]
    assert value["generation"] == str(root / "generation")
    assert value["profile"] == os.environ["CASTLE_TEST_SEMANTIC_PROFILE"]
    assert hashlib.sha256(Path(value["profile"]).read_bytes()).hexdigest() == value["profile_hash"]
    assert all((root / ("indexed-" + str(n) + ".json")).is_file() for n in range(8))
    return root, value


def _new_fact():
    from cognitive_castle.canonical_records import prepare_remember

    return prepare_remember("new-fact", TEXT, namespace="registered-operator-fixture")


def _status(root, value):
    with _session(value) as session:
        result = session.store(
            root / "generation", "queue_status", dict(envelope_hash=value["pin"])
        )
    assert result["status"] == "ok" and result["registered"]["confirmed_records"] == 9
    return result


def test_operator_native_prepare_private_selection(monkeypatch):
    from cognitive_castle.canonical_records import _hash, _json
    from cognitive_castle.memory_selection import load_selection
    from test_layout_native_integration import _start, _record, _qualification

    root, value = _case()
    # Initialization is a separately supervised control operation, never host SQL.
    with _session(value) as session:
        call = _start("direct_operator_control_initialize")
        result = session._run_stage(
            dict(
                directory=str(root / "operator-control"),
                operation="initialize",
                arguments=dict(max_bytes=3 * 1024**2),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("test_capture_control_native.py")),
                "initialize",
            ],
            cap=3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        _record(session, "direct_operator_control_initialize", result, 0, call)
    assert result["status"] == "ok", result
    palace = root / "operator-palace"
    palace.mkdir(mode=0o700)
    (palace / ".memory").mkdir(mode=0o700)
    _, envelope = _qualification(root / "generation")
    assert _hash(_json(envelope)) == value["pin"]
    selected = dict(
        format="castle-canonical-selection-v1",
        backend="lance",
        envelope=envelope,
        control_authority=result["authority"],
        control_hash=result["authority_hash"],
        profile=dict(path=value["profile"], hash=value["profile_hash"]),
        wing="project",
        room="conversation",
    )
    _write(palace / ".memory/canonical-selection.json", selected)
    _write(
        palace / ".memory/canonical-backend.json",
        dict(
            format="castle-canonical-backend-v1",
            backend="lance",
            selection_hash=_hash(_json(selected)),
        ),
    )
    assert load_selection(palace) == selected
    _observe(monkeypatch)
    with _session(value) as session:
        status = session.store(
            root / "generation", "queue_status", dict(envelope_hash=value["pin"])
        )
    assert status["status"] == "ok" and status["registered"]["confirmed_records"] == 8
    assert status["counts"]["complete"] == 8
    _write(
        root / "operator-handoff.json",
        dict(
            execution_source=os.environ["CASTLE_SELECTED_NATIVE_SOURCE"],
            data_source=DATA_SOURCE,
            original_handoff_hash=hashlib.sha256(
                (root / "model-handoff.json").read_bytes()
            ).hexdigest(),
        ),
    )
    _pure_host()


def test_operator_native_append_new_exact_fact():
    from cognitive_castle.live_publication_codec import prepare_publication
    from test_lance_live_native import _attempt

    root, value = _case()
    assert (root / "operator-handoff.json").is_file()
    prepared = _new_fact()
    publication = prepare_publication(prepared, [SCOPE for _ in prepared.records])
    for expected in ("in_progress", "ok", "in_progress", "ok", "ok"):
        result, _ = _attempt(root / "generation", publication, value["pin"])
        assert result["status"] == expected, result
    assert result["state"] == "snapshot_export_exhausted"
    receipt, _ = _attempt(root / "generation", publication, value["pin"], complete=True)
    assert receipt == prepared.receipt
    _write(root / "operator-exact.json", dict(receipt=receipt, committed_ns=time.time_ns()))
    _pure_host()


def _tick(monkeypatch, *, forbid_model):
    from cognitive_castle import semantic_coordinator, semantic_runtime

    root, value = _case()
    _observe(monkeypatch)
    if forbid_model:

        def forbidden(*args, **kwargs):
            raise AssertionError("outbox/receipt recovery must not repeat model inference")

        monkeypatch.setattr(semantic_runtime.SemanticSession, "tokenize", forbidden)
        monkeypatch.setattr(semantic_runtime.SemanticSession, "infer", forbidden)
    started = time.monotonic()
    result = semantic_coordinator.index_tick(root / "operator-palace", diagnostic=True)
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(
            dict(operation="operator_background_tick", seconds=elapsed, status=result["status"])
        ),
        flush=True,
    )
    assert elapsed <= 5, result
    return root, value, result


def test_operator_native_recovers_exact_outbox_without_model(monkeypatch):
    root, value, result = _tick(monkeypatch, forbid_model=True)
    assert result["status"] == "in_progress" and result["state"] == "exact_outbox_recovered", result
    assert result["intent_recovery"] == dict(status="ok", recovered=1, pending=[])
    status = _status(root, value)
    assert status["counts"]["complete"] == 8 and status["counts"]["pending"] == 1
    _write(root / "operator-recovered.json", result)
    _pure_host()


def test_operator_native_publishes_real_vector_without_ack(monkeypatch):
    root, value, result = _tick(monkeypatch, forbid_model=False)
    assert result["status"] == "in_progress" and result["state"] == "verification_pending", result
    assert result["position"] == 0 and "receipt_id" not in result
    assert _status(root, value)["counts"]["complete"] == 8
    _write(root / "operator-vector-pending.json", result)
    _pure_host()


def test_operator_native_cold_verifies_then_acknowledges(monkeypatch):
    root, value, result = _tick(monkeypatch, forbid_model=True)
    assert result["status"] == "ok" and result["state"] == "complete", result
    assert result["recovered_vector"] is True
    assert _status(root, value)["counts"]["complete"] == 9
    _write(root / "operator-vector-verified.json", result)
    _pure_host()


def test_operator_native_new_fact_paraphrase_and_originals_preserved(monkeypatch):
    from cognitive_castle.semantic_indexer import query_exact

    root, value = _case()
    assert (root / "operator-vector-verified.json").is_file()
    _observe(monkeypatch)
    started = time.monotonic()
    with _session(value) as session:
        result = query_exact(
            session, root / "generation", "Where is the spare amber lens stored?", SCOPE
        )
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(
            dict(operation="operator_new_fact_query", seconds=elapsed, status=result["status"])
        ),
        flush=True,
    )
    assert elapsed <= 5 and result["status"] == "ok", result
    original = _new_fact().records[0].source()
    assert result["candidates"][0]["canonical_id"] == original["id"]
    assert original in result["sources"]
    originals = [_fact(n).records[0].source() for n in range(8)]
    with _session(value) as session:
        previous = session.exact(
            root / "generation/exact", "read_sources", dict(ids=[s["id"] for s in originals])
        )
    assert previous == dict(status="ok", sources=originals)
    assert _status(root, value)["counts"]["complete"] == 9
    # Preserve the original provenance contract and the actual original fragment bytes.
    handoff = json.loads((root / "operator-handoff.json").read_text())
    assert (
        hashlib.sha256((root / "model-handoff.json").read_bytes()).hexdigest()
        == handoff["original_handoff_hash"]
    )
    fragments = json.loads(Path(os.environ["CASTLE_OPERATOR_FRAGMENT_MANIFEST"]).read_text())
    assert fragments and len(fragments) <= 1024
    for relative, expected in fragments.items():
        path = root / relative
        assert path.is_relative_to(root) and "/data/" in relative and path.suffix == ".lance"
        assert path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 * 1024**2
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected
    _pure_host()


def _prepare_schedule(monkeypatch):
    from cognitive_castle import semantic_coordinator

    root, value = _case()
    _observe(monkeypatch)
    started = time.monotonic()
    result = semantic_coordinator.prepare_index_schedule(root / "operator-palace", diagnostic=True)
    elapsed = time.monotonic() - started
    print(
        "REGISTERED_MODEL_OPERATION "
        + json.dumps(
            dict(operation="operator_schedule_prepare", seconds=elapsed, status=result["status"])
        ),
        flush=True,
    )
    assert elapsed <= 5, result
    return root, value, result


def test_operator_schedule_native_prepares_only_one_bounded_page(monkeypatch):
    root, value, result = _prepare_schedule(monkeypatch)
    assert result == dict(
        status="in_progress",
        format="castle-index-schedule-v1",
        processed=8,
        backfill_cursor=8,
        ready=False,
    )
    assert _status(root, value)["counts"]["complete"] == 9
    _pure_host()


def test_operator_schedule_native_resumes_preparation_without_changing_receipts(monkeypatch):
    root, value, result = _prepare_schedule(monkeypatch)
    assert result == dict(
        status="ok", format="castle-index-schedule-v1", processed=1, backfill_cursor=9, ready=True
    )
    assert _status(root, value)["counts"]["complete"] == 9
    _pure_host()


def test_operator_schedule_native_idle_preserves_all_canonical_sources(monkeypatch):
    root, value, result = _tick(monkeypatch, forbid_model=True)
    assert result["status"] == "idle", result
    originals = [_fact(n).records[0].source() for n in range(8)]
    with _session(value) as session:
        first = session.exact(
            root / "generation/exact", "read_sources", dict(ids=[s["id"] for s in originals])
        )
        second = session.exact(
            root / "generation/exact",
            "read_sources",
            dict(ids=[_new_fact().records[0].source()["id"]]),
        )
    assert first == dict(status="ok", sources=originals)
    assert second == dict(status="ok", sources=[_new_fact().records[0].source()])
    assert _status(root, value)["counts"]["complete"] == 9
    fragments = json.loads(Path(os.environ["CASTLE_OPERATOR_FRAGMENT_MANIFEST"]).read_text())
    for relative, expected in fragments.items():
        path = root / relative
        assert path.is_file() and not path.is_symlink() and path.stat().st_size <= 2 * 1024**2
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected
    _pure_host()

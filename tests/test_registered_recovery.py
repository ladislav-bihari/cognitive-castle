"""Registered recovery proof contracts; native IO is a separate qualification."""

import copy
from types import SimpleNamespace

import pytest

from cognitive_castle import migration_codec as migration
from cognitive_castle.partitioned_exact_store import PartitionedExactStore
from cognitive_castle.semantic_intents import prepare_intent
from cognitive_castle.semantic_profile import digest
from cognitive_castle.canonical_records import prepare_remember
from test_partitioned_exact_store import qualification_case


def recovery_case(monkeypatch, text="original Ω\x00\r\n"):
    _, _, _, envelope = qualification_case()
    prepared = prepare_remember("request", text, {"note": "original caller"})
    intent = prepare_intent(
        prepared,
        [dict(workspace=None, wing="project", room="conversation") for _ in prepared.records],
        "new",
    )
    sources = {record.id: record.source() for record in prepared.records}
    receipt = dict(status="ok", digest=prepared.digest, receipt=prepared.receipt)
    target = PartitionedExactStore(envelope["generation"]["path"])
    authority = SimpleNamespace(envelope=envelope)
    seen, transactions = [], []
    original_read = target._read

    def run(operation):
        transactions.append(authority)
        return operation(authority)

    def read(current, operation, arguments, identities, context):
        assert current is authority and context is None
        if operation == "verify_intent":
            return original_read(current, operation, arguments, identities, context)
        seen.append((operation, arguments))
        if operation == "receipt":
            return copy.deepcopy(receipt)
        assert operation == "read_sources"
        return dict(status="ok", sources=[sources[i] for i in arguments["ids"] if i in sources])

    monkeypatch.setattr(target, "_run", run)
    monkeypatch.setattr(target, "_read", read)
    return target, intent, sources, receipt, seen, transactions, migration.hashed(envelope)


@pytest.mark.parametrize("text", ["original Ω\x00\r\n", "€" * 21845], ids=["one", "nine"])
def test_registered_recovery_verifies_every_original_in_one_authority_transaction(
    monkeypatch, text
):
    target, intent, sources, receipt, seen, transactions, pin = recovery_case(monkeypatch, text)
    result = target.layout_read("verify_intent", dict(intent=intent), envelope_hash=pin)
    assert result == dict(**receipt, intent_binding=digest(intent))
    assert "sources" not in result and "original caller" not in str(result)
    assert len(transactions) == 1
    assert seen[0] == ("receipt", dict(namespace=intent["namespace"], key=intent["key"]))
    assert [i for op, args in seen[1:] for i in args["ids"]] == list(sources)
    assert [len(args["ids"]) for op, args in seen[1:]] == ([1] if len(sources) == 1 else [8, 1])


@pytest.mark.parametrize(
    "fault", ["digest", "receipt", "missing_ninth", "ninth_text", "caller", "binding"]
)
def test_registered_recovery_never_proves_changed_or_missing_originals(monkeypatch, fault):
    target, intent, sources, receipt, _, _, pin = recovery_case(monkeypatch, "€" * 21845)
    last = intent["receipt"]["ids"][-1]
    if fault == "digest":
        receipt["digest"] = "0" * 64
    elif fault == "receipt":
        receipt["receipt"]["text_sha256"] = "0" * 64
    elif fault == "missing_ninth":
        del sources[last]
    elif fault == "ninth_text":
        sources[last] = dict(sources[last], text="changed")
    elif fault == "caller":
        sources[last]["provenance"]["caller"] = dict(note="changed")
    else:
        intent["groups"][-1]["input_binding"] = "0" * 64
    result = target.layout_read("verify_intent", dict(intent=intent), envelope_hash=pin)
    assert result["status"] in {"stale", "conflict"}
    assert "intent_binding" not in result


def test_registered_recovery_wrong_authority_refuses_before_native_access(monkeypatch):
    target, intent, _, _, seen, _, _ = recovery_case(monkeypatch)
    result = target.layout_read("verify_intent", dict(intent=intent), envelope_hash="f" * 64)
    assert result == dict(status="conflict", reason="selected_layout_authority")
    assert seen == []


def test_registered_recovery_malformed_intent_refuses_before_transaction(monkeypatch):
    target, intent, _, _, _, transactions, pin = recovery_case(monkeypatch)
    intent["groups"] *= 3
    with pytest.raises(ValueError):
        target.layout_read("verify_intent", dict(intent=intent), envelope_hash=pin)
    assert transactions == []


@pytest.mark.parametrize(
    "fault", [None, "binding", "digest", "receipt", "scope", "missing_binding", "refusal"]
)
def test_registered_coordinator_completes_only_matching_guarded_proof(monkeypatch, fault):
    from pathlib import Path

    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.semantic_indexer import _recover_intent

    _, intent, _, receipt, _, _, pin = recovery_case(monkeypatch)
    generation = Path("/trusted/generation")
    proof = dict(**receipt, intent_binding=digest(intent))
    if fault == "binding":
        proof["intent_binding"] = "0" * 64
    elif fault == "digest":
        proof["digest"] = "0" * 64
    elif fault == "receipt":
        proof["receipt"] = dict(proof["receipt"], text_sha256="0" * 64)
    elif fault == "scope":
        group = intent["groups"][0]
        group["references"][0]["scope"]["room"] = "changed"
        group["batch_key"] = "index:" + digest(
            [
                intent["namespace"],
                intent["key"],
                intent["digest"],
                0,
                group["references"],
                group["profile"],
            ]
        )
    elif fault == "missing_binding":
        del proof["intent_binding"]
    elif fault == "refusal":
        proof = dict(status="unavailable", reason="worker_deadline")
    stages, completed = [], []

    def stage(self, request, **limits):
        stages.append((request, limits))
        assert request == dict(
            directory=str(generation),
            operation="layout_read",
            arguments=dict(
                operation="verify_intent", arguments=dict(intent=intent), envelope_hash=pin
            ),
        )
        return proof

    monkeypatch.setattr(runtime.SemanticSession, "_run_stage", stage)
    session = runtime.LayoutSemanticSession(Path("/profile"), generation, envelope_hash=pin)
    session.store = lambda directory, operation, arguments: (
        completed.append((directory, operation, arguments)) or dict(status="ok")
    )
    result = _recover_intent(session, generation, intent)
    if fault is None:
        assert result == dict(status="ok")
        assert completed == [
            (
                generation,
                "queue_intent_complete",
                dict(intent_id=intent["intent_id"], binding=digest(intent)),
            )
        ]
    else:
        assert result["status"] != "ok" and completed == []
    assert len(stages) == 1
    limits = stages[0][1]
    assert (limits["cap"], limits["rss"], limits["total"]) == (2, 192 * 1024**2, 256 * 1024**2)

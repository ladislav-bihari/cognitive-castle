"""Inactive durable publication/index protocol; native state stays in workers."""

import os
from pathlib import Path
import sys

import pytest

from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.semantic_profile import PROFILE_ID, digest
from test_semantic_queue import invoke, SCOPE


@pytest.fixture(autouse=True)
def admission(monkeypatch, tmp_path):
    from cognitive_castle import memory_runtime

    monkeypatch.setattr(memory_runtime, "_admission_directory", lambda: tmp_path)


def _publication():
    from cognitive_castle.semantic_intents import source_exposure_hash
    from cognitive_castle.semantic_windows import make_unit, page_units

    source = prepare_remember("fixture", "exact source").records[0].source()
    envelopes = [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    tokens = {"input_ids": [0, 42, 2], "attention_mask": [1, 1, 1], "token_type_ids": [0, 0, 0]}
    unit = make_unit(
        source, 0, 1, 0, len(source["text"]), tokens, [[0, 0], [0, len(source["text"])], [0, 0]]
    )
    page = page_units(envelopes, [unit])
    inference = {key: value for key, value in page.items() if key not in {"units"}}
    inference.update(
        vectors=[{"unit_id": unit["unit_id"], "vector": [1.0] + [0.0] * 383}],
        runtime={
            "onnxruntime": "1.25.1",
            "providers": ["CPUExecutionProvider"],
            "tokenizer_loaded": False,
        },
    )
    return {
        "batch_key": "fixture",
        "references": [
            {
                "canonical_id": source["id"],
                "source_hash": source["provenance"]["text_hash"],
                "exposure_hash": source_exposure_hash(source),
                "scope": SCOPE,
            }
        ],
        "page": page,
        "inference": inference,
    }, source


def intent(text="secret exact body", scope=None):
    from cognitive_castle.semantic_intents import prepare_intent

    prepared = prepare_remember("request", text)
    return prepare_intent(prepared, [scope or SCOPE] * len(prepared.records), "new")


def test_intent_is_body_free_deterministic_and_binds_scopes():
    first = intent()
    assert first == intent()
    assert "secret exact body" not in str(first)
    assert first != intent(scope={**SCOPE, "room": "other"})
    assert first["digest"] == prepare_remember("request", "secret exact body").digest
    ninth = intent("\u20ac" * 21845)
    assert [len(group["references"]) for group in ninth["groups"]] == [8, 1]
    assert all(group["profile"] == PROFILE_ID for group in ninth["groups"])


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_intent_durable_conflict_and_atomic_registration(tmp_path):
    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    first = intent()
    assert invoke(directory, "queue_intent_prepare", {"intent": first})["status"] == "ok"
    assert invoke(directory, "queue_intent_prepare", {"intent": first})["status"] == "ok"
    assert invoke(directory, "queue_intent_prepare", {"intent": intent("changed")}) == {
        "status": "conflict"
    }
    assert invoke(directory, "queue_intents")["intents"] == [first]
    raw = (directory / "coordination/queue.sqlite3").read_bytes()
    assert b"secret exact body" not in raw
    # No caller receipt authorizes registration: the coordinator must verify Lance first.
    assert invoke(directory, "queue_status")["counts"]["pending"] == 0


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_exact_publish_recovery_after_lost_response_and_registration(tmp_path):
    from cognitive_castle.semantic_indexer import ExactPublication, recover_intents
    from cognitive_castle.semantic_runtime import SemanticSession

    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    prepared = prepare_remember("request", "secret exact body")
    first = intent()
    assert invoke(directory, "queue_intent_prepare", {"intent": first})["status"] == "ok"
    # A fresh supervised exact process commits, then loses its response.
    from test_lance_exact_store import invoke as exact_invoke

    result = exact_invoke(
        directory / "exact",
        fixture="lost_response",
        arguments={"request_id": "request", "text": "secret exact body"},
    )
    assert result["status"] == "unavailable"
    with SemanticSession(tmp_path / "unused", interactive=False) as session:
        recovered = recover_intents(session, directory)
    assert recovered["status"] == "ok", recovered
    assert recovered["recovered"] == 1
    assert invoke(directory, "queue_intents")["intents"] == []
    assert invoke(directory, "queue_status")["counts"]["pending"] == 1
    with ExactPublication(directory) as publisher:
        result = publisher.remember("request", "secret exact body", scopes=[SCOPE])
    assert result["receipt"] == prepared.receipt
    assert result["exact_status"] == "committed" and result["semantic_status"] == "pending"
    with ExactPublication(directory) as publisher:
        result = publisher.remember(
            "request", "secret exact body", scopes=[{**SCOPE, "room": "different"}]
        )
    assert result["status"] == "conflict"


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_fixed_stage_caps_share_absolute_deadline(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime

    monkeypatch.setattr(runtime.memory_runtime, "_admission_directory", lambda: tmp_path)
    clock = [0.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    seen = []

    def supervise(request, fd, **kwargs):
        seen.append((request, kwargs))
        clock[0] += 0.6
        return {"status": "ok"}

    monkeypatch.setattr(runtime.memory_runtime, "_supervise", supervise)
    with runtime.SemanticSession(tmp_path / "profile") as session:
        session.store(tmp_path, "queue_intents", {})
        session.exact(tmp_path, "read_sources", {"ids": []})
        clock[0] = 4.8
        session.store(tmp_path, "vector_receipt", {"batch_key": "x", "start_window": 0})
        assert session.store(tmp_path, "queue_status", {})["reason"] == "semantic_deadline"
    assert [round(kw["deadline"], 2) for _, kw in seen] == [3, 2, 0.2]
    assert [kw["rss_limit"] // 1024**2 for _, kw in seen] == [128, 192, 192]
    assert all(kw["absolute_deadline"] == 5 for _, kw in seen)
    assert all("worker_command" in kw for _, kw in seen)


def test_indexer_recovers_committed_vector_before_any_inference():
    from cognitive_castle.semantic_indexer import index_one

    args, source = _publication()
    group = {
        **args,
        "profile": PROFILE_ID,
        "input_binding": args["page"]["input_binding"],
        "reference_binding": digest(args["references"]),
        "lane": "new",
        "state": "inflight",
        "position": 0,
        "total_units": None,
    }
    group.pop("page")
    group.pop("inference")
    from cognitive_castle.lance_vector_store import prepare_publication

    receipt = prepare_publication(**args)[1]["receipt"]

    class Session:
        resources = []

        def store(self, directory, operation, arguments):
            if operation == "queue_intents":
                return {"status": "ok", "intents": []}
            if operation == "queue_select":
                return {"status": "ok", "group": group}
            if operation == "vector_receipt":
                return receipt
            assert operation == "queue_acknowledge"
            return {"status": "ok", "state": "complete", "position": 1}

        def exact(self, directory, operation, arguments):
            return {"status": "ok", "sources": [source]}

        def tokenize(self, *args):
            pytest.fail("durable vector receipt must recover without inference")

    result = index_one(Session(), Path("/fixture"))
    assert result["status"] == "ok" and result["position"] == 1
    assert result["recovered_vector"]


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_live_interactive_waiter_preempts_background_and_stale_marker_does_not(tmp_path):
    import subprocess
    import time
    import psutil
    from cognitive_castle import memory_runtime as runtime

    # A separate live process waits on the same admission; no production models.
    waiter_program = """
import os,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from cognitive_castle.semantic_runtime import SemanticSession
from cognitive_castle import memory_runtime
memory_runtime._admission_directory=lambda: Path(sys.argv[3])
started=time.monotonic()
with SemanticSession(Path('/unused')) as session:
    Path(sys.argv[2]).write_text(str(session._refusal or 'acquired')+':'+str(time.monotonic()-started))
"""
    worker = tmp_path / "slow.py"
    ready, acquired = tmp_path / "ready", tmp_path / "acquired"
    worker.write_text(
        "import os,time\nfrom pathlib import Path\nPath("
        + repr(str(ready))
        + ").write_text(str(os.getpid()))\ntime.sleep(10)\n"
    )
    # The supervisor owns admission until this test releases it, matching session exit.
    import threading

    result = {}
    stale = runtime._priority_descriptor(create=True)
    os.close(stale)
    assert runtime.interactive_waiting() is False
    fd = runtime._admission()

    def background():
        try:
            result.update(
                runtime._supervise(
                    {},
                    fd,
                    worker_command=[sys.executable, "-I", str(worker)],
                    deadline=3,
                    background_preempt=True,
                )
            )
        finally:
            os.close(fd)

    thread = threading.Thread(target=background)
    thread.start()
    until = time.monotonic() + 2
    while not ready.exists() and time.monotonic() < until:
        time.sleep(0.01)
    assert ready.exists()
    pid = int(ready.read_text())
    waiter = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            waiter_program,
            str(Path(__file__).resolve().parents[1]),
            str(acquired),
            str(tmp_path),
        ]
    )
    waiter.wait(timeout=2)
    thread.join(timeout=2)
    assert result.get("reason") == "background_preempted", result
    status, elapsed = acquired.read_text().split(":")
    assert status == "acquired" and float(elapsed) < 0.5
    assert not psutil.pid_exists(pid)
    print(
        {"priority_acquire_seconds": float(elapsed), "background_resources": result["resources"]},
        flush=True,
    )
    # The marker inode remains, but no shared lock means nobody is waiting.
    assert runtime.interactive_waiting() is False


@pytest.mark.parametrize(
    "phase",
    ["hydrate", "receipt_recovery", "tokenize", "infer", "receipt_verification", "acknowledge"],
)
def test_refused_stage_preserves_position_and_never_launches_later_work(phase):
    from cognitive_castle.semantic_indexer import index_one
    from cognitive_castle.lance_vector_store import prepare_publication

    args, source = _publication()
    receipt = prepare_publication(**args)[1]["receipt"]
    group = {
        "batch_key": args["batch_key"],
        "references": args["references"],
        "profile": PROFILE_ID,
        "input_binding": args["page"]["input_binding"],
        "position": 0,
        "total_units": None,
    }
    calls = []

    class Session:
        resources = []

        def response(self, name, value):
            calls.append(name)
            return (
                {"status": "unavailable", "reason": "fixture_refusal"} if name == phase else value
            )

        def store(self, directory, operation, arguments):
            if operation == "queue_intents":
                return {"status": "ok", "intents": []}
            if operation == "queue_select":
                return {"status": "ok", "group": group}
            if operation == "vector_receipt":
                name = "receipt_verification" if "publish" in calls else "receipt_recovery"
                # A spent deadline makes the verification return the same
                # refusal; it cannot launch a new worker with a fresh budget.
                return self.response(
                    name, receipt if name == "receipt_verification" else {"status": "not_found"}
                )
            if operation == "vector_publish":
                return self.response("publish", receipt)
            assert operation == "queue_acknowledge"
            return self.response("acknowledge", {"status": "ok", "position": 1})

        def exact(self, *args):
            return self.response("hydrate", {"status": "ok", "sources": [source]})

        def tokenize(self, *unused):
            return self.response("tokenize", args["page"])

        def infer(self, *unused):
            return self.response("infer", args["inference"])

    result = index_one(Session(), Path("/fixture"))
    assert result["reason"] == "fixture_refusal" and result["position"] == 0
    assert calls[-1] == phase


@pytest.mark.parametrize("change", ["source", "scope", "profile", "binding", "total"])
def test_stale_bindings_never_acknowledge(change):
    import copy
    from cognitive_castle.semantic_indexer import index_one
    from cognitive_castle.lance_vector_store import prepare_publication

    args, source = _publication()
    receipt = prepare_publication(**args)[1]["receipt"]
    group = {
        "batch_key": args["batch_key"],
        "references": copy.deepcopy(args["references"]),
        "profile": PROFILE_ID,
        "input_binding": args["page"]["input_binding"],
        "position": 0,
        "total_units": None,
    }
    if change == "source":
        group["references"][0]["source_hash"] = "0" * 64
    elif change == "scope":
        group["references"][0]["scope"]["room"] = "other"
    elif change == "profile":
        group["profile"] = "0" * 64
    elif change == "binding":
        group["input_binding"] = "0" * 64
    else:
        group["total_units"] = 99

    class Session:
        resources = []

        def store(self, directory, operation, arguments):
            if operation == "queue_intents":
                return {"status": "ok", "intents": []}
            if operation == "queue_select":
                return {"status": "ok", "group": group}
            assert operation == "vector_receipt", "stale group must never acknowledge"
            return receipt

        def exact(self, *args):
            return {"status": "ok", "sources": [source]}

    assert index_one(Session(), Path("/fixture"))["status"] == "stale"


def test_query_helper_hydrates_under_same_session():
    from cognitive_castle.semantic_indexer import query_exact

    args, source = _publication()

    class Session:
        remaining = 5

        def tokenize(self, sources):
            return args["page"]

        def infer(self, page):
            return args["inference"]

        def store(self, directory, operation, arguments):
            assert operation == "vector_nearest"
            return {
                "status": "ok",
                "generation": 1,
                "candidates": [
                    {
                        "canonical_id": source["id"],
                        "source_hash": source["provenance"]["text_hash"],
                        "exposure_hash": args["references"][0]["exposure_hash"],
                    }
                ],
            }

        def exact(self, directory, operation, arguments):
            assert arguments == {"ids": [source["id"]]}
            return {"status": "ok", "sources": [source]}

    result = query_exact(Session(), Path("/fixture"), "paraphrase", SCOPE)
    assert result["sources"] == [source]


def test_pipeline_host_imports_no_native_or_sqlite_runtime():
    import subprocess

    code = """
import sys
sys.path.insert(0,sys.argv[1])
import cognitive_castle.semantic_indexer
blocked={'lance','pyarrow','numpy','onnxruntime','tokenizers','torch','sqlite3','_sqlite3'} & sys.modules.keys()
assert not blocked, blocked
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(Path(__file__).resolve().parents[1])],
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_intent_without_exact_receipt_never_advances(tmp_path):
    from cognitive_castle.semantic_indexer import recover_intents
    from cognitive_castle.semantic_runtime import SemanticSession

    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    first = intent()
    assert invoke(directory, "queue_intent_prepare", {"intent": first})["status"] == "ok"
    with SemanticSession(tmp_path / "unused", interactive=False) as session:
        result = recover_intents(session, directory)
    assert result["recovered"] == 0 and result["pending"][0]["status"] == "not_initialized"
    assert invoke(directory, "queue_intents")["intents"] == [first]
    assert invoke(directory, "queue_status")["counts"]["pending"] == 0


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_exact_receipt_lookup_reuses_full_replay_proof(tmp_path):
    from test_lance_exact_store import invoke as exact_invoke

    directory = tmp_path / "exact"
    args = {"request_id": "request", "text": "exact"}
    receipt = exact_invoke(directory, "remember", args)
    proof = exact_invoke(directory, "receipt", {"namespace": "operator", "key": "request"})
    assert proof == {"status": "ok", "digest": prepare_remember(**args).digest, "receipt": receipt}
    assert exact_invoke(directory, fixture="bad_digest")["status"] == "ok"
    assert exact_invoke(directory, "receipt", {"namespace": "operator", "key": "request"}) == {
        "status": "corrupt"
    }


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_actual_pinned_multilingual_pipeline_restart_fairness_and_long_final_page(tmp_path):
    import json
    import time
    from cognitive_castle.semantic_indexer import ExactPublication, index_one, query_exact
    from cognitive_castle.semantic_runtime import SemanticSession
    from test_semantic_worker import DOCUMENTS, QUERIES

    location = os.environ.get("CASTLE_TEST_SEMANTIC_PROFILE")
    if not location:
        pytest.skip("operator-prepared pinned offline profile not selected")
    profile = Path(location)
    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    evidence = []

    def remember(key, text, lane="new"):
        chunks = prepare_remember(key, text).records
        with ExactPublication(directory) as publisher:
            result = publisher.remember(key, text, scopes=[SCOPE] * len(chunks), lane=lane)
            evidence.append(
                {
                    "operation": "remember",
                    "key": key,
                    "result": result,
                    "resources": publisher.resources,
                }
            )
        print(json.dumps(evidence[-1]), flush=True)
        assert result["status"] == "ok", result
        return result["receipt"]

    def tick():
        for attempt in range(3):
            started = time.monotonic()
            with SemanticSession(profile, interactive=False) as session:
                result = index_one(session, directory)
            evidence.append(
                {
                    "operation": "index",
                    "attempt": attempt,
                    "result": result,
                    "elapsed": time.monotonic() - started,
                }
            )
            print(json.dumps(evidence[-1]), flush=True)
            if result["status"] in {"ok", "idle"}:
                return result
            # A boundary tick may commit vectors and spend its absolute budget
            # before ACK. The next fixed-budget tick verifies durable receipts.
            assert result.get("reason") in {"worker_deadline", "semantic_deadline"}, result
        pytest.fail("index made no durable progress in three fixed-budget ticks")

    receipts = [remember(str(index), text) for index, text in enumerate(DOCUMENTS)]
    for _ in DOCUMENTS:
        tick()
    # An incomplete old group cannot keep newly registered facts from progressing.
    long = "  Česky\x00 Německy 尾 🇨🇿 " * 600
    old = remember("long", long, "history")
    first = tick()
    assert first["state"] == "pending" and first["position"] == 32
    new = remember("new", "The deployment access code is cobalt pear.")
    advanced = tick()
    assert advanced["batch_key"] != first["batch_key"] and advanced["state"] == "complete"
    assert new["ids"]
    while True:
        advanced = tick()
        if advanced["status"] == "idle":
            # Queue ACK can itself commit before its response is lost. A fresh
            # status worker proves the completed durable position in that case.
            status = invoke(directory, "queue_status")
            completed = next(
                group for group in status["groups"] if group["batch_key"] == first["batch_key"]
            )
            assert completed["state"] == "complete"
            assert completed["position"] == completed["total_units"] > 32
            evidence.append({"operation": "lost_ack_recovery", "group": completed})
            break
        if advanced["state"] == "complete":
            assert advanced["position"] == advanced["total_units"] > 32
            break
    # Every stage uses a fresh worker; each query starts a new absolute lease.
    for query, expected in zip(QUERIES, [0, 1, 3]):
        started = time.monotonic()
        with SemanticSession(profile) as session:
            result = query_exact(session, directory, query, SCOPE)
            resources = session.resources
        evidence.append(
            {
                "operation": "query",
                "query": query,
                "status": result.get("status"),
                "reason": result.get("reason"),
                "resources": resources,
                "elapsed": time.monotonic() - started,
            }
        )
        print(json.dumps(evidence[-1]), flush=True)
        assert result["status"] == "ok", result
        assert result["candidates"][0]["canonical_id"] == receipts[expected]["ids"][0]
        assert (
            result["sources"][0]["provenance"]["text_hash"]
            == prepare_remember(str(expected), DOCUMENTS[expected])
            .records[0]
            .source()["provenance"]["text_hash"]
        )
    assert old["ids"]
    assert (
        not {
            "lance",
            "pyarrow",
            "numpy",
            "onnxruntime",
            "tokenizers",
            "torch",
            "sqlite3",
            "_sqlite3",
        }
        & sys.modules.keys()
    )
    print("PIPELINE_EVIDENCE " + json.dumps(evidence), flush=True)


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_committed_vector_lost_response_recovers_in_fresh_process_without_model(tmp_path):
    from cognitive_castle.semantic_indexer import index_one
    from cognitive_castle.semantic_runtime import SemanticSession
    from test_lance_vector_store import fixture

    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    args, source = _publication()
    assert fixture(directory, "seed", sources=[source])["status"] == "ok"
    assert (
        invoke(
            directory,
            "queue_register",
            {key: value for key, value in args.items() if key in {"batch_key", "references"}}
            | {
                "profile": PROFILE_ID,
                "input_binding": args["page"]["input_binding"],
                "lane": "new",
            },
        )["status"]
        == "ok"
    )
    assert fixture(directory, "lost_response", arguments=args)["status"] == "unavailable"
    with SemanticSession(tmp_path / "unused", interactive=False) as session:
        result = index_one(session, directory)
    assert result["status"] == "ok" and result["state"] == "complete" and result["recovered_vector"]
    with SemanticSession(tmp_path / "unused", interactive=False) as session:
        assert index_one(session, directory)["status"] == "idle"


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_uncommitted_old_intents_do_not_starve_later_recovery(tmp_path):
    from cognitive_castle.semantic_intents import prepare_intent

    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    intents = [
        prepare_intent(prepare_remember(str(index), "body"), [SCOPE], "new") for index in range(3)
    ]
    for pending in intents:
        assert invoke(directory, "queue_intent_prepare", {"intent": pending})["status"] == "ok"
    assert invoke(directory, "queue_intents")["intents"] == intents[:2]
    assert invoke(directory, "queue_intents")["intents"] == [intents[2], intents[0]]


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_exact_receipt_survives_late_coordination_refusal_with_three_second_total(
    monkeypatch, tmp_path
):
    from cognitive_castle import semantic_runtime
    from cognitive_castle.semantic_indexer import ExactPublication

    clock = [0.0]
    monkeypatch.setattr(semantic_runtime.time, "monotonic", lambda: clock[0])
    prepared = prepare_remember("request", "exact body")
    calls = []

    def supervise(request, fd, **kwargs):
        operation = request["operation"]
        calls.append((operation, kwargs))
        if operation == "remember":
            return prepared.receipt
        if operation == "receipt":
            return {"status": "ok", "digest": prepared.digest, "receipt": prepared.receipt}
        if operation == "read_sources":
            return {"status": "ok", "sources": [record.source() for record in prepared.records]}
        if operation == "queue_intent_complete":
            clock[0] = 3.0
            return {"status": "unavailable", "reason": "worker_deadline"}
        return {"status": "ok"}

    monkeypatch.setattr(semantic_runtime.memory_runtime, "_supervise", supervise)
    with ExactPublication(tmp_path / "generation") as publisher:
        result = publisher.remember("request", "exact body", scopes=[SCOPE])
    assert result["status"] == "ok" and result["exact_status"] == "committed"
    assert result["receipt"] == prepared.receipt and result["intent_pending"]
    assert result["reason"] == "worker_deadline"
    assert calls[0][0] == "queue_intent_prepare"
    assert all(options["absolute_deadline"] == 3 for _, options in calls)
    assert all(options["rss_limit"] <= 192 * 1024**2 for _, options in calls)


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_session_releases_admission_after_preemption_and_cannot_launch_more_work(
    monkeypatch, tmp_path
):
    from cognitive_castle import semantic_runtime

    calls = []

    def supervise(*args, **kwargs):
        calls.append(kwargs)
        return {"status": "unavailable", "reason": "background_preempted"}

    monkeypatch.setattr(semantic_runtime.memory_runtime, "_supervise", supervise)
    with semantic_runtime.SemanticSession(tmp_path / "profile", interactive=False) as session:
        assert session.store(tmp_path, "queue_status", {})["reason"] == "background_preempted"
        fd = semantic_runtime.memory_runtime._admission()
        os.close(fd)
        assert session.store(tmp_path, "queue_status", {})["reason"] == "background_preempted"
    assert len(calls) == 1 and calls[0]["background_preempt"]


def test_query_validation_cannot_finish_after_shared_deadline(monkeypatch):
    from cognitive_castle import semantic_indexer

    args, source = _publication()

    class Session:
        remaining = 1

        def tokenize(self, sources):
            return args["page"]

        def infer(self, page):
            return args["inference"]

        def store(self, *unused):
            return {
                "status": "ok",
                "candidates": [
                    {
                        "canonical_id": source["id"],
                        "source_hash": source["provenance"]["text_hash"],
                        "exposure_hash": args["references"][0]["exposure_hash"],
                    }
                ],
            }

        def exact(self, *unused):
            return {"status": "ok", "sources": [source]}

    session = Session()
    original = semantic_indexer.source_exposure_hash

    def hash_and_expire(value):
        session.remaining = 0
        return original(value)

    monkeypatch.setattr(semantic_indexer, "source_exposure_hash", hash_and_expire)
    assert semantic_indexer.query_exact(session, Path("/fixture"), "query", SCOPE) == {
        "status": "unavailable",
        "reason": "semantic_deadline",
    }


@pytest.mark.skipif(sys.platform != "darwin", reason="owned guardian supervision")
def test_intent_registration_and_completion_commit_survive_lost_ack(tmp_path, monkeypatch):
    from cognitive_castle import memory_runtime
    from cognitive_castle.semantic_indexer import ExactPublication, recover_intents
    from cognitive_castle.semantic_runtime import SemanticSession

    directory = tmp_path / "generation"
    directory.mkdir(mode=0o700)
    original = memory_runtime._supervise

    def lose_completion(request, fd, **kwargs):
        if request["operation"] == "queue_intent_complete":
            kwargs["worker_command"] = [
                sys.executable,
                "-I",
                str(Path(__file__).with_name("semantic_intent_fixture.py")),
            ]
        return original(request, fd, **kwargs)

    monkeypatch.setattr(memory_runtime, "_supervise", lose_completion)
    with ExactPublication(directory) as publisher:
        result = publisher.remember("request", "exact", scopes=[SCOPE])
    assert result["exact_status"] == "committed" and result["intent_pending"]
    assert result["reason"] == "worker_invalid_frame"
    monkeypatch.setattr(memory_runtime, "_supervise", original)
    with SemanticSession(tmp_path / "unused", interactive=False) as session:
        assert recover_intents(session, directory) == {
            "status": "ok",
            "recovered": 0,
            "pending": [],
        }
    assert invoke(directory, "queue_status")["counts"]["pending"] == 1
    with ExactPublication(directory) as publisher:
        replay = publisher.remember("request", "exact", scopes=[SCOPE])
    assert replay["receipt"] == result["receipt"] and not replay["intent_pending"]
    assert invoke(directory, "queue_status")["counts"]["pending"] == 1


def test_corrupt_pending_intent_reports_phase_without_advancing():
    from cognitive_castle.semantic_indexer import index_one

    class Session:
        resources = []

        def store(self, directory, operation, arguments):
            assert operation == "queue_intents", "corrupt recovery must stop before queue selection"
            return {"status": "ok", "intents": [{"intent_id": "0" * 64}]}

    result = index_one(Session(), Path("/fixture"))
    assert result["status"] == "corrupt" and result["phase"] == "intent_recovery"

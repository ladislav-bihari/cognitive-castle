"""Coordinator behavior; native work remains in guarded disposable workers."""

import os
import sys

import pytest


@pytest.fixture(autouse=True)
def isolated_admission(monkeypatch, tmp_path):
    from cognitive_castle import memory_runtime

    monkeypatch.setattr(memory_runtime, "_admission_directory", lambda: tmp_path)


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_shared_deadline_expiry_and_admission_release(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime

    clock = [100.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        clock[0] = 105.0
        assert session.tokenize([]) == {"status": "unavailable", "reason": "semantic_deadline"}
    fd = runtime.memory_runtime._admission()
    os.close(fd)


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_background_yields_to_interactive_and_host_has_no_native_imports(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime

    monkeypatch.setattr(runtime.memory_runtime, "interactive_waiting", lambda: True)
    with runtime.SemanticSession(tmp_path / "profile.json", interactive=False) as session:
        assert session.tokenize([])["reason"] == "memory_busy"
    assert not {"tokenizers", "numpy", "onnxruntime", "lancedb", "torch"} & sys.modules.keys()


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_deadline_consumes_validation_and_both_stages(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.semantic_windows import make_unit, page_units

    clock = [0.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    source = prepare_remember("fixture", "x").records[0].source()
    sources = [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    unit = make_unit(
        source,
        0,
        1,
        0,
        1,
        {"input_ids": [0, 42, 2], "attention_mask": [1, 1, 1], "token_type_ids": [0, 0, 0]},
        [[0, 0], [0, 1], [0, 0]],
    )
    page = page_units(sources, [unit])
    deadlines = []

    def supervise(request, admission, **kwargs):
        deadlines.append(kwargs["deadline"])
        clock[0] += 2.0
        if request["operation"] == "tokenize":
            return page.copy()
        return {
            "status": "ok",
            "profile": page["profile"],
            "handoff_hash": page["handoff_hash"],
            "input_binding": page["input_binding"],
            "start_window": 0,
            "total_units": 1,
            "next_continuation": None,
            "complete": True,
            "vectors": [{"unit_id": unit["unit_id"], "vector": [1.0] + [0.0] * 383}],
            "runtime": {},
        }

    monkeypatch.setattr(runtime.memory_runtime, "_supervise", supervise)
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        result = session.tokenize(sources)
        assert session.infer(result)["status"] == "ok"
        clock[0] = 5.0
        assert session.infer(result)["reason"] == "semantic_deadline"
    assert deadlines == [5.0, 3.0]


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_infer_rejects_unissued_handoff_and_malformed_worker_vector(monkeypatch, tmp_path):
    import pytest
    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.semantic_windows import make_unit, page_units

    source = prepare_remember("fixture", "x").records[0].source()
    sources = [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    unit = make_unit(
        source,
        0,
        1,
        0,
        1,
        {"input_ids": [0, 42, 2], "attention_mask": [1, 1, 1], "token_type_ids": [0, 0, 0]},
        [[0, 0], [0, 1], [0, 0]],
    )
    page = page_units(sources, [unit])
    monkeypatch.setattr(
        runtime.memory_runtime,
        "_supervise",
        lambda request, fd, **kw: (
            page.copy() if request["operation"] == "tokenize" else {"status": "ok", "vectors": []}
        ),
    )
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        with pytest.raises(ValueError, match="unissued"):
            session.infer(page)
        session.tokenize(sources)
        assert session.infer(page) == {
            "status": "unavailable",
            "reason": "semantic_invalid_response",
        }


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_success_arriving_after_absolute_deadline_is_unavailable(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.semantic_windows import make_unit, page_units

    clock = [0.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    source = prepare_remember("fixture", "x").records[0].source()
    sources = [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    unit = make_unit(
        source,
        0,
        1,
        0,
        1,
        {"input_ids": [0, 42, 2], "attention_mask": [1, 1, 1], "token_type_ids": [0, 0, 0]},
        [[0, 0], [0, 1], [0, 0]],
    )
    page = page_units(sources, [unit])

    def supervise(*args, **kwargs):
        clock[0] = 5.1
        return page.copy()

    monkeypatch.setattr(runtime.memory_runtime, "_supervise", supervise)
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        assert session.tokenize(sources) == {"status": "unavailable", "reason": "semantic_deadline"}


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_tokenizer_response_must_bind_requested_source_set(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.semantic_windows import make_unit, page_units

    records = [prepare_remember(name, name).records[0].source() for name in ("first", "other")]
    envelopes = [
        [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
        for source in records
    ]
    unit = make_unit(
        records[1],
        0,
        1,
        0,
        5,
        {"input_ids": [0, 42, 2], "attention_mask": [1, 1, 1], "token_type_ids": [0, 0, 0]},
        [[0, 0], [0, 5], [0, 0]],
    )
    wrong = page_units(envelopes[1], [unit])
    monkeypatch.setattr(runtime.memory_runtime, "_supervise", lambda *args, **kwargs: wrong.copy())
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        assert session.tokenize(envelopes[0]) == {
            "status": "unavailable",
            "reason": "semantic_invalid_response",
        }


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_real_supervisor_preflight_cannot_extend_shared_session_deadline(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime
    from cognitive_castle.canonical_records import prepare_remember

    clock = [100.0]
    checks = []
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])

    def pressure(**kwargs):
        checks.append(kwargs)
        clock[0] = 105.1
        return True

    monkeypatch.setattr(runtime.memory_runtime, "_pressure_ok", pressure)
    monkeypatch.setattr(
        runtime.memory_runtime.subprocess,
        "Popen",
        lambda *a, **kw: pytest.fail("expired native stage launched"),
    )
    source = prepare_remember("fixture", "exact").records[0].source()
    sources = [{"source": source, "expected_hash": source["provenance"]["text_hash"]}]
    with runtime.SemanticSession(tmp_path / "profile.json") as session:
        clock[0] = 104.9
        result = session.tokenize(sources)
    assert result == {"status": "unavailable", "reason": "worker_deadline"}
    assert checks == [{"deadline": 105.0}]


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_migration_source_queue_exact_share_one_deadline_and_fixed_caps(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime
    from test_migration_codec import descriptor

    clock = [0.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    calls = []

    def supervise(request, fd, **limits):
        calls.append((request, limits))
        clock[0] += 0.5
        return {"status": "unavailable", "reason": "worker_memory_limit"}

    monkeypatch.setattr(runtime.memory_runtime, "_supervise", supervise)
    with runtime.SemanticSession(tmp_path / "unused", interactive=False) as session:
        result = session.sqlite_source(descriptor())
        assert result["reason"] == "worker_memory_limit"
        assert result["cursor_before"] == result["cursor_after"]
        session.store(tmp_path, "queue_migration_state", {"job_id": "a" * 64})
        session.exact(tmp_path / "exact", "import_batch", {})
        clock[0] = 4.75
        session.sqlite_source(descriptor())
        clock[0] = 5
        assert session.sqlite_source(descriptor())["reason"] == "semantic_deadline"
    assert len(calls) == 4
    assert [c[1]["deadline"] for c in calls] == [2, 2, 3, 0.25]
    assert [c[1]["absolute_deadline"] for c in calls] == [5] * 4
    assert [c[1]["rss_limit"] for c in calls] == [n * 1024**2 for n in (128, 128, 192, 128)]
    assert [c[1]["total_rss_limit"] for c in calls] == [n * 1024**2 for n in (192, 192, 256, 192)]
    assert all(c[1]["background_preempt"] for c in calls)


@pytest.mark.skipif(
    sys.platform != "darwin", reason="requires supported macOS supervisor admission"
)
def test_source_response_validation_cannot_escape_absolute_deadline(monkeypatch, tmp_path):
    from cognitive_castle import semantic_runtime as runtime, sqlite_source_runtime as source
    from cognitive_castle import migration_codec as codec
    from test_migration_codec import descriptor, exported

    clock = [0.0]
    monkeypatch.setattr(runtime.time, "monotonic", lambda: clock[0])
    reg = codec.registration(descriptor(), "/private/generation")
    body = exported(reg)
    monkeypatch.setattr(runtime.memory_runtime, "_supervise", lambda *args, **kwargs: body)
    original = source.source_result

    def validating(request, result):
        value = original(request, result)
        clock[0] = 5.0
        return value

    monkeypatch.setattr(source, "source_result", validating)
    with runtime.SemanticSession(tmp_path / "unused", interactive=False) as session:
        result = session.sqlite_source(descriptor())
        assert result["status"] == "unavailable" and result["reason"] == "semantic_deadline"
        assert result["cursor_after"] == result["cursor_before"]
        assert not result["entries"] and not result["finalizations"]

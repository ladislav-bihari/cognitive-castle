"""Synthetic registered exact-to-vector lifecycle; no models or user palace.

All native IO runs in supervised short-lived workers. Fixed vectors exercise
publication and hydration contracts, not tokenizer/model relevance quality.
"""

import sys

import test_layout_native_integration as native
from test_layout_native_tail import _tree
from test_layout_packed_native import _setup, _step, _assert_frozen

pytestmark = native.pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def _arguments(source):
    from cognitive_castle.semantic_intents import source_exposure_hash
    from cognitive_castle.semantic_windows import make_unit, page_units

    envelopes = [dict(source=source, expected_hash=source["provenance"]["text_hash"])]
    tokens = dict(input_ids=[0, 42, 2], attention_mask=[1, 1, 1], token_type_ids=[0, 0, 0])
    unit = make_unit(
        source, 0, 1, 0, len(source["text"]), tokens, [[0, 0], [0, len(source["text"])], [0, 0]]
    )
    page = page_units(envelopes, [unit])
    inference = {key: value for key, value in page.items() if key != "units"}
    inference.update(
        vectors=[dict(unit_id=unit["unit_id"], vector=[1.0] + [0.0] * 383)],
        runtime=dict(
            onnxruntime="1.25.1", providers=["CPUExecutionProvider"], tokenizer_loaded=False
        ),
    )
    reference = dict(
        canonical_id=source["id"],
        source_hash=source["provenance"]["text_hash"],
        exposure_hash=source_exposure_hash(source),
        scope=dict(workspace="/synthetic", wing="w", room="r"),
    )
    return dict(
        batch_key="registered-original", references=[reference], page=page, inference=inference
    )


def _call(generation, method, *args):
    from cognitive_castle.semantic_runtime import LayoutSemanticSession

    with LayoutSemanticSession(
        generation / "unused-profile", generation, interactive=False
    ) as session:
        observed = native.ObservedLayoutSession(session)
        return getattr(observed, method)(*args)


def _hydrate(generation, group):
    from cognitive_castle.semantic_indexer import _hydrate as hydrate
    from cognitive_castle.semantic_runtime import LayoutSemanticSession

    with LayoutSemanticSession(
        generation / "unused-profile", generation, interactive=False
    ) as session:
        return hydrate(native.ObservedLayoutSession(session), generation, group)


def test_registered_originals_gate_vector_publication_receipt_and_exact_hydration(tmp_path):
    start = native._CALLS
    source, generation, reg, _ = _setup(tmp_path)
    frozen_source = _tree(tmp_path / "source", hashes=True)
    artifact = generation / "exact/layout-qualification.json"
    frozen_artifact = artifact.read_bytes()
    fresh, first = _step(reg)
    assert fresh["status"] == "in_progress" and fresh["phase"] == "verify_replay", fresh
    manifests = first.arguments["layout_import_batch:1"][2]["finalizations"]
    identity = manifests[0]["live"]["receipt"]["ids"][0]
    original = next(s for s in source["original_public"] if s["id"] == identity)
    arguments = _arguments(original)
    denied = _call(generation, "store", generation, "vector_publish", arguments)
    assert denied["status"] in {"stale", "unavailable"}, denied
    assert not (generation / "vectors").exists()
    published, _ = _step(reg)
    assert published["status"] == "ok", published
    group = dict(
        references=arguments["references"],
        profile=arguments["page"]["profile"],
        input_binding=arguments["page"]["input_binding"],
    )
    expected = dict(
        status="ok",
        sources=[dict(source=original, expected_hash=original["provenance"]["text_hash"])],
    )
    assert _hydrate(generation, group) == expected
    live = manifests[0]["live"]
    receipt = _call(
        generation,
        "exact",
        generation / "exact",
        "receipt",
        dict(namespace=live["namespace"], key=live["key"]),
    )
    assert receipt == dict(status="ok", digest=live["digest"], receipt=live["receipt"])
    vector = _call(generation, "store", generation, "vector_publish", arguments)
    assert vector["status"] == "ok", vector
    before_reads = _tree(generation, hashes=True)
    recovered = _call(
        generation,
        "store",
        generation,
        "vector_receipt",
        dict(batch_key=arguments["batch_key"], start_window=0),
    )
    assert recovered == vector
    reference = arguments["references"][0]
    nearest = _call(
        generation,
        "store",
        generation,
        "vector_nearest",
        dict(
            vector=[1.0] + [0.0] * 383,
            profile=group["profile"],
            generation=None,
            scope=reference["scope"],
            allowed_source_hashes=None,
            allowed_exposure_hashes=[reference["exposure_hash"]],
            limit=1,
        ),
    )
    assert nearest["status"] == "ok", nearest
    assert len(nearest["candidates"]) == 1
    candidate = nearest["candidates"][0]
    assert candidate["canonical_id"] == identity
    assert candidate["source_hash"] == reference["source_hash"]
    assert candidate["exposure_hash"] == reference["exposure_hash"]
    assert _hydrate(generation, group) == expected
    assert _call(generation, "store", generation, "vector_publish", arguments) == vector
    assert _tree(generation, hashes=True) == before_reads
    assert not (generation / "exact/records.lance").exists()
    _assert_frozen(tmp_path, frozen_source, artifact, frozen_artifact)
    assert native._CALLS - start == 20

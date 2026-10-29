"""Real Lance prefilter for same-text records with distinct exposure grants.

Vectors are fixed synthetic inputs, not model inference or relevance evidence.
The pytest host does not import native engines or open a user palace.
"""

from pathlib import Path
import sys

import test_layout_native_integration as native
from test_layout_native_tail import _tree
from test_vector_exposure_grants import publication

pytestmark = native.pytest.mark.skipif(sys.platform != "darwin", reason="owned supervision")


def test_actual_vector_exposure_prefilter_keeps_distinct_origins_and_query_is_readonly(tmp_path):
    start = native._CALLS
    first, first_args, first_source = publication("first")
    second, second_args, second_source = publication("second")
    assert first["source_hash"] == second["source_hash"]
    assert first["exposure_hash"] != second["exposure_hash"]
    second_args["inference"]["vectors"][0]["vector"] = [0.8, 0.6] + [0.0] * 382
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    with native._session(generation) as session:
        call = native._start("vector_seed")
        result = session._run_stage(
            dict(directory=str(generation), fixture="seed", sources=[first_source, second_source]),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("test_lance_vector_store.py")),
            ],
            cap=3,
            rss=192 * 1024**2,
            total=256 * 1024**2,
        )
        native._record(session, "vector_seed", result, 0, call)
    assert result["status"] == "ok", result
    for arguments in (first_args, second_args):
        with native._session(generation) as session:
            observed = native.ObservedLayoutSession(session)
            result = observed.store(generation, "vector_publish", arguments)
        assert result["status"] == "ok", result
    before = _tree(generation, hashes=True)
    query = dict(
        vector=[1.0] + [0.0] * 383,
        profile=first["profile"],
        generation=2,
        scope=first_args["references"][0]["scope"],
        allowed_source_hashes=None,
        limit=1,
    )
    for text_grants, exposure_grants, expected in (
        (None, [second["exposure_hash"]], second["canonical_id"]),
        (None, [], None),
        (None, [first["source_hash"]], None),
        (None, None, first["canonical_id"]),
        ([first["source_hash"]], [second["exposure_hash"]], second["canonical_id"]),
    ):
        with native._session(generation) as session:
            observed = native.ObservedLayoutSession(session)
            result = observed.store(
                generation,
                "vector_nearest",
                dict(
                    query,
                    allowed_source_hashes=text_grants,
                    allowed_exposure_hashes=exposure_grants,
                ),
            )
        assert result["status"] == "ok", result
        assert [c["canonical_id"] for c in result["candidates"]] == (
            [] if expected is None else [expected]
        )
        if expected == second["canonical_id"]:
            assert result["candidates"][0]["exposure_hash"] == second["exposure_hash"]
            assert result["candidates"][0]["distance"] > 0
        assert _tree(generation, hashes=True) == before
    assert native._CALLS - start == 8
    assert not {
        "lance",
        "pyarrow",
        "numpy",
        "torch",
        "onnxruntime",
        "tokenizers",
        "sqlite3",
    }.intersection(sys.modules)

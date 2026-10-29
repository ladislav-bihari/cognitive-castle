"""Real vector-row validation and filtering contract; no native engine loaded."""

import sys
from types import SimpleNamespace as NS

import pytest


def publication(key):
    from cognitive_castle.canonical_records import prepare_remember
    from cognitive_castle.semantic_intents import source_exposure_hash
    from cognitive_castle.semantic_profile import PROFILE_ID
    from cognitive_castle.semantic_windows import make_unit, page_units
    from cognitive_castle.lance_vector_store import prepare_publication

    scope = dict(workspace="/trusted", wing="w", room="r")
    source = prepare_remember(key, "same exact text", dict(origin=key)).records[0].source()
    envelopes = [dict(source=source, expected_hash=source["provenance"]["text_hash"])]
    tokens = dict(input_ids=[0, 42, 2], attention_mask=[1, 1, 1], token_type_ids=[0, 0, 0])
    unit = make_unit(
        source, 0, 1, 0, len(source["text"]), tokens, [[0, 0], [0, len(source["text"])], [0, 0]]
    )
    page = page_units(envelopes, [unit])
    inference = {k: v for k, v in page.items() if k != "units"}
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
        scope=scope,
    )
    args = dict(batch_key=key, references=[reference], page=page, inference=inference)
    rows, _ = prepare_publication(**args)
    assert rows[0]["profile"] == PROFILE_ID
    return rows[0], args, source


class Predicate:
    def __init__(self, evaluate):
        self.evaluate = evaluate

    def __and__(self, other):
        return Predicate(lambda row: self.evaluate(row) and other.evaluate(row))


class Field:
    def __init__(self, name):
        self.name = name

    def __eq__(self, value):
        return Predicate(lambda row: row[self.name] == value)

    def isin(self, values):
        return Predicate(lambda row: row[self.name] in values)


def store_case(monkeypatch):
    from cognitive_castle.lance_vector_store import LanceVectorStore

    first, _, _ = publication("first")
    second, args, _ = publication("second")
    assert first["source_hash"] == second["source_hash"]
    assert first["exposure_hash"] != second["exposure_hash"]
    rows = [dict(first, _distance=0.0), dict(second, _distance=1.0)]
    store = LanceVectorStore("/trusted/generation")
    store._paths = lambda: None
    store._open = lambda: NS(version=3)
    calls = []

    def scanner(dataset, **kwargs):
        assert dataset.version == 3 and kwargs["prefilter"] is True
        calls.append(kwargs)
        eligible = [r for r in rows if kwargs["filter"].evaluate(r)]
        result = sorted(eligible, key=lambda row: row["_distance"])[: kwargs["limit"]]
        return NS(to_table=lambda: NS(to_pylist=lambda: result))

    store._scanner = scanner
    compute = NS(field=Field)
    monkeypatch.setitem(sys.modules, "pyarrow", NS(compute=compute))
    monkeypatch.setitem(sys.modules, "pyarrow.compute", compute)
    query = dict(
        vector=[1.0] + [0.0] * 383,
        profile=first["profile"],
        generation=3,
        scope=args["references"][0]["scope"],
        allowed_source_hashes=None,
        limit=1,
    )
    return store, query, first, second, calls


def test_exposure_grant_filters_same_text_different_origins_before_nearest_limit(monkeypatch):
    store, query, _, authorized, calls = store_case(monkeypatch)
    result = store.nearest(**query, allowed_exposure_hashes=[authorized["exposure_hash"]])
    assert result["status"] == "ok", result
    assert [c["canonical_id"] for c in result["candidates"]] == [authorized["canonical_id"]]
    assert len(calls) == 1


@pytest.mark.parametrize("grant", ["empty", "text_hash", "all"])
def test_exposure_grants_keep_empty_text_hash_and_unrestricted_domains_distinct(monkeypatch, grant):
    store, query, first, _, _ = store_case(monkeypatch)
    grants = {"empty": [], "text_hash": [first["source_hash"]], "all": None}[grant]
    result = store.nearest(**query, allowed_exposure_hashes=grants)
    assert result["status"] == "ok", result
    assert [c["canonical_id"] for c in result["candidates"]] == (
        [first["canonical_id"]] if grant == "all" else []
    )


@pytest.mark.parametrize("grants", ["f" * 64, ["not-a-hash"], ["f" * 64] * 257, [None]])
def test_bad_exposure_grants_refuse_before_storage_or_ranking(monkeypatch, grants):
    store, query, _, _, calls = store_case(monkeypatch)
    store._paths = lambda: pytest.fail("invalid grants reached storage")
    with pytest.raises(ValueError):
        store.nearest(**query, allowed_exposure_hashes=grants)
    assert calls == []


def test_query_boundary_routes_snapshot_grants_to_exposure_not_text_filter():
    from cognitive_castle.semantic_indexer import query_exact

    row, args, _ = publication("authorized")
    grants = [row["exposure_hash"]]

    class Session:
        remaining = 5

        def tokenize(self, sources):
            return args["page"]

        def infer(self, page):
            return args["inference"]

        def store(self, directory, operation, arguments):
            assert operation == "vector_nearest"
            assert arguments["allowed_source_hashes"] is None
            assert arguments["allowed_exposure_hashes"] == grants
            return dict(status="ok", candidates=[])

    result = query_exact(
        Session(),
        "/trusted/generation",
        "question",
        args["references"][0]["scope"],
        allowed_source_hashes=grants,
    )
    assert result == dict(status="ok", candidates=[], sources=[])


def test_query_hydration_rechecks_grant_against_the_complete_original_source():
    from cognitive_castle.semantic_indexer import query_exact

    row, args, source = publication("returned")
    authorized, _, _ = publication("authorized")

    class Session:
        remaining = 5

        def tokenize(self, sources):
            return args["page"]

        def infer(self, page):
            return args["inference"]

        def store(self, *unused):
            return dict(
                status="ok",
                candidates=[{k: row[k] for k in ("canonical_id", "source_hash", "exposure_hash")}],
            )

        def exact(self, *unused):
            return dict(status="ok", sources=[source])

    result = query_exact(
        Session(),
        "/trusted/generation",
        "question",
        args["references"][0]["scope"],
        allowed_source_hashes=[authorized["exposure_hash"]],
    )
    assert result == dict(status="stale")


def test_worker_preserves_exposure_grant_through_its_closed_dispatch(monkeypatch):
    from cognitive_castle import lance_vector_store, semantic_store_worker as worker

    store, query, _, authorized, _ = store_case(monkeypatch)
    monkeypatch.setattr(lance_vector_store, "LanceVectorStore", lambda *args, **kwargs: store)
    result = worker._execute(
        dict(
            directory="/trusted/generation",
            operation="vector_nearest",
            _host_pid=1,
            arguments=dict(query, allowed_exposure_hashes=[authorized["exposure_hash"]]),
        )
    )
    assert [c["canonical_id"] for c in result["candidates"]] == [authorized["canonical_id"]]


def test_worker_refuses_unknown_exposure_field_before_constructing_storage(monkeypatch):
    from cognitive_castle import lance_vector_store, semantic_store_worker as worker

    store, query, _, _, _ = store_case(monkeypatch)
    monkeypatch.setattr(
        lance_vector_store,
        "LanceVectorStore",
        lambda *args, **kwargs: pytest.fail("unknown request field reached storage"),
    )
    with pytest.raises(ValueError, match="arguments"):
        worker._execute(
            dict(
                directory="/trusted/generation",
                operation="vector_nearest",
                _host_pid=1,
                arguments=dict(query, exposure_grants=[]),
            )
        )

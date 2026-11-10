"""Explicit unscoped records retain words and cannot match concrete projects."""

import copy
import json
import pytest
from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.live_publication_codec import prepare_publication
from cognitive_castle.semantic_queue import validate_scope
from test_lance_vector_store import publication

SCOPE = dict(workspace=None, wing="projects", room="conversations")


def test_unscoped_direct_publication_keeps_null_scope_and_original_words():
    canonical = prepare_remember("unscoped", "  Ω\x00\r\noriginal  ", {"source": "fixture"})
    direct = prepare_publication(canonical, [SCOPE])
    assert direct.canonical.records[0].text == "  Ω\x00\r\noriginal  "
    assert direct.canonical == canonical
    assert direct.intent["semantic"]["groups"][0]["references"][0]["scope"] == SCOPE
    scoped = prepare_publication(canonical, [{**SCOPE, "workspace": "/project"}])
    assert direct.intent["source_binding"] != scoped.intent["source_binding"]
    assert direct.canonical.receipt == scoped.canonical.receipt
    # Bulk owner scopes are actual literal JSON pages, retaining null, not a label.
    pages = [
        entry["text"]
        for entry in direct.entries
        if entry["kind"] == "bulk_page" and json.loads(entry["receipt"])["field"] == "scopes"
    ]
    assert json.loads("".join(pages))["members"][0]["scope"]["workspace"] is None


@pytest.mark.parametrize(
    "bad",
    [
        dict(workspace="", wing="projects", room="conversations"),
        dict(workspace=None, wing=None, room="conversations"),
        dict(workspace=None, wing="projects", room=None),
    ],
)
def test_unscoped_does_not_accept_empty_project_or_missing_taxonomy(bad):
    with pytest.raises(ValueError):
        validate_scope(bad)


def test_semantic_unit_retains_null_authority_and_checks_reserved_projection():
    from cognitive_castle.lance_vector_store import prepare_publication, LanceVectorStore
    from cognitive_castle.lance_exact_store import _StoreError

    args, _ = publication(scope=SCOPE)
    rows, _ = prepare_publication(**args)
    row = rows[0]
    assert row["workspace"] == ""  # reserved internal projection; never a public label
    assert json.loads(row["compact"])["scope"] == SCOPE
    assert LanceVectorStore._unit(row)["scope"] == SCOPE
    forged = copy.deepcopy(row)
    forged["workspace"] = "/project"
    with pytest.raises(_StoreError, match="corrupt"):
        LanceVectorStore._unit(forged)


def test_actual_arrow_prefilter_keeps_unscoped_separate_from_every_project():
    import pyarrow as pa
    import pyarrow.dataset as ds
    from cognitive_castle.lance_vector_store import scope_filter

    table = pa.Table.from_pylist(
        [
            dict(id="unscoped", workspace="", wing="projects", room="conversations"),
            dict(id="project", workspace="/project", wing="projects", room="conversations"),
            dict(id="other", workspace="/other", wing="projects", room="conversations"),
            dict(id="wrong-room", workspace="", wing="projects", room="other"),
        ]
    )
    dataset = ds.dataset(table)

    def selected(scope):
        return dataset.to_table(filter=scope_filter(scope)).column("id").to_pylist()

    assert selected(SCOPE) == ["unscoped"]
    assert selected({**SCOPE, "workspace": "/project"}) == ["project"]
    assert selected({**SCOPE, "workspace": "/other"}) == ["other"]

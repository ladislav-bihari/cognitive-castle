"""Direct canonical requests retain exact identity without archive descriptors."""

import copy
import os
from pathlib import Path
import subprocess
import sys

import pytest

from cognitive_castle.canonical_records import (
    CanonicalRecord,
    PreparedRemember,
    _json,
    prepare_remember,
)
from cognitive_castle import bulk_exact_codec as bulk

SCOPE = {"workspace": "/trusted/project", "wing": "project", "room": "conversation"}


def codec():
    from cognitive_castle import live_publication_codec

    return live_publication_codec


def prepared(text="BODY_SENTINEL Ω\x00\r\n  ", **changes):
    return prepare_remember(
        changes.get("key", "request"),
        text,
        changes.get("caller", {"note": "CALLER_PRIVATE \x00 Ω"}),
        namespace=changes.get("namespace", "operator"),
    )


def scopes(value):
    return [dict(SCOPE) for _ in value.records]


def test_direct_publication_preserves_original_records_and_expected_receipt():
    source = prepared("Ω\r\n \x00😀" * 2000)
    result = codec().prepare_publication(source, scopes(source))
    assert result.canonical == source
    records = [
        CanonicalRecord(row["id"], row["text"], row["provenance"])
        for row in result.entries
        if row["kind"] == "bulk_record"
    ]
    assert records == list(source.records)
    assert result.manifest["live"] == dict(
        namespace=source.namespace,
        key=source.request_id,
        digest=source.digest,
        receipt=source.receipt,
    )
    assert (
        result.intent["manifest_hash"]
        == __import__("hashlib").sha256(_json(result.manifest).encode()).hexdigest()
    )
    assert not hasattr(result, "status")  # Preparation is not a durable success.


def test_intent_has_no_text_or_caller_payload_and_has_no_archive_descriptor():
    source = prepared()
    result = codec().prepare_publication(source, scopes(source))
    raw = _json(result.intent)
    assert "BODY_SENTINEL" not in raw and "CALLER_PRIVATE" not in raw
    assert set(result.intent) == {"format", "source_binding", "owner", "manifest_hash", "semantic"}
    assert result.intent["semantic"]["receipt"] == source.receipt
    assert (
        result.intent["semantic"]["groups"][0]["references"][0]["canonical_id"]
        == source.records[0].id
    )
    assert "path" not in result.intent and "snapshot_id" not in result.intent
    assert codec().validate_intent(result.intent) == result.intent


@pytest.mark.parametrize(
    "field,value",
    [
        ("format", "other"),
        ("source_binding", "0" * 64),
        ("owner", "receipt:" + "0" * 64),
        ("manifest_hash", "invalid"),
        ("text", "forbidden body"),
        ("semantic", {}),
    ],
)
def test_intent_rejects_changed_signature_and_bindings(field, value):
    source = prepared()
    intent = copy.deepcopy(codec().prepare_publication(source, scopes(source)).intent)
    intent[field] = value
    with pytest.raises(ValueError):
        codec().validate_intent(intent)


@pytest.mark.parametrize("change", ["text", "caller", "namespace", "key", "scope", "manifest"])
def test_identical_regeneration_required_before_any_replay(change):
    source = prepared()
    scope = scopes(source)
    publication = codec().prepare_publication(source, scope)
    assert codec().regenerate(publication.intent, source, scope) == publication
    intent = copy.deepcopy(publication.intent)
    if change == "text":
        source = prepared("changed text")
    elif change == "caller":
        source = prepared(caller={"note": "changed caller"})
    elif change == "namespace":
        source = prepared(namespace="other")
    elif change == "key":
        source = prepared(key="other")
    elif change == "scope":
        scope[0]["room"] = "other"
    else:
        intent["manifest_hash"] = "0" * 64
    with pytest.raises(ValueError):
        codec().regenerate(intent, source, scope)


def test_forged_prepared_digest_and_missing_scopes_refuse():
    source = prepared()
    forged = PreparedRemember(source.namespace, source.request_id, "0" * 64, source.records)
    with pytest.raises(ValueError):
        codec().prepare_publication(forged, scopes(source))
    with pytest.raises(ValueError):
        codec().prepare_publication(source, [])


def test_scope_input_is_snapshotted_before_caller_mutation():
    source = prepared()
    scope = scopes(source)
    publication = codec().prepare_publication(source, scope)
    before = _json(publication.intent)
    scope[0]["room"] = "later mutation"
    assert _json(publication.intent) == before


def test_maximum_escaped_input_yields_bounded_stable_batches_and_finalizes_last():
    text = ("\x00" * 8191 + "€") * 7 + "\x00" * 8178
    assert len(text.encode()) == 65536
    source = prepared(text)
    assert len(source.records) == 9
    scope = [
        {"workspace": "\x00" * 2048, "wing": "\x00" * 512, "room": "\x00" * 512}
        for _ in source.records
    ]
    publication = codec().prepare_publication(source, scope)
    batches = list(codec().batches(publication))
    assert len(batches) >= 3
    assert batches == list(codec().batches(publication))
    assert [row for batch in batches for row in batch["entries"]] == list(publication.entries)
    assert all(not batch["finalizations"] for batch in batches[:-1])
    assert batches[-1]["entries"] == [] and batches[-1]["finalizations"] == [publication.manifest]
    for batch in batches:
        bulk.validate_batch(**batch)
    assert publication.canonical.receipt == source.receipt


def test_import_and_preparation_keep_host_free_of_storage_and_model_runtimes():
    root = Path(__file__).resolve().parents[1]
    code = """
import sys
sys.dont_write_bytecode=True
sys.path.insert(0,sys.argv[1])
from cognitive_castle.canonical_records import prepare_remember
from cognitive_castle.live_publication_codec import prepare_publication
p=prepare_remember('request','original')
prepare_publication(p,[{'workspace':'/project','wing':'project','room':'room'}])
blocked={'sqlite3','_sqlite3','lance','pyarrow','numpy','torch','onnxruntime','tokenizers'} & sys.modules.keys()
assert not blocked,blocked
"""
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("COV_CORE_") and k not in {"COVERAGE_FILE", "COVERAGE_PROCESS_START"}
    }
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(root)],
        capture_output=True,
        text=True,
        timeout=3,
        env=env,
    )
    assert result.returncode == 0, result.stderr


def test_scope_bounds_are_checked_before_copying_caller_owned_payload(monkeypatch):
    source = prepared()
    live = codec()
    monkeypatch.setattr(live, "_json", lambda *_: pytest.fail("scope snapshot before validation"))
    for invalid in (
        [dict(SCOPE), dict(SCOPE)],
        [{**SCOPE, "workspace": "x" * 2049}],
        [{**SCOPE, "text": "unexpected payload"}],
    ):
        with pytest.raises(ValueError):
            live.prepare_publication(source, invalid)

"""Direct live request preparation; no storage, authority or durable ACK here.

Canonical bodies use the established bulk owner encoding. Only hashes, bounded
identity/scope metadata and the semantic outbox belong in the durable intent.
Scope labels are supplied by the trusted coordinator and never grant exposure.
"""

from dataclasses import dataclass
import json

from . import bulk_exact_codec as bulk
from .canonical_records import PreparedRemember, CanonicalRecord, _hash, _json, prepare_remember
from .semantic_intents import prepare_intent, validate_intent as validate_semantic_intent
from .semantic_queue import validate_scope


FORMAT = "lance-live-publication-v1"
SOURCE_FORMAT = "lance-live-source-v1"
SCOPES_FORMAT = "lance-live-scopes-v1"
MAX_INTENT_BYTES = 192 * 1024


@dataclass(frozen=True)
class PreparedPublication:
    """Expected records/manifest/intent only; none of these proves publication."""

    canonical: PreparedRemember
    entries: tuple[dict, ...]
    manifest: dict
    intent: dict


def _canonical(prepared):
    if (
        type(prepared) is not PreparedRemember
        or type(prepared.records) is not tuple
        or not 1 <= len(prepared.records) <= 9
        or any(type(record) is not CanonicalRecord for record in prepared.records)
    ):
        raise ValueError("canonical live publication")
    try:
        caller = prepared.records[0].source()["provenance"]["caller"]
        again = prepare_remember(
            prepared.request_id,
            "".join(record.text for record in prepared.records),
            caller,
            namespace=prepared.namespace,
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("canonical live publication") from exc
    if again != prepared:
        raise ValueError("canonical live publication")
    return again


def _scopes(semantic):
    return [reference["scope"] for group in semantic["groups"] for reference in group["references"]]


def _binding(semantic):
    return _hash(
        _json(
            [
                SOURCE_FORMAT,
                semantic["namespace"],
                semantic["key"],
                semantic["digest"],
                _scopes(semantic),
            ]
        )
    )


def _owner_key(semantic):
    return "direct-live:" + _hash(_json([semantic["namespace"], semantic["key"]]))


def validate_intent(intent):
    if not isinstance(intent, dict) or intent.keys() != {
        "format",
        "source_binding",
        "owner",
        "manifest_hash",
        "semantic",
    }:
        raise ValueError("live publication intent")
    if intent["format"] != FORMAT:
        raise ValueError("live publication format")
    semantic = validate_semantic_intent(intent["semantic"])
    binding = _binding(semantic)
    if intent["source_binding"] != binding or intent["owner"] != bulk.owner_id(
        _owner_key(semantic), binding
    ):
        raise ValueError("live publication binding")
    bulk.sha(intent["manifest_hash"])
    if len(_json(intent).encode("utf-8")) > MAX_INTENT_BYTES:
        raise ValueError("live publication intent bytes")
    return intent


def prepare_publication(prepared, scopes):
    canonical = _canonical(prepared)
    if not isinstance(scopes, list) or len(scopes) != len(canonical.records):
        raise ValueError("one trusted scope per canonical record")
    for scope in scopes:
        validate_scope(scope)
    # Snapshot caller-owned dictionaries before binding the intent and pages.
    scopes = json.loads(_json(scopes))
    semantic = prepare_intent(canonical, scopes, "new")
    binding = _binding(semantic)
    live = dict(
        namespace=canonical.namespace,
        key=canonical.request_id,
        digest=canonical.digest,
        receipt=canonical.receipt,
    )
    descriptor = _json(dict(format=SOURCE_FORMAT, **live))
    scope_document = _json(
        dict(
            format=SCOPES_FORMAT,
            members=[
                dict(id=record.id, scope=scope) for record, scope in zip(canonical.records, scopes)
            ],
        )
    )
    entries, manifest = bulk.prepare_owner(
        _owner_key(semantic),
        binding,
        canonical.records,
        scopes=scope_document,
        descriptor=descriptor,
        live=live,
    )
    intent = dict(
        format=FORMAT,
        source_binding=binding,
        owner=manifest["owner"],
        manifest_hash=_hash(_json(manifest)),
        semantic=semantic,
    )
    validate_intent(intent)
    return PreparedPublication(canonical, tuple(entries), manifest, intent)


def regenerate(intent, prepared, scopes):
    validate_intent(intent)
    publication = prepare_publication(prepared, scopes)
    if publication.intent != intent:
        raise ValueError("changed live publication")
    return publication


def _batch(publication, ordinal, entries, finalizations):
    semantic = publication.intent["semantic"]
    identity = bulk.key(
        "live-publication-batch",
        semantic["intent_id"],
        publication.intent["source_binding"],
        ordinal,
        _hash(_json(entries)),
        _hash(_json(finalizations)),
    )
    frame = dict(
        batch_id=identity,
        source_binding=publication.intent["source_binding"],
        entries=entries,
        finalizations=finalizations,
    )
    bulk.validate_batch(**frame)
    return frame


def batches(publication):
    if type(publication) is not PreparedPublication:
        raise ValueError("prepared live publication")
    validate_intent(publication.intent)
    checked = regenerate(
        publication.intent, publication.canonical, _scopes(publication.intent["semantic"])
    )
    if checked != publication:
        raise ValueError("changed prepared live publication")
    ordinal = 0
    for entries in bulk.entry_batches(publication.entries):
        yield _batch(publication, ordinal, entries, [])
        ordinal += 1
    yield _batch(publication, ordinal, [], [publication.manifest])

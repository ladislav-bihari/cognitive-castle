"""Pure exact-record codec compatible with the existing memory contract.

Public provenance is immutable source lineage. Its legacy format marker is
preserved when records move; physical backend locations do not belong here.
This module performs no storage, publication, or request-conflict resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re


def _json(value):
    def validate(item, depth=0):
        if depth > 32:
            raise ValueError("JSON nesting")
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise ValueError("JSON keys must be strings")
                validate(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                validate(child, depth + 1)
        elif item is not None and type(item) not in (str, int, float, bool):
            raise ValueError("invalid JSON value")

    try:
        validate(value)
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError("invalid JSON") from exc


def _string(value, maximum, name):
    if not isinstance(value, str) or not value:
        raise ValueError(name)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise ValueError(name) from exc
    if size > maximum:
        raise ValueError(name)
    return value


def _hash(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CanonicalRecord:
    """One validated exact chunk, with independently decoded public snapshots."""

    id: str
    text: str
    provenance_json: str

    def __post_init__(self):
        if not isinstance(self.id, str) or not re.fullmatch(r"memory:[a-f0-9]{64}", self.id):
            raise ValueError("id")
        _string(self.text, 8192, "persisted chunk")
        try:
            provenance = json.loads(_string(self.provenance_json, 1024, "final provenance"))
            if not isinstance(provenance, dict) or not isinstance(provenance["caller"], dict):
                raise ValueError("provenance")
            _string(_json(provenance), 1024, "final provenance")
            _string(_json(provenance["caller"]), 512, "provenance")
            start, end = provenance["chunk_start"], provenance["chunk_end"]
            if (
                provenance["text_hash"] != _hash(self.text)
                or type(start) is not int
                or type(end) is not int
                or start < 0
                or end - start != len(self.text)
            ):
                raise ValueError("source checksum or offsets")
        except (KeyError, TypeError, ValueError, RecursionError) as exc:
            raise ValueError("invalid canonical provenance") from exc

    @classmethod
    def from_source(cls, source: dict) -> CanonicalRecord:
        """Preserve full prior provenance; never synthesize a backend marker."""
        try:
            return cls(source["id"], source["text"], _json(source["provenance"]))
        except (KeyError, TypeError) as exc:
            raise ValueError("source") from exc

    def source(self) -> dict:
        return {"id": self.id, "text": self.text, "provenance": json.loads(self.provenance_json)}


@dataclass(frozen=True)
class PreparedRemember:
    """Validated publication input; a matching digest is the replay contract."""

    namespace: str
    request_id: str
    digest: str
    records: tuple[CanonicalRecord, ...]

    @property
    def receipt(self) -> dict:
        """Expected receipt, to acknowledge only after durable publication."""
        text = "".join(record.text for record in self.records)
        return {
            "status": "ok",
            "ids": [record.id for record in self.records],
            "stored_text_bytes": len(text.encode("utf-8")),
            "text_sha256": _hash(text),
        }


def validate_remember_receipt(prepared: PreparedRemember, receipt: dict) -> dict:
    """Check an actual publication receipt before advancing a source checkpoint.

    This verifies the response contract, not durability by itself: the trusted
    storage publisher remains responsible for confirming the actual append.
    """
    if (
        not isinstance(prepared, PreparedRemember)
        or not isinstance(receipt, dict)
        or receipt.keys() != {"status", "ids", "stored_text_bytes", "text_sha256"}
        or type(receipt["stored_text_bytes"]) is not int
        or not isinstance(receipt["ids"], list)
        or receipt != prepared.receipt
    ):
        raise ValueError("publication receipt mismatch")
    return receipt


def prepare_remember(
    request_id: str,
    text: str,
    provenance: dict | None = None,
    *,
    namespace: str = "operator",
) -> PreparedRemember:
    """Prepare the same IDs, chunks, provenance and receipt as MemoryStore."""
    _string(request_id, 128, "request_id")
    _string(namespace, 128, "namespace")
    _string(text, 65536, "text")
    if provenance is not None and not isinstance(provenance, dict):
        raise ValueError("provenance")
    caller_json = _string(_json(provenance or {}), 512, "provenance")
    caller = json.loads(caller_json)
    digest = _hash(_json([namespace, request_id, text, caller]))
    pieces, pending, size, start = [], [], 0, 0
    for character in text:
        width = len(character.encode("utf-8"))
        if size + width > 8192:
            piece = "".join(pending)
            pieces.append((start, piece))
            start += len(piece)
            pending, size = [], 0
        pending.append(character)
        size += width
    if pending:
        pieces.append((start, "".join(pending)))
    records = []
    for ordinal, (offset, piece) in enumerate(pieces):
        public_provenance = {
            "origin": "archive",
            "local_only": True,
            "storage_format": "sqlite-fts5-utf8-v1",
            "text_hash": _hash(piece),
            "chunk_start": offset,
            "chunk_end": offset + len(piece),
            "caller": caller,
        }
        records.append(
            CanonicalRecord(
                "memory:" + _hash(f"{digest}:{ordinal}"), piece, _json(public_provenance)
            )
        )
    return PreparedRemember(namespace, request_id, digest, tuple(records))

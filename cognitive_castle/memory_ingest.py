"""Bounded original JSONL ingestion; roles are attribution, never authorization."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import time

from .canonical_records import prepare_remember, validate_remember_receipt

from .memory_accounting import (
    add_counts,
    classify_record,
    coverage_summary,
    empty_accounting,
    observe_workspace,
)

MAX_RECORD_BYTES = 1024 * 1024
MAX_SLICE_BYTES = 8 * 1024 * 1024
MAX_RECORDS = 128


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def extract_messages(record: dict, format: str) -> list[dict]:
    """Keep individual decoded text fields exact, excluding internal/tool blocks."""
    if format not in ("codex", "claude"):
        raise ValueError("unsupported transcript format")
    if not isinstance(record, dict):
        return []
    kind = record.get("type")
    if format == "codex":
        message = record.get("payload")
        if kind != "response_item" or not isinstance(message, dict):
            return []
        if message.get("type") != "message":
            return []
        role = message.get("role")
        if role not in ("user", "assistant"):
            return []
        if role == "assistant" and message.get("channel") not in (None, "final", "commentary"):
            return []
        accepted = ("input_text", "output_text", "text")
    else:
        role = kind
        message = record.get("message")
        if role not in ("user", "human", "assistant") or not isinstance(message, dict):
            return []
        if role == "assistant" and message.get("channel", record.get("channel")) not in (
            None,
            "final",
            "commentary",
        ):
            return []
        accepted = ("text",)
    content = message.get("content")
    if isinstance(content, str) and format == "claude":
        return (
            [{"text": content, "role": role, "kind": kind, "block": "content"}] if content else []
        )
    if not isinstance(content, list):
        return []
    return [
        {"text": block["text"], "role": role, "kind": kind, "block": index}
        for index, block in enumerate(content)
        if isinstance(block, dict)
        and block.get("type") in accepted
        and isinstance(block.get("text"), str)
        and block["text"]
    ]


def _open_source(path, root):
    path, root = Path(path).absolute(), Path(root).absolute()
    if ".." in path.parts or ".." in root.parts or not path.is_relative_to(root):
        raise ValueError("source outside registered root")
    # Walk from the filesystem root through pinned no-follow directory descriptors.
    directory = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:-1]:
            child = os.open(
                component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory
            )
            os.close(directory)
            directory = child
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
    except OSError as exc:
        raise ValueError("unsafe source path") from exc
    finally:
        os.close(directory)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise ValueError("source must be regular file")
    return os.fdopen(fd, "rb", buffering=0), info, path


class _Budget:
    def __init__(self, source):
        self.source = source
        self.remaining = MAX_SLICE_BYTES

    def read(self, size):
        value = self.source.read(min(size, self.remaining))
        self.remaining -= len(value)
        return value

    def readline(self, size):
        pieces = []
        remaining = min(size, self.remaining)
        while remaining:
            value = self.read(min(8192, remaining))
            if not value:
                break
            newline = value.find(b"\n")
            if newline >= 0:
                unused = len(value) - newline - 1
                if unused:
                    self.source.seek(-unused, os.SEEK_CUR)
                pieces.append(value[: newline + 1])
                break
            pieces.append(value)
            remaining -= len(value)
        return b"".join(pieces)

    def seek(self, *args):
        return self.source.seek(*args)

    def tell(self):
        return self.source.tell()


def _anchor(source, offset):
    source.seek(0)
    first = source.read(min(offset, 4096))
    source.seek(max(0, offset - 4096))
    last = source.read(min(offset, 4096))
    return _hash(first + last)


def _pieces(text):
    start, size, pending = 0, 0, []
    for char in text:
        width = len(char.encode("utf-8"))
        if size + width > 65536:
            yield start, "".join(pending)
            start += len(pending)
            size, pending = 0, []
        pending.append(char)
        size += width
    if pending:
        yield start, "".join(pending)


def _observation(
    generation, reason, start, end, *, record_start=None, raw=None, scope="unknown", **extra
):
    value = {
        "generation": generation,
        "reason": reason,
        "start": start,
        "end": end,
        "record_start": start if record_start is None else record_start,
        "hash_scope": scope,
        **extra,
    }
    if raw is not None:
        value["sha256"] = _hash(raw)
    return value


def _publish_piece(store, request, piece, provenance, on_published, on_prepared):
    prepared = prepare_remember(request, piece, provenance, namespace="transcript")
    if on_prepared is not None:
        reserved = on_prepared(prepared)
        if not isinstance(reserved, dict):
            return dict(status="unavailable")
        if reserved.get("status") != "ok":
            return reserved
    receipt = store.remember(request, piece, provenance, namespace="transcript")
    if receipt.get("status") != "ok":
        return receipt
    try:
        validate_remember_receipt(prepared, receipt)
    except ValueError:
        return dict(status="unavailable", reason="publication_receipt")
    if on_published is None:
        return receipt
    observed = on_published(prepared, receipt)
    if not isinstance(observed, dict):
        return dict(status="unavailable")
    if observed.get("status") != "ok":
        return observed
    try:
        return validate_remember_receipt(prepared, receipt)
    except ValueError:
        return dict(status="unavailable", reason="publication_receipt")


def ingest_slice(
    store,
    path: Path,
    *,
    allowed_root: Path,
    format: str,
    on_published=None,
    on_prepared=None,
) -> dict:
    """Ingest a bounded slice under a caller-selected trusted registered root.

    Small anchors cannot prove unchanged historical prefixes. Continuations retain
    only hashes/positions, never transcript text; source files remain untouched.
    An optional trusted observer receives the prepared request and verified
    receipt before the piece checkpoint. A refusal leaves that piece replayable.
    A trusted preparation observer can reserve body-free intent before append.
    """
    if format not in ("codex", "claude") or (
        any(
            observer is not None and not callable(observer)
            for observer in (on_published, on_prepared)
        )
    ):
        raise ValueError("unsupported transcript format or invalid publication observer")
    deadline = time.monotonic() + 0.250
    opened, info, path = _open_source(path, allowed_root)
    with opened:
        source = _Budget(opened)
        key = _hash((str(path) + "\0" + format).encode())
        old = store.load_checkpoint(key)
        identity = [info.st_dev, info.st_ino, getattr(info, "st_birthtime_ns", 0)]
        generation = 0 if old is None else old["generation"]
        offset = 0 if old is None else old["offset"]
        previous_generation = None
        if old and (
            old["identity"] != identity
            or info.st_size < offset
            or _anchor(source, offset) != old["anchor"]
        ):
            previous_generation = generation
            generation += 1
            old, offset = None, 0
        checkpoint = dict(
            old
            or {
                "generation": generation,
                "offset": 0,
                "identity": identity,
                "gaps": 0,
                "unsupported": 0,
                "skipping": False,
                "anchor": _hash(b""),
            }
        )
        checkpoint.setdefault("accounting", empty_accounting())
        checkpoint.setdefault("accounting_start", offset)
        checkpoint["observed_size"] = info.st_size
        checkpoint["observed_mtime_ns"] = info.st_mtime_ns
        # Reserve the physical source generation before any text transaction.
        if old is None:
            initial = [
                _observation(generation, "pending_record", 0, 0, outcome="unknown_or_interrupted")
            ]
            if previous_generation is not None:
                initial.append(
                    _observation(
                        generation, "changed", 0, 0, previous_generation=previous_generation
                    )
                )
            store.save_checkpoint(key, checkpoint, observations=initial)
        source.seek(offset)
        records = 0
        coverage = "indexed_prefix"
        interrupted = False
        # Reserve the worst-case anchor reads before admitting each record/segment.
        while source.remaining > 8192 and records < MAX_RECORDS and time.monotonic() < deadline:
            start = source.tell()
            raw = source.readline(min(MAX_RECORD_BYTES + 1, source.remaining - 8192))
            end = source.tell()
            if not raw:
                break
            terminated = raw.endswith(b"\n")
            observed = []
            if checkpoint["skipping"] or len(raw) > MAX_RECORD_BYTES:
                if not checkpoint["skipping"]:
                    checkpoint["gaps"] += 1
                    checkpoint["skip_start"] = start
                    add_counts(checkpoint, {"oversized_records": 1, "processed_records": 1})
                add_counts(checkpoint, {"oversized_bytes": len(raw)})
                record_start = checkpoint.get("skip_start", start)
                observed.append(
                    _observation(
                        generation,
                        "oversized",
                        start,
                        end,
                        record_start=record_start,
                        raw=raw,
                        scope="segment",
                    )
                )
                checkpoint["skipping"] = not terminated
                checkpoint["offset"] = end
                if terminated:
                    checkpoint.pop("skip_start", None)
            elif not terminated:
                coverage = "pending_tail" if end >= info.st_size else "partial"
                store.save_checkpoint(
                    key,
                    checkpoint,
                    observations=[
                        _observation(
                            generation,
                            "pending_tail" if coverage == "pending_tail" else "pending_record",
                            start,
                            end,
                            raw=raw,
                            scope="segment",
                            outcome="unknown_or_interrupted",
                        )
                    ],
                )
                break
            else:
                records += 1
                record_hash = _hash(raw)
                pending = checkpoint.get("pending")
                if pending and pending["hash"] != record_hash:
                    # An in-place rewrite of an unfinished record is a new revision.
                    checkpoint = {
                        "generation": generation + 1,
                        "offset": 0,
                        "identity": identity,
                        "gaps": 0,
                        "unsupported": 0,
                        "skipping": False,
                        "anchor": _hash(b""),
                        "accounting": empty_accounting(),
                        "accounting_start": 0,
                        "observed_size": info.st_size,
                        "observed_mtime_ns": info.st_mtime_ns,
                    }
                    generation += 1
                    store.save_checkpoint(
                        key,
                        checkpoint,
                        observations=[
                            _observation(
                                generation,
                                "changed",
                                start,
                                end,
                                raw=raw,
                                scope="record",
                                previous_generation=generation - 1,
                            )
                        ],
                    )
                    coverage = "partial"
                    break
                try:
                    record = json.loads(raw)
                    messages = extract_messages(record, format)
                    observe_workspace(record, checkpoint)
                    if messages:
                        store.save_checkpoint(
                            key,
                            checkpoint,
                            observations=[
                                _observation(
                                    generation,
                                    "write_attempt",
                                    start,
                                    end,
                                    raw=raw,
                                    scope="record",
                                    outcome="unknown_or_interrupted",
                                )
                            ],
                        )
                    cursor = pending.get("cursor", [-1, 0]) if pending else [-1, 0]
                    for index, block in enumerate(messages):
                        if index < cursor[0]:
                            continue
                        block_hash = _hash(block["text"].encode())
                        for piece_start, piece in _pieces(block["text"]):
                            piece_end = piece_start + len(piece)
                            if index == cursor[0] and piece_end <= cursor[1]:
                                continue
                            if time.monotonic() >= deadline:
                                interrupted = True
                                break
                            provenance = {
                                "source": key,
                                "generation": generation,
                                "record_start": start,
                                "record_end": end,
                                "record_hash": record_hash,
                                "role": block["role"],
                                "kind": block["kind"],
                                "block": block["block"],
                                "block_hash": block_hash,
                                "block_start": piece_start,
                                "block_end": piece_end,
                            }
                            request = _hash(
                                f"{key}:{generation}:{start}:{record_hash}:{block['block']}:{piece_start}".encode()
                            )
                            receipt = _publish_piece(
                                store, request, piece, provenance, on_published, on_prepared
                            )
                            if receipt.get("status") != "ok":
                                outcome = receipt.get("status")
                                if outcome not in {
                                    "busy",
                                    "quota",
                                    "deadline",
                                    "corrupt",
                                    "unavailable",
                                    "conflict",
                                }:
                                    outcome = "unavailable"
                                store.save_checkpoint(
                                    key,
                                    checkpoint,
                                    observations=[
                                        _observation(
                                            generation,
                                            "failed",
                                            start,
                                            end,
                                            raw=raw,
                                            scope="record",
                                            outcome=outcome,
                                        )
                                    ],
                                )
                                return {
                                    "status": receipt.get("status", "unavailable"),
                                    "coverage": "partial",
                                    "source_key": key,
                                }
                            checkpoint["pending"] = {
                                "hash": record_hash,
                                "cursor": [index, piece_end],
                            }
                            store.save_checkpoint(key, checkpoint)
                        if interrupted:
                            break
                    if interrupted:
                        coverage = "partial"
                        break
                    if not messages:
                        checkpoint["unsupported"] += 1
                    add_counts(checkpoint, classify_record(record, format, messages))
                    observed.append(
                        _observation(
                            generation,
                            "indexed_record" if messages else "unsupported",
                            start,
                            end,
                            raw=raw,
                            scope="record",
                        )
                    )
                except (UnicodeError, json.JSONDecodeError, RecursionError):
                    checkpoint["gaps"] += 1
                    add_counts(checkpoint, {"processed_records": 1, "malformed_records": 1})
                    observed.append(
                        _observation(generation, "malformed", start, end, raw=raw, scope="record")
                    )
                checkpoint.pop("pending", None)
                checkpoint["offset"] = end
            checkpoint["anchor"] = _anchor(source, checkpoint["offset"])
            store.save_checkpoint(key, checkpoint, observations=observed)
            source.seek(checkpoint["offset"])
        waiting_for_append = coverage == "pending_tail"
        if (
            checkpoint["gaps"]
            or checkpoint["unsupported"]
            or checkpoint["skipping"]
            or checkpoint["offset"] < info.st_size
            and coverage != "pending_tail"
        ):
            coverage = "partial"
        store.save_checkpoint(key, checkpoint)
        return {
            "status": "ok",
            "source_key": key,
            "offset": checkpoint["offset"],
            "generation": generation,
            "records": records,
            "gaps": checkpoint["gaps"],
            "coverage": coverage,
            "waiting_for_append": waiting_for_append,
            "representation": format + ("_response_item" if format == "codex" else "_text"),
            "currentness": "anchor_checked_only",
            **coverage_summary(checkpoint),
        }

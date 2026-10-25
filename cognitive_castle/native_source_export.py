"""Inactive read-only native archive export, imported only by its fixed worker.

The existing native decoder still materializes admitted header fields and may
allocate a complete scalar internally. Output windows do not qualify giant rows.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
import time

from . import bulk_exact_codec as bulk
from .canonical_records import CanonicalRecord, _hash, _json
from .fast_memory import MemoryStore
from .historical_export_evidence import historical_scope_header, member_text, pages, quoted_pieces
from .history_projection import NativeProjectedRow
from .memory_history import (
    _HistoryError,
    _cursor_key,
    _stage_caller,
    _stage_header,
    _validate_snapshot,
)
from .native_export_codec import (
    FORMAT,
    MAX_MANIFEST,
    MAX_SCHEMA,
    PACKING,
    empty_counts,
    file_identity,
    frame_bytes,
    initial_cursor,
    source_binding,
    validate_cursor,
    validate_identity,
    validate_manifest,
    record_entry,
    layout_result,
)

PROJECTION_CHARACTERS = 8192
MAX_PROJECTIONS = 64
MAX_PROJECTED_BYTES = 2 * 1024**2


class Refusal(Exception):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason


def _kind(value, name):
    if name not in value:
        return None
    item = value[name]
    if item is None:
        return "null"
    if type(item) is bool:
        return "true" if item else "false"
    return {str: "text", int: "integer", float: "real", list: "array", dict: "object"}[type(item)]


def _invalid_json_constant(value):
    raise ValueError("non-JSON metadata constant")


class Source:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.binding = source_binding(descriptor)
        self.until = time.monotonic() + 1.65
        self.reader = self.manifest = None
        self.opened = False
        self.projections = self.projected_bytes = 0
        self.active_field = None
        self.active_operation = "validate_snapshot"

    def check_time(self):
        if time.monotonic() >= self.until:
            raise Refusal("qualification_required", "native_summary_slice_deadline")

    @staticmethod
    def private(path, expected, *, directory=False):
        path = Path(path)
        for ancestor in (path, *path.parents):
            if stat.S_ISLNK(ancestor.lstat().st_mode):
                raise Refusal("stale", "linked_native_source_path")
        for target in (path.parent, path):
            info = target.lstat()
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise Refusal("stale", "unsafe_native_source_permissions")
        info = path.lstat()
        validate_identity(file_identity(info), directory=directory)
        if file_identity(info) != expected:
            raise Refusal("stale", "native_source_identity_changed")
        return path

    def validate_files(self):
        d = self.descriptor
        snapshot = self.private(d["snapshot_path"], d["snapshot_identity"], directory=True)
        manifest_path = self.private(d["manifest_path"], d["manifest_identity"])
        fd = os.open(manifest_path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            if file_identity(os.fstat(handle.fileno())) != d["manifest_identity"]:
                raise Refusal("stale", "native_manifest_identity_changed")
            raw = handle.read(MAX_MANIFEST + 1)
        if len(raw) > MAX_MANIFEST or hashlib.sha256(raw).hexdigest() != d["manifest_hash"]:
            raise Refusal("stale", "native_manifest_hash_changed")
        manifest = validate_manifest(json.loads(raw), d)
        if self.manifest is not None and self.manifest != manifest:
            raise Refusal("stale", "native_manifest_changed")
        self.manifest = manifest
        # Reuse the reviewed structural walker; its five-value identities do
        # not independently certify the qualified copied content hashes.
        view = dict(
            snapshot=str(snapshot),
            inventory=[
                dict(
                    path=item["path"],
                    snapshot_identity=[
                        item["snapshot_identity"][key]
                        for key in ("device", "inode", "size", "mtime_ns", "ctime_ns")
                    ],
                )
                for item in manifest["inventory"]
            ],
        )
        _validate_snapshot(view)
        allowed_dirs = set()
        for item in manifest["inventory"]:
            self.check_time()
            path = snapshot / item["path"]
            self.private(path, item["snapshot_identity"])
            allowed_dirs.update(path.parents)
        for directory, dirs, _ in os.walk(snapshot, followlinks=False):
            for name in dirs:
                if Path(directory) / name not in allowed_dirs:
                    raise Refusal("stale", "extra_native_directory")
        if self.reader is not None:
            self.validate_native()
        self.private(d["snapshot_path"], d["snapshot_identity"], directory=True)
        self.private(d["manifest_path"], d["manifest_identity"])

    def validate_native(self):
        schema = self.reader.dataset.schema.serialize()
        if schema.size > MAX_SCHEMA:
            raise Refusal("qualification_required", "native_schema_limit")
        if (
            dict(hash=hashlib.sha256(schema).hexdigest(), bytes=schema.size)
            != self.manifest["schema"]
        ):
            raise Refusal("stale", "native_schema_changed")
        fragments = [fragment.metadata.to_json() for fragment in self.reader.fragments]
        if (
            _json(fragments) != _json(self.manifest["fragments"])
            or self.reader.dataset.version != self.descriptor["version"]
        ):
            raise Refusal("stale", "native_fragment_version_changed")

    def open(self, cursor):
        self.opened = True
        self.validate_files()
        self.check_time()
        self.raw_before = json.loads(cursor["key"][0])
        self.reader = NativeProjectedRow(
            self.descriptor["snapshot_path"], self.descriptor["version"], self.raw_before
        )
        self.validate_native()
        self.header = self.reader.header_or_none()
        if self.header is None:
            if cursor["owner"] is not None:
                raise Refusal("corrupt", "native_active_owner_exhausted")
            return False
        self.job = dict(
            archive_id=self.descriptor["archive_id"],
            snapshot_id=self.descriptor["original_snapshot_id"],
            version=self.descriptor["version"],
        )
        self.following, self.occurrence, self.metadata, self.columns = _stage_header(
            self.header, self.job, self.raw_before
        )
        if cursor["owner"] is not None and cursor["owner"] != self.occurrence:
            raise Refusal("corrupt", "native_cursor_owner_mismatch")
        self.alias, self.caller = _stage_caller(
            self.job,
            dict(identity=self.occurrence, metadata_json=self.metadata, columns_json=self.columns),
        )
        columns = json.loads(self.columns)
        metadata = json.loads(self.metadata, parse_constant=_invalid_json_constant)
        self.kinds = {
            scope: (_kind(columns, scope), _kind(metadata, scope))
            for scope in ("workspace", "wing", "room")
        }
        return True

    def records(self):
        self.active_field = "text"
        whole = hashlib.sha256()
        count = size = 0

        def windows():
            start = 0
            while start < self.header["text_characters"]:
                self.check_time()
                if self.projections >= MAX_PROJECTIONS:
                    raise Refusal("qualification_required", "native_projection_count")
                self.projections += 1
                text = self.reader.page(start, PROJECTION_CHARACTERS)
                self.projected_bytes += len(text.encode())
                if self.projected_bytes > MAX_PROJECTED_BYTES:
                    raise Refusal("qualification_required", "native_projection_bytes")
                start += len(text)
                yield text

        # The packer is byte-for-byte _chunks across projection windows; it
        # retains at most one 8192-byte canonical chunk and no chunk-ID list.
        for text in pages(windows()):
            self.check_time()
            encoded = text.encode()
            whole.update(encoded)
            end = count + len(text)
            record = CanonicalRecord.from_source(
                dict(
                    id="memory:" + _hash(_json([self.occurrence, count])),
                    text=text,
                    provenance=MemoryStore._provenance(self.caller, _hash(text), count, end),
                )
            )
            count = end
            size += len(encoded)
            yield record
        if (
            count != self.header["text_characters"]
            or size != self.header["text_bytes"]
            or whole.hexdigest() != self.header["text_hash"]
        ):
            raise Refusal("corrupt", "native_original_text_summary")

    def scopes(self):
        yield from historical_scope_header(
            quoted_pieces([self.columns]), quoted_pieces([self.metadata]), self.kinds
        )
        for ordinal, record in enumerate(self.records()):
            if ordinal:
                yield ","
            yield (
                '{"id":'
                + _json(record.id)
                + ',"caller":'
                + _json(self.caller)
                + ',"capture_mapping":{"present":false}}'
            )
        yield "]}"

    def descriptor_text(self):
        yield _json(
            dict(
                format=FORMAT,
                packing=PACKING,
                kind="history",
                source_binding=self.binding,
                registered_manifest=self.descriptor["manifest_hash"],
                archive_id=self.job["archive_id"],
                snapshot_id=self.job["snapshot_id"],
                version=self.job["version"],
                fragment=self.header["fragment"],
                row_address=self.header["row_address"],
                raw_before=self.raw_before,
                following=self.following,
                cursor_key=_cursor_key(self.raw_before),
                occurrence=self.occurrence,
                alias=self.alias,
                whole=dict(
                    hash=self.header["text_hash"],
                    bytes=self.header["text_bytes"],
                    chars=self.header["text_characters"],
                ),
                metadata=dict(
                    hash=_hash(self.metadata),
                    bytes=len(self.metadata.encode()),
                    chars=len(self.metadata),
                ),
                columns=dict(
                    hash=_hash(self.columns),
                    bytes=len(self.columns.encode()),
                    chars=len(self.columns),
                ),
            )
        )

    def fields(self):
        yield "metadata", iter((self.metadata,))
        yield "columns", iter((self.columns,))
        yield "scopes", self.scopes()
        yield "descriptor", self.descriptor_text()

    def owner(self, *, planning=None):
        self.active_operation = "owner_summary"
        owner = bulk.owner_id(self.occurrence, self.binding)
        summaries = {}
        for field, pieces in self.fields():
            self.active_field = field
            digest = hashlib.sha256()
            size = chars = count = 0
            for text in pages(pieces):
                self.check_time()
                if planning is not None:
                    planning.add_entry(bulk.page(owner, field, count, text, chars, size))
                digest.update(text.encode())
                chars += len(text)
                size += len(text.encode())
                count += 1
            summaries[field] = dict(hash=digest.hexdigest(), bytes=size, chars=chars, pages=count)
        membership = hashlib.sha256()
        count = 0
        for ordinal, record in enumerate(self.records()):
            member = member_text(owner, ordinal, record, summaries["scopes"])
            if planning is not None:
                planning.add_entry(
                    bulk.page(owner, "membership", ordinal, member, ordinal, ordinal)
                )
                planning.add_entry(record_entry(owner, ordinal, record))
            membership.update(member.encode())
            count += 1
        return bulk.validate_owner(
            dict(
                format=bulk.FORMAT,
                state="published_owner",
                owner=owner,
                owner_key=self.occurrence,
                source_binding=self.binding,
                members=count,
                membership_hash=membership.hexdigest(),
                text_hash=self.header["text_hash"],
                text_bytes=self.header["text_bytes"],
                text_chars=self.header["text_characters"],
                fields=summaries,
                metadata_id=self.occurrence,
                live=None,
            )
        )

    def entries(self, owner):
        for field, pieces in self.fields():
            chars = size = 0
            for ordinal, text in enumerate(pages(pieces)):
                self.check_time()
                yield (
                    bulk.page(owner["owner"], field, ordinal, text, chars, size),
                    (field, 0, size, chars),
                )
                chars += len(text)
                size += len(text.encode())
        for ordinal, record in enumerate(self.records()):
            text = member_text(owner["owner"], ordinal, record, owner["fields"]["scopes"])
            yield (
                bulk.page(owner["owner"], "membership", ordinal, text, ordinal, ordinal),
                ("membership", ordinal, ordinal, ordinal),
            )
            yield (
                bulk.validate_entry(
                    dict(
                        id=record.id,
                        kind="bulk_record",
                        text=record.text,
                        provenance=record.provenance_json,
                        digest=owner["owner"],
                        receipt=_json(
                            dict(
                                format=bulk.FORMAT,
                                page=bulk.page_id(owner["owner"], "membership", ordinal),
                                ordinal=ordinal,
                            )
                        ),
                    )
                ),
                ("record", ordinal, ordinal, ordinal),
            )


def _export_owner(source, result, cursor, *, owner=None):
    if owner is None:
        owner = source.owner()
    start = cursor["entry"] if cursor["owner"] is not None else 0
    after = initial_cursor(source.binding, source.descriptor["store_id"], source.raw_before)
    source.active_operation = "emit_entries"
    selected, expected, following = [], None, None
    total = 0
    for ordinal, (entry, position) in enumerate(source.entries(owner)):
        source.check_time()
        total = ordinal + 1
        if ordinal < start:
            continue
        if expected is None:
            expected = position
        candidate = selected + [entry]
        batches = bulk.entry_batches(iter(candidate))
        first = next(batches)
        try:
            if len(first) != len(candidate):
                raise ValueError("batch packing")
            frame_bytes(
                {
                    **result,
                    "entries": candidate,
                    "cursor_after": {
                        **after,
                        "owner": owner["owner_key"],
                        "entry": ordinal + 1,
                        "field": "finalization",
                    },
                }
            )
        except ValueError:
            following = (ordinal, position)
            break
        selected = candidate
    if cursor["owner"] is not None:
        given = tuple(
            cursor[key]
            for key in ("field", "member_ordinal", "field_byte_offset", "field_char_offset")
        )
        if given != (expected or ("finalization", owner["members"], 0, 0)):
            raise Refusal("corrupt", "native_cursor_position_mismatch")
    if start > total:
        raise Refusal("corrupt", "native_cursor_past_owner")
    result["entries"] = selected
    if following is None:
        try:
            bulk.validate_batch("export", source.binding, selected, [owner])
            frame_bytes({**result, "entries": selected, "finalizations": [owner]})
        except ValueError:
            following = (total, ("finalization", owner["members"], 0, 0))
        else:
            after["key"] = [_json(source.following)]
            result.update(finalizations=[owner], phase_complete=True)
            result["counts"].update(historical_occurrences=1, members=owner["members"])
    if following is not None:
        ordinal, position = following
        after.update(
            owner=owner["owner_key"],
            entry=ordinal,
            field=position[0],
            member_ordinal=position[1],
            field_byte_offset=position[2],
            field_char_offset=position[3],
        )
    if not selected and not result["finalizations"]:
        raise Refusal("qualification_required", "native_export_no_progress")
    result["cursor_after"] = after
    bulk.validate_batch("export", source.binding, selected, result["finalizations"])


def _normalize_refusal(result, before, source):
    result.update(
        cursor_after=before,
        entries=[],
        finalizations=[],
        counts=empty_counts(),
        complete=False,
        phase_complete=False,
        frame_bytes=0,
        coverage="incomplete",
        refused_field=source.active_field,
        refused_operation=source.active_operation,
    )


def export_slice(descriptor, cursor, *, layout=None):
    source = Source(descriptor)
    validate_cursor(cursor, source.binding, descriptor["store_id"], descriptor["start_cursor"])
    before = dict(cursor)
    result = dict(
        status="ok",
        reason=None,
        source_binding=source.binding,
        cursor_before=before,
        cursor_after=before,
        entries=[],
        finalizations=[],
        counts=empty_counts(),
        phase="history",
        phase_complete=False,
        complete=False,
        coverage="incomplete",
        frame_bytes=0,
    )
    plans = []
    try:
        if layout is not None:
            from .lance_layout_codec import LayoutPlanningCounters, validate_layout_source_window
            from .lance_layout_directory import load_layout_qualification
            from .migration_codec import registration, hashed

            validate_layout_source_window(layout, descriptor, cursor)
            qualification = load_layout_qualification(
                layout["generation"], layout["qualification_hash"]
            )
            reg = registration(descriptor, layout["generation"]["path"])
            planning = LayoutPlanningCounters(qualification)
        if cursor["phase"] == "done":
            raise Refusal("corrupt", "native_export_already_done")
        if source.open(cursor):
            if layout is None:
                _export_owner(source, result, cursor)
            else:
                owner = source.owner(planning=planning)
                plan = planning.finish(reg, owner, "history", [_json(source.following)])
                source.check_time()
                if layout["action"] == "export" and layout["allowed_plan_hashes"] != [hashed(plan)]:
                    raise Refusal("conflict", "layout_owner_plan_mismatch")
                plans = [plan]
                if layout["action"] in {"export", "prepare"}:
                    _export_owner(source, result, cursor, owner=owner)
        else:
            after = initial_cursor(
                source.binding, descriptor["store_id"], descriptor["start_cursor"]
            )
            after.update(phase="done", key=None)
            result.update(phase="done", cursor_after=after, complete=True, phase_complete=True)
    except Refusal as exc:
        result.update(status=exc.status, reason=exc.reason)
    except _HistoryError as exc:
        result.update(
            status="stale"
            if exc.status == "changed"
            else "qualification_required"
            if exc.status in {"native_batch_size", "metadata_size", "quota", "unsupported"}
            else "corrupt",
            reason="native_" + exc.status,
        )
    except MemoryError:
        result.update(status="qualification_required", reason="native_decode_summary_limit")
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result.update(status="corrupt", reason="native_source_contract")
    except OSError:
        result.update(status="stale", reason="native_source_files")
    finally:
        # A pre/post identity refusal clears *all* transient progress, even if
        # an owner or genuine exhaustion was tentatively computed above.
        if source.opened:
            try:
                source.validate_files()
            except Refusal as exc:
                if exc.status == "stale" or result["status"] == "ok":
                    result.update(status=exc.status, reason=exc.reason)
            except MemoryError:
                if result["status"] == "ok":
                    result.update(
                        status="qualification_required", reason="native_decode_summary_limit"
                    )
            except (_HistoryError, OSError, ValueError, TypeError, KeyError):
                result.update(status="stale", reason="native_source_post_slice_validation")
        if source.reader is not None:
            source.reader.close()
        if result["status"] != "ok":
            _normalize_refusal(result, before, source)
        else:
            try:
                for _ in range(3):
                    result["frame_bytes"] = len(frame_bytes(result))
            except (ValueError, MemoryError):
                result.update(status="qualification_required", reason="native_source_frame_limit")
                _normalize_refusal(result, before, source)
    if layout is not None:
        return _layout_response(source, descriptor, before, layout, result, plans)
    return result


def _layout_response(source, descriptor, before, layout, result, plans):
    try:
        wrapped = layout_result(descriptor, before, layout, result, plans)
        if result["status"] == "ok":
            source.check_time()
        return wrapped
    except Refusal as exc:
        result.update(status=exc.status, reason=exc.reason)
    except MemoryError:
        result.update(status="qualification_required", reason="native_decode_summary_limit")
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        result.update(status="qualification_required", reason="layout_source_frame_limit")
    _normalize_refusal(result, before, source)
    return layout_result(descriptor, before, layout, result, [])

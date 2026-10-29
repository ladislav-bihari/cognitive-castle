"""Supervised bounded bulk exact publication on the canonical Lance dataset."""

from __future__ import annotations

import hashlib
import json

from . import bulk_exact_codec as codec
from .canonical_records import _hash, _json, _string
from .lance_exact_store import COLUMNS, DATASET, LanceExactStore, _StoreError, _schema

LOOKUP_IDS = 32
MAX_TAIL_ROWS = 1024
MAX_TAIL_FRAGMENTS = 32
MAX_INDEX_ROWS = 16384
INDEX_NAME = "castle_exact_id"


def coverage(dataset):
    """Inspect manifest coverage, never build/optimize an index during a read."""
    if dataset is None:
        return {
            "version": 0,
            "index_version": None,
            "indexed_fragments": 0,
            "tail_fragments": 0,
            "tail_rows": 0,
        }
    indices = [index for index in dataset.describe_indices() if index.name == INDEX_NAME]
    if len(indices) > 1:
        raise _StoreError("corrupt")
    covered, index_version = set(), None
    if indices:
        index = indices[0]
        if index.field_names != ["id"] or index.index_type.lower() != "btree":
            raise _StoreError("corrupt")
        for segment in index.segments:
            covered.update(segment.fragment_ids)
            index_version = max(index_version or 0, segment.dataset_version_at_last_update)
    tail_rows = tail_fragments = indexed_fragments = 0
    for fragment in dataset.get_fragments():
        if fragment.fragment_id in covered:
            indexed_fragments += 1
        else:
            tail_fragments += 1
            tail_rows += fragment.physical_rows
    if tail_rows > MAX_TAIL_ROWS or tail_fragments > MAX_TAIL_FRAGMENTS:
        raise _StoreError("index_required")
    return {
        "version": dataset.version,
        "index_version": index_version,
        "indexed_fragments": indexed_fragments,
        "tail_fragments": tail_fragments,
        "tail_rows": tail_rows,
    }


def lookup_many(dataset, ids, *, checked=False):
    """Bounded IN scans include both BTREE and its unindexed tail, detecting duplicates."""
    ids = list(dict.fromkeys(ids))
    for value in ids:
        codec.identity(value, "(?:memory|receipt)")
    if not checked:
        coverage(dataset)
    if dataset is None:
        return {}
    found = {}
    for start in range(0, len(ids), LOOKUP_IDS):
        group = ids[start : start + LOOKUP_IDS]
        scanner = dataset.scanner(
            columns=COLUMNS,
            filter="id IN (" + ",".join("'" + i + "'" for i in group) + ")",
            limit=len(group) + 1,
            batch_size=LOOKUP_IDS,
            strict_batch_size=True,
            batch_readahead=0,
            fragment_readahead=0,
            io_buffer_size=1024**2,
            use_scalar_index=True,
        )
        for batch in scanner.to_batches():
            for row in batch.to_pylist():
                if row["id"] not in group or row["id"] in found:
                    raise _StoreError("corrupt")
                found[row["id"]] = row
    return found


def decode_receipt(row):
    if (
        row is None
        or row["kind"] != "bulk_receipt"
        or row["text"]
        or row["provenance"]
        or row["digest"] != _hash(row["receipt"])
    ):
        raise _StoreError("corrupt")
    try:
        manifest = codec.decode(row["receipt"], 128 * 1024)
        if manifest["format"] != codec.FORMAT:
            raise ValueError("bulk format")
        if manifest["state"] == "published_owner":
            codec.validate_owner(manifest)
            if row["id"] != manifest["owner"]:
                raise ValueError("owner identity")
        elif manifest["state"] in {"stage_batch", "committed_batch"}:
            if manifest.keys() != {
                "format",
                "state",
                "batch_id",
                "source_binding",
                "digest",
                "proof",
                "records",
                "published_owners",
                "entries",
            }:
                raise ValueError("batch receipt")
            _string(manifest["batch_id"], 128, "batch id")
            _string(manifest["source_binding"], 1024, "binding")
            codec.sha(manifest["digest"])
            if row["id"] != codec.key("batch", manifest["batch_id"]):
                raise ValueError("batch identity")
            for name in ("records", "published_owners", "entries"):
                codec.integer(manifest[name])
            if (manifest["state"] == "committed_batch") != bool(manifest["published_owners"]):
                raise ValueError("batch state")
            proof = manifest["proof"]
            if not isinstance(proof, list) or not 1 <= len(proof) <= 2 * codec.MAX_ENTRIES:
                raise ValueError("batch proof")
            seen = set()
            for item in proof:
                if not isinstance(item, list) or len(item) != 2:
                    raise ValueError("batch proof entry")
                codec.identity(item[0], "(?:memory|receipt)")
                codec.sha(item[1])
                if item[0] in seen:
                    raise ValueError("duplicate proof")
                seen.add(item[0])
        else:
            raise ValueError("bulk state")
        return manifest
    except (KeyError, TypeError, ValueError, RecursionError):
        raise _StoreError("corrupt") from None


def checked_page(row, owner, field, ordinal):
    try:
        codec.validate_entry(row)
        h = json.loads(row["receipt"])
        if row["kind"] != "bulk_page" or (h["owner"], h["field"], h["ordinal"]) != (
            owner,
            field,
            ordinal,
        ):
            raise ValueError("page binding")
        return h
    except (KeyError, TypeError, ValueError, RecursionError):
        raise _StoreError("corrupt") from None


def check_member(record, member, manifest):
    source = record.source()
    p = source["provenance"]
    if {k: member["scopes"][k] for k in ("hash", "bytes", "chars", "pages")} != manifest["fields"][
        "scopes"
    ]:
        raise _StoreError("corrupt")
    if (
        member["id"] != record.id
        or member["hash"] != p["text_hash"]
        or member["exposure"] != _hash(_json(source))
        or member["start"] != p["chunk_start"]
        or member["end"] != p["chunk_end"]
        or member["bytes"] != len(record.text.encode())
    ):
        raise _StoreError("corrupt")


def visible_source(store, dataset, row):
    """Common exact/page/receipt/semantic hydration gate on a pinned dataset."""
    record = store._record(row)
    if row["kind"] == "record":
        return record.source()
    owner = row["digest"]
    pointer = json.loads(row["receipt"])
    found = store._lookup_rows(dataset, [owner, pointer["page"]])
    if owner not in found:
        raise _StoreError("not_found")
    manifest = decode_receipt(found[owner])
    if manifest["state"] != "published_owner" or pointer["ordinal"] >= manifest["members"]:
        raise _StoreError("corrupt")
    membership = found.get(pointer["page"])
    checked_page(membership, owner, "membership", pointer["ordinal"])
    member = json.loads(membership["text"])
    check_member(record, member, manifest)
    source = record.source()
    if manifest["metadata_id"] is not None:
        source["metadata"] = {
            "id": manifest["metadata_id"],
            "metadata_hash": manifest["fields"]["metadata"]["hash"],
            "columns_hash": manifest["fields"]["columns"]["hash"],
            "metadata_bytes": manifest["fields"]["metadata"]["bytes"],
            "columns_bytes": manifest["fields"]["columns"]["bytes"],
        }
    return source


class BulkExactStore(LanceExactStore):
    def _coverage(self, dataset):
        """Default geometry; private registered stores may require stricter coverage."""
        return coverage(dataset)

    def _verify_owner(self, dataset, manifest, pending):
        """Stream original text/field SHA256, never digest-of-page-digests."""

        def fetch(ids):
            found = self._lookup_rows(dataset, [i for i in ids if i not in pending], checked=True)
            found.update({i: pending[i] for i in ids if i in pending})
            if any(i not in found for i in ids):
                raise _StoreError("corrupt")
            return found

        owner = manifest["owner"]
        whole, membership_hash = hashlib.sha256(), hashlib.sha256()
        text_chars = text_bytes = 0
        live_records = []
        if manifest["live"] is not None and not 1 <= manifest["members"] <= 9:
            raise _StoreError("corrupt")
        for start in range(0, manifest["members"], LOOKUP_IDS):
            ordinals = range(start, min(start + LOOKUP_IDS, manifest["members"]))
            pages = fetch([codec.page_id(owner, "membership", n) for n in ordinals])
            members = []
            for n in ordinals:
                row = pages[codec.page_id(owner, "membership", n)]
                h = checked_page(row, owner, "membership", n)
                if h["offset"] != n or h["byte_offset"] != n:
                    raise _StoreError("corrupt")
                members.append(json.loads(row["text"]))
                membership_hash.update(row["text"].encode())
            records = fetch([member["id"] for member in members])
            for member in members:
                row = records[member["id"]]
                record = self._record(row)
                check_member(record, member, manifest)
                if row["kind"] == "bulk_record":
                    if row["digest"] == owner:
                        if json.loads(row["receipt"])["ordinal"] != member["ordinal"]:
                            raise _StoreError("corrupt")
                    else:
                        # Reuse already visible content, never a staged other owner.
                        visible_source(self, dataset, row)
                if member["start"] != text_chars:
                    raise _StoreError("corrupt")
                whole.update(record.text.encode())
                text_chars += len(record.text)
                text_bytes += len(record.text.encode())
                if manifest["live"] is not None:
                    live_records.append(record)
        if (whole.hexdigest(), text_chars, text_bytes, membership_hash.hexdigest()) != (
            manifest["text_hash"],
            manifest["text_chars"],
            manifest["text_bytes"],
            manifest["membership_hash"],
        ):
            raise _StoreError("corrupt")
        for field, descriptor in manifest["fields"].items():
            digest = hashlib.sha256()
            chars = size = 0
            for start in range(0, descriptor["pages"], LOOKUP_IDS):
                ordinals = range(start, min(start + LOOKUP_IDS, descriptor["pages"]))
                pages = fetch([codec.page_id(owner, field, n) for n in ordinals])
                for n in ordinals:
                    row = pages[codec.page_id(owner, field, n)]
                    h = checked_page(row, owner, field, n)
                    if h["offset"] != chars or h["byte_offset"] != size:
                        raise _StoreError("corrupt")
                    digest.update(row["text"].encode())
                    chars += len(row["text"])
                    size += len(row["text"].encode())
            if (digest.hexdigest(), chars, size) != (
                descriptor["hash"],
                descriptor["chars"],
                descriptor["bytes"],
            ):
                raise _StoreError("corrupt")
        if manifest["live"] is not None:
            live = manifest["live"]
            self._verify_receipt_records(
                live["namespace"], live["key"], live["digest"], live["receipt"], live_records
            )

    def _verify_batch(self, dataset, row):
        manifest = decode_receipt(row)
        if manifest["state"] not in {"stage_batch", "committed_batch"}:
            raise _StoreError("corrupt")
        counts = {"records": 0, "pages": 0, "owners": 0}
        live_ids, expected_live_ids = set(), set()
        for start in range(0, len(manifest["proof"]), LOOKUP_IDS):
            proof = manifest["proof"][start : start + LOOKUP_IDS]
            found = self._lookup_rows(dataset, [i for i, _ in proof], checked=True)
            for identity, digest in proof:
                prior = found.get(identity)
                if prior is None or _hash(_json(prior)) != digest:
                    raise _StoreError("corrupt")
                if prior["kind"] == "bulk_receipt":
                    owner = decode_receipt(prior)
                    if owner["state"] != "published_owner":
                        raise _StoreError("corrupt")
                    if owner["source_binding"] != manifest["source_binding"]:
                        raise _StoreError("corrupt")
                    counts["owners"] += 1
                    if owner["live"] is not None:
                        live = owner["live"]
                        expected_live_ids.add(
                            "receipt:" + _hash(_json([live["namespace"], live["key"]]))
                        )
                    self._verify_owner(dataset, owner, {})
                elif prior["kind"] in {"bulk_record", "bulk_page"}:
                    try:
                        codec.validate_entry(prior)
                    except (TypeError, ValueError):
                        raise _StoreError("corrupt") from None
                    counts["records" if prior["kind"] == "bulk_record" else "pages"] += 1
                elif prior["kind"] == "record":
                    self._record(prior)
                    counts["records"] += 1
                elif prior["kind"] == "receipt":
                    live_ids.add(identity)
                else:
                    raise _StoreError("corrupt")
        if (
            counts["records"] != manifest["records"]
            or counts["records"] + counts["pages"] != manifest["entries"]
            or counts["owners"] != manifest["published_owners"]
            or live_ids != expected_live_ids
        ):
            raise _StoreError("corrupt")
        return manifest

    @staticmethod
    def _compact(manifest, receipt_id):
        return {
            "status": "ok",
            "state": manifest["state"],
            "receipt_id": receipt_id,
            **{key: manifest[key] for key in ("digest", "records", "published_owners", "entries")},
        }

    def _before_append(self, dataset, rows):
        """Private qualified subclasses may reserve append geometry under the lock."""

    def _after_append(self, dataset):
        """Private qualified subclasses may check already charged output geometry."""

    def _existing_import_result(
        self, dataset, manifest, receipt_id, batch_id, source_binding, expected_ids
    ):
        return self._compact(manifest, receipt_id)

    def _fresh_import_result(self, manifest, receipt_id):
        return self._compact(manifest, receipt_id)

    def import_batch(self, batch_id, source_binding, entries, finalizations):
        # All body/count/frame validation precedes _writer/path creation/native IO.
        digest = codec.validate_batch(batch_id, source_binding, entries, finalizations)
        receipt_id = codec.key("batch", batch_id)
        try:
            with self._writer() as dataset:
                self._coverage(dataset)
                old = self._lookup_rows(dataset, [receipt_id], checked=True).get(receipt_id)
                if old is not None:
                    manifest = self._verify_batch(dataset, old)
                    if manifest["digest"] != digest or manifest["source_binding"] != source_binding:
                        return {"status": "conflict"}
                    expected_ids = {row["id"] for row in entries} | {
                        owner["owner"] for owner in finalizations
                    }
                    for owner in finalizations:
                        live = owner["live"]
                        if live is not None:
                            expected_ids.add(
                                "receipt:" + _hash(_json([live["namespace"], live["key"]]))
                            )
                    if expected_ids != {item[0] for item in manifest["proof"]}:
                        raise _StoreError("corrupt")
                    return self._existing_import_result(
                        dataset, manifest, receipt_id, batch_id, source_binding, expected_ids
                    )
                candidates = {r["id"]: r for r in entries}
                for owner in finalizations:
                    candidates[owner["owner"]] = codec.receipt_row(owner, owner["owner"])
                    live = owner["live"]
                    if live is not None:
                        legacy_id = "receipt:" + _hash(_json([live["namespace"], live["key"]]))
                        row = {
                            "id": legacy_id,
                            "kind": "receipt",
                            "text": "",
                            "provenance": "",
                            "digest": live["digest"],
                            "receipt": _json(live["receipt"]),
                        }
                        if legacy_id in candidates and candidates[legacy_id] != row:
                            raise _StoreError("conflict")
                        candidates[legacy_id] = row
                found = self._lookup_rows(dataset, candidates, checked=True)
                pending = {}
                for identity, row in candidates.items():
                    prior = found.get(identity)
                    if prior is None:
                        pending[identity] = row
                    elif row["kind"] == "bulk_record":
                        if self._record(prior) != self._record(row):
                            raise _StoreError("conflict")
                        if prior["kind"] == "bulk_record" and prior["digest"] != row["digest"]:
                            try:
                                visible_source(self, dataset, prior)
                            except _StoreError as exc:
                                if exc.status == "not_found":
                                    raise _StoreError("conflict") from None
                                raise
                        elif prior["kind"] == "bulk_record" and prior != row:
                            raise _StoreError("conflict")
                    elif prior != row:
                        raise _StoreError("conflict")
                for owner in finalizations:
                    self._verify_owner(dataset, owner, pending)
                proof = [
                    [identity, _hash(_json(found.get(identity, row)))]
                    for identity, row in candidates.items()
                ]
                manifest = {
                    "format": codec.FORMAT,
                    "state": "committed_batch" if finalizations else "stage_batch",
                    "batch_id": batch_id,
                    "source_binding": source_binding,
                    "digest": digest,
                    "proof": proof,
                    "records": sum(r["kind"] == "bulk_record" for r in entries),
                    "published_owners": len(finalizations),
                    "entries": len(entries),
                }
                rows = [*pending.values(), codec.receipt_row(manifest, receipt_id)]
                self._before_append(dataset, rows)
                import lance
                import pyarrow as pa

                appended = lance.write_dataset(
                    pa.Table.from_pylist(rows, schema=_schema()),
                    str(self.directory / DATASET),
                    mode="create" if dataset is None else "append",
                    max_rows_per_file=1024,
                    max_rows_per_group=1024,
                )
                self._after_append(appended)
            return self._fresh_import_result(manifest, receipt_id)
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def build_id_index(self):
        try:
            with self._writer() as dataset:
                if dataset is None:
                    raise _StoreError("not_initialized")
                if dataset.count_rows() > MAX_INDEX_ROWS:
                    raise _StoreError("maintenance_limit")
                dataset.create_scalar_index("id", "BTREE", name=INDEX_NAME, replace=True)
                result = self._coverage(self._open())
            return {"status": "ok", "lookup": result}
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def _read(self, *, version=None):
        if version is not None:
            codec.integer(version)
        self._paths()
        dataset = self._open()
        if dataset is None:
            raise _StoreError("not_initialized")
        if version is not None and dataset.version != version:
            raise _StoreError("stale")
        info = self._coverage(dataset)
        return dataset, info

    def bulk_owner(self, owner, *, version=None):
        codec.identity(owner)
        try:
            dataset, info = self._read(version=version)
            row = self._lookup_rows(dataset, [owner], checked=True).get(owner)
            if row is None:
                raise _StoreError("not_found")
            manifest = decode_receipt(row)
            if manifest["state"] != "published_owner":
                raise _StoreError("corrupt")
            return {
                "status": "ok",
                "manifest": manifest,
                "version": dataset.version,
                "lookup": info,
            }
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def bulk_page(self, owner, field, ordinal, *, version=None):
        identity = codec.page_id(owner, field, ordinal)
        try:
            dataset, info = self._read(version=version)
            found = self._lookup_rows(dataset, [owner, identity], checked=True)
            if owner not in found:
                raise _StoreError("not_found")
            manifest = decode_receipt(found[owner])
            if manifest["state"] != "published_owner":
                raise _StoreError("corrupt")
            count = (
                manifest["members"] if field == "membership" else manifest["fields"][field]["pages"]
            )
            if ordinal >= count:
                raise _StoreError("not_found")
            row = found.get(identity)
            header = checked_page(row, owner, field, ordinal)
            return {
                "status": "ok",
                "text": row["text"],
                "hash": row["digest"],
                "header": header,
                "version": dataset.version,
                "lookup": info,
            }
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def bulk_receipt(self, batch_id, source_binding, *, offset=0, version=None):
        _string(batch_id, 128, "batch id")
        _string(source_binding, 1024, "source binding")
        codec.integer(offset)
        try:
            dataset, info = self._read(version=version)
            identity = codec.key("batch", batch_id)
            row = self._lookup_rows(dataset, [identity], checked=True).get(identity)
            if row is None:
                raise _StoreError("not_found")
            manifest = self._verify_batch(dataset, row)
            if manifest["source_binding"] != source_binding:
                raise _StoreError("conflict")
            end = min(len(manifest["proof"]), offset + 16)
            return {
                **self._compact(manifest, identity),
                "proof": manifest["proof"][offset:end],
                "next_offset": end,
                "complete": end == len(manifest["proof"]),
                "version": dataset.version,
                "lookup": info,
            }
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}


class OrderedBulkExactStore(BulkExactStore):
    """Internal fixture/operator store for one digest-qualified ordered owner."""

    def __init__(self, directory, *, qualification, writable=False):
        from .bulk_owner_stream import OrderedQualification

        if not isinstance(qualification, OrderedQualification):
            raise _StoreError("unavailable")
        super().__init__(directory, writable=writable)
        if str(self.directory) != qualification.destination:
            raise _StoreError("unavailable")
        self.qualification = qualification
        self._artifacts = None
        self._charged_append = None

    def _paths(self, *, create=False):
        import os
        import stat
        from pathlib import Path
        from . import lance_exact_store as exact

        # Qualification precedes supervision/path inspection and never authorizes
        # mkdir. The fixed fixture setup precreates and binds this private home.
        q = self.qualification
        exact._require_supervision()
        self._check_namespace_access()
        for path in [self.directory, *self.directory.parents]:
            if path.is_symlink():
                raise _StoreError("unavailable")
        for path, identity in (
            (self.directory.parent, q.parent_identity),
            (self.directory, q.destination_identity),
        ):
            self._private(path, directory=True)
            info = path.lstat()
            if (info.st_dev, info.st_ino) != identity:
                raise _StoreError("unavailable")

        counts = {
            "artifact_entries": 0,
            "logical_bytes": 0,
            "allocated_bytes": 0,
            "versions": 0,
            "file_bytes": 0,
        }

        def account(path, info, depth):
            directory = stat.S_ISDIR(info.st_mode)
            self._private(path, directory=directory)
            counts["artifact_entries"] += 1
            counts["logical_bytes"] += info.st_size
            counts["allocated_bytes"] += info.st_blocks * 512
            if not directory:
                counts["file_bytes"] = max(counts["file_bytes"], info.st_size)
            if depth > q.max_artifact_depth or any(
                counts[name] > getattr(q, "max_" + name) for name in counts
            ):
                raise _StoreError("unavailable")
            if not directory and info.st_size > q.max_file_bytes:
                raise _StoreError("unavailable")
            if not directory and path.suffix == ".manifest":
                counts["versions"] += 1
                if counts["versions"] > q.max_versions:
                    raise _StoreError("unavailable")
            return directory

        account(self.directory, self.directory.lstat(), 0)
        stack = [(os.scandir(self.directory), 1)]
        try:
            while stack:
                iterator, depth = stack[-1]
                entry = next(iterator, None)
                if entry is None:
                    iterator.close()
                    stack.pop()
                    continue
                path = Path(entry.path)
                if depth == 1 and entry.name not in {DATASET, ".write.lock"}:
                    raise _StoreError("unavailable")
                info = entry.stat(follow_symlinks=False)
                if account(path, info, depth):
                    stack.append((os.scandir(path), depth + 1))
        finally:
            for iterator, _ in stack:
                iterator.close()
        self._artifacts = counts

    def _geometry(self, dataset, *, expected=True):
        q = self.qualification
        if dataset is None:
            geometry = {"version": 0, "physical_rows": 0, "fragments": 0}
        else:
            if dataset.data_storage_version != q.storage_mode:
                raise _StoreError("corrupt")
            rows = fragments = 0
            for fragment in dataset.get_fragments():
                metadata = fragment.metadata
                if (
                    fragment.fragment_id != fragments
                    or metadata.deletion_file is not None
                    or metadata.row_id_meta is not None
                    or metadata.created_at_version_meta is not None
                    or metadata.last_updated_at_version_meta is not None
                ):
                    raise _StoreError("corrupt")
                fragments += 1
                rows += fragment.physical_rows
                if fragments > q.max_fragments or rows > q.max_physical_rows:
                    raise _StoreError("unavailable")
            geometry = {"version": dataset.version, "physical_rows": rows, "fragments": fragments}
        if geometry["version"] > q.max_versions:
            raise _StoreError("unavailable")
        if expected and geometry not in q.expected_states:
            raise _StoreError("stale")
        return geometry

    def _open(self):
        # Base _writer calls this under its flock. Revalidate identity/artifacts
        # here, rather than relying on the earlier pre-lock _paths call.
        self._paths()
        dataset = super()._open()
        self._geometry(dataset)
        return dataset

    def _bound_row(self, row):
        q = self.qualification
        if row["kind"] == "bulk_page":
            codec.validate_entry(row)
            if json.loads(row["receipt"])["owner"] != q.owner:
                raise _StoreError("corrupt")
        elif row["kind"] == "bulk_record":
            codec.validate_entry(row)
            if row["digest"] != q.owner:
                raise _StoreError("corrupt")
        elif row["kind"] == "bulk_receipt":
            receipt = decode_receipt(row)
            if receipt["source_binding"] != q.source_binding:
                raise _StoreError("corrupt")
            if receipt["state"] == "published_owner" and receipt["owner"] != q.owner:
                raise _StoreError("corrupt")
        elif row["kind"] != "receipt":
            raise _StoreError("corrupt")

    def _bound_rows(self, rows):
        """Dedicated stores bind every row to their one qualified owner."""
        for row in rows:
            self._bound_row(row)

    def _owner_rows(self, dataset, owner, pending):
        from .bulk_owner_stream import _DecodedOwnerRow

        q = self.qualification
        if owner != q.owner:
            raise _StoreError("corrupt")
        geometry = self._geometry(dataset)
        count = 0
        receipt_ids = set()
        marker_seen = live_seen = False
        if dataset is not None:
            scanner = dataset.scanner(
                columns=COLUMNS,
                scan_in_order=True,
                limit=q.max_physical_rows + 1,
                batch_size=LOOKUP_IDS,
                strict_batch_size=True,
                batch_readahead=0,
                fragment_readahead=0,
                io_buffer_size=1024**2,
                use_scalar_index=False,
            )
            for batch in scanner.to_batches():
                for row in batch.to_pylist():
                    count += 1
                    if count > q.max_physical_rows:
                        raise _StoreError("unavailable")
                    row = _DecodedOwnerRow(row)
                    if row["kind"] == "bulk_page" and row.header["owner"] != q.owner:
                        raise _StoreError("corrupt")
                    if row["kind"] == "bulk_record" and row["digest"] != q.owner:
                        raise _StoreError("corrupt")
                    if row["kind"] == "bulk_receipt":
                        receipt = row.receipt
                        if receipt["source_binding"] != q.source_binding:
                            raise _StoreError("corrupt")
                        if receipt["state"] == "published_owner":
                            if marker_seen or receipt["owner"] != q.owner:
                                raise _StoreError("corrupt")
                            marker_seen = True
                        else:
                            if row["id"] in receipt_ids or len(receipt_ids) == q.max_batch_receipts:
                                raise _StoreError("corrupt")
                            receipt_ids.add(row["id"])
                    elif row["kind"] == "receipt":
                        if live_seen:
                            raise _StoreError("corrupt")
                        live_seen = True
                    elif row["kind"] not in {"bulk_page", "bulk_record"}:
                        raise _StoreError("corrupt")
                    yield row
        if count != geometry["physical_rows"]:
            raise _StoreError("corrupt")

    def _verify_owner(self, dataset, manifest, pending):
        from .bulk_owner_stream import _verify_decoded_owner_rows

        if (manifest["owner"], manifest["source_binding"]) != (
            self.qualification.owner,
            self.qualification.source_binding,
        ):
            raise _StoreError("corrupt")
        # Task1 admits no foreign-record reuse. A missing pair is corruption;
        # remote reuse needs its own registered qualification in Task3.
        _verify_decoded_owner_rows(
            self._owner_rows(dataset, manifest["owner"], pending),
            manifest,
            pending,
            resolve_reused=lambda ids: {},
        )

    def _before_append(self, dataset, rows):
        q = self.qualification
        geometry = self._mutation_geometry(dataset)
        reserve = q.reservations["append"]
        if (
            len(rows) > reserve["physical_rows"]
            or reserve["fragments"] < 1
            or reserve["versions"] < 1
        ):
            raise _StoreError("unavailable")
        if (
            geometry["physical_rows"] + reserve["physical_rows"] > q.max_physical_rows
            or geometry["fragments"] + reserve["fragments"] > q.max_fragments
            or geometry["version"] + reserve["versions"] > q.max_versions
            or reserve["file_bytes"] > q.max_file_bytes
            or self._artifacts["file_bytes"] > reserve["file_bytes"]
        ):
            raise _StoreError("unavailable")
        info = self._coverage(dataset)
        if (
            info["tail_rows"] + len(rows) > MAX_TAIL_ROWS
            or info["tail_fragments"] + 1 > MAX_TAIL_FRAGMENTS
        ):
            raise _StoreError("index_required")
        for name in ("artifact_entries", "logical_bytes", "allocated_bytes", "versions"):
            if self._artifacts[name] + reserve[name] > getattr(q, "max_" + name):
                raise _StoreError("unavailable")
        try:
            self._bound_rows(rows)
        except (KeyError, TypeError, ValueError, RecursionError):
            raise _StoreError("corrupt") from None
        # Retain the charge until this exact operation is checked. A lost reply
        # cannot reuse its old pin; only its frozen before/after case can reconcile.
        existing_receipts, published = self._batch_receipt_state(dataset)
        if published and any(row["kind"] in {"bulk_page", "bulk_record"} for row in rows):
            raise _StoreError("conflict")
        appended_receipts = sum(
            row["kind"] == "bulk_receipt" and decode_receipt(row)["state"] != "published_owner"
            for row in rows
        )
        resulting_receipts = existing_receipts + appended_receipts
        if resulting_receipts > q.max_batch_receipts:
            raise _StoreError("unavailable")
        self._charged_append = (geometry, dict(self._artifacts), reserve, resulting_receipts)

    def _batch_receipt_state(self, dataset):
        """Bounded receipt metadata proof; never reread owner text for staging."""
        if dataset is None:
            return 0, False
        q = self.qualification
        identities = set()
        marker_seen = False
        scanner = dataset.scanner(
            columns=COLUMNS,
            filter="kind = 'bulk_receipt'",
            scan_in_order=True,
            limit=q.max_batch_receipts + 2,
            batch_size=LOOKUP_IDS,
            strict_batch_size=True,
            batch_readahead=0,
            fragment_readahead=0,
            io_buffer_size=1024**2,
            use_scalar_index=False,
        )
        for batch in scanner.to_batches():
            for row in batch.to_pylist():
                receipt = decode_receipt(row)
                if receipt["source_binding"] != q.source_binding:
                    raise _StoreError("corrupt")
                if receipt["state"] == "published_owner":
                    if marker_seen or receipt["owner"] != q.owner:
                        raise _StoreError("corrupt")
                    marker_seen = True
                else:
                    if row["id"] in identities:
                        raise _StoreError("corrupt")
                    if len(identities) == q.max_batch_receipts:
                        raise _StoreError("unavailable")
                    identities.add(row["id"])
        return len(identities), marker_seen

    def _after_append(self, dataset):
        self._check_charged_operation(dataset, self._charged_append)

    def _check_charged_operation(self, dataset, charge, *, preserve_rows=False):
        self._paths()
        geometry = self._geometry(dataset, expected=False)
        before, artifacts, reserve, resulting_receipts = charge
        if preserve_rows and any(
            geometry[name] != before[name] for name in ("physical_rows", "fragments")
        ):
            raise _StoreError("corrupt")
        for name in ("physical_rows", "fragments"):
            if geometry[name] > before[name] + reserve[name]:
                raise _StoreError("unavailable")
        if geometry["version"] > before["version"] + reserve["versions"]:
            raise _StoreError("unavailable")
        for name in ("artifact_entries", "logical_bytes", "allocated_bytes", "versions"):
            if self._artifacts[name] > artifacts[name] + reserve[name]:
                raise _StoreError("unavailable")
        # file_bytes is the largest resulting retained regular file, not an
        # additive allowance. Retained indexes must fit this frozen charge too.
        if self._artifacts["file_bytes"] > reserve["file_bytes"]:
            raise _StoreError("unavailable")
        if self._batch_receipt_state(dataset)[0] != resulting_receipts:
            raise _StoreError("corrupt")

    def _before_maintenance(self, dataset):
        if dataset is None:
            raise _StoreError("not_initialized")
        geometry = self._mutation_geometry(dataset)
        if geometry["physical_rows"] > MAX_INDEX_ROWS:
            raise _StoreError("maintenance_limit")
        q = self.qualification
        reserve = q.reservations["maintenance"]
        if (
            reserve["versions"] < 1
            or geometry["version"] + reserve["versions"] > q.max_versions
            or geometry["physical_rows"] + reserve["physical_rows"] > q.max_physical_rows
            or geometry["fragments"] + reserve["fragments"] > q.max_fragments
            or self._artifacts["file_bytes"] > reserve["file_bytes"]
        ):
            raise _StoreError("unavailable")
        for name in ("artifact_entries", "logical_bytes", "allocated_bytes", "versions"):
            if self._artifacts[name] + reserve[name] > getattr(q, "max_" + name):
                raise _StoreError("unavailable")
        self._charged_maintenance = (
            geometry,
            dict(self._artifacts),
            reserve,
            self._batch_receipt_state(dataset)[0],
        )

    def _mutation_geometry(self, dataset):
        geometry = self._geometry(dataset)
        states = self.qualification.expected_states
        if len(states) == 2 and geometry == states[1]:
            # The second pin reconciles an existing receipt only. Inherited
            # identical import replay returns before either mutating hook.
            raise _StoreError("stale")
        return geometry

    def _after_maintenance(self, dataset):
        self._check_charged_operation(dataset, self._charged_maintenance, preserve_rows=True)

    def build_id_index(self):
        """Explicit qualified fixture maintenance; ordinary ceiling is unchanged."""
        try:
            with self._writer() as dataset:
                self._before_maintenance(dataset)
                dataset.create_scalar_index("id", "BTREE", name=INDEX_NAME, replace=True)
                # Index creation mutates this handle. Opening again with the
                # pre-operation pin would incorrectly refuse its new version.
                self._after_maintenance(dataset)
                result = self._coverage(dataset)
            return {"status": "ok", "lookup": result}
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}


class _ReplayOutcomeStore(OrderedBulkExactStore):
    """Private fixed-worker transport; ordinary import_batch stores stay v1."""

    def _existing_import_result(
        self, dataset, manifest, receipt_id, batch_id, source_binding, expected_ids
    ):
        from .persisted_replay import existing_outcome

        if source_binding != self.qualification.source_binding:
            raise _StoreError("conflict")
        # Revalidate the registered path after the complete persisted proof,
        # while the existing writer still holds its lock. No second content scan.
        self._paths()
        try:
            result = existing_outcome(
                self._compact(manifest, receipt_id),
                self.directory,
                source_binding,
                batch_id,
                expected_ids,
                dataset.version,
            )
        except ValueError:
            raise _StoreError("corrupt") from None
        witness = result["witness"]
        if (
            tuple(witness["parent_identity"]) != self.qualification.parent_identity
            or tuple(witness["directory_identity"]) != self.qualification.destination_identity
        ):
            raise _StoreError("stale")
        return result

    def _fresh_import_result(self, manifest, receipt_id):
        from .persisted_replay import fresh_outcome

        return fresh_outcome(self._compact(manifest, receipt_id))

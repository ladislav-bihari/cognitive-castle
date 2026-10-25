"""Private atomic exact records and replay receipts; native use is supervised only.

Small operations remain compatible. Bulk visibility and explicit indexed batched
lookups are shared with the private bulk store; no public routes live here.
"""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import re
import stat
import sys

from .canonical_records import CanonicalRecord, _hash, _json, _string, prepare_remember

TASK_RSS = 192 * 1024**2
TOTAL_RSS = 256 * 1024**2
DATASET = "records.lance"
COLUMNS = ["id", "kind", "text", "provenance", "digest", "receipt"]


class _StoreError(Exception):
    def __init__(self, status):
        self.status = status


def _require_supervision(host_pid=None):
    """Reject standalone/native-host storage access before inspecting any path."""
    import psutil
    from .brain_reader_guardian import _group_rss

    if sys.platform != "darwin":
        raise _StoreError("unavailable")
    try:
        guardian = psutil.Process(os.getppid())
        command = guardian.cmdline()
        expected = str(Path(__file__).with_name("brain_reader_guardian.py"))
        if (
            len(command) != 3
            or command[1:] != ["-I", expected]
            or os.getpgrp() != guardian.pid
            or (host_pid is not None and (type(host_pid) is not int or guardian.ppid() != host_pid))
        ):
            raise _StoreError("unavailable")
        task_rss = _group_rss(os.getpgrp())
        if task_rss > TASK_RSS:
            raise _StoreError("worker_memory_limit")
        if task_rss + guardian.parent().memory_info().rss > TOTAL_RSS:
            raise _StoreError("total_memory_limit")
    except (psutil.Error, OSError):
        raise _StoreError("unavailable") from None


def _identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"memory:[a-f0-9]{64}", value):
        raise ValueError("id")
    return value


def _schema():
    import pyarrow as pa

    return pa.schema(
        [pa.field(name, pa.string(), nullable=False) for name in COLUMNS],
        metadata={b"castle_exact_format": b"1"},
    )


class LanceExactStore:
    def __init__(self, directory: Path, *, writable: bool = False):
        if type(writable) is not bool:
            raise ValueError("writable")
        self.directory = Path(directory).absolute()
        self.writable = writable

    @staticmethod
    def _private(path, *, directory=False):
        info = path.lstat()
        expected = stat.S_ISDIR if directory else stat.S_ISREG
        if (
            not expected(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or (not directory and info.st_nlink != 1)
        ):
            raise _StoreError("unavailable")

    def _paths(self, *, create=False):
        _require_supervision()
        self._check_namespace_access()
        if create:
            self._check_mutation_namespace()
        for path in [self.directory, *self.directory.parents]:
            if path.is_symlink():
                raise _StoreError("unavailable")
        if not self.directory.exists():
            if not create:
                raise _StoreError("not_initialized")
            self.directory.mkdir(mode=0o700)
        self._private(self.directory, directory=True)

        # Validate native artifacts without reading record content or following links.
        def visit(directory):
            for path in directory.iterdir():
                info = path.lstat()
                is_directory = stat.S_ISDIR(info.st_mode)
                self._private(path, directory=is_directory)
                if is_directory:
                    visit(path)

        visit(self.directory)
        if any(path.name not in {DATASET, ".write.lock"} for path in self.directory.iterdir()):
            raise _StoreError("unavailable")

    def _open(self):
        self._check_namespace_access()
        import lance

        path = self.directory / DATASET
        if not path.exists():
            return None
        dataset = lance.dataset(
            str(path),
            metadata_cache_size_bytes=1024**2,
            index_cache_size_bytes=1024**2,
            read_params={"cache_repetition_index": False, "validate_on_decode": True},
        )
        if not dataset.schema.equals(_schema(), check_metadata=True):
            raise _StoreError("corrupt")
        return dataset

    @contextmanager
    def _writer(self):
        if not self.writable:
            raise _StoreError("unavailable")
        _require_supervision()
        self._check_mutation_namespace()
        self._paths(create=True)
        fd = os.open(self.directory / ".write.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or info.st_nlink != 1
                or info.st_size
            ):
                raise _StoreError("unavailable")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise _StoreError("busy") from None
            self._check_mutation_namespace()
            yield self._open()
        finally:
            os.close(fd)

    def _check_mutation_namespace(self):
        """Writes additionally require the facade's current mutation authority."""
        self._check_namespace_access()

    def _check_namespace_access(self):
        """Registered homes need a private authority override for reads and writes."""
        # Lexical normalization denies '..' aliases without following symlinks.
        home = Path(os.path.normpath(str(self.directory)))
        if (
            home.parent.name.casefold() != "partitions"
            or home.parent.parent.name.casefold() != "exact"
            or re.fullmatch(r"0|[1-9][0-9]*", home.name) is None
        ):
            return
        generation = home.parent.parent.parent
        canonical_parent = generation / "exact/partitions"
        if home.parent != canonical_parent:
            # Case variants alias on common Darwin volumes, but may denote an
            # unrelated legacy directory on a case-sensitive volume. Compare
            # existing directory identities without authorizing through a link.
            try:
                for parent in (home.parent, canonical_parent):
                    if any(path.is_symlink() for path in (parent, *parent.parents)):
                        raise _StoreError("unavailable")
                actual, canonical = home.parent.lstat(), canonical_parent.lstat()
            except FileNotFoundError:
                return
            if (
                not stat.S_ISDIR(actual.st_mode)
                or not stat.S_ISDIR(canonical.st_mode)
                or (actual.st_dev, actual.st_ino) != (canonical.st_dev, canonical.st_ino)
            ):
                return
        if os.path.lexists(generation / "exact/layout-qualification.json") or os.path.lexists(
            generation / "coordination/queue.sqlite3"
        ):
            raise _StoreError("unavailable")

    def _lookup_rows(self, dataset, ids, *, checked=False):
        """Instance indirection for private registered canonical routing."""
        from .bulk_exact_store import lookup_many

        return lookup_many(dataset, ids, checked=checked)

    @staticmethod
    def _find_many(dataset, identities):
        from .bulk_exact_store import lookup_many

        return lookup_many(dataset, identities)

    @staticmethod
    def _find(dataset, identity):
        return LanceExactStore._find_many(dataset, [identity]).get(identity)

    @staticmethod
    def _record(row):
        try:
            if row["kind"] == "bulk_record":
                from .bulk_exact_codec import validate_entry

                validate_entry(row)
            elif row["kind"] != "record" or row["digest"] != "" or row["receipt"] != "":
                raise ValueError("record kind")
            return CanonicalRecord(row["id"], row["text"], row["provenance"])
        except (KeyError, TypeError, ValueError, RecursionError):
            raise _StoreError("corrupt") from None

    def _visible_source(self, dataset, row):
        if row["kind"] == "record":
            return self._record(row).source()
        from .bulk_exact_store import visible_source

        return visible_source(self, dataset, row)

    def remember(self, request_id, text, provenance=None, *, namespace="operator"):
        prepared = prepare_remember(request_id, text, provenance, namespace=namespace)
        return self._publish(
            namespace, request_id, prepared.digest, prepared.records, prepared.receipt
        )

    def import_sources(self, batch_id, sources, *, namespace="migration"):
        _string(batch_id, 128, "batch_id")
        _string(namespace, 128, "namespace")
        if not isinstance(sources, list) or not 1 <= len(sources) <= 8:
            raise ValueError("sources")
        records = tuple(CanonicalRecord.from_source(source) for source in sources)
        if len({record.id for record in records}) != len(records):
            raise ValueError("duplicate import ids")
        digest = _hash(_json([namespace, batch_id, [record.source() for record in records]]))
        text = "".join(record.text for record in records)
        receipt = {
            "status": "ok",
            "ids": [record.id for record in records],
            "stored_text_bytes": len(text.encode("utf-8")),
            "text_sha256": _hash(text),
        }
        return self._publish(namespace, batch_id, digest, records, receipt)

    def _verify_receipt(self, dataset, old, namespace, key):
        if (
            old["kind"] != "receipt"
            or old["text"] != ""
            or old["provenance"] != ""
            or not re.fullmatch(r"[a-f0-9]{64}", old["digest"] or "")
        ):
            raise _StoreError("corrupt")
        # Validate durable receipt even when the incoming key conflicts.
        import json

        try:
            stored = json.loads(_string(old["receipt"], 4096, "receipt"))
            stored_ids = stored["ids"]
            if (
                not isinstance(stored_ids, list)
                # A 64 KiB remember can need a ninth chunk because
                # UTF-8 characters cannot split at the 8 KiB boundary.
                or not 1 <= len(stored_ids) <= 9
                or len(set(stored_ids)) != len(stored_ids)
            ):
                raise ValueError("receipt ids")
            stored_records = []
            found = self._lookup_rows(dataset, [_identity(identity) for identity in stored_ids])
            for identity in stored_ids:
                row = found.get(identity)
                if row is None:
                    raise ValueError("missing published record")
                stored_records.append(
                    CanonicalRecord.from_source(self._visible_source(dataset, row))
                )
            self._verify_receipt_records(namespace, key, old["digest"], stored, stored_records)
            if old["receipt"] != _json(stored):
                raise ValueError("receipt encoding")
        except (KeyError, TypeError, ValueError, RecursionError):
            raise _StoreError("corrupt") from None
        return stored, tuple(stored_records)

    @staticmethod
    def _verify_receipt_records(namespace, key, digest, stored, records):
        """Shared original receipt/content proof, also used before bulk finalization."""
        try:
            if not 1 <= len(records) <= 9 or len({r.id for r in records}) != len(records):
                raise ValueError("receipt records")
            text = "".join(record.text for record in records)
            verified = {
                "status": "ok",
                "ids": [record.id for record in records],
                "stored_text_bytes": len(text.encode()),
                "text_sha256": _hash(text),
            }
            if stored != verified:
                raise ValueError("receipt content")
            import_digest = _hash(_json([namespace, key, [r.source() for r in records]]))
            valid_digests = {import_digest} if len(records) <= 8 else set()
            prior = prepare_remember(
                key, text, records[0].source()["provenance"]["caller"], namespace=namespace
            )
            if prior.records == tuple(records):
                valid_digests.add(prior.digest)
            if digest not in valid_digests:
                raise ValueError("receipt digest")
        except (KeyError, TypeError, ValueError, RecursionError):
            raise _StoreError("corrupt") from None
        return stored

    def _publish(self, namespace, key, digest, records, receipt):
        receipt_id = "receipt:" + _hash(_json([namespace, key]))
        try:
            with self._writer() as dataset:
                old = self._lookup_rows(dataset, [receipt_id]).get(receipt_id)
                if old:
                    stored, stored_records = self._verify_receipt(dataset, old, namespace, key)
                    if old["digest"] != digest:
                        return {"status": "conflict"}
                    if stored != receipt or tuple(stored_records) != records:
                        raise _StoreError("corrupt")
                    return stored
                rows = []
                found = self._lookup_rows(dataset, [record.id for record in records])
                for record in records:
                    prior = found.get(record.id)
                    if prior is not None:
                        if (
                            CanonicalRecord.from_source(self._visible_source(dataset, prior))
                            != record
                        ):
                            raise _StoreError("corrupt")
                    else:
                        rows.append(
                            {
                                "id": record.id,
                                "kind": "record",
                                "text": record.text,
                                "provenance": record.provenance_json,
                                "digest": "",
                                "receipt": "",
                            }
                        )
                rows.append(
                    {
                        "id": receipt_id,
                        "kind": "receipt",
                        "text": "",
                        "provenance": "",
                        "digest": digest,
                        "receipt": _json(receipt),
                    }
                )
                import lance
                import pyarrow as pa

                # One version atomically publishes all new records with the replay receipt.
                lance.write_dataset(
                    pa.Table.from_pylist(rows, schema=_schema()),
                    str(self.directory / DATASET),
                    mode="create" if dataset is None else "append",
                    max_rows_per_file=1024,
                    max_rows_per_group=1024,
                )
            return receipt
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def receipt(self, namespace, key):
        """Read-only durable proof; identical validation to append replay."""
        _string(namespace, 128, "namespace")
        _string(key, 128, "key")
        try:
            self._paths()
            dataset = self._open()
            receipt_id = "receipt:" + _hash(_json([namespace, key]))
            row = self._lookup_rows(dataset, [receipt_id]).get(receipt_id)
            if row is None:
                return {"status": "not_found"}
            stored, _ = self._verify_receipt(dataset, row, namespace, key)
            return {"status": "ok", "digest": row["digest"], "receipt": stored}
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def read_sources(self, ids):
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8:
            raise ValueError("ids")
        for identity in ids:
            _identity(identity)
        try:
            self._paths()
            dataset = self._open()
            if dataset is None:
                raise _StoreError("not_initialized")
            sources = []
            found = self._lookup_rows(dataset, ids)
            for identity in ids:
                row = found.get(identity)
                if row is None:
                    return {"status": "not_found", "sources": []}
                sources.append(self._visible_source(dataset, row))
            return {"status": "ok", "sources": sources}
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def page(self, source_id, *, offset=0, limit=2048, expected_hash=None):
        if (
            type(offset) is not int
            or offset < 0
            or type(limit) is not int
            or not 1 <= limit <= 2048
        ):
            raise ValueError("page bounds")
        if expected_hash is not None:
            _string(expected_hash, 128, "expected_hash")
        result = self.read_sources([source_id])
        if result["status"] != "ok":
            return result
        source = result["sources"][0]
        if expected_hash is not None and expected_hash != source["provenance"]["text_hash"]:
            return {"status": "stale"}
        text = source["text"]
        source.update(
            status="ok",
            text_hash=source["provenance"]["text_hash"],
            text=text[offset : offset + limit],
            offset=offset,
            next_offset=min(len(text), offset + limit),
            complete=offset + limit >= len(text),
        )
        return source

"""Compact, atomic semantic units and receipts in one versioned Lance dataset.

No model runtime or native dependency is imported by the host. Full ordered
canonical references and scopes must come from the trusted coordinator registry.
"""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import struct

from .canonical_records import _string
from .lance_exact_store import LanceExactStore, _StoreError, _require_supervision
from .semantic_profile import PROFILE_ID, canonical_json, digest, validate_vector
from .semantic_queue import (
    SCOPE_LIMITS,
    validate_hash,
    validate_receipt,
    validate_references,
    validate_scope,
)
from .semantic_windows import MAX_SOURCES, PAGE_SIZE, input_binding, validate_page

DATASET = "units.lance"
COLUMNS = [
    "id",
    "kind",
    "profile",
    "canonical_id",
    "source_hash",
    "exposure_hash",
    "workspace",
    "wing",
    "room",
    "nonsemantic",
    "compact",
    "record_hash",
    "vector_digest",
    "receipt",
    "vector",
]


MAX_BATCH_KEY_BYTES = 128
MAX_TOTAL_UNITS = 1048576


def _proof_limit():
    """Bound every accepted proof, including worst-case JSON escaping.

    Hashes and IDs have fixed ASCII widths. The only free strings are the batch
    key and scopes: any valid UTF-8 input byte needs at most six JSON bytes
    (a control byte becomes \u0000). The shape below includes maximum arrays,
    cursor digit widths, and the larger nonterminal continuation. Empty free
    strings account for their quotes; their contents are bounded separately.
    """
    sha = "0" * 64
    receipt = {
        "status": "ok",
        "receipt_id": "vector-receipt:" + sha,
        "batch_key": "",
        "profile": PROFILE_ID,
        "input_binding": sha,
        "reference_binding": sha,
        "start_window": MAX_TOTAL_UNITS,
        "total_units": MAX_TOTAL_UNITS,
        "count": PAGE_SIZE,
        "next_continuation": {"binding": sha, "next_window": MAX_TOTAL_UNITS},
        "unit_ids": [sha] * PAGE_SIZE,
        "vector_digests": [sha] * PAGE_SIZE,
    }
    reference = {
        "canonical_id": "memory:" + sha,
        "source_hash": sha,
        "exposure_hash": sha,
        "scope": {name: "" for name, _ in SCOPE_LIMITS},
    }
    fixed = len(
        canonical_json(
            {
                "receipt": receipt,
                "references": [reference] * MAX_SOURCES,
                "handoff_hash": sha,
                "records": [{"id": "unit:" + sha, "record_hash": sha}] * PAGE_SIZE,
            }
        ).encode("utf-8")
    )
    return fixed + 6 * (MAX_BATCH_KEY_BYTES + MAX_SOURCES * sum(limit for _, limit in SCOPE_LIMITS))


MAX_PROOF_BYTES = _proof_limit()


def _encode_proof(proof):
    return _string(canonical_json(proof), MAX_PROOF_BYTES, "receipt")


def _float32(vector):
    validate_vector(vector)
    binary = struct.pack("<384f", *vector)
    values = list(struct.unpack("<384f", binary))
    validate_vector(values)
    return values, hashlib.sha256(binary).hexdigest()


def prepare_publication(batch_key, references, page, inference):
    """Pure validated compact handoff; inference JSON spelling is not identity."""
    _string(batch_key, MAX_BATCH_KEY_BYTES, "batch key")
    validate_references(references)
    validate_page(page)
    if page["total_units"] > MAX_TOTAL_UNITS:
        raise ValueError("total units limit")
    fields = {
        "status",
        "profile",
        "handoff_hash",
        "input_binding",
        "start_window",
        "total_units",
        "next_continuation",
        "complete",
        "vectors",
        "runtime",
    }
    if not isinstance(inference, dict) or set(inference) != fields:
        raise ValueError("inference signature")
    for key in fields - {"vectors", "runtime"}:
        if inference[key] != page[key]:
            raise ValueError("inference page mismatch")
    if inference["runtime"] != {
        "onnxruntime": "1.25.1",
        "providers": ["CPUExecutionProvider"],
        "tokenizer_loaded": False,
    }:
        raise ValueError("inference runtime")
    vectors = inference["vectors"]
    if not isinstance(vectors, list) or len(vectors) != len(page["units"]):
        raise ValueError("inference count")
    refs = {reference["canonical_id"]: reference for reference in references}
    rows = []
    seen_units = set()
    for unit, vector in zip(page["units"], vectors):
        if (
            not isinstance(vector, dict)
            or set(vector) != {"unit_id", "vector"}
            or vector["unit_id"] != unit["unit_id"]
        ):
            raise ValueError("inference unit")
        if unit["unit_id"] in seen_units:
            raise ValueError("duplicate page unit")
        seen_units.add(unit["unit_id"])
        reference = refs.get(unit["canonical_id"])
        if reference is None or reference["source_hash"] != unit["source_hash"]:
            raise ValueError("unit outside bound references")
        values, vector_hash = _float32(vector["vector"])
        compact = {
            key: value for key, value in unit.items() if key not in {"tokens", "token_offsets"}
        }
        compact.update(exposure_hash=reference["exposure_hash"], scope=reference["scope"])
        identity = "unit:" + digest(compact)
        record_hash = digest([compact, vector_hash])
        rows.append(
            {
                "id": identity,
                "kind": "unit",
                "profile": PROFILE_ID,
                "canonical_id": unit["canonical_id"],
                "source_hash": unit["source_hash"],
                "exposure_hash": reference["exposure_hash"],
                **reference["scope"],
                "nonsemantic": unit["nonsemantic"],
                "compact": canonical_json(compact),
                "record_hash": record_hash,
                "vector_digest": vector_hash,
                "receipt": "",
                "vector": values,
            }
        )
    receipt = {
        "status": "ok",
        "receipt_id": "vector-receipt:" + digest([batch_key, page["start_window"]]),
        "batch_key": batch_key,
        "profile": PROFILE_ID,
        "input_binding": page["input_binding"],
        "reference_binding": digest(references),
        "start_window": page["start_window"],
        "total_units": page["total_units"],
        "count": len(rows),
        "next_continuation": page["next_continuation"],
        "unit_ids": [unit["unit_id"] for unit in page["units"]],
        "vector_digests": [row["vector_digest"] for row in rows],
    }
    validate_receipt(receipt)
    proof = {
        "receipt": receipt,
        "references": references,
        "handoff_hash": page["handoff_hash"],
        "records": [{"id": row["id"], "record_hash": row["record_hash"]} for row in rows],
    }
    return rows, proof


def _schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field(
                name,
                pa.bool_()
                if name == "nonsemantic"
                else pa.list_(pa.float32(), 384)
                if name == "vector"
                else pa.string(),
                nullable=False,
            )
            for name in COLUMNS
        ],
        metadata={b"castle_semantic_format": b"1", b"profile": PROFILE_ID.encode()},
    )


class LanceVectorStore:
    def __init__(self, directory: Path, *, writable=False, exact_layout=False):
        if type(writable) is not bool or type(exact_layout) is not bool:
            raise ValueError("writable or exact layout")
        self.generation = Path(directory).absolute()
        self.directory = self.generation / "vectors"
        self.writable = writable
        self.exact_layout = exact_layout

    def _generation_paths(self):
        _require_supervision()
        for path in (self.generation, *self.generation.parents):
            if path.is_symlink():
                raise _StoreError("unavailable")
        LanceExactStore._private(self.generation, directory=True)

    def _paths(self, create=False):
        self._generation_paths()
        if not self.directory.exists():
            if not create:
                raise _StoreError("not_initialized")
            self.directory.mkdir(mode=0o700)
        LanceExactStore._private(self.directory, directory=True)

        def visit(directory):
            for path in directory.iterdir():
                is_directory = path.is_dir() and not path.is_symlink()
                LanceExactStore._private(path, directory=is_directory)
                if is_directory:
                    visit(path)

        visit(self.directory)
        if any(path.name not in {DATASET, ".write.lock"} for path in self.directory.iterdir()):
            raise _StoreError("unavailable")

    def _open(self):
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
        self._paths(True)
        fd = os.open(self.directory / ".write.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            LanceExactStore._private(self.directory / ".write.lock")
            if os.fstat(fd).st_size:
                raise _StoreError("unavailable")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise _StoreError("busy") from None
            yield self._open()
        finally:
            os.close(fd)

    @staticmethod
    def _scanner(dataset, **kwargs):
        return dataset.scanner(
            columns=COLUMNS,
            batch_size=32,
            strict_batch_size=True,
            batch_readahead=0,
            fragment_readahead=0,
            io_buffer_size=1024**2,
            use_scalar_index=False,
            **kwargs,
        )

    def _find(self, dataset, identity):
        if not isinstance(identity, str) or not re.fullmatch(
            r"(?:unit|vector-receipt):[a-f0-9]{64}", identity
        ):
            raise ValueError("internal identity")
        if dataset is None:
            return None
        import pyarrow.compute as pc

        rows = (
            self._scanner(dataset, filter=pc.field("id") == identity, limit=2)
            .to_table()
            .to_pylist()
        )
        if len(rows) > 1:
            raise _StoreError("corrupt")
        return rows[0] if rows else None

    def _sources(self, references, expected_binding):
        from .semantic_intents import source_exposure_hash

        ids = [reference["canonical_id"] for reference in references]
        if self.exact_layout:
            from .partitioned_exact_store import PartitionedExactStore

            result = PartitionedExactStore(self.generation).layout_read(
                "read_sources", {"ids": ids}
            )
        else:
            result = LanceExactStore(self.generation / "exact").read_sources(ids)
        if result["status"] != "ok":
            raise _StoreError(
                "stale"
                if result["status"] in {"not_initialized", "not_found"}
                else result["status"]
            )
        envelopes = []
        for reference, source in zip(references, result["sources"]):
            if (
                source["id"] != reference["canonical_id"]
                or source["provenance"]["text_hash"] != reference["source_hash"]
                or source_exposure_hash(source) != reference["exposure_hash"]
            ):
                raise _StoreError("stale")
            envelopes.append({"source": source, "expected_hash": reference["source_hash"]})
        if input_binding(envelopes) != expected_binding:
            raise _StoreError("stale")
        return {source["id"]: source for source in result["sources"]}

    @staticmethod
    def _unit(row):
        try:
            if row["kind"] != "unit" or row["profile"] != PROFILE_ID or row["receipt"] != "":
                raise ValueError("unit kind")
            compact = json.loads(row["compact"])
            vector, vector_hash = _float32(row["vector"])
            if (
                row["compact"] != canonical_json(compact)
                or row["id"] != "unit:" + digest(compact)
                or row["record_hash"] != digest([compact, vector_hash])
                or row["vector_digest"] != vector_hash
            ):
                raise ValueError("compact digest")
            fields = {
                "canonical_id",
                "source_hash",
                "profile",
                "token_start",
                "token_end",
                "char_start",
                "char_end",
                "source_length",
                "chunk_start",
                "absolute_start",
                "absolute_end",
                "nonsemantic",
                "input_token_hash",
                "unit_id",
                "exposure_hash",
                "scope",
            }
            if set(compact) != fields:
                raise ValueError("compact fields")
            validate_hash(compact["unit_id"])
            validate_hash(compact["input_token_hash"])
            validate_scope(compact["scope"])
            for name in ("canonical_id", "source_hash", "exposure_hash", "profile", "nonsemantic"):
                if row[name] != compact[name]:
                    raise ValueError("compact projection")
            if any(row[name] != compact["scope"][name] for name in ("workspace", "wing", "room")):
                raise ValueError("scope projection")
            for name in (
                "token_start",
                "token_end",
                "char_start",
                "char_end",
                "source_length",
                "chunk_start",
                "absolute_start",
                "absolute_end",
            ):
                if type(compact[name]) is not int or compact[name] < 0:
                    raise ValueError("offset")
            if (
                not 0 <= compact["token_start"] <= compact["token_end"]
                or not 0
                <= compact["char_start"]
                <= compact["char_end"]
                <= compact["source_length"]
                <= 8192
                or compact["absolute_start"] != compact["chunk_start"] + compact["char_start"]
                or compact["absolute_end"] != compact["chunk_start"] + compact["char_end"]
                or type(compact["nonsemantic"]) is not bool
                or compact["nonsemantic"] != (compact["token_start"] == compact["token_end"])
            ):
                raise ValueError("offset identity")
            return compact
        except (ValueError, TypeError, KeyError, RecursionError):
            raise _StoreError("corrupt") from None

    @staticmethod
    def _source_unit(compact, source):
        if (
            compact["source_length"] != len(source["text"])
            or compact["chunk_start"] != source["provenance"]["chunk_start"]
            or compact["source_hash"] != source["provenance"]["text_hash"]
        ):
            raise _StoreError("stale")

    def _receipt(self, dataset, identity):
        row = self._find(dataset, identity)
        if row is None:
            return None
        try:
            proof = json.loads(_string(row["receipt"], MAX_PROOF_BYTES, "receipt"))
            if (
                set(proof) != {"receipt", "references", "handoff_hash", "records"}
                or row["kind"] != "receipt"
                or row["profile"] != PROFILE_ID
                or row["record_hash"] != digest(proof)
                or row["receipt"] != canonical_json(proof)
            ):
                raise ValueError("receipt proof")
            receipt = validate_receipt(proof["receipt"])
            references = validate_references(proof["references"])
            validate_hash(proof["handoff_hash"])
            if receipt["reference_binding"] != digest(references):
                raise ValueError("reference binding")
            if (
                identity != receipt["receipt_id"]
                or identity
                != "vector-receipt:" + digest([receipt["batch_key"], receipt["start_window"]])
                or not isinstance(proof["records"], list)
                or len(proof["records"]) != receipt["count"]
            ):
                raise ValueError("receipt identity")
            if (
                any(
                    row[key] != ""
                    for key in (
                        "canonical_id",
                        "source_hash",
                        "exposure_hash",
                        "workspace",
                        "wing",
                        "room",
                        "compact",
                        "vector_digest",
                    )
                )
                or row["nonsemantic"] is not True
                or row["vector"] != [0.0] * 384
            ):
                raise ValueError("receipt projection")
            sources = self._sources(references, receipt["input_binding"])
            refs = {ref["canonical_id"]: ref for ref in references}
            for index, record in enumerate(proof["records"]):
                if not isinstance(record, dict) or set(record) != {"id", "record_hash"}:
                    raise ValueError("receipt record")
                stored = self._find(dataset, record["id"])
                if stored is None or stored["record_hash"] != record["record_hash"]:
                    raise ValueError("missing compact record")
                compact = self._unit(stored)
                ref = refs[compact["canonical_id"]]
                if (
                    compact["unit_id"] != receipt["unit_ids"][index]
                    or stored["vector_digest"] != receipt["vector_digests"][index]
                    or compact["scope"] != ref["scope"]
                    or compact["exposure_hash"] != ref["exposure_hash"]
                ):
                    raise ValueError("receipt unit binding")
                self._source_unit(compact, sources[compact["canonical_id"]])
            return proof
        except (ValueError, TypeError, KeyError, RecursionError):
            raise _StoreError("corrupt") from None

    def publish(self, batch_key, references, page, inference):
        rows, proof = prepare_publication(batch_key, references, page, inference)
        encoded_proof = _encode_proof(proof)
        try:
            self._generation_paths()
            sources = self._sources(references, page["input_binding"])
            for row in rows:
                self._source_unit(self._unit(row), sources[row["canonical_id"]])
            with self._writer() as dataset:
                old = self._receipt(dataset, proof["receipt"]["receipt_id"])
                if old is not None:
                    return old["receipt"] if old == proof else {"status": "conflict"}
                additions = []
                for row in rows:
                    prior = self._find(dataset, row["id"])
                    if prior is None:
                        additions.append(row)
                    elif self._unit(prior) != self._unit(row) or prior != row:
                        raise _StoreError("corrupt")
                additions.append(
                    {
                        "id": proof["receipt"]["receipt_id"],
                        "kind": "receipt",
                        "profile": PROFILE_ID,
                        **{
                            key: ""
                            for key in (
                                "canonical_id",
                                "source_hash",
                                "exposure_hash",
                                "workspace",
                                "wing",
                                "room",
                                "compact",
                                "vector_digest",
                            )
                        },
                        "nonsemantic": True,
                        "record_hash": digest(proof),
                        "receipt": encoded_proof,
                        "vector": [0.0] * 384,
                    }
                )
                import lance
                import pyarrow as pa

                lance.write_dataset(
                    pa.Table.from_pylist(additions, schema=_schema()),
                    str(self.directory / DATASET),
                    mode="create" if dataset is None else "append",
                    max_rows_per_file=1024,
                    max_rows_per_group=1024,
                )
            return proof["receipt"]
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def receipt(self, batch_key, start_window):
        _string(batch_key, MAX_BATCH_KEY_BYTES, "batch key")
        if (
            type(start_window) is not int
            or not 0 <= start_window < MAX_TOTAL_UNITS
            or start_window % 32
        ):
            raise ValueError("page start")
        try:
            self._paths()
            proof = self._receipt(
                self._open(), "vector-receipt:" + digest([batch_key, start_window])
            )
            return proof["receipt"] if proof else {"status": "not_found"}
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

    def nearest(
        self,
        vector,
        profile,
        generation,
        scope,
        allowed_source_hashes,
        limit,
        *,
        allowed_exposure_hashes=None,
    ):
        """Restrict text hashes and full-source exposure grants independently."""
        vector, _ = _float32(vector)
        validate_scope(scope)
        if (
            profile != PROFILE_ID
            or (generation is not None and (type(generation) is not int or generation < 1))
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise ValueError("query profile generation or limit")
        for grants in (allowed_source_hashes, allowed_exposure_hashes):
            if grants is None:
                continue
            if not isinstance(grants, list) or len(grants) > 256:
                raise ValueError("grants")
            for value in grants:
                validate_hash(value)
        try:
            self._paths()
            dataset = self._open()
            if dataset is None:
                raise _StoreError("not_initialized")
            if generation is not None and dataset.version != generation:
                return {"status": "stale"}
            if allowed_source_hashes == [] or allowed_exposure_hashes == []:
                return {
                    "status": "ok",
                    "profile": PROFILE_ID,
                    "generation": dataset.version,
                    "candidates": [],
                }
            import pyarrow.compute as pc

            predicate = (
                (pc.field("kind") == "unit")
                & (pc.field("profile") == profile)
                & (pc.field("nonsemantic") == False)  # noqa: E712
            )
            for name in ("workspace", "wing", "room"):
                predicate = predicate & (pc.field(name) == scope[name])
            if allowed_source_hashes is not None:
                predicate = predicate & pc.field("source_hash").isin(allowed_source_hashes)
            if allowed_exposure_hashes is not None:
                predicate = predicate & pc.field("exposure_hash").isin(allowed_exposure_hashes)
            rows = (
                self._scanner(
                    dataset,
                    filter=predicate,
                    prefilter=True,
                    nearest={
                        "column": "vector",
                        "q": vector,
                        "k": limit,
                        "metric": "l2",
                        "use_index": False,
                    },
                    limit=limit,
                )
                .to_table()
                .to_pylist()
            )
            candidates = []
            for row in rows:
                compact = self._unit(row)
                # Defensive projection check; ranking already applied the typed policy.
                if (
                    compact["nonsemantic"]
                    or compact["scope"] != scope
                    or (
                        allowed_source_hashes is not None
                        and compact["source_hash"] not in allowed_source_hashes
                    )
                    or (
                        allowed_exposure_hashes is not None
                        and compact["exposure_hash"] not in allowed_exposure_hashes
                    )
                ):
                    raise _StoreError("corrupt")
                candidates.append(
                    {
                        key: value
                        for key, value in compact.items()
                        if key not in {"nonsemantic", "scope"}
                    }
                    | {"distance": row["_distance"]}
                )
            return {
                "status": "ok",
                "profile": PROFILE_ID,
                "generation": dataset.version,
                "candidates": candidates,
            }
        except _StoreError as exc:
            return {"status": exc.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

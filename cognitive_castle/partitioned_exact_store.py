"""Inactive qualified exact facade. Routes locate evidence; they never prove it.

Only registered metadata selects paths, versions and resource reservations.
Each operation retains its home dataset and at most one remote dataset. Bodies
exist only in the current request, strict scanner batch or bounded lookup.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import stat
import tempfile

from . import bulk_exact_codec as bulk
from . import lance_layout_codec as layout
from . import migration_codec as migration
from . import lance_layout_packed as packed
from .bulk_exact_store import (
    OrderedBulkExactStore,
    LOOKUP_IDS,
    INDEX_NAME,
    COLUMNS,
    coverage,
    lookup_many,
    decode_receipt,
    visible_source,
)
from .bulk_owner_stream import OrderedQualification, _DecodedOwnerRow, _verify_decoded_owner_rows
from .canonical_records import _hash, _json, _string
from .lance_exact_store import _StoreError

_READ_FIELDS = {
    "read_sources": ({"ids"}, set()),
    "page": ({"source_id"}, {"offset", "limit", "expected_hash"}),
    "receipt": ({"namespace", "key"}, set()),
    "bulk_owner": ({"owner"}, {"version"}),
    "bulk_page": ({"owner", "field", "ordinal"}, {"version"}),
    "bulk_receipt": ({"batch_id", "source_binding"}, {"offset", "version"}),
    "directory_page": ({"partition_id", "version", "cursor"}, set()),
}


def _temporary_charge(authority):
    path = Path(authority.envelope["generation"]["path"]) / "exact/tmp"
    if any(parent.is_symlink() for parent in (path, *path.parents)):
        raise _StoreError("unavailable")
    maximum = authority.qualification["working_temp_bytes"]
    if maximum > 64 * 1024**2:
        raise _StoreError("unavailable")
    logical = allocated = count = 0
    stack = []

    def account(file, depth):
        nonlocal logical, allocated, count
        info = file.lstat()
        directory = stat.S_ISDIR(info.st_mode)
        if (
            (not directory and not stat.S_ISREG(info.st_mode))
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or (not directory and info.st_nlink != 1)
        ):
            raise _StoreError("unavailable")
        count += 1
        logical += info.st_size
        allocated += info.st_blocks * 512
        if (
            count > authority.qualification["partition"]["max_artifact_entries"]
            or depth > authority.qualification["partition"]["max_artifact_depth"]
        ):
            raise _StoreError("unavailable")
        if directory:
            stack.append((os.scandir(file), depth + 1))

    account(path, 0)
    try:
        while stack:
            iterator, depth = stack[-1]
            item = next(iterator, None)
            if item is None:
                iterator.close()
                stack.pop()
            else:
                account(Path(item.path), depth)
    finally:
        for iterator, _ in stack:
            iterator.close()
    if max(logical, allocated) + maximum > authority.envelope["limits"]["temp_bytes"]:
        raise _StoreError("unavailable")
    growth = max(
        max(c["logical_bytes"], c["allocated_bytes"])
        for c in authority.qualification["reservations"].values()
    )
    if shutil.disk_usage(path).free < maximum + growth:
        raise _StoreError("unavailable")
    os.environ["LANCE_MEM_POOL_SIZE"] = str(16 * 1024**2)
    os.environ["LANCE_MAX_TEMP_DIRECTORY_SIZE"] = str(maximum)
    os.environ.pop("LANCE_BYPASS_SPILLING", None)
    os.environ["TMPDIR"] = str(path)
    tempfile.tempdir = None


def _geometry(pin):
    return {name: pin[name] for name in ("version", "physical_rows", "fragments")}


def _route_ids(operation, arguments):
    if operation == "read_sources":
        ids = arguments["ids"]
        if not isinstance(ids, list) or not 1 <= len(ids) <= 8:
            raise ValueError("layout read IDs")
        for identity in ids:
            bulk.identity(identity, "memory")
        return list(dict.fromkeys(ids))
    if operation == "page":
        return [bulk.identity(arguments["source_id"], "memory")]
    if operation == "receipt":
        for key in ("namespace", "key"):
            _string(arguments[key], 128, key)
        return ["receipt:" + _hash(_json([arguments["namespace"], arguments["key"]]))]
    if operation == "bulk_owner":
        return [bulk.identity(arguments["owner"])]
    if operation == "bulk_page":
        return [
            bulk.identity(arguments["owner"]),
            bulk.page_id(arguments["owner"], arguments["field"], arguments["ordinal"]),
        ]
    if operation == "bulk_receipt":
        _string(arguments["batch_id"], 128, "batch id")
        _string(arguments["source_binding"], 1024, "source binding")
        return [bulk.key("batch", arguments["batch_id"])]
    return []


class PartitionedExactStore:
    def __init__(self, generation, *, writable=False):
        if type(writable) is not bool:
            raise ValueError("writable")
        self.generation = Path(generation)
        if (
            not self.generation.is_absolute()
            or ".." in self.generation.parts
            or str(self.generation) != str(self.generation.absolute())
        ):
            raise ValueError("layout generation")
        self.writable = writable

    @contextmanager
    def _authority(self):
        from . import lance_layout_directory as directory

        with directory.readonly_directory(self.generation) as base:
            authority = (
                base
                if isinstance(base, directory._NativeLayoutDirectory)
                else directory._NativeLayoutDirectory(base.conn)
            )
            if authority.envelope["generation"]["path"] != str(self.generation):
                raise _StoreError("unavailable")
            _temporary_charge(authority)
            try:
                yield authority
            finally:
                _temporary_charge(authority)

    def _run(self, action):
        try:
            with self._authority() as authority:
                return action(authority)
        except _StoreError as error:
            return {"status": error.status}
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, RecursionError):
            return {"status": "unavailable"}

    def layout_import_batch(self, intent_id, batch_id, source_binding, entries, finalizations):
        bulk.sha(intent_id)
        digest = bulk.validate_batch(batch_id, source_binding, entries, finalizations)
        if intent_id != batch_id:
            raise ValueError("layout batch/intent identity")
        if not self.writable:
            return {"status": "unavailable"}

        def publish(authority):
            pending = authority.pending(intent_id)
            if (
                pending["digest"] != digest
                or pending["source_binding"] != source_binding
                or pending["entries"] != len(entries)
            ):
                raise _StoreError("conflict")
            candidates = {row["id"]: row for row in entries}
            for owner in finalizations:
                candidates[owner["owner"]] = bulk.receipt_row(owner, owner["owner"])
                if owner["live"] is not None:
                    live = owner["live"]
                    identity = "receipt:" + _hash(_json([live["namespace"], live["key"]]))
                    candidates[identity] = dict(
                        id=identity,
                        kind="receipt",
                        text="",
                        provenance="",
                        digest=live["digest"],
                        receipt=_json(live["receipt"]),
                    )
            expected = {r["id"]: r for r in pending["routes"][:-1]}
            if candidates.keys() != expected.keys() or any(
                migration.hashed(row) != expected[identity]["row_hash"]
                for identity, row in candidates.items()
            ):
                raise _StoreError("conflict")
            owner = pending["routes"][0]["gate"]
            store = _PartitionStore(
                authority,
                pending["target"]["partition_id"],
                owner,
                source_binding,
                intent_id=intent_id,
                writable=True,
            )
            try:
                return store.import_batch(batch_id, source_binding, entries, finalizations)
            finally:
                store.release_remote()

        return self._run(publish)

    def layout_read(self, operation, arguments, *, context=None):
        if operation not in _READ_FIELDS or not isinstance(arguments, dict):
            raise ValueError("layout read operation")
        required, optional = _READ_FIELDS[operation]
        if not required <= arguments.keys() or arguments.keys() - required - optional:
            raise ValueError("layout read arguments")
        if context is not None:
            if (
                operation not in {"bulk_owner", "receipt"}
                or not isinstance(context, dict)
                or context.keys() != {"intent_id", "version"}
            ):
                raise ValueError("layout proof context")
            bulk.sha(context["intent_id"])
            layout.positive(context["version"])
        identities = _route_ids(operation, arguments)
        if operation == "directory_page":
            for key in ("partition_id", "version", "cursor"):
                bulk.integer(arguments[key])
        return self._run(
            lambda authority: self._read(authority, operation, arguments, identities, context)
        )

    def _read(self, authority, operation, arguments, identities, context):
        if operation == "directory_page":
            return self._directory_page(authority, **arguments)
        if context is not None:
            pending = authority.pending(context["intent_id"])
            owner_refs = pending["owners"]
            if pending.get("format") != packed.INTENT and len(owner_refs) != 1:
                raise _StoreError("conflict")
            if operation == "bulk_owner":
                matching = [
                    ref
                    for ref in owner_refs
                    if arguments == {"owner": ref["owner"], "version": context["version"]}
                ]
            else:
                matching = [
                    ref
                    for ref in owner_refs
                    if ref["classification"] == "live"
                    and ref["source_key"] == [arguments["namespace"], arguments["key"]]
                ]
            if len(matching) != 1:
                raise _StoreError("conflict")
            pid = pending["target"]["partition_id"]
            owner = matching[0]["owner"]
            source_binding = pending["source_binding"]
        else:
            resolved = authority.resolve(identities)
            if resolved["status"] != "ok":
                return {"status": resolved["status"]}
            if resolved["absent"]:
                return {
                    "status": "not_found",
                    **({"sources": []} if operation == "read_sources" else {}),
                }
            route = resolved["routes"][0]
            pid, owner = route["partition_id"], route["gate"]
            source_binding = authority.owner_plan(owner)["source_binding"]
        store = _PartitionStore(
            authority,
            pid,
            owner,
            source_binding,
            intent_id=context["intent_id"] if context else None,
            proof_version=context["version"] if context else None,
        )
        try:
            return getattr(store, operation)(**arguments)
        finally:
            store.release_remote()

    def layout_maintain(self, partition_id):
        bulk.integer(partition_id)
        if not self.writable:
            return {"status": "unavailable"}

        def maintain(authority):
            store = _PartitionStore(
                authority,
                partition_id,
                "receipt:" + authority.envelope["qualification_hash"],
                authority.envelope["qualification_hash"],
                writable=True,
                maintenance=True,
            )
            try:
                return store.maintain_registered_index()
            finally:
                store.release_remote()

        return self._run(maintain)

    def _observe_owner(self, store, dataset, pending, reference):
        """Observe original marker and LIVE receipt on the already opened home."""
        pid, owner = pending["target"]["partition_id"], reference["owner"]
        version = dataset.version
        identities = [owner]
        if reference["classification"] == "live":
            identities.append("receipt:" + migration.hashed(reference["source_key"]))
        # Owner and receipt observations have no foreign/older route
        # exception. Canonical record hydration below retains its usual
        # bounded resolver and original first-owner visibility checks.
        resolved = store._resolve_ids(identities)
        locations = {row["id"]: row for row in [*resolved["routes"], *resolved["claims"]]}
        if resolved["status"] != "ok" or set(locations) != set(identities):
            raise _StoreError("unavailable")
        if any(
            row["partition_id"] != pid
            or row["gate"] != owner
            or row.get("version") not in (None, version)
            for row in locations.values()
        ):
            raise _StoreError("conflict")
        found = store._lookup_rows(dataset, identities, checked=True)
        if set(found) != set(identities):
            raise _StoreError("corrupt")
        manifest = decode_receipt(found[owner])
        try:
            observed_reference = migration.owner_reference(
                manifest, reference["source_phase"], reference["source_key"]
            )
        except (ValueError, TypeError, KeyError):
            raise _StoreError("corrupt") from None
        if (
            manifest["state"] != "published_owner"
            or manifest["source_binding"] != pending["source_binding"]
            or observed_reference != reference
        ):
            raise _StoreError("corrupt")
        live = manifest["live"]
        live_proof = None
        if live is not None:
            if reference["source_key"] != [live["namespace"], live["key"]]:
                raise _StoreError("corrupt")
            row = found[identities[1]]
            stored, _ = store._verify_receipt(dataset, row, live["namespace"], live["key"])
            if row["digest"] != live["digest"] or stored != live["receipt"]:
                raise _StoreError("corrupt")
            live_proof = dict(
                namespace=live["namespace"],
                key=live["key"],
                digest=row["digest"],
                receipt_hash=migration.hashed(stored),
            )
        return dict(
            owner=manifest["owner"],
            manifest_hash=migration.hashed(manifest),
            partition_id=pid,
            version=dataset.version,
            live_receipt_ids=[] if live is None else list(stored["ids"]),
            live=live_proof,
        ), manifest

    def layout_verify_owners(self, intent_id, version):
        """Observe every original owner on one handle; import replay is separate."""
        bulk.sha(intent_id)
        layout.positive(version)

        def verify(authority):
            pending = authority.pending(intent_id)
            is_packed = pending.get("format") == packed.INTENT
            if is_packed:
                packed.validate_prefix_intent(
                    pending,
                    migration.registration(pending["window"]["descriptor"], pending["destination"]),
                )
            elif len(pending["owners"]) != 1:
                raise _StoreError("conflict")
            store = _PartitionStore(
                authority,
                pending["target"]["partition_id"],
                pending["owners"][0]["owner"],
                pending["source_binding"],
                intent_id=intent_id,
                proof_version=version,
            )
            try:
                dataset, _ = store._read(version=version)
                observations, manifests = [], []
                for reference in pending["owners"]:
                    observation, manifest = self._observe_owner(store, dataset, pending, reference)
                    observations.append(observation)
                    manifests.append(manifest)
                result = dict(
                    status="ok",
                    format=layout.GROUPED_OWNER_PROOF,
                    intent_hash=migration.hashed(pending),
                    owners=observations,
                )
                validator = (
                    packed.validate_grouped_owner_proof
                    if is_packed
                    else layout.validate_grouped_owner_proof
                )
                validator(result, pending, {"finalizations": manifests}, version)
                return result
            finally:
                store.release_remote()

        return self._run(verify)

    def _directory_page(self, authority, partition_id, version, cursor):
        store = _PartitionStore(
            authority,
            partition_id,
            "receipt:" + authority.envelope["qualification_hash"],
            authority.envelope["qualification_hash"],
            enumerating=True,
        )
        try:
            dataset = store._open()
            actual = store.actual_pin(dataset)
            if actual["version"] != version or cursor > actual["physical_rows"]:
                raise _StoreError("stale")
            count = min(LOOKUP_IDS, actual["physical_rows"] - cursor)
            rows = []
            if count:
                scanner = dataset.scanner(
                    columns=COLUMNS,
                    scan_in_order=True,
                    offset=cursor,
                    limit=count,
                    batch_size=LOOKUP_IDS,
                    strict_batch_size=True,
                    batch_readahead=0,
                    fragment_readahead=0,
                    io_buffer_size=1024**2,
                    use_scalar_index=False,
                )
                for batch in scanner.to_batches():
                    for raw in batch.to_pylist():
                        _DecodedOwnerRow(raw)
                        claim = authority._claim(raw["id"])
                        if claim is None or claim["partition_id"] != partition_id:
                            raise _StoreError("unavailable")
                        if claim["state"] == "confirmed":
                            route = authority.claim_route(raw["id"])
                        else:
                            mutation = store.registry["mutation"]
                            if (
                                mutation is None
                                or mutation["kind"] != "append"
                                or _geometry(actual) != mutation["after"]
                                or claim["intent_id"] != mutation["intent_id"]
                                or raw["id"] not in mutation["new_ids"]
                            ):
                                raise _StoreError("unavailable")
                            receipt_id = bulk.key("batch", mutation["intent_id"])
                            proof = store._context_batch(dataset)
                            if raw["id"] != receipt_id and proof.get(raw["id"]) != migration.hashed(
                                raw
                            ):
                                raise _StoreError("corrupt")
                            route = dict(
                                format=layout.ROUTE,
                                id=raw["id"],
                                kind=raw["kind"],
                                row_hash=migration.hashed(raw),
                                partition_id=partition_id,
                                gate=claim["gate"],
                                version=actual["version"],
                                receipt=receipt_id,
                                state="staged",
                            )
                        if (
                            route is None
                            or route["row_hash"] != migration.hashed(raw)
                            or route["kind"] != claim["kind"]
                            or route["partition_id"] != partition_id
                            or claim["row_hash"] is not None
                            and route["row_hash"] != claim["row_hash"]
                        ):
                            raise _StoreError("corrupt")
                        rows.append(layout.validate_route(route))
            if len(rows) != count:
                raise _StoreError("corrupt")
            registry = authority.partition(partition_id)
            page = dict(
                format="lance-layout-directory-page-v1",
                generation=authority.envelope["generation"],
                qualification_hash=authority.envelope["qualification_hash"],
                epoch=authority.epoch,
                rebuild_token=registry["rebuild_token"],
                partition_id=partition_id,
                version=version,
                cursor_before=cursor,
                cursor_after=cursor + count,
                pin=actual,
                rows=rows,
                complete=cursor + count == actual["physical_rows"],
            )
            return {
                "status": "ok",
                "page": layout.validate_directory_page(
                    page, authority.qualification, authority.envelope
                ),
            }
        finally:
            store.release_remote()


class _PartitionStore(OrderedBulkExactStore):
    """One selected physical home; the registry supplies the owner scan span."""

    def __init__(
        self,
        authority,
        pid,
        owner,
        source_binding,
        *,
        intent_id=None,
        proof_version=None,
        writable=False,
        maintenance=False,
        enumerating=False,
    ):
        self.authority, self.pid, self.intent_id = authority, pid, intent_id
        self.proof_version, self.maintenance, self.enumerating = (
            proof_version,
            maintenance,
            enumerating,
        )
        self.registry = authority.partition(pid)
        pin = self.registry["pin"]
        layout.validate_partition_pin(pin, authority.qualification, authority.envelope)
        mutation = self.registry["mutation"]
        states = [_geometry(pin)]
        if mutation is not None and (intent_id is not None or maintenance or enumerating):
            if intent_id is not None and mutation["intent_id"] != intent_id:
                raise _StoreError("conflict")
            states = [_geometry(mutation["before"]), mutation["after"]]
        if proof_version is not None:
            if mutation is None or proof_version != mutation["after"]["version"]:
                raise _StoreError("stale")
            states = [mutation["after"]]
        policy = authority.qualification
        charges = {op: dict(charge) for op, charge in policy["reservations"].items()}
        if mutation is not None and mutation["kind"] == "append":
            charges["append"]["physical_rows"] = len(mutation["new_ids"])
        manifest = dict(
            format="ordered-owner-qualification-v1",
            destination=str(
                Path(authority.envelope["generation"]["path"]) / "exact/partitions" / str(pid)
            ),
            parent_identity=pin["parent_identity"],
            destination_identity=pin["directory_identity"],
            owner=owner,
            source_binding=source_binding,
            schema=policy["schema"],
            storage_mode=policy["storage_mode"],
            expected_states=states,
            reservations=charges,
            **policy["partition"],
        )
        q = OrderedQualification.from_manifest(manifest, expected_digest=migration.hashed(manifest))
        super().__init__(Path(manifest["destination"]), qualification=q, writable=writable)
        self.remote_pid = self.remote_store = self.remote_dataset = None
        self.remote_opens = 0
        self._requested_entries = None

    def import_batch(self, batch_id, source_binding, entries, finalizations):
        self._requested_entries = {row["id"]: row for row in entries}
        try:
            return super().import_batch(batch_id, source_binding, entries, finalizations)
        finally:
            self._requested_entries = None

    def _check_namespace_access(self):
        from .lance_layout_directory import load_layout_qualification

        policy = load_layout_qualification(
            self.authority.envelope["generation"], self.authority.envelope["qualification_hash"]
        )
        if policy != self.authority.qualification:
            raise _StoreError("unavailable")
        current = self.authority.partition(self.pid)
        if current != self.registry:
            raise _StoreError("stale")
        if (
            self.proof_version is not None
            or self.intent_id is not None
            or self.maintenance
            or self.enumerating
        ):
            return
        resolution = self.authority.resolve([self.qualification.owner])
        if resolution["status"] != "ok" or resolution["absent"]:
            raise _StoreError("unavailable")

    def _check_mutation_namespace(self):
        self._check_namespace_access()
        mutation = self.registry["mutation"]
        if (
            not self.writable
            or mutation is None
            or mutation["kind"] != ("maintenance" if self.maintenance else "append")
            or (not self.maintenance and mutation["intent_id"] != self.intent_id)
        ):
            raise _StoreError("unavailable")

    def _coverage(self, dataset):
        if dataset is not None:
            indices = dataset.describe_indices()
            if len(indices) > 1 or any(
                index.name != INDEX_NAME or len(index.segments) != 1 for index in indices
            ):
                raise _StoreError("corrupt")
        return coverage(dataset)

    def _dataset_version(self):
        mutation = self.registry["mutation"]
        if (
            mutation is not None
            and not self.writable
            and self.intent_id is None
            and self.proof_version is None
            and not self.maintenance
            and not self.enumerating
        ):
            version = self.registry["pin"]["version"]
            if (
                mutation["kind"] not in {"append", "maintenance"}
                or type(version) is not int
                or not 0 < version < 2**63
            ):
                raise _StoreError("unavailable")
            # A pending mutation never revokes already published words. Open
            # their registered old snapshot once, retaining all usual checks.
            return version
        return None

    def _open(self):
        dataset = super()._open()
        self.actual_pin(dataset)
        if self.proof_version is not None:
            self._context_batch(dataset)
        return dataset

    def _context_batch(self, dataset):
        identity = self.intent_id
        if identity is None:
            mutation = self.registry["mutation"]
            if mutation is None or mutation["kind"] != "append":
                raise _StoreError("unavailable")
            identity = mutation["intent_id"]
        pending = self.authority.pending(identity)
        receipt_id = layout.batch_receipt_id(pending)
        row = lookup_many(dataset, [receipt_id], checked=True).get(receipt_id)
        if row is None:
            raise _StoreError("corrupt")
        manifest = decode_receipt(row)
        expected_ids = {r["id"] for r in pending["routes"][:-1]}
        if (
            self._compact(manifest, receipt_id) != layout.expected_receipt(pending)
            or manifest["source_binding"] != pending["source_binding"]
            or manifest["batch_id"] != pending["intent_id"]
            or {item[0] for item in manifest["proof"]} != expected_ids
        ):
            raise _StoreError("corrupt")
        return dict(manifest["proof"])

    def actual_pin(self, dataset):
        geometry = self._geometry(dataset)
        info = self._coverage(dataset)
        result = dict(
            self.registry["pin"],
            **geometry,
            index=dict(
                version=info["index_version"],
                segments=int(info["index_version"] is not None),
                indexed_fragments=info["indexed_fragments"],
                tail_fragments=info["tail_fragments"],
                tail_rows=info["tail_rows"],
            ),
        )
        layout.validate_partition_pin(result, self.authority.qualification, self.authority.envelope)
        registered = self.registry["pin"]
        if geometry == _geometry(registered):
            if result["index"] != registered["index"]:
                raise _StoreError("stale")
        else:
            mutation = self.registry["mutation"]
            if mutation is None or geometry != mutation["after"]:
                raise _StoreError("stale")
            if mutation["kind"] == "append":
                expected = dict(
                    registered["index"],
                    tail_fragments=registered["index"]["tail_fragments"] + 1,
                    tail_rows=registered["index"]["tail_rows"]
                    + geometry["physical_rows"]
                    - registered["physical_rows"],
                )
                if result["index"] != expected:
                    raise _StoreError("corrupt")
            elif (
                result["index"]["segments"] != 1
                or result["index"]["tail_rows"]
                or result["index"]["tail_fragments"]
            ):
                raise _StoreError("corrupt")
        return result

    def release_remote(self):
        # Installed Lance has no Dataset.close(): release all strong references
        # before any next acquisition. lookup_many returns plain Python rows.
        self.remote_dataset = None
        self.remote_store = None
        self.remote_pid = None

    def _remote(self, pid, owner):
        if self.remote_pid != pid:
            self.release_remote()
            if self.remote_opens == self.authority.qualification["max_remote_opens"]:
                raise _StoreError("unavailable")
            self.remote_opens += 1
            store = _PartitionStore(
                self.authority, pid, owner, self.authority.owner_plan(owner)["source_binding"]
            )
            if store.registry["mutation"] is not None:
                raise _StoreError("unavailable")
            self.remote_store = store
            self.remote_dataset = store._open()
            store.actual_pin(self.remote_dataset)
            self.remote_pid = pid
        return self.remote_dataset

    def _resolve_ids(self, ids):
        if len(ids) > layout.MAX_ROUTES:
            raise _StoreError("unavailable")
        result = dict(status="ok", routes=[], absent=[], unavailable=[], claims=[])
        for start in range(0, len(ids), LOOKUP_IDS):
            observed = self.authority.resolve(
                ids[start : start + LOOKUP_IDS], intent_id=self.intent_id
            )
            for field in ("routes", "absent", "unavailable", "claims"):
                result[field].extend(observed[field])
            if observed["status"] != "ok":
                result["status"] = observed["status"]
        return result

    def _lookup_rows(self, dataset, ids, *, checked=False):
        ids = list(dict.fromkeys(ids))
        if not checked:
            self._coverage(dataset)
        resolved = self._resolve_ids(ids)
        if resolved["status"] != "ok":
            raise _StoreError(resolved["status"])
        locations = {r["id"]: r for r in resolved["routes"]}
        locations.update({r["id"]: r for r in resolved["claims"]})
        result = {}
        groups = {}
        for identity in ids:
            if identity in locations:
                groups.setdefault(locations[identity]["partition_id"], []).append(identity)
        # The local dataset is one immutable version under the existing access guard.
        # Read its actual batch evidence lazily, once per bounded lookup, never
        # retain it on the store or reuse it across separate proof operations.
        geometry_after = None
        batch_proof = None
        for pid, group in groups.items():
            if pid == self.pid:
                rows = lookup_many(dataset, group, checked=True)
            else:
                # Do not retain a local reference to a previous remote dataset.
                rows = lookup_many(
                    self._remote(pid, locations[group[0]]["gate"]), group, checked=True
                )
            for identity, row in rows.items():
                _DecodedOwnerRow(row)
                location = locations[identity]
                read_version = dataset.version if pid == self.pid else self.remote_dataset.version
                if location.get("version") is not None and location["version"] > read_version:
                    raise _StoreError("stale")
                if (
                    location.get("row_hash") is not None
                    and migration.hashed(row) != location["row_hash"]
                ):
                    raise _StoreError("corrupt")
                if (
                    pid == self.pid
                    and location["state"] == "reserved"
                    and self.intent_id is not None
                ):
                    if geometry_after is None:
                        geometry_after = (
                            self._geometry(dataset) == self.registry["mutation"]["after"]
                        )
                    if geometry_after and identity != bulk.key("batch", self.intent_id):
                        if batch_proof is None:
                            batch_proof = self._context_batch(dataset)
                        if migration.hashed(row) != batch_proof.get(identity):
                            raise _StoreError("corrupt")
                result[identity] = row
            if any(
                identity not in rows and locations[identity]["state"] != "reserved"
                for identity in group
            ):
                raise _StoreError("corrupt")
        return result

    def _owner_rows(self, dataset, owner, pending):
        span = self.authority.span(owner)
        geometry = self._geometry(dataset)
        if self.intent_id is not None:
            intent = self.authority.pending(self.intent_id)
            if intent.get("format") == packed.INTENT:
                from .lance_layout_packed_rows import owner_spans

                mutation = self.registry["mutation"]
                if span != owner_spans(intent, mutation).get(owner):
                    raise _StoreError("corrupt")
                if span["next_row"] > geometry["physical_rows"]:
                    if geometry != _geometry(mutation["before"]):
                        raise _StoreError("corrupt")
                    # Later owners have no old rows before this exact append.
                    if span["first_row"] < geometry["physical_rows"]:
                        raise _StoreError("corrupt")
                    return
        start = span["first_row"]
        end = min(span["next_row"], geometry["physical_rows"])
        if not 0 <= start <= end:
            raise _StoreError("corrupt")
        count = 0
        if dataset is not None and end > start:
            scanner = dataset.scanner(
                columns=COLUMNS,
                scan_in_order=True,
                offset=start,
                limit=end - start,
                batch_size=LOOKUP_IDS,
                strict_batch_size=True,
                batch_readahead=0,
                fragment_readahead=0,
                io_buffer_size=1024**2,
                use_scalar_index=False,
            )
            for batch in scanner.to_batches():
                for raw in batch.to_pylist():
                    count += 1
                    if count > end - start:
                        raise _StoreError("corrupt")
                    row = _DecodedOwnerRow(raw)
                    if row["kind"] == "bulk_page" and row.header["owner"] != owner:
                        raise _StoreError("corrupt")
                    if row["kind"] == "bulk_record" and row["digest"] != owner:
                        raise _StoreError("corrupt")
                    yield row
        if count != end - start:
            raise _StoreError("corrupt")

    def _verify_owner(self, dataset, manifest, pending):
        intent = self.authority.pending(self.intent_id) if self.intent_id is not None else None
        if intent is not None and intent.get("format") == packed.INTENT:
            references = [r for r in intent["owners"] if r["owner"] == manifest["owner"]]
            if (
                len(references) != 1
                or manifest["source_binding"] != self.qualification.source_binding
                or migration.owner_reference(
                    manifest, references[0]["source_phase"], references[0]["source_key"]
                )
                != references[0]
            ):
                raise _StoreError("corrupt")
            if pending:
                routes = {r["id"]: r for r in intent["routes"][:-1]}
                expected = set(self.registry["mutation"]["new_ids"]) - {intent["routes"][-1]["id"]}
                if set(pending) != expected or any(
                    identity not in routes
                    or row["id"] != identity
                    or migration.hashed(row) != routes[identity]["row_hash"]
                    for identity, row in pending.items()
                ):
                    raise _StoreError("corrupt")
                pending = {
                    route["id"]: pending[route["id"]]
                    for route in intent["routes"][:-1]
                    if route["gate"] == manifest["owner"] and route["id"] in pending
                }
        elif (manifest["owner"], manifest["source_binding"]) != (
            self.qualification.owner,
            self.qualification.source_binding,
        ):
            raise _StoreError("corrupt")

        def reused(ids):
            rows = self._lookup_rows(dataset, ids, checked=True)
            for row in rows.values():
                visible_source(self, dataset, row)
            return rows

        _verify_decoded_owner_rows(
            self._owner_rows(dataset, manifest["owner"], pending),
            manifest,
            pending,
            resolve_reused=reused,
        )

    def _batch_receipt_state(self, dataset):
        if dataset is None:
            return 0, False
        ids = set()
        marker = False
        scanner = dataset.scanner(
            columns=COLUMNS,
            filter="kind = 'bulk_receipt'",
            scan_in_order=True,
            limit=self.qualification.max_physical_rows + 1,
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
                if receipt["state"] == "published_owner":
                    marker = marker or receipt["owner"] == self.qualification.owner
                else:
                    if row["id"] in ids or len(ids) == self.qualification.max_batch_receipts:
                        raise _StoreError("corrupt")
                    ids.add(row["id"])
        return len(ids), marker

    def _bound_rows(self, rows):
        intent = self.authority.pending(self.intent_id)
        if intent.get("format") != packed.INTENT:
            return super()._bound_rows(rows)
        from .lance_layout_packed_rows import ordered_rows

        # The dedicated base guard has one owner; this registered mutation has
        # an exact, complete owner set. Validate every original row against that
        # authority, including the global receipt whose hash is not known yet.
        if ordered_rows(intent, self.registry["mutation"], rows) != rows:
            raise _StoreError("corrupt")
        for row in rows:
            _DecodedOwnerRow(row)
        receipt = decode_receipt(rows[-1])
        if (
            receipt["source_binding"] != intent["source_binding"]
            or receipt["batch_id"] != intent["intent_id"]
            or self._compact(receipt, rows[-1]["id"]) != layout.expected_receipt(intent)
        ):
            raise _StoreError("corrupt")

    def _before_append(self, dataset, rows):
        self.release_remote()
        mutation = self.registry["mutation"]
        if len(rows) != len(mutation["new_ids"]) or {row["id"] for row in rows} != set(
            mutation["new_ids"]
        ):
            raise _StoreError("conflict")
        intent = self.authority.pending(self.intent_id)
        if intent.get("format") == packed.INTENT:
            from .lance_layout_packed_rows import ordered_rows

            rows[:] = ordered_rows(intent, mutation, rows)
        super()._before_append(dataset, rows)

    def _after_append(self, dataset):
        super()._after_append(dataset)
        if self._geometry(dataset, expected=False) != self.registry["mutation"]["after"]:
            raise _StoreError("corrupt")

    def _fresh_import_result(self, manifest, receipt_id):
        return dict(
            status="ok",
            format=layout.BATCH_RESULT,
            outcome="fresh_append",
            receipt=self._compact(manifest, receipt_id),
            routes=[],
        )

    def _existing_import_result(
        self, dataset, manifest, receipt_id, batch_id, source_binding, expected_ids
    ):
        self._paths()
        geometry = self._geometry(dataset)
        if geometry != self.registry["mutation"]["after"]:
            raise _StoreError("stale")
        pending = self.authority.pending(self.intent_id)
        resolved = self._resolve_ids([r["id"] for r in pending["routes"]])
        if resolved["status"] != "ok":
            raise _StoreError("unavailable")
        existing = {r["id"]: r for r in resolved["routes"]}
        observed = []
        for start in range(0, len(pending["routes"]), LOOKUP_IDS):
            candidates = pending["routes"][start : start + LOOKUP_IDS]
            found = self._lookup_rows(dataset, [r["id"] for r in candidates], checked=True)
            for candidate in candidates:
                row = found.get(candidate["id"])
                if row is None:
                    raise _StoreError("corrupt")
                incoming = self._requested_entries.get(row["id"])
                if incoming is not None and incoming["kind"] == "bulk_record":
                    if self._record(row) != self._record(incoming):
                        raise _StoreError("conflict")
                    if row["digest"] != incoming["digest"]:
                        visible_source(self, dataset, row)
                observed.append(
                    existing.get(row["id"])
                    or dict(
                        format=layout.ROUTE,
                        id=row["id"],
                        kind=row["kind"],
                        row_hash=migration.hashed(row),
                        partition_id=self.pid,
                        gate=candidate["gate"],
                        version=geometry["version"],
                        receipt=receipt_id,
                        state="staged",
                    )
                )
        return dict(
            status="ok",
            format=layout.BATCH_RESULT,
            outcome="existing_verified",
            receipt=self._compact(manifest, receipt_id),
            routes=observed,
        )

    def maintain_registered_index(self):
        try:
            with self._writer() as dataset:
                if dataset is None:
                    raise _StoreError("not_initialized")
                before = self._mutation_geometry(dataset)
                info = self._coverage(dataset)
                if info["index_version"] is None:
                    self._before_maintenance(dataset)  # Unchanged bootstrap16384 ceiling.
                    dataset.create_scalar_index("id", "BTREE", name=INDEX_NAME, replace=True)
                else:
                    reserve = self.qualification.reservations["maintenance"]
                    for name in (
                        "artifact_entries",
                        "logical_bytes",
                        "allocated_bytes",
                        "versions",
                    ):
                        if self._artifacts[name] + reserve[name] > getattr(
                            self.qualification, "max_" + name
                        ):
                            raise _StoreError("unavailable")
                    if self._artifacts["file_bytes"] > reserve["file_bytes"]:
                        raise _StoreError("unavailable")
                    self._charged_maintenance = (
                        before,
                        dict(self._artifacts),
                        reserve,
                        self._batch_receipt_state(dataset)[0],
                    )
                    dataset.optimize.optimize_indices(
                        index_names=[INDEX_NAME], num_indices_to_merge=1
                    )
                self._after_maintenance(dataset)
                actual = self.actual_pin(dataset)
                if (
                    _geometry(actual) != self.registry["mutation"]["after"]
                    or actual["index"]["segments"] != 1
                    or actual["index"]["tail_rows"]
                ):
                    raise _StoreError("corrupt")
            return {"status": "ok", "pin": actual}
        except _StoreError as error:
            return {"status": error.status}
        except (OSError, RuntimeError):
            return {"status": "unavailable"}

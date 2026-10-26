"""Closed, body-free metadata for the inactive bounded Lance layout.

These values describe reservations and current-tick proof references. They do
not grant production authority or replace an actual exact-row comparison.
Original registration, source and v1 interval identities remain unchanged.
"""

import json
from pathlib import PurePosixPath

from . import bulk_exact_codec as bulk
from . import migration_codec as migration
from .canonical_records import _hash, _json, _string

OWNER_PLAN = "lance-owner-plan-v1"
ENVELOPE = "lance-layout-envelope-v1"
TARGET = "lance-layout-target-v1"
INTENT = "lance-layout-interval-v1"
SOURCE_WINDOW = "lance-layout-source-v1"
ATTACHMENT = "lance-layout-attachment-v1"
ROUTE = "lance-layout-route-v1"
VERIFIED = "lance-layout-verified-v1"
BATCH_RESULT = "lance-layout-batch-result-v1"
QUALIFICATION = "lance-layout-qualification-v1"
SOURCE_REQUEST = "lance-layout-source-request-v1"
SOURCE_RESULT = "lance-layout-source-result-v1"
PREFIX_SOURCE_REQUEST = "lance-layout-source-prefix-request-v1"
PREFIX_SOURCE_RESULT = "lance-layout-source-prefix-result-v1"
PARTITION_PIN = "lance-layout-partition-pin-v1"
GROUPED_OWNER_PROOF = "lance-layout-owner-verification-v1"
PACKING = "bulk-v1-ascii-layout-1"
EXPANSION = (
    "physical_rows",
    "encoded_bytes",
    "retained_bytes",
    "batches",
    "fragments",
    "versions",
    "artifacts",
    "index_outputs",
    "index_bytes",
    "temp_bytes",
)
MAX_PLANS = 256
MAX_ROUTES = 513
KINDS = {"bulk_record", "bulk_page", "bulk_receipt", "receipt"}
CANDIDATE_FIELDS = {"id", "kind", "row_hash", "gate"}


def positive(value):
    if bulk.integer(value) == 0:
        raise ValueError("positive layout count")
    return value


def _fields(value, names, label):
    if not isinstance(value, dict) or value.keys() != set(names):
        raise ValueError(label)


def _expansion(value):
    _fields(value, EXPANSION, "layout expansion")
    for number in value.values():
        positive(number)
    return value


def owner_plan(reg, reference, expansion):
    return validate_owner_plan(
        dict(
            reference, format=OWNER_PLAN, source_binding=reg["source_binding"], expansion=expansion
        ),
        reg,
    )


def validate_owner_plan(value, reg):
    migration.validate_registration(reg)
    _fields(value, migration.OWNER_FIELDS | {"format", "source_binding", "expansion"}, "owner plan")
    if value["format"] != OWNER_PLAN or value["source_binding"] != reg["source_binding"]:
        raise ValueError("owner plan binding")
    migration.validate_owner_reference({k: value[k] for k in migration.OWNER_FIELDS}, reg)
    _expansion(value["expansion"])
    return migration.bounded(value)


def validate_generation(value):
    _fields(value, {"path", "device", "inode"}, "generation identity")
    path = _string(value["path"], 4096, "generation path")
    if (
        not PurePosixPath(path).is_absolute()
        or str(PurePosixPath(path)) != path
        or ".." in PurePosixPath(path).parts
    ):
        raise ValueError("generation path")
    bulk.integer(value["device"])
    positive(value["inode"])
    return value


def validate_envelope(value):
    _fields(
        value,
        {"format", "generation", "qualification_hash", "max_partitions", "limits"},
        "layout envelope",
    )
    if value["format"] != ENVELOPE:
        raise ValueError("layout envelope version")
    validate_generation(value["generation"])
    bulk.sha(value["qualification_hash"])
    positive(value["max_partitions"])
    _expansion(value["limits"])
    return migration.bounded(value)


def validate_qualification(value, envelope, *, expected_hash):
    """Check an independently pinned policy; this pure check grants no authority."""
    validate_envelope(envelope)
    validate_qualification_body(
        value, expected_hash=expected_hash, generation=envelope["generation"]
    )
    if expected_hash != envelope["qualification_hash"]:
        raise ValueError("registered qualification hash")
    if (
        value["max_remote_opens"] > envelope["max_partitions"]
        or value["working_temp_bytes"] > envelope["limits"]["temp_bytes"]
    ):
        raise ValueError("layout qualification fanout/temp")
    for name, expansion in (
        ("physical_rows", "physical_rows"),
        ("fragments", "fragments"),
        ("versions", "versions"),
        ("artifact_entries", "artifacts"),
        ("logical_bytes", "retained_bytes"),
        ("allocated_bytes", "retained_bytes"),
        ("batch_receipts", "batches"),
    ):
        if value["partition"]["max_" + name] > envelope["limits"][expansion]:
            raise ValueError("qualified partition exceeds envelope")
    return value


def validate_qualification_body(value, *, expected_hash, generation):
    """Artifact consumer validation; trust comes from the registered expected hash."""
    validate_generation(generation)
    bulk.sha(expected_hash)
    _fields(
        value,
        {
            "format",
            "generation",
            "schema",
            "storage_mode",
            "partition",
            "reservations",
            "max_remote_opens",
            "working_temp_bytes",
            "source_cost",
        },
        "layout qualification",
    )
    if (
        value["format"] != QUALIFICATION
        or value["schema"] != "castle-exact-six-string-v1"
        or value["storage_mode"] != "2.0"
        or value["generation"] != generation
        or migration.hashed(value) != expected_hash
    ):
        raise ValueError("independently registered layout qualification")
    validate_generation(value["generation"])
    for name in ("max_remote_opens", "working_temp_bytes"):
        positive(value[name])
    _qualification_limits(value)
    cost = value["source_cost"]
    _fields(cost, {"maintenance_every_batches", "frame_overhead_bytes"}, "layout source cost")
    for number in cost.values():
        positive(number)
    if cost["maintenance_every_batches"] > 32 or cost["frame_overhead_bytes"] > 32 * 1024:
        raise ValueError("layout source cost ceiling")
    if (
        max(len(_json(value).encode()), len(json.dumps(value, ensure_ascii=True).encode()))
        > 64 * 1024
    ):
        raise ValueError("layout qualification bytes")
    return value


def _qualification_limits(value):
    limits = value["partition"]
    names = {
        "physical_rows",
        "fragments",
        "versions",
        "artifact_entries",
        "artifact_depth",
        "logical_bytes",
        "allocated_bytes",
        "file_bytes",
        "batch_receipts",
    }
    _fields(limits, {"max_" + n for n in names}, "qualified partition limits")
    for number in limits.values():
        positive(number)
    if limits["max_file_bytes"] > min(limits["max_logical_bytes"], limits["max_allocated_bytes"]):
        raise ValueError("qualified resulting file ceiling")
    reservations = value["reservations"]
    _fields(reservations, {"append", "maintenance"}, "qualified operation charges")
    charge_names = names - {"artifact_depth", "batch_receipts"}
    for operation, charge in reservations.items():
        _fields(charge, charge_names, "qualified operation charge")
        for name, number in charge.items():
            bulk.integer(number)
            if name not in {"physical_rows", "fragments"}:
                positive(number)
            if number > limits["max_" + name]:
                raise ValueError("qualified operation exceeds partition")
        if (
            charge["versions"] != 1
            or charge["fragments"] != int(operation == "append")
            or (operation == "maintenance" and charge["physical_rows"] != 0)
            or (operation == "append" and not 1 <= charge["physical_rows"] <= 513)
        ):
            raise ValueError("qualified append/maintenance geometry")


def validate_partition_pin(value, qualification, envelope):
    validate_qualification(qualification, envelope, expected_hash=envelope["qualification_hash"])
    _fields(
        value,
        {
            "format",
            "generation",
            "qualification_hash",
            "partition_id",
            "parent_identity",
            "directory_identity",
            "version",
            "physical_rows",
            "fragments",
            "index",
        },
        "partition pin",
    )
    if (
        value["format"] != PARTITION_PIN
        or value["generation"] != envelope["generation"]
        or value["qualification_hash"] != envelope["qualification_hash"]
    ):
        raise ValueError("registered partition binding")
    if bulk.integer(value["partition_id"]) >= envelope["max_partitions"]:
        raise ValueError("registered partition bound")
    for name in ("parent_identity", "directory_identity"):
        identity = value[name]
        if not isinstance(identity, list) or len(identity) != 2:
            raise ValueError("registered partition identity")
        bulk.integer(identity[0])
        positive(identity[1])
    for key, maximum in (
        ("version", "max_versions"),
        ("physical_rows", "max_physical_rows"),
        ("fragments", "max_fragments"),
    ):
        if bulk.integer(value[key]) > qualification["partition"][maximum]:
            raise ValueError("registered partition geometry ceiling")
    if (
        bool(value["version"]) != bool(value["fragments"])
        or bool(value["physical_rows"]) != bool(value["fragments"])
        or value["version"] < value["fragments"]
        or value["physical_rows"] < value["fragments"]
    ):
        raise ValueError("registered partition geometry")
    index = value["index"]
    _fields(
        index,
        {"version", "segments", "indexed_fragments", "tail_fragments", "tail_rows"},
        "partition index",
    )
    for key in ("segments", "indexed_fragments", "tail_fragments", "tail_rows"):
        bulk.integer(index[key])
    if (
        index["segments"] not in {0, 1}
        or index["tail_rows"] > 1024
        or index["tail_fragments"] > 32
        or index["tail_rows"] > value["physical_rows"]
        or index["indexed_fragments"] + index["tail_fragments"] != value["fragments"]
    ):
        raise ValueError("single index and bounded tail")
    if index["segments"]:
        if (
            positive(index["version"]) > value["version"]
            or not index["indexed_fragments"]
            or value["physical_rows"] <= index["tail_rows"]
        ):
            raise ValueError("active index pin")
    elif (
        index["version"] is not None
        or index["indexed_fragments"]
        or index["tail_rows"] != value["physical_rows"]
    ):
        raise ValueError("unindexed partition pin")
    return migration.bounded(value)


def validate_directory_page(value, qualification, envelope):
    _fields(
        value,
        {
            "format",
            "generation",
            "qualification_hash",
            "epoch",
            "rebuild_token",
            "partition_id",
            "version",
            "cursor_before",
            "cursor_after",
            "pin",
            "rows",
            "complete",
        },
        "directory page",
    )
    pin = validate_partition_pin(value["pin"], qualification, envelope)
    bulk.sha(value["epoch"])
    bulk.sha(value["rebuild_token"])
    if (
        value["format"] != "lance-layout-directory-page-v1"
        or value["generation"] != envelope["generation"]
        or value["qualification_hash"] != envelope["qualification_hash"]
        or bulk.integer(value["partition_id"]) != pin["partition_id"]
        or bulk.integer(value["version"]) != pin["version"]
    ):
        raise ValueError("pinned directory page")
    before = bulk.integer(value["cursor_before"])
    after = bulk.integer(value["cursor_after"])
    rows = value["rows"]
    if (
        not isinstance(rows, list)
        or len(rows) > 32
        or not 0 <= before <= after <= pin["physical_rows"]
        or after - before != len(rows)
        or type(value["complete"]) is not bool
        or value["complete"] != (after == pin["physical_rows"])
    ):
        raise ValueError("contiguous physical directory page")
    identities = set()
    for route in rows:
        validate_route(route)
        if (
            route["partition_id"] != pin["partition_id"]
            or route["version"] > pin["version"]
            or route["id"] in identities
        ):
            raise ValueError("directory row physical binding")
        identities.add(route["id"])
    return migration.bounded(value)


def validate_layout_source_window(value, descriptor, before):
    if isinstance(value, dict) and value.get("format") == PREFIX_SOURCE_REQUEST:
        from .lance_layout_packing import validate_prefix_request

        return validate_prefix_request(value, descriptor, before)
    _fields(
        value,
        {"format", "action", "generation", "qualification_hash", "allowed_plan_hashes", "packing"},
        "layout source request",
    )
    validate_generation(value["generation"])
    reg = migration.registration(descriptor, value["generation"]["path"])
    migration.cursor(before, reg)
    bulk.sha(value["qualification_hash"])
    if (
        value["format"] != SOURCE_REQUEST
        or value["packing"] != PACKING
        or value["action"] not in {"plan", "export", "prepare"}
    ):
        raise ValueError("layout source request version")
    if value["action"] == "prepare" and (
        reg["format"] != migration.NATIVE_FORMAT
        or before["owner"] is not None
        or before["phase"] != "history"
    ):
        raise ValueError("native first-slice preparation only")
    hashes = value["allowed_plan_hashes"]
    if not isinstance(hashes, list) or len(hashes) != int(value["action"] == "export"):
        raise ValueError("single admitted source plan")
    for digest in hashes:
        bulk.sha(digest)
    migration.frame_bytes(dict(descriptor=descriptor, cursor=before, layout=value))
    return value


def validate_layout_source_result(value, window, descriptor, before):
    if isinstance(window, dict) and window.get("format") == PREFIX_SOURCE_REQUEST:
        from .lance_layout_packing import validate_prefix_result

        return validate_prefix_result(value, window, descriptor, before)
    validate_layout_source_window(window, descriptor, before)
    _fields(
        value,
        {
            "status",
            "reason",
            "format",
            "action",
            "source_binding",
            "cursor_before",
            "cursor_after",
            "plans",
            "export",
        }
        | ({"preparation_hash"} if window["action"] == "prepare" else set()),
        "layout source response",
    )
    migration.frame_bytes(value)
    reg = migration.registration(descriptor, window["generation"]["path"])
    if (
        value["format"] != SOURCE_RESULT
        or value["action"] != window["action"]
        or value["source_binding"] != reg["source_binding"]
        or value["cursor_before"] != before
    ):
        raise ValueError("layout source response binding")
    migration.cursor(value["cursor_after"], reg)
    _string(value["status"], 64, "source status")
    if value["reason"] is not None:
        _string(value["reason"], 1024, "source reason")
    if value["status"] != "ok":
        if value["plans"] != [] or value["export"] is not None or value["cursor_after"] != before:
            raise ValueError("layout refusal progress")
        if window["action"] == "prepare" and value["preparation_hash"] is not None:
            raise ValueError("preparation refusal evidence")
        return value
    if (
        value["reason"] is not None
        or not isinstance(value["plans"], list)
        or len(value["plans"]) > 1
    ):
        raise ValueError("layout source success")
    if window["action"] in {"plan", "prepare"}:
        if value["cursor_after"] != before:
            raise ValueError("planning never advances frontier")
        if value["plans"]:
            validate_owner_plan(value["plans"][0], reg)
            if window["action"] == "prepare":
                bulk.sha(value["preparation_hash"])
                if value["preparation_hash"] != migration.hashed([value["plans"], value["export"]]):
                    raise ValueError("prepared source association")
                # Validate the original inner export against its actual plan.
                # This does not admit a home or advance a durable frontier.
                admitted_window = dict(
                    window,
                    action="export",
                    allowed_plan_hashes=[migration.hashed(value["plans"][0])],
                )
                exported = {k: v for k, v in value.items() if k != "preparation_hash"}
                exported.update(action="export", cursor_after=value["export"]["cursor_after"])
                validate_layout_source_result(exported, admitted_window, descriptor, before)
            elif value["export"] is not None:
                raise ValueError("planning body")
        else:
            terminal = migration.validate_export(value["export"], reg, before)
            if terminal["status"] != "ok" or not terminal["complete"]:
                raise ValueError("planning exhaustion evidence")
            if window["action"] == "prepare" and value["preparation_hash"] is not None:
                raise ValueError("terminal preparation evidence")
        return value
    if len(value["plans"]) != 1:
        raise ValueError("export admitted owner plan")
    plan = validate_owner_plan(value["plans"][0], reg)
    if [migration.hashed(plan)] != window["allowed_plan_hashes"]:
        raise ValueError("source plan changed")
    exported = migration.validate_export(value["export"], reg, before)
    if (
        exported["status"] != "ok"
        or exported["complete"]
        or exported["cursor_after"] != value["cursor_after"]
        or exported["phase"] != plan["source_phase"]
    ):
        raise ValueError("layout exported owner")
    if exported["finalizations"]:
        actual = migration.owner_reference(
            exported["finalizations"][0], exported["phase"], exported["cursor_after"]["key"]
        )
        if actual != {k: plan[k] for k in migration.OWNER_FIELDS}:
            raise ValueError("layout final owner plan")
    elif exported["cursor_after"]["owner"] != plan["owner_key"]:
        raise ValueError("layout staged owner plan")
    return value


class LayoutPlanningCounters:
    """Current-owner scalar size/cost counters; never retains a row or proof."""

    def __init__(self, qualification):
        validate_qualification_body(
            qualification,
            expected_hash=migration.hashed(qualification),
            generation=qualification["generation"],
        )
        for operation, prefix in (("append", "append"), ("maintenance", "index")):
            charge = qualification["reservations"][operation]
            setattr(
                self, prefix + "_bytes", max(charge["logical_bytes"], charge["allocated_bytes"])
            )
            setattr(self, prefix + "_artifacts", charge["artifact_entries"])
        self.append_rows = qualification["reservations"]["append"]["physical_rows"]
        self.working_temp = qualification["working_temp_bytes"]
        self.maintenance_batches = qualification["source_cost"]["maintenance_every_batches"]
        self.overhead = qualification["source_cost"]["frame_overhead_bytes"]
        # Max-width scalar receipt sizing. Never retain IDs or proof lists.
        maximum_id = "receipt:" + "f" * 64
        prototype = dict(
            format=bulk.FORMAT,
            state="committed_batch",
            batch_id="f" * 128,
            source_binding="f" * 64,
            digest="f" * 64,
            proof=[],
            records=bulk.MAX_ENTRIES,
            published_owners=bulk.MAX_ENTRIES,
            entries=bulk.MAX_ENTRIES,
        )
        self.receipt_base = len(
            json.dumps(bulk.receipt_row(prototype, maximum_id), allow_nan=False).encode()
        )
        self.proof_item_bytes = (
            len(json.dumps(_json([maximum_id, "f" * 64]), allow_nan=False).encode()) - 2
        )
        self.entries = self.total_encoded = self.batches = self.physical_rows = (
            self.max_batch_rows
        ) = 0
        self.current_entries = 0
        self.current_decoded = 0
        self.current_encoded = 2
        self.finished = 0

    def _fits(self, decoded, encoded):
        return (
            self.current_entries < bulk.MAX_ENTRIES
            and self.current_decoded + decoded <= bulk.MAX_DECODED_BYTES
            and self.current_encoded + encoded <= bulk.MAX_ENCODED_BYTES - 16 * 1024
            and self.current_encoded + encoded + self.overhead <= 512 * 1024
        )

    def _flush(self, *, live=0):
        rows = self.current_entries + 1 + live  # Include this physical batch receipt.
        proof_count = rows - 1
        self.total_encoded += (
            self.receipt_base + proof_count * self.proof_item_bytes + max(0, proof_count - 1)
        )
        self.max_batch_rows = max(self.max_batch_rows, rows)
        self.physical_rows += rows
        self.batches += 1
        self.current_entries = 0
        self.current_decoded = 0
        self.current_encoded = 2

    def add_entry(self, row):
        if self.finished:
            raise ValueError("finished owner planning")
        _fields(row, bulk.COLUMNS, "planning six-string row")
        if row["kind"] not in {"bulk_page", "bulk_record"} or any(
            type(v) is not str for v in row.values()
        ):
            raise ValueError("planning entry")
        decoded = sum(len(v.encode()) for v in row.values())
        encoded = len(json.dumps(row, ensure_ascii=True, allow_nan=False).encode()) + 2
        if not self._fits(decoded, encoded) and self.current_entries:
            self._flush()
        if not self._fits(decoded, encoded):
            raise ValueError("planning entry exceeds fixed frame")
        self.current_entries += 1
        self.entries += 1
        self.current_decoded += decoded
        self.current_encoded += encoded
        self.total_encoded += encoded

    def finish(self, reg, manifest, phase, source_key):
        if self.finished:
            raise ValueError("finished owner planning")
        bulk.validate_owner(manifest)
        if (
            self.entries
            != sum(f["pages"] for f in manifest["fields"].values()) + 2 * manifest["members"]
        ):
            raise ValueError("planning complete original expansion")
        decoded = len(_json(manifest).encode())
        encoded = len(json.dumps(manifest, ensure_ascii=True, allow_nan=False).encode())

        def fits_final():
            envelope = len(json.dumps(["f" * 64, reg["source_binding"], [], []]).encode())
            wire = self.current_encoded + encoded + envelope + 2
            return (
                self.current_entries < bulk.MAX_ENTRIES
                and self.current_decoded + decoded <= bulk.MAX_DECODED_BYTES
                and wire <= bulk.MAX_ENCODED_BYTES
                and wire + self.overhead <= 512 * 1024
            )

        if not fits_final() and self.current_entries:
            self._flush()
        if not fits_final():
            raise ValueError("planning owner exceeds fixed frame")
        self.current_entries += 1  # Owner marker is one finalization, not an input entry.
        self.total_encoded += (
            max(
                encoded,
                len(
                    json.dumps(
                        bulk.receipt_row(manifest, manifest["owner"]),
                        ensure_ascii=True,
                        allow_nan=False,
                    ).encode()
                ),
            )
            + 2
        )
        if manifest["live"] is not None:
            live = manifest["live"]
            row = dict(
                id="receipt:" + _hash(_json([live["namespace"], live["key"]])),
                kind="receipt",
                text="",
                provenance="",
                digest=live["digest"],
                receipt=_json(live["receipt"]),
            )
            self.total_encoded += len(json.dumps(row, allow_nan=False).encode()) + 2
        self._flush(live=int(manifest["live"] is not None))
        if (
            self.max_batch_rows > self.append_rows
            or self.max_batch_rows * self.maintenance_batches > 1024
        ):
            raise ValueError("owner needs a qualified tail cadence")
        indexes = (self.batches + self.maintenance_batches - 1) // self.maintenance_batches
        expansion = dict(
            physical_rows=self.physical_rows,
            encoded_bytes=self.total_encoded + self.batches * self.overhead,
            retained_bytes=self.batches * self.append_bytes + indexes * self.index_bytes,
            batches=self.batches,
            fragments=self.batches,
            versions=self.batches + indexes,
            artifacts=self.batches * self.append_artifacts + indexes * self.index_artifacts,
            index_outputs=indexes,
            index_bytes=indexes * self.index_bytes,
            temp_bytes=self.working_temp,
        )
        self.finished = 1
        return owner_plan(reg, migration.owner_reference(manifest, phase, source_key), expansion)


def validate_attachment(value):
    _fields(value, {"format", "envelope_hash"}, "layout attachment marker")
    if value["format"] != ATTACHMENT:
        raise ValueError("layout attachment version")
    bulk.sha(value["envelope_hash"])
    return value


def target(envelope, partition_id, plans):
    validate_envelope(envelope)
    bulk.integer(partition_id)
    if (
        partition_id >= envelope["max_partitions"]
        or not isinstance(plans, list)
        or not 1 <= len(plans) <= MAX_PLANS
    ):
        raise ValueError("layout placement bound")
    hashes = [migration.hashed(plan) for plan in plans]
    value = dict(
        format=TARGET,
        generation=envelope["generation"],
        envelope_hash=migration.hashed(envelope),
        partition_id=partition_id,
        owner_plan_hashes=hashes,
    )
    value["reservation_id"] = migration.hashed([TARGET, value])
    return validate_target(value)


def validate_target(value):
    _fields(
        value,
        {
            "format",
            "generation",
            "envelope_hash",
            "partition_id",
            "owner_plan_hashes",
            "reservation_id",
        },
        "layout target",
    )
    if value["format"] != TARGET:
        raise ValueError("layout target version")
    validate_generation(value["generation"])
    bulk.sha(value["envelope_hash"])
    bulk.integer(value["partition_id"])
    hashes = value["owner_plan_hashes"]
    if not isinstance(hashes, list) or not 1 <= len(hashes) <= MAX_PLANS:
        raise ValueError("target plans")
    for item in hashes:
        bulk.sha(item)
    if len(set(hashes)) != len(hashes):
        raise ValueError("duplicate target plan")
    expected = migration.hashed([TARGET, {k: v for k, v in value.items() if k != "reservation_id"}])
    if value["reservation_id"] != expected:
        raise ValueError("target identity")
    return migration.bounded(value)


def partition_path(value):
    validate_target(value)
    return str(
        PurePosixPath(value["generation"]["path"])
        / "exact"
        / "partitions"
        / str(value["partition_id"])
    )


def _window(reg, before, fixed_target):
    return dict(
        format=SOURCE_WINDOW,
        action="export",
        descriptor=reg["descriptor"],
        cursor=before,
        allowed_plan_hashes=fixed_target["owner_plan_hashes"],
        packing=PACKING,
    )


def interval_id(reg, before, fixed_target, window):
    return migration.hashed(
        [
            INTENT,
            reg["format"],
            reg["job_id"],
            reg["source_binding"],
            reg["destination"],
            PACKING,
            before,
            fixed_target,
            window,
        ]
    )


def batch_receipt_id(intent):
    return bulk.key("batch", intent["intent_id"])


def expected_receipt(intent):
    return migration.expected_receipt(intent)


def _candidate(row, gate):
    return dict(id=row["id"], kind=row["kind"], row_hash=migration.hashed(row), gate=gate)


def prepare_layout_interval(reg, exported, fixed_target):
    migration.validate_registration(reg)
    validate_target(fixed_target)
    migration.validate_export(exported, reg, exported["cursor_before"])
    if (
        exported["status"] != "ok"
        or exported["complete"]
        or fixed_target["generation"]["path"] != reg["destination"]
    ):
        raise ValueError("not a layout content interval")
    window = _window(reg, exported["cursor_before"], fixed_target)
    identity = interval_id(reg, exported["cursor_before"], fixed_target, window)
    # Reuse the original strict source/owner/counter contract without changing its domain.
    original = migration.prepare_interval(reg, exported)
    value = {
        **original,
        "format": INTENT,
        "registration_format": reg["format"],
        "packing": PACKING,
        "intent_id": identity,
        "target": fixed_target,
        "window": window,
        "digest": bulk.validate_batch(
            identity, reg["source_binding"], exported["entries"], exported["finalizations"]
        ),
    }
    gates = set()
    routes = []
    for row in exported["entries"]:
        gate = (
            row["digest"] if row["kind"] == "bulk_record" else json.loads(row["receipt"])["owner"]
        )
        gates.add(gate)
        routes.append(_candidate(row, gate))
    for owner in exported["finalizations"]:
        gates.add(owner["owner"])
        routes.append(_candidate(bulk.receipt_row(owner, owner["owner"]), owner["owner"]))
        if owner["live"] is not None:
            live = owner["live"]
            if [live["namespace"], live["key"]] != exported["cursor_after"]["key"]:
                raise ValueError("original live receipt source key")
            row = dict(
                id="receipt:" + _hash(_json([live["namespace"], live["key"]])),
                kind="receipt",
                text="",
                provenance="",
                digest=live["digest"],
                receipt=_json(live["receipt"]),
            )
            routes.append(_candidate(row, owner["owner"]))
    if len(gates) != 1:
        raise ValueError("one admitted owner per layout frame")
    routes.append(
        dict(id=batch_receipt_id(value), kind="bulk_receipt", row_hash=None, gate=next(iter(gates)))
    )
    value["routes"] = routes
    return validate_layout_intent(value, reg)


def _candidates(values, intent, reg):
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_ROUTES:
        raise ValueError("layout route bound")
    ids, gates = set(), set()
    for value in values:
        _fields(value, CANDIDATE_FIELDS, "layout candidate")
        bulk.identity(value["id"], "memory" if value["kind"] == "bulk_record" else "receipt")
        bulk.identity(value["gate"])
        if value["kind"] not in KINDS or value["id"] in ids:
            raise ValueError("layout route kind/duplicate")
        ids.add(value["id"])
        gates.add(value["gate"])
        if value["row_hash"] is None:
            if value["id"] != batch_receipt_id(intent) or value["kind"] != "bulk_receipt":
                raise ValueError("unknown row hash")
        else:
            bulk.sha(value["row_hash"])
    if (
        len(gates) != 1
        or values[-1]["id"] != batch_receipt_id(intent)
        or values[-1]["row_hash"] is not None
    ):
        raise ValueError("layout batch route")
    if intent["owners"] and next(iter(gates)) != intent["owners"][0]["owner"]:
        raise ValueError("layout owner gate")
    gate = (
        intent["owners"][0]["owner"]
        if intent["owners"]
        else bulk.owner_id(intent["cursor_after"]["owner"], reg["source_binding"])
    )
    if gates != {gate}:
        raise ValueError("layout source owner gate")
    expected_kinds = dict(
        bulk_record=intent["records"],
        bulk_page=intent["entries"] - intent["records"],
        bulk_receipt=len(intent["owners"]) + 1,
        receipt=sum(o["classification"] == "live" for o in intent["owners"]),
    )
    if any(
        sum(v["kind"] == kind for v in values) != count for kind, count in expected_kinds.items()
    ):
        raise ValueError("complete layout route cardinality")
    by_id = {v["id"]: v for v in values}
    for owner in intent["owners"]:
        if owner["owner"] not in by_id or by_id[owner["owner"]]["kind"] != "bulk_receipt":
            raise ValueError("owner receipt candidate")
        if owner["classification"] == "live":
            identity = "receipt:" + migration.hashed(owner["source_key"])
            if identity not in by_id or by_id[identity]["kind"] != "receipt":
                raise ValueError("original live receipt candidate")
    return values


def validate_layout_intent(value, reg):
    original_keys = {
        "format",
        "packing",
        "job_id",
        "source_binding",
        "destination",
        "intent_id",
        "cursor_before",
        "cursor_after",
        "digest",
        "entries",
        "records",
        "owners",
        "counts",
        "coverage",
        "phase",
    }
    _fields(
        value,
        original_keys | {"registration_format", "target", "window", "routes"},
        "layout intent",
    )
    if (
        value["format"] != INTENT
        or value["registration_format"] != reg["format"]
        or value["packing"] != PACKING
    ):
        raise ValueError("layout intent version")
    # Validate unchanged original cursor/counter/owner rules using its own identity.
    original = {k: value[k] for k in original_keys}
    original.update(
        format=reg["format"],
        packing=reg["packing"],
        intent_id=migration.interval_id(reg, value["cursor_before"]),
    )
    migration.validate_interval(original, reg)
    validate_target(value["target"])
    if value["target"]["generation"]["path"] != reg["destination"] or value["window"] != _window(
        reg, value["cursor_before"], value["target"]
    ):
        raise ValueError("layout source window")
    if value["intent_id"] != interval_id(
        reg, value["cursor_before"], value["target"], value["window"]
    ):
        raise ValueError("layout intent identity")
    _candidates(value["routes"], value, reg)
    return migration.bounded(value)


def validate_route(value):
    _fields(
        value,
        CANDIDATE_FIELDS | {"format", "partition_id", "version", "receipt", "state"},
        "layout route",
    )
    if (
        value["format"] != ROUTE
        or value["kind"] not in KINDS
        or value["state"] not in {"staged", "published"}
    ):
        raise ValueError("layout route version/state")
    bulk.identity(value["id"], "memory" if value["kind"] == "bulk_record" else "receipt")
    bulk.identity(value["gate"])
    bulk.sha(value["row_hash"])
    bulk.integer(value["partition_id"])
    positive(value["version"])
    bulk.identity(value["receipt"])
    return migration.bounded(value)


def verified_summary(intent, batch, owner_proofs):
    return validate_layout_verified(
        dict(
            format=VERIFIED,
            intent_hash=migration.hashed(intent),
            proof_version=migration.PROOF_VERSION,
            batch=batch,
            owner_proofs=owner_proofs,
        ),
        intent,
    )


def owner_observation(manifest, partition_id, version):
    """Transport shape only; callers still must verify actual canonical evidence."""
    bulk.validate_owner(manifest)
    bulk.integer(partition_id)
    positive(version)
    live = manifest["live"]
    return dict(
        owner=manifest["owner"],
        manifest_hash=migration.hashed(manifest),
        partition_id=partition_id,
        version=version,
        live_receipt_ids=[] if live is None else list(live["receipt"]["ids"]),
        live=None
        if live is None
        else dict(
            namespace=live["namespace"],
            key=live["key"],
            digest=live["digest"],
            receipt_hash=migration.hashed(live["receipt"]),
        ),
    )


def _grouped_reply_bound(value):
    if max(len(_json(value).encode()), len(json.dumps(value, allow_nan=False).encode())) > 4096:
        raise ValueError("grouped owner result bound")
    return migration.bounded(value)


def _validate_owner_observation(proof, owner, manifest, partition_id, version):
    positive(version)
    bulk.integer(partition_id)
    manifest = bulk.validate_owner(manifest)
    _fields(
        proof,
        {"owner", "manifest_hash", "partition_id", "version", "live_receipt_ids", "live"},
        "grouped owner observation",
    )
    bulk.identity(proof["owner"])
    bulk.sha(proof["manifest_hash"])
    bulk.integer(proof["partition_id"])
    positive(proof["version"])
    ids = proof["live_receipt_ids"]
    if not isinstance(ids, list) or len(ids) > 9:
        raise ValueError("original live memory IDs")
    for identity in ids:
        bulk.identity(identity, "memory")
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate live memory IDs")
    live = proof["live"]
    if live is not None:
        _fields(live, {"namespace", "key", "digest", "receipt_hash"}, "grouped live observation")
        _string(live["namespace"], 128, "namespace")
        _string(live["key"], 128, "key")
        bulk.sha(live["digest"])
        bulk.sha(live["receipt_hash"])
    original = manifest["live"]
    if (
        migration.owner_reference(manifest, owner["source_phase"], owner["source_key"]) != owner
        or proof["owner"] != owner["owner"]
        or proof["manifest_hash"] != migration.hashed(manifest)
        or proof["partition_id"] != partition_id
        or proof["version"] != version
        or ids != owner["live_receipt_ids"]
        or live
        != (
            None
            if original is None
            else dict(
                namespace=original["namespace"],
                key=original["key"],
                digest=original["digest"],
                receipt_hash=migration.hashed(original["receipt"]),
            )
        )
    ):
        raise ValueError("grouped original owner binding")
    return {
        **{key: item for key, item in proof.items() if key != "live"},
        "live_receipt_ids": list(ids),
    }


def validate_grouped_owner_proof(value, intent, exported, version):
    """Check current actual observations against the original source, not a cache."""
    positive(version)
    _fields(value, {"status", "format", "intent_hash", "owners"}, "grouped owner result")
    bulk.sha(value["intent_hash"])
    if (
        value["status"] != "ok"
        or value["format"] != GROUPED_OWNER_PROOF
        or value["intent_hash"] != migration.hashed(intent)
        or len(intent["owners"]) != 1
        or len(exported["finalizations"]) != 1
        or not isinstance(value["owners"], list)
        or len(value["owners"]) != 1
    ):
        raise ValueError("single pending owner observations")
    proof = _validate_owner_observation(
        value["owners"][0],
        intent["owners"][0],
        exported["finalizations"][0],
        intent["target"]["partition_id"],
        version,
    )
    _grouped_reply_bound(value)
    return [proof]


def validate_attached_layout_intent(value, reg):
    """Explicit attached domains; the legacy interval validator stays strict."""
    from .lance_layout_packed import INTENT as PACKED_INTENT, validate_prefix_intent

    if isinstance(value, dict) and value.get("format") == PACKED_INTENT:
        return validate_prefix_intent(value, reg)
    return validate_layout_intent(value, reg)


def validate_layout_verified(value, intent):
    _fields(
        value,
        {"format", "intent_hash", "proof_version", "batch", "owner_proofs"},
        "layout verified",
    )
    if (
        value["format"] != VERIFIED
        or value["intent_hash"] != migration.hashed(intent)
        or value["proof_version"] != migration.PROOF_VERSION
    ):
        raise ValueError("layout proof binding")
    batch = value["batch"]
    _fields(batch, {"format", "outcome", "receipt", "routes"}, "layout batch result")
    _fields(batch["receipt"], expected_receipt(intent).keys(), "layout bulk receipt")
    for key in ("records", "published_owners", "entries"):
        bulk.integer(batch["receipt"][key])
    if (
        batch["format"] != BATCH_RESULT
        or batch["outcome"] != "existing_verified"
        or batch["receipt"] != expected_receipt(intent)
    ):
        raise ValueError("identical persisted replay required")
    routes = batch["routes"]
    if not isinstance(routes, list) or len(routes) != len(intent["routes"]):
        raise ValueError("complete route observations")
    home_version = positive(routes[-1]["version"])
    for route, candidate in zip(routes, intent["routes"]):
        validate_route(route)
        if route["id"] != candidate["id"] or route["kind"] != candidate["kind"]:
            raise ValueError("route observation identity/order")
        # A record reused from an earlier owner keeps its original physical row.
        # The directory must match that observation to an already published route.
        if (
            candidate["kind"] != "bulk_record"
            and candidate["row_hash"] is not None
            and route["row_hash"] != candidate["row_hash"]
        ):
            raise ValueError("route observation hash")
        if (
            route["partition_id"] != intent["target"]["partition_id"]
            and candidate["kind"] != "bulk_record"
        ):
            raise ValueError("route home")
        if route["gate"] != candidate["gate"] and candidate["kind"] != "bulk_record":
            raise ValueError("route gate")
        if route["receipt"] != batch_receipt_id(intent) and candidate["kind"] != "bulk_record":
            raise ValueError("route receipt")
        reused = candidate["kind"] == "bulk_record" and route["state"] == "published"
        if not reused and (
            route["version"] != home_version
            or route["partition_id"] != intent["target"]["partition_id"]
            or route["gate"] != candidate["gate"]
            or route["receipt"] != batch_receipt_id(intent)
        ):
            raise ValueError("current pinned home observation")
    proofs = value["owner_proofs"]
    if not isinstance(proofs, list) or len(proofs) != len(intent["owners"]):
        raise ValueError("owner observations")
    for proof, owner in zip(proofs, intent["owners"]):
        _fields(
            proof,
            {"owner", "manifest_hash", "partition_id", "version", "live_receipt_ids"},
            "owner observation",
        )
        bulk.integer(proof["partition_id"])
        if (
            any(proof[k] != owner[k] for k in ("owner", "manifest_hash", "live_receipt_ids"))
            or proof["partition_id"] != intent["target"]["partition_id"]
        ):
            raise ValueError("owner observation binding")
        if positive(proof["version"]) != home_version:
            raise ValueError("owner pinned home version")
    return migration.bounded(value)


def prospective_ack(intent):
    """Reject a prepare whose complete maximum-width success proof cannot fit."""
    version = 2**63 - 1
    # Capacity only, never an observation or cached proof. Enforce this again
    # at the queue boundary even if an older producer froze a larger prefix.
    _grouped_reply_bound(
        dict(
            status="ok",
            format=GROUPED_OWNER_PROOF,
            intent_hash="f" * 64,
            owners=[
                dict(
                    owner=o["owner"],
                    manifest_hash=o["manifest_hash"],
                    partition_id=version,
                    version=version,
                    live_receipt_ids=o["live_receipt_ids"],
                    live=dict(
                        namespace=o["source_key"][0],
                        key=o["source_key"][1],
                        digest="f" * 64,
                        receipt_hash="f" * 64,
                    )
                    if o["classification"] == "live"
                    else None,
                )
                for o in intent["owners"]
            ],
        )
    )
    routes = [
        {
            **r,
            "format": ROUTE,
            "row_hash": r["row_hash"] or "f" * 64,
            "partition_id": version,
            "version": version,
            "receipt": batch_receipt_id(intent),
            "state": "published",
        }
        for r in intent["routes"]
    ]
    proofs = [
        dict(
            owner=o["owner"],
            manifest_hash=o["manifest_hash"],
            partition_id=version,
            version=version,
            live_receipt_ids=o["live_receipt_ids"],
        )
        for o in intent["owners"]
    ]
    summary = dict(
        format=VERIFIED,
        intent_hash=migration.hashed(intent),
        proof_version=migration.PROOF_VERSION,
        batch=dict(
            format=BATCH_RESULT,
            outcome="existing_verified",
            receipt=expected_receipt(intent),
            routes=routes,
        ),
        owner_proofs=proofs,
    )
    return migration.bounded(
        dict(job_id=intent["job_id"], intent_id=intent["intent_id"], verified_summary=summary)
    )

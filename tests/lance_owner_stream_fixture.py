"""Fixed52MiB historical qualification fixture; never a public provider/profile.

Preparation only until root freezes the configuration/code and grants launch.
Original source/queue/coordinator contracts remain the authority for progress.
"""

from pathlib import Path
import sys

MIB = 1024**2
ROOT = Path(__file__).resolve().parents[1]
RETIRED_PACKET_ROOT = ROOT / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b_fixture"
TRUSTED_PACKET_ROOT = (
    ROOT / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b_fixture-continuation1"
)
RETIRED_CONFIGURATION_PATH = (
    ROOT / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification.json"
)
PREVIOUS_CONFIGURATION_PATH = (
    ROOT
    / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification-continuation1.json"
)
PUBLISHED_CONFIGURATION_PATH = (
    ROOT
    / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification-continuation2.json"
)
COLD_CONFIGURATION_PATH = (
    ROOT / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification-cold.json"
)
CONFIGURATION_PATH = (
    ROOT
    / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification-persisted.json"
)
COLD_BASETEMP = (
    ROOT / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b-pytest-20261025-6fa6659-cold"
)
PUBLISHED_BASETEMP = (
    ROOT
    / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b-pytest-20261025-6fa6659-continuation2"
)
SEAL_NAME = "final-seal-persisted.json"
PIN_NAME = "launch-pin-persisted.json"
PREVIOUS_BASETEMP = (
    ROOT
    / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b-pytest-20261025-6fa6659-continuation1"
)
RETIRED_BASETEMP = (
    ROOT / ".superpowers/sdd/2026-10-25-bounded-lance-layout/task1b-pytest-20261025-6fa6659-run1"
)
WORKER_PATH = ROOT / "tests/test_lance_owner_stream.py"
HELPER_PATH = Path(__file__).absolute()
sys.dont_write_bytecode = True
for _trusted in (ROOT, ROOT / "tests"):
    if str(_trusted) not in sys.path:
        sys.path.insert(0, str(_trusted))
TOTAL_INTERVALS = 171
FINAL_ROWS = 13696
FINAL_FRAGMENTS = 171
FINAL_VERSIONS = 189
MAX_ROWS_PER_APPEND = 103
MAINTENANCE_BEFORE = tuple(range(10, 164, 9))


def maintenance_before(ordinal):
    if type(ordinal) is not int or not 1 <= ordinal <= TOTAL_INTERVALS:
        raise ValueError("fixed historical interval")
    return ordinal in MAINTENANCE_BEFORE


def physical_rows(*, entries, owners):
    if type(entries) is not int or entries < 0 or type(owners) is not int or owners not in (0, 1):
        raise ValueError("historical candidate rows")
    result = entries + owners + 1  # Original entries + optional owner + batch receipt.
    if not 1 <= entries + owners <= 256 or result > MAX_ROWS_PER_APPEND:
        raise ValueError("fixed historical physical-row bound")
    return result


def after_append(before, *, entries, owners):
    result = {
        "version": before["version"] + 1,
        "physical_rows": before["physical_rows"] + physical_rows(entries=entries, owners=owners),
        "fragments": before["fragments"] + 1,
    }
    if (
        result["physical_rows"] > FINAL_ROWS
        or result["fragments"] > FINAL_FRAGMENTS
        or result["version"] > FINAL_VERSIONS
    ):
        raise ValueError("fixed historical geometry exceeded")
    return result


def after_maintenance(before):
    if not 1 <= before["physical_rows"] <= 16384 or before["version"] >= FINAL_VERSIONS:
        raise ValueError("fixed historical maintenance geometry")
    return {**before, "version": before["version"] + 1}


def exact_stage(operation):
    mapping = {
        "import_batch": ("ordered_import_batch", 3),
        "import_batch_outcome": ("ordered_import_batch_outcome", 3),
        "build_id_index": ("ordered_build_id_index", 3),
        "bulk_owner": ("ordered_bulk_owner", 2),
        "bulk_receipt": ("ordered_bulk_receipt", 2),
    }
    if operation not in mapping:
        raise ValueError("fixed historical exact operation")
    mapped, cap = mapping[operation]
    return {"operation": mapped, "cap": cap, "rss": 192 * MIB, "total": 256 * MIB}


def candidate_profile():
    return {
        "destination_logical_bytes": 320 * MIB,
        "destination_allocated_bytes": 320 * MIB,
        "destination_entries": 1024,
        "destination_depth": 6,
        "resulting_file_bytes": 2 * MIB,
        "append_growth_bytes": MIB,
        "maintenance_growth_bytes": 2 * MIB,
        "mutation_artifact_entries": 16,
        "whole_bytes": 512 * MIB,
        "source_entries": 128,
        "harness_entries": 288,
        "harness_bytes": 24 * MIB,
        "queue_resulting_bytes": 8 * MIB,
        "temp_entries": 1008,
        "temp_bytes": 80 * MIB,
        "working_temp_bytes": 64 * MIB,
        "minimum_free_bytes": 2 * 1024**3,
        "lance_mem_pool_bytes": 16 * MIB,
        "lance_tracked_spill_bytes": 64 * MIB,
        "normal_supervised_calls": 1070,
        "max_supervised_calls": 1071,
        "logical_exact_workspaces": 380,
        "worker_fd_slots": 512,
        "queue_working_temp_bytes": 8 * MIB,
        "event_file_bytes": 4 * MIB,
        "peak_file_bytes": 4 * MIB,
        "source_task_rss_bytes": 128 * MIB,
        "source_total_rss_bytes": 192 * MIB,
        "exact_task_rss_bytes": 192 * MIB,
        "exact_total_rss_bytes": 256 * MIB,
        "source_seconds": 2,
        "read_seconds": 2,
        "write_seconds": 3,
        "session_seconds": 5,
        "cold_host_transport_seconds": 7,
        "cold_output_bytes": 65536,
        "outer_timeout_seconds": 600,
        "outer_sampled_rss_mib": 700,
    }


def qualification_for(binding, owner, source_binding, before, after, *, operation):
    if operation not in {"append", "maintenance"} or after["version"] != before["version"] + 1:
        raise ValueError("fixed operation geometry")
    row_growth = after["physical_rows"] - before["physical_rows"]
    fragment_growth = after["fragments"] - before["fragments"]
    if operation == "append":
        if not 1 <= row_growth <= MAX_ROWS_PER_APPEND or fragment_growth != 1:
            raise ValueError("fixed append geometry")
    elif row_growth or fragment_growth:
        raise ValueError("maintenance changed canonical geometry")
    if (
        after["physical_rows"] > FINAL_ROWS
        or after["fragments"] > FINAL_FRAGMENTS
        or after["version"] > FINAL_VERSIONS
    ):
        raise ValueError("fixed resulting geometry")
    profile = candidate_profile()
    reservations = {}
    for kind, growth in (("append", MIB), ("maintenance", 2 * MIB)):
        active = kind == operation
        reservations[kind] = {
            "physical_rows": row_growth if active else 0,
            "fragments": fragment_growth if active else 0,
            "versions": int(active),
            "artifact_entries": 16 if active else 0,
            "logical_bytes": growth,
            "allocated_bytes": growth,
            "file_bytes": 2 * MIB,
        }
    return {
        "format": "ordered-owner-qualification-v1",
        **binding,
        "owner": owner,
        "source_binding": source_binding,
        "schema": "castle-exact-six-string-v1",
        "storage_mode": "2.0",
        "expected_states": [dict(before), dict(after)],
        "max_physical_rows": 16384,
        "max_fragments": FINAL_FRAGMENTS,
        "max_versions": FINAL_VERSIONS,
        "max_batch_receipts": TOTAL_INTERVALS,
        "max_artifact_entries": profile["destination_entries"],
        "max_artifact_depth": profile["destination_depth"],
        "max_logical_bytes": profile["destination_logical_bytes"],
        "max_allocated_bytes": profile["destination_allocated_bytes"],
        "max_file_bytes": profile["resulting_file_bytes"],
        "reservations": reservations,
    }


def load_frozen_config(path, expected_hash):
    import hashlib
    import json
    import os
    import stat

    if (
        not isinstance(path, (str, Path))
        or not isinstance(expected_hash, str)
        or len(expected_hash) != 64
    ):
        raise ValueError("missing root freeze")
    file = Path(path)
    if not file.is_absolute() or any(part.is_symlink() for part in [file, *file.parents]):
        raise ValueError("frozen configuration path")
    info = file.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
        or info.st_nlink != 1
        or info.st_size > 65536
    ):
        raise ValueError("private bounded configuration")
    with file.open("rb") as source:
        raw = source.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError("root freeze digest")
    value = json.loads(raw)
    if (
        value.get("format") != "ordered-history-52mib-fixture-v1"
        or value.get("profile") != candidate_profile()
    ):
        raise ValueError("fixed candidate profile")
    required = {
        "format",
        "profile",
        "run_root",
        "packet_root",
        "pytest_basetemp",
        "source_roots",
        "source_descriptor",
        "source_binding",
        "owner_manifest",
        "final_cursor",
        "source_evidence",
        "source_evidence_hash",
        "code_pins",
        "mode",
        "continuation",
    }
    if value.keys() != required or value["mode"] != "persisted" or len(value["source_roots"]) != 8:
        raise ValueError("fixed configuration fields")
    from cognitive_castle import bulk_exact_codec, migration_codec

    registration = migration_codec.registration(
        value["source_descriptor"], str(Path(value["run_root"]) / "generation")
    )
    bulk_exact_codec.validate_owner(value["owner_manifest"])
    owner = value["owner_manifest"]
    if (
        value["source_binding"]
        != "5896e4030bafea7f84fa4e9c21f105ad395645cf319086b704a09e30b5c0f049"
        or registration["source_binding"] != value["source_binding"]
        or owner["source_binding"] != value["source_binding"]
        or owner["members"] != 6656
        or owner["text_bytes"] != 54525952
        or owner["text_hash"] != "641df0406585127d51c04579543c53ac2dae29bdbf30ddc69f75de58647ed615"
        or owner["live"] is not None
    ):
        raise ValueError("accepted exact historical source")
    accepted_path = (
        ROOT
        / ".superpowers/sdd/2026-10-24-unified-lance-memory/layout52-windowed-numerical-qualification-20261025/results.json"
    )
    if (
        Path(value["source_evidence"]) != accepted_path
        or value["source_evidence_hash"]
        != "3301bc9f2dc9fa0fa49d58d58c4e0c6598bc2f8f7ef35c6d425215d6977e85d2"
        or accepted_path.stat().st_size > 512 * 1024
        or hashlib.sha256(accepted_path.read_bytes()).hexdigest() != value["source_evidence_hash"]
    ):
        raise ValueError("accepted numerical evidence")
    for name in ("run_root", "packet_root", "pytest_basetemp"):
        target = Path(value[name])
        if (
            not target.is_absolute()
            or str(target) != value[name]
            or ".." in target.parts
            or any(p.is_symlink() for p in [target, *target.parents])
        ):
            raise ValueError("fixed private retention root")
    value["_config_path"], value["_config_hash"] = str(file), expected_hash
    if file != CONFIGURATION_PATH or Path(value["packet_root"]) != TRUSTED_PACKET_ROOT:
        raise ValueError("fixed trusted configuration/pin location")
    value["_attested_files"] = verify_trusted_seal(value)
    continuation = value["continuation"]
    if (
        value["mode"] != "persisted"
        or continuation.keys()
        != {"format", "prior_starts", "max_new_starts", "max_cumulative_starts", "prior_files"}
        or continuation["format"] != "ordered-history52-persisted-replay-v1"
    ):
        raise ValueError("fixed continuation authorization")
    run = Path(value["run_root"])
    expected_prior = {
        RETIRED_CONFIGURATION_PATH,
        PREVIOUS_CONFIGURATION_PATH,
        PUBLISHED_CONFIGURATION_PATH,
        COLD_CONFIGURATION_PATH,
        RETIRED_PACKET_ROOT / "final-seal.json",
        RETIRED_PACKET_ROOT / "launch-pin.json",
        TRUSTED_PACKET_ROOT / "worker-run1.py",
        TRUSTED_PACKET_ROOT / "helper-run1.py",
        TRUSTED_PACKET_ROOT / "final-seal.json",
        TRUSTED_PACKET_ROOT / "launch-pin.json",
        TRUSTED_PACKET_ROOT / "worker-run2.py",
        TRUSTED_PACKET_ROOT / "helper-run2.py",
        TRUSTED_PACKET_ROOT / "worker-run3.py",
        TRUSTED_PACKET_ROOT / "helper-run3.py",
        TRUSTED_PACKET_ROOT / "worker-run4.py",
        TRUSTED_PACKET_ROOT / "helper-run4.py",
        TRUSTED_PACKET_ROOT / "final-seal-cold.json",
        TRUSTED_PACKET_ROOT / "launch-pin-cold.json",
        ROOT / "tests/lance_phase_cost_probe.py",
        run / "evidence/phase-cost-events.bin",
        run / "evidence/phase-cost-result.json",
        run
        / "qualifications/append-171-fb5a90aaf74540004785b61f1066d34e81185cb28f3a35889b653afc408ab992.json",
        *(
            run / "evidence" / name
            for name in ("events-cold.jsonl", "peaks-cold.jsonl", "first-refusal-cold.json")
        ),
        TRUSTED_PACKET_ROOT / "final-seal-continuation2.json",
        TRUSTED_PACKET_ROOT / "launch-pin-continuation2.json",
        run / "evidence/publication-loss.json",
        *(
            run / "evidence" / name
            for name in (
                "events-continuation2.jsonl",
                "peaks-continuation2.jsonl",
                "first-refusal-continuation2.json",
            )
        ),
        *(
            run / "evidence" / name
            for name in (
                "events-continuation1.jsonl",
                "peaks-continuation1.jsonl",
                "first-refusal-continuation1.json",
            )
        ),
        *(
            run / "evidence" / name
            for name in ("events.jsonl", "peaks.jsonl", "first-refusal.json")
        ),
    }
    if {Path(item["path"]) for item in continuation["prior_files"]} != expected_prior:
        raise ValueError("fixed retained attempt files")
    value["_prior_attested_files"] = verify_continuation(value)
    return value


def census(roots, *, max_entries, max_depth, skip=(), until=float("inf"), sampling=False):
    """Streaming metadata-only inventory; includes roots, directories and orphans."""
    import os
    import stat
    import time

    omitted = {str(Path(path).absolute()) for path in skip}
    result = {
        "entries": 0,
        "files": 0,
        "directories": 0,
        "logical_bytes": 0,
        "allocated_bytes": 0,
        "max_file_bytes": 0,
        "depth": 0,
    }
    for supplied in roots:
        root = Path(supplied).absolute()
        stack = []

        def account(path, depth):
            if time.monotonic() >= until:
                raise ValueError("fixture census deadline")
            try:
                info = path.lstat()
            except FileNotFoundError:
                if sampling:
                    return
                raise
            directory = stat.S_ISDIR(info.st_mode)
            if (
                (not directory and not stat.S_ISREG(info.st_mode))
                or info.st_uid != os.getuid()
                or info.st_mode & 0o077
                or (not directory and info.st_nlink != 1)
            ):
                raise ValueError("fixture private artifact")
            result["entries"] += 1
            result["directories" if directory else "files"] += 1
            result["logical_bytes"] += info.st_size
            result["allocated_bytes"] += info.st_blocks * 512
            result["depth"] = max(result["depth"], depth)
            if not directory:
                result["max_file_bytes"] = max(result["max_file_bytes"], info.st_size)
            if result["entries"] > max_entries or depth > max_depth:
                raise ValueError("fixture artifact ceiling")
            if directory:
                try:
                    stack.append((os.scandir(path), depth + 1))
                except FileNotFoundError:
                    if not sampling:
                        raise

        if str(root) in omitted:
            continue
        account(root, 0)
        try:
            while stack:
                iterator, depth = stack[-1]
                entry = next(iterator, None)
                if entry is None:
                    iterator.close()
                    stack.pop()
                elif entry.path not in omitted:
                    account(Path(entry.path), depth)
        finally:
            for iterator, _ in stack:
                iterator.close()
    return result


def check_temp_entries(retained, *, unlinked=0, workspace=0):
    if (
        any(type(n) is not int or n < 0 for n in (retained, unlinked, workspace))
        or retained + unlinked + workspace > 1008
    ):
        raise ValueError("fixed temp entry reservation")


def reserve_stage(snapshot, kind):
    """Charge complete retained families plus the next fixed operation BEFORE IO."""
    profile = candidate_profile()
    if "groups" in snapshot:
        check_temp_entries(
            snapshot["groups"]["temp"]["entries"],
            workspace=int(kind in {"append", "maintenance", "read"}),
        )
    if kind not in {"append", "maintenance", "read", "queue", "source"}:
        raise ValueError("fixture stage family")
    if any(
        type(snapshot[name]) is not int or snapshot[name] < 0
        for name in ("source", "destination", "harness", "temp", "queue", "free")
    ):
        raise ValueError("fixture capacity snapshot")
    growth = {"append": MIB, "maintenance": 2 * MIB}.get(kind, 0)
    working = (
        64 * MIB if kind in {"append", "maintenance", "read"} else 8 * MIB if kind == "queue" else 0
    )
    queue_bound = 8 * MIB if kind == "queue" else snapshot["queue"]
    metadata = 65536
    harness = snapshot["harness"] + max(0, queue_bound - snapshot["queue"]) + metadata
    destination = snapshot["destination"] + growth
    temporary = snapshot["temp"] + working
    resulting = snapshot["source"] + destination + harness + temporary
    reserve = growth + max(0, queue_bound - snapshot["queue"]) + working + metadata
    if (
        snapshot["queue"] > 8 * MIB
        or destination > profile["destination_allocated_bytes"]
        or harness > profile["harness_bytes"]
        or temporary > profile["temp_bytes"]
        or resulting > profile["whole_bytes"]
        or snapshot["free"] < profile["minimum_free_bytes"] + reserve
    ):
        raise ValueError("fixture capacity reservation")
    return {
        "destination_resulting_bytes": destination,
        "harness_resulting_bytes": harness,
        "temp_resulting_bytes": temporary,
        "queue_resulting_bytes": queue_bound,
        "working_temp_bytes": working,
        "whole_resulting_bytes": resulting,
        "reservation_bytes": reserve,
    }


def set_exact_environment(temporary):
    """The fixed child-only16/64MiB configuration, before any native import."""
    import os
    import sys
    import tempfile
    from cognitive_castle.lance_exact_store import LanceExactStore

    if {"lance", "pyarrow"}.intersection(sys.modules):
        raise ValueError("native profile must precede engine import")
    path = Path(temporary).absolute()
    if any(part.is_symlink() for part in [path, *path.parents]):
        raise ValueError("private native temp path")
    LanceExactStore._private(path, directory=True)
    os.environ["LANCE_MEM_POOL_SIZE"] = str(16 * MIB)
    os.environ["LANCE_MAX_TEMP_DIRECTORY_SIZE"] = str(64 * MIB)
    os.environ.pop("LANCE_BYPASS_SPILLING", None)
    os.environ["TMPDIR"] = str(path)
    tempfile.tempdir = None
    return {
        "pool_bytes": 16 * MIB,
        "tracked_spill_bytes": 64 * MIB,
        "bypass_present": False,
        "tmpdir": str(path),
    }


from cognitive_castle.semantic_runtime import SemanticSession  # noqa: E402


class FixedSession(SemanticSession):
    """Real fixed session with stricter in-clock fixture accounting only."""

    def __init__(self, config, ledger, book, *, role="stage", not_after=None):
        super().__init__(ROOT / "unused-layout52-profile", interactive=False)
        if role not in {"stage", "loss", "cold"}:
            raise ValueError("fixed fixture session role")
        self.config, self.ledger, self.book = config, ledger, book
        self.role, self.import_calls = role, 0
        self.actual_exact_metrics = []
        self.last_exact_result = None
        self.not_after = not_after

    def _run_stage(self, request, *, worker_command, cap=5, rss=512 * MIB, total=640 * MIB):
        import time

        operation = request.get("operation")
        if operation is None and set(request) == {"descriptor", "cursor"}:
            kind = "source"
        elif operation in {
            "queue_migration_open",
            "queue_migration_state",
            "queue_migration_prepare",
            "queue_migration_ack",
            "queue_migration_refuse",
        }:
            kind = "queue"
        elif operation in {"ordered_import_batch", "ordered_import_batch_outcome"}:
            kind = "append"
        elif operation == "ordered_build_id_index":
            kind = "maintenance"
        elif operation in {"ordered_bulk_owner", "ordered_bulk_receipt"}:
            kind = "read"
        else:
            raise ValueError("fixed fixture stage operation")
        until = min(self._until, time.monotonic() + cap)
        try:
            self.ledger.context = {
                "operation": operation or "source",
                "ordinal": self.book.ordinal if self.book else None,
            }
            self.ledger.before(kind, until)
            remaining = until - time.monotonic()
            if remaining <= 0:
                raise ValueError("fixture stage preflight deadline")
            previous_resources = len(self.resources)
            result = super()._run_stage(
                request,
                worker_command=worker_command,
                cap=min(cap, remaining),
                rss=rss,
                total=total,
            )
            self.ledger.resource = (
                self.resources[-1] if len(self.resources) > previous_resources else None
            )
            self.ledger.after(kind, result, until)
            if time.monotonic() >= until:
                raise ValueError("fixture stage accounting deadline")
            return result
        except (ValueError, OSError) as error:
            self._refusal = "fixture_disk_guard"
            if hasattr(self.ledger, "refusal"):
                self.ledger.refusal(kind, error)
            return {"status": "unavailable", "reason": "fixture_disk_guard", "detail": str(error)}

    def store(self, directory, operation, arguments):
        import time

        started = time.monotonic()
        cap = 2 if operation == "queue_migration_state" else 3
        result = super().store(directory, operation, arguments)
        if result.get("status") == "ok":
            if operation in {"queue_migration_open", "queue_migration_state"}:
                self.book.observe_state(result)
            elif operation == "queue_migration_prepare":
                self.book.prepare(arguments["intent"])
            elif operation == "queue_migration_ack":
                self.book.acknowledged(result)
        if time.monotonic() >= min(self._until, started + cap):
            self._refusal = "fixture_control_deadline"
            return {"status": "unavailable", "reason": self._refusal}
        return result

    def exact(self, directory, operation, arguments):
        import os
        import time

        spec = exact_stage(operation)
        until = min(self._until, time.monotonic() + spec["cap"])
        if self.not_after is not None:
            until = min(until, self.not_after)
        expected = Path(self.config["run_root"]) / "generation/exact"
        if Path(directory).absolute() != expected:
            raise ValueError("fixed dedicated destination")
        reference, digest = self.book.for_call(operation, arguments)
        kind = (
            "append"
            if operation in {"import_batch", "import_batch_outcome"}
            else "maintenance"
            if operation == "build_id_index"
            else "read"
        )
        # Reserve before creating a private native workspace; actual run-stage
        # preflight then charges that retained directory too, under the same cap.
        reserve_stage(self.ledger.snapshot(until), kind)
        if operation in {"import_batch", "import_batch_outcome"}:
            self.import_calls += 1
            if self.import_calls > (1 if self.role == "loss" else 2):
                raise ValueError("fixed actual import call count")
            phase = (
                ("publish" if self.import_calls == 1 else "replay")
                if self.role != "cold"
                else f"replay{self.import_calls}"
            )
            label = f"exact-{self.role}-{self.book.ordinal:03}-{phase}"
        elif operation in {"build_id_index", "bulk_receipt"}:
            label = f"exact-index-{self.book.ordinal:03}-" + (
                "build" if operation == "build_id_index" else "read"
            )
        else:
            label = "exact-cold-171-owner"
        temporary = Path(self.config["run_root"]) / "tmp" / label
        try:
            temporary.mkdir(mode=0o700)
        except FileExistsError:
            from cognitive_castle.lance_exact_store import LanceExactStore

            LanceExactStore._private(temporary, directory=True)
        with os.scandir(Path(self.config["run_root"]) / "tmp") as items:
            if sum(entry.name.startswith("exact-") for entry in items) > 380:
                raise ValueError("fixed logical exact workspace count")
        request = {
            "directory": str(expected),
            "operation": spec["operation"],
            "arguments": arguments,
            "qualification": reference,
            "qualification_hash": digest,
        }
        result = self._run_stage(
            request,
            worker_command=[
                sys.executable,
                "-I",
                str(ROOT / "tests/test_lance_owner_stream.py"),
                reference,
                digest,
                self.config["_config_path"],
                self.config["_config_hash"],
                str(temporary),
            ],
            cap=max(0, min(spec["cap"], until - time.monotonic())),
            rss=spec["rss"],
            total=spec["total"],
        )
        # Evidence was retained by the ledger. The unchanged coordinator must
        # compare the original compact receipt, with no fixture-only additions.
        if result.get("fixture_metrics") is not None:
            self.actual_exact_metrics.append({"operation": operation, **result["fixture_metrics"]})
        clean = {key: value for key, value in result.items() if not key.startswith("fixture_")}
        if (
            operation == "import_batch_outcome"
            and clean.get("status") == "ok"
            and clean.get("outcome") != "ExistingVerified"
        ):
            self._refusal = "fixture_expected_existing_verified"
            return {"status": "unavailable", "reason": self._refusal}
        self.last_exact_result = clean
        return clean


class QualificationBook:
    """Immutable metadata pins; only actual queue state selects source progress."""

    def __init__(self, config, binding):
        from cognitive_castle import migration_codec

        self.config, self.binding = config, binding
        self.run = Path(config["run_root"])
        self.registration = migration_codec.registration(
            config["source_descriptor"], str(self.run / "generation")
        )
        if self.registration["source_binding"] != config["source_binding"]:
            raise ValueError("original registration binding")
        self.ordinal = 1
        self.geometry = {"version": 0, "physical_rows": 0, "fragments": 0}
        self.active = self.reference = self.maintenance = self.last_ack = None
        self.confirmed_maintenance = None

    def _read(self, path, operation):
        import json
        from cognitive_castle.canonical_records import _hash, _json
        from cognitive_castle.bulk_owner_stream import OrderedQualification
        from cognitive_castle.lance_exact_store import LanceExactStore

        LanceExactStore._private(path)
        if path.stat().st_size > 16384:
            raise ValueError("qualification metadata bound")
        raw = path.read_text()
        value = json.loads(raw)
        if raw != _json(value):
            raise ValueError("canonical qualification file")
        OrderedQualification(raw, _hash(raw))
        states = value["expected_states"]
        if len(states) != 2 or value != qualification_for(
            self.binding,
            self.config["owner_manifest"]["owner"],
            self.config["source_binding"],
            states[0],
            states[1],
            operation=operation,
        ):
            raise ValueError("qualification differs from fixed root envelope")
        return value, (str(path), _hash(raw))

    def _write(self, path, value, operation):
        import os
        from cognitive_castle.canonical_records import _json

        raw = _json(value).encode()
        if len(raw) > 16384:
            raise ValueError("qualification metadata bound")
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            observed, reference = self._read(path, operation)
            if observed != value:
                raise ValueError("immutable qualification conflict")
            return reference
        try:
            if os.write(fd, raw) != len(raw):
                raise ValueError("incomplete qualification write")
            os.fsync(fd)
        finally:
            os.close(fd)
        return self._read(path, operation)[1]

    def _find_ack(self, identity):
        import os
        import re

        found = None
        with os.scandir(self.run / "qualifications") as entries:
            for count, entry in enumerate(entries, 1):
                if count > 189:
                    raise ValueError("qualification count")
                match = re.fullmatch(r"append-(\d{3})-([a-f0-9]{64})\.json", entry.name)
                if match and match[2] == identity:
                    if found is not None:
                        raise ValueError("duplicate ACK qualification")
                    ordinal = int(match[1])
                    if not 1 <= ordinal <= TOTAL_INTERVALS:
                        raise ValueError("qualification ordinal")
                    value, reference = self._read(Path(entry.path), "append")
                    prior = ordinal - 1 + sum(item <= ordinal for item in MAINTENANCE_BEFORE)
                    if (
                        value["expected_states"][0]["version"] != prior
                        or value["expected_states"][1]["fragments"] != ordinal
                    ):
                        raise ValueError("ACK pin schedule")
                    found = ordinal, value, reference
        if found is None:
            raise ValueError("actual ACK lacks retained qualification")
        return found

    def observe_state(self, state):
        from cognitive_castle import migration_codec

        if state.get("status") != "ok" or state["registration"] != self.registration:
            raise ValueError("real queue registration")
        migration_codec.cursor(state["staging_cursor"], self.registration)
        last = state["last_ack"]
        self.active = self.reference = None
        if last is None:
            if state["staging_cursor"] != migration_codec.initial_source_cursor(self.registration):
                raise ValueError("unacknowledged advanced cursor")
            self.ordinal = 1
            self.geometry = {"version": 0, "physical_rows": 0, "fragments": 0}
        else:
            ordinal, value, _ = self._find_ack(last["intent_id"])
            if last["result"]["staging_cursor"] != state["staging_cursor"] or state[
                "staging_cursor"
            ] == migration_codec.initial_source_cursor(self.registration):
                raise ValueError("actual ACK cursor")
            self.ordinal, self.geometry = ordinal + 1, dict(value["expected_states"][1])
            self.last_ack = last["intent_id"]
        if self.confirmed_maintenance is not None:
            ordinal, before, after = self.confirmed_maintenance
            if ordinal == self.ordinal and self.geometry == before:
                self.geometry = dict(after)
        if state["pending"] is not None:
            self.prepare(state["pending"])

    def prepare(self, intent):
        from cognitive_castle import migration_codec
        from cognitive_castle.canonical_records import _json

        migration_codec.validate_interval(intent, self.registration)
        if not 1 <= self.ordinal <= TOTAL_INTERVALS or intent["phase"] != "history":
            raise ValueError("dedicated history interval")
        expected_owners = int(self.ordinal == TOTAL_INTERVALS)
        if len(intent["owners"]) != expected_owners:
            raise ValueError("original final interval position")
        if expected_owners:
            expected = self.config["owner_manifest"]
            if (
                intent["entries"] != 73
                or intent["cursor_before"] != self.config["final_cursor"]
                or intent["owners"][0]["owner"] != expected["owner"]
            ):
                raise ValueError("original final interval identity")
        path = self.run / "qualifications" / f"append-{self.ordinal:03}-{intent['intent_id']}.json"
        if path.exists():
            value, reference = self._read(path, "append")
            before, after = value["expected_states"]
            if after["physical_rows"] - before["physical_rows"] != physical_rows(
                entries=intent["entries"], owners=expected_owners
            ):
                raise ValueError("prepared rows changed")
        else:
            before = dict(self.geometry)
            scheduled = self.ordinal - 1 + sum(item <= self.ordinal for item in MAINTENANCE_BEFORE)
            if before["fragments"] != self.ordinal - 1 or before["version"] != scheduled:
                raise ValueError("required maintenance not reconciled")
            after = after_append(before, entries=intent["entries"], owners=expected_owners)
            value = qualification_for(
                self.binding,
                self.config["owner_manifest"]["owner"],
                self.config["source_binding"],
                before,
                after,
                operation="append",
            )
            reference = self._write(path, value, "append")
        scheduled_version = (
            self.ordinal - 1 + sum(item <= self.ordinal for item in MAINTENANCE_BEFORE)
        )
        if before["fragments"] != self.ordinal - 1 or before["version"] != scheduled_version:
            raise ValueError("required maintenance not reconciled")
        if expected_owners and after != {"version": 189, "physical_rows": 13696, "fragments": 171}:
            raise ValueError("exact original final physical geometry")
        # Persisted pin contains only geometry, identities and numerical ceilings.
        # The actual body-free pending object remains supplied by the real queue.
        if len(_json(intent).encode()) > migration_codec.MAX_METADATA:
            raise ValueError("pending metadata bound")
        self.active, self.reference = intent, reference
        self.geometry = dict(before)

    def for_call(self, operation, arguments):
        from cognitive_castle import bulk_exact_codec

        if self.maintenance is not None:
            if operation == "build_id_index" and arguments == {}:
                return self.maintenance["reference"]
            if operation == "bulk_receipt" and arguments == {
                "batch_id": self.last_ack,
                "source_binding": self.config["source_binding"],
            }:
                return self.maintenance["reference"]
            raise ValueError("fixed maintenance operation")
        if self.reference is None or self.active is None:
            raise ValueError("no actual pending interval")
        if operation in {"import_batch", "import_batch_outcome"}:
            if (
                arguments["batch_id"] != self.active["intent_id"]
                or bulk_exact_codec.validate_batch(**arguments) != self.active["digest"]
            ):
                raise ValueError("actual regenerated batch differs")
            if self.active["owners"] and arguments["finalizations"] != [
                self.config["owner_manifest"]
            ]:
                raise ValueError("actual full source owner manifest differs")
        elif operation == "bulk_owner":
            if (
                not self.active["owners"]
                or arguments["owner"] != self.config["owner_manifest"]["owner"]
            ):
                raise ValueError("actual historical owner read")
        else:
            raise ValueError("no bound maintenance read")
        return self.reference

    def acknowledged(self, result):
        if (
            self.active is None
            or self.reference is None
            or result.get("status") != "ok"
            or result["intent_id"] != self.active["intent_id"]
            or result["staging_cursor"] != self.active["cursor_after"]
        ):
            raise ValueError("actual queue ACK binding")
        value, _ = self._read(Path(self.reference[0]), "append")
        self.geometry = dict(value["expected_states"][1])
        self.last_ack = self.active["intent_id"]
        self.ordinal += 1
        self.active = self.reference = None

    def start_maintenance(self):
        if self.active is not None or self.last_ack is None or not maintenance_before(self.ordinal):
            raise ValueError("fixed maintenance boundary")
        before = dict(self.geometry)
        after = after_maintenance(before)
        value = qualification_for(
            self.binding,
            self.config["owner_manifest"]["owner"],
            self.config["source_binding"],
            before,
            after,
            operation="maintenance",
        )
        path = self.run / "qualifications" / f"maintenance-{self.ordinal:03}.json"
        created = not path.exists()
        reference = self._write(path, value, "maintenance")
        self.maintenance = {"value": value, "reference": reference, "created": created}
        return reference

    def maintenance_state(self, observed):
        if self.maintenance is None or observed.get("status") != "ok":
            raise ValueError("actual maintenance read required")
        before, after = self.maintenance["value"]["expected_states"]
        coverage = observed["lookup"]
        if coverage["version"] == before["version"]:
            return "before"
        if (
            coverage["version"] != after["version"]
            or coverage["indexed_fragments"] != after["fragments"]
            or coverage["tail_rows"]
            or coverage["tail_fragments"]
        ):
            raise ValueError("actual persisted index coverage")
        return "after"

    def finish_maintenance(self, observed):
        if self.maintenance_state(observed) != "after":
            raise ValueError("maintenance remains unacknowledged")
        before, after = self.maintenance["value"]["expected_states"]
        self.geometry = dict(after)
        # Ephemeral actual-read version evidence; source cursor remains solely
        # the real queue's. A fresh process must perform its own read again.
        self.confirmed_maintenance = (self.ordinal, dict(before), dict(after))
        self.maintenance = None


def verify_continuation(config):
    """Charge the immutable failed attempt, then allow only its fixed remainder."""
    import hashlib
    import json
    import os
    import stat

    claim = config["continuation"]
    if any(
        type(claim.get(key)) is not int or claim[key] != expected
        for key, expected in (
            ("prior_starts", 1083),
            ("max_new_starts", 7),
            ("max_cumulative_starts", 1090),
        )
    ):
        raise ValueError("fixed cumulative continuation schedule")
    if not isinstance(claim["prior_files"], list) or not 14 <= len(claim["prior_files"]) <= 37:
        raise ValueError("bounded retained file claims")
    retained, attested = {}, []
    for item in claim["prior_files"]:
        if item.keys() != {"path", "bytes", "sha256"}:
            raise ValueError("retained exact file claim")
        path = Path(item["path"])
        info = path.lstat()
        if (
            not path.is_absolute()
            or path in retained
            or any(p.is_symlink() for p in [path, *path.parents])
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_mode & 0o022
            or type(item["bytes"]) is not int
            or not 1 <= item["bytes"] <= 4 * MIB
            or info.st_size != item["bytes"]
        ):
            raise ValueError("retained bounded private evidence")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != item["sha256"]:
            raise ValueError("retained attempt digest changed")
        retained[path] = raw
        attested.append(
            {"path": str(path), "identity": file_identity(info), "sha256": item["sha256"]}
        )
    evidence = Path(config["run_root"]) / "evidence"
    starts = 0
    for suffix, expected_starts, expected_ends, failure_kind, failure_reason in (
        ("", 122, 122, "queue", "background_preempted"),
        ("-continuation1", 225, 225, "append", "background_preempted"),
        ("-continuation2", 1075, 1074, "append", "fixture census deadline"),
        ("-cold", 1080, 1079, "append", "fixture census deadline"),
    ):
        events, peaks, refusal = (
            evidence / (name + suffix + extension)
            for name, extension in (
                ("events", ".jsonl"),
                ("peaks", ".jsonl"),
                ("first-refusal", ".json"),
            )
        )
        if not {events, peaks, refusal}.issubset(retained):
            raise ValueError("all retained attempt streams required")
        initial, ended, pending = starts, starts, None
        for line in retained[events].splitlines(keepends=True):
            if len(line) > 1536 or not line.endswith(b"\n"):
                raise ValueError("retained event framing")
            row = json.loads(line)
            if row.get("phase") == "start" and pending is None:
                starts += 1
                if row["stage"] != starts:
                    raise ValueError("retained stage sequence")
                pending = starts
            elif row.get("phase") == "end" and row["stage"] == pending:
                ended, pending = row["stage"], None
                if (row.get("status"), row.get("reason")) != (
                    ("unavailable", "background_preempted")
                    if expected_starts == expected_ends == ended
                    else ("ok", None)
                ):
                    raise ValueError("retained result sequence")
            else:
                raise ValueError("retained event pairing")
        if (
            starts != expected_starts
            or ended != expected_ends
            or len(retained[peaks].splitlines()) != ended - initial
        ):
            raise ValueError("retained observed call count")
        if (pending is None) != (expected_starts == expected_ends):
            raise ValueError("retained unpaired start changed")
        if len(retained[refusal]) != 4096 or json.loads(retained[refusal]) != {
            "stage": expected_starts,
            "kind": failure_kind,
            "reason": failure_reason,
        }:
            raise ValueError("retained original refusal")
    slot = retained[evidence / "phase-cost-events.bin"]
    result = json.loads(retained[evidence / "phase-cost-result.json"])
    if len(slot) != 32768 or slot[:4096].strip() or slot[31744:].strip():
        raise ValueError("retained diagnostic arena")
    cells = [json.loads(slot[4096 + i * 3072 : 4096 + (i + 1) * 3072]) for i in range(9)]
    for index, stage in enumerate(range(1081, 1084)):
        begin, end, peak = cells[index * 3 : index * 3 + 3]
        if (
            begin.get("phase"),
            begin.get("stage"),
            end.get("phase"),
            end.get("stage"),
            end.get("status"),
            end.get("reason"),
            peak.get("stage"),
        ) != ("start", stage, "end", stage, "ok", None, stage):
            raise ValueError("retained diagnostic sequence")
        starts += 1
    if (
        starts != 1083
        or result.get("calls") != starts
        or result.get("diagnostic_only") is not True
        or result.get("receipt_matches") is not True
    ):
        raise ValueError("diagnostic count only; no cached proof authority")
    return attested


class DiskLedger:
    """Fixed diagnostic families and sampled evidence, not a filesystem quota."""

    def __init__(self, config):
        import json

        self.config = config
        self.run = Path(config["run_root"])
        continuation = config.get("continuation")
        suffix = "-persisted" if continuation else ""
        self.events = self.run / "evidence" / f"events{suffix}.jsonl"
        self.peaks = self.run / "evidence" / f"peaks{suffix}.jsonl"
        if continuation:
            config["_prior_attested_files"] = verify_continuation(config)
        self.calls = continuation["prior_starts"] if continuation else 0
        self.call_ceiling = continuation["max_cumulative_starts"] if continuation else 1071
        self.context, self.resource = {}, None
        self.charge = None
        if self.events.exists():
            if self.events.stat().st_size > 4 * MIB:
                raise ValueError("bounded existing evidence")
            with self.events.open() as stream:
                for line in stream:
                    if len(line.encode()) > 1536 or not line.endswith("\n"):
                        raise ValueError("partial bounded evidence")
                    item = json.loads(line)
                    if item.get("phase") == "start":
                        self.calls += 1
                        if item.get("stage") != self.calls:
                            raise ValueError("continuation cumulative stage sequence")
                    if self.calls > self.call_ceiling:
                        raise ValueError("fixed diagnostic call ceiling")

    @staticmethod
    def _size(counts):
        return max(counts["logical_bytes"], counts["allocated_bytes"])

    def snapshot(self, until=float("inf"), *, sampling=False):
        import hashlib
        import os
        import shutil
        import stat
        import time

        exact, temporary = self.run / "generation/exact", self.run / "tmp"
        groups = {
            "source": census(
                self.config["source_roots"], max_entries=128, max_depth=4, until=until
            ),
            "destination": census(
                [exact], max_entries=1024, max_depth=6, until=until, sampling=sampling
            ),
            "harness": census(
                [
                    self.run,
                    self.config["packet_root"],
                    *([RETIRED_PACKET_ROOT] if self.config.get("continuation") else []),
                ],
                max_entries=288,
                max_depth=6,
                skip=[exact, temporary],
                until=until,
            ),
            "temp": census(
                [temporary], max_entries=1008, max_depth=8, until=until, sampling=sampling
            ),
        }
        additional = []
        if self.config.get("_config_path") and not Path(self.config["_config_path"]).is_relative_to(
            Path(self.config["packet_root"])
        ):
            additional.append(self.config["_config_path"])
        if self.config.get("pytest_basetemp") and Path(self.config["pytest_basetemp"]).exists():
            additional.append(self.config["pytest_basetemp"])
        if self.config.get("continuation"):
            additional.extend(
                [
                    RETIRED_CONFIGURATION_PATH,
                    RETIRED_BASETEMP,
                    PREVIOUS_CONFIGURATION_PATH,
                    PREVIOUS_BASETEMP,
                    PUBLISHED_CONFIGURATION_PATH,
                    PUBLISHED_BASETEMP,
                    COLD_CONFIGURATION_PATH,
                    COLD_BASETEMP,
                ]
            )
        if additional:
            extra = census(additional, max_entries=288, max_depth=2, until=until)
            for name in ("entries", "files", "directories", "logical_bytes", "allocated_bytes"):
                groups["harness"][name] += extra[name]
        # Pinned existing checkout code is read-only baseline, not native output.
        # Account its files too; do not traverse the checkout or follow links.
        code_bytes = 0
        for attested in self.config.get("_attested_files", []) + self.config.get(
            "_prior_attested_files", []
        ):
            file = Path(attested["path"])
            info = file.lstat()
            if file_identity(info) != attested["identity"] or (
                not sampling and hashlib.sha256(file.read_bytes()).hexdigest() != attested["sha256"]
            ):
                raise ValueError("fixed launch seal/code identity changed")
            if file == WORKER_PATH:
                groups["harness"]["entries"] += 1
                groups["harness"]["files"] += 1
                groups["harness"]["logical_bytes"] += info.st_size
                groups["harness"]["allocated_bytes"] += info.st_blocks * 512
        pins = self.config.get("code_pins", [])
        if len(pins) > 64:
            raise ValueError("fixed code pin count")
        for pin in pins:
            file = Path(pin["path"])
            info = file.lstat()
            if (
                time.monotonic() >= until
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or info.st_mode & 0o022
                or info.st_size != pin["bytes"]
                or info.st_size > MIB
                or (not sampling and any(p.is_symlink() for p in [file, *file.parents]))
            ):
                raise ValueError("pinned read-only fixture code")
            code_bytes += info.st_size
            if code_bytes > 2 * MIB or (
                not sampling and hashlib.sha256(file.read_bytes()).hexdigest() != pin["sha256"]
            ):
                raise ValueError("fixed code digest")
            groups["harness"]["entries"] += 1
            groups["harness"]["files"] += 1
            groups["harness"]["logical_bytes"] += info.st_size
            groups["harness"]["allocated_bytes"] += info.st_blocks * 512
        coordinate = self.run / "generation/coordination"
        queue = census([coordinate], max_entries=5, max_depth=1, until=until)
        sizes = {key: self._size(value) for key, value in groups.items()}
        profile = candidate_profile()
        if (
            groups["harness"]["entries"] > 288
            or sizes["source"] > 512 * MIB
            or sizes["destination"] > 320 * MIB
            or sizes["harness"] > 24 * MIB
            or sizes["temp"] > 80 * MIB
            or self._size(queue) > 8 * MIB
            or sum(sizes.values()) > 512 * MIB
        ):
            raise ValueError("complete retained capacity")
        if groups["destination"]["max_file_bytes"] > 2 * MIB:
            raise ValueError("resulting native file bound")
        return {
            **sizes,
            "queue": self._size(queue),
            "free": shutil.disk_usage(self.run).free,
            "groups": groups,
            "minimum_free_floor": profile["minimum_free_bytes"],
        }

    @staticmethod
    def _append(path, value, maximum_line):
        import json
        import os

        raw = (
            json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        ).encode()
        current = path.stat().st_size if path.exists() else 0
        if len(raw) > maximum_line or current + len(raw) > 4 * MIB:
            raise ValueError("bounded diagnostic metadata log")
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            if os.write(fd, raw) != len(raw):
                raise ValueError("partial diagnostic log")
        finally:
            os.close(fd)

    def before(self, kind, until):
        snapshot = self.snapshot(until)
        reservation = reserve_stage(snapshot, kind)
        if self.calls >= self.call_ceiling:
            raise ValueError("fixed diagnostic call ceiling")
        self.calls += 1
        self.charge = snapshot, reservation
        self._append(
            self.events, {"phase": "start", "stage": self.calls, "kind": kind, **self.context}, 1536
        )

    def after(self, kind, result, until):
        snapshot = self.snapshot(until)
        before, reserved = self.charge
        if (
            snapshot["source"] != before["source"]
            or snapshot["destination"] > reserved["destination_resulting_bytes"]
            or snapshot["harness"] > reserved["harness_resulting_bytes"]
            or snapshot["temp"] > reserved["temp_resulting_bytes"]
        ):
            raise ValueError("charged stage capacity exceeded")
        summary = {
            "phase": "end",
            "stage": self.calls,
            "kind": kind,
            **self.context,
            "status": result.get("status"),
            "reason": result.get("reason"),
            "resources": self.resource,
        }
        if kind == "source":
            summary.update(
                frame_bytes=result.get("frame_bytes"),
                entries=len(result.get("entries", [])),
                finalizations=len(result.get("finalizations", [])),
            )
        if "fixture_metrics" in result:
            summary["fixture_metrics"] = result["fixture_metrics"]
        self._append(self.events, summary, 1536)
        self._append(
            self.peaks,
            {
                "stage": self.calls,
                "kind": kind,
                "groups": snapshot["groups"],
                "free": snapshot["free"],
                "reservation_bytes": reserved["reservation_bytes"],
                "queue_unlinked_peak": "unmeasured",
                "source_transient_disk_peak": "unmeasured",
            },
            2048,
        )
        if result.get("status") != "ok":
            self.refusal(kind, result.get("reason") or result.get("status"))

    def refusal(self, kind, reason):
        import json
        import os

        # This fixed4KiB slot is allocated/charged at initial setup; rewriting
        # its whitespace does not grow a file after a capacity refusal.
        suffix = "-persisted" if self.config.get("continuation") else ""
        path = self.run / "evidence" / f"first-refusal{suffix}.json"
        if not path.exists() or path.stat().st_size != 4096 or path.read_bytes().strip():
            return
        raw = json.dumps(
            {"stage": self.calls, "kind": kind, "reason": str(reason)}, sort_keys=True
        ).encode()
        if len(raw) > 4000:
            raw = b'{"reason":"bounded_fixture_refusal"}'
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        try:
            if os.pwrite(fd, raw.ljust(4096, b" "), 0) != 4096:
                raise ValueError("refusal metadata write")
            os.fsync(fd)
        finally:
            os.close(fd)


def own_fd_metrics():
    import errno
    import os
    import stat

    result = {
        "worker_fd_slots": 0,
        "open_unlinked_logical_bytes": 0,
        "open_unlinked_allocated_bytes": 0,
        "open_unlinked_entries": 0,
    }
    unlinked = set()
    with os.scandir("/dev/fd") as entries:
        for entry in entries:
            if not entry.name.isdecimal():
                raise ValueError("unrecognized worker fd slot")
            result["worker_fd_slots"] += 1
            if result["worker_fd_slots"] > 512 or int(entry.name) >= 512:
                raise ValueError("bounded worker fd slots")
            try:
                info = os.fstat(int(entry.name))
            except OSError as error:
                if error.errno == errno.EBADF:
                    continue
                raise
            if stat.S_ISREG(info.st_mode) and info.st_nlink == 0:
                if (info.st_dev, info.st_ino) not in unlinked:
                    unlinked.add((info.st_dev, info.st_ino))
                    result["open_unlinked_entries"] += 1
                    result["open_unlinked_logical_bytes"] += info.st_size
                    result["open_unlinked_allocated_bytes"] += info.st_blocks * 512
    return result


def exact_worker_call(config, qualification, store, operation, temporary, function):
    """One fixed exact call with actual proof observers and own-fd disk sampler."""
    import json
    import os
    import threading

    run = Path(config["run_root"])
    binding = read_metadata(run / "binding.json", 16384)["binding"]
    value = json.loads(qualification.manifest_json)
    states = value["expected_states"]
    kind = (
        "maintenance"
        if operation == "ordered_build_id_index"
        else "append"
        if operation in {"ordered_import_batch", "ordered_import_batch_outcome"}
        else "read"
    )
    qualified_kind = "maintenance" if value["reservations"]["maintenance"]["versions"] else "append"
    if len(states) != 2 or value != qualification_for(
        binding,
        config["owner_manifest"]["owner"],
        config["source_binding"],
        states[0],
        states[1],
        operation=qualified_kind,
    ):
        raise ValueError("large call differs from frozen envelope")
    temp = Path(temporary)
    if temp.parent != run / "tmp" or not temp.name.startswith("exact-"):
        raise ValueError("fixed exact temp family")
    configured = set_exact_environment(temp)
    ledger = DiskLedger(config)
    baseline = ledger.snapshot()
    reserved = reserve_stage(baseline, kind)
    metrics = {
        "owner_proofs": 0,
        "proof_pending_rows": [],
        "owner_stream_rows": [],
        "verified_owner_hashes": [],
        "configured_pool_bytes": configured["pool_bytes"],
        "configured_tracked_spill_bytes": configured["tracked_spill_bytes"],
        "configured_bypass_present": configured["bypass_present"],
    }
    peak = {
        "logical_bytes": 0,
        "allocated_bytes": 0,
        "entries": 0,
        "temp_bytes": 0,
        "worker_fd_slots": 0,
        "open_unlinked_bytes": 0,
        "minimum_free_bytes": baseline["free"],
    }
    original_geometry, original_proof = store._geometry, store._verify_owner
    original_receipts = store._batch_receipt_state
    original_rows = store._owner_rows

    def geometry(*args, **kwargs):
        return observe_geometry(config, original_geometry, metrics, *args, **kwargs)

    def proof(dataset, manifest, pending):
        from cognitive_castle.canonical_records import _hash, _json

        original_proof(dataset, manifest, pending)
        metrics["owner_proofs"] += 1
        metrics["proof_pending_rows"].append(len(pending))
        metrics["verified_owner_hashes"].append(_hash(_json(manifest)))

    def owner_rows(*args, **kwargs):
        count = batches = 0
        for row in original_rows(*args, **kwargs):
            count += 1
            batches += (
                row["kind"] == "bulk_receipt" and row["id"] != config["owner_manifest"]["owner"]
            )
            yield row
        metrics["owner_stream_rows"].append(count)
        metrics["batch_receipts"] = batches

    def receipts(*args, **kwargs):
        actual = original_receipts(*args, **kwargs)
        metrics["batch_receipts"] = actual[0]
        return actual

    store._geometry, store._verify_owner, store._batch_receipt_state = geometry, proof, receipts
    store._owner_rows = owner_rows
    stopped = threading.Event()

    def sample():
        snapshot = ledger.snapshot(sampling=True)
        fd = own_fd_metrics()
        check_temp_entries(
            snapshot["groups"]["temp"]["entries"], unlinked=fd["open_unlinked_entries"]
        )
        unlinked = max(fd["open_unlinked_logical_bytes"], fd["open_unlinked_allocated_bytes"])
        logical = (
            sum(group["logical_bytes"] for group in snapshot["groups"].values())
            + fd["open_unlinked_logical_bytes"]
        )
        allocated = (
            sum(group["allocated_bytes"] for group in snapshot["groups"].values())
            + fd["open_unlinked_allocated_bytes"]
        )
        if (
            snapshot["destination"] > reserved["destination_resulting_bytes"]
            or snapshot["harness"] > reserved["harness_resulting_bytes"]
            or snapshot["temp"] + unlinked > reserved["temp_resulting_bytes"]
            or snapshot["groups"]["temp"]["entries"] + fd["open_unlinked_entries"] > 1008
            or max(logical, allocated) > 512 * MIB
            or snapshot["free"] < 2 * 1024**3
        ):
            raise ValueError("sampled charged exact disk envelope")
        for name, actual in (
            ("logical_bytes", logical),
            ("allocated_bytes", allocated),
            (
                "entries",
                sum(g["entries"] for g in snapshot["groups"].values())
                + fd["open_unlinked_entries"],
            ),
            ("temp_bytes", snapshot["temp"] + unlinked),
            ("worker_fd_slots", fd["worker_fd_slots"]),
            ("open_unlinked_bytes", unlinked),
        ):
            peak[name] = max(peak[name], actual)
        peak["minimum_free_bytes"] = min(peak["minimum_free_bytes"], snapshot["free"])

    def monitor():
        while not stopped.wait(0.05):
            try:
                sample()
            except Exception as error:
                ledger.refusal(kind, error)
                os._exit(73)

    sample()
    watcher = threading.Thread(target=monitor, name="fixed-exact-disk-sample", daemon=True)
    watcher.start()
    try:
        result = function()
        sample()
    except Exception as error:
        ledger.refusal(kind, error)
        raise
    finally:
        stopped.set()
        watcher.join(timeout=0.2)
        if watcher.is_alive():
            ledger.refusal(kind, "fixture sampler did not stop")
            raise ValueError("fixture sampler shutdown deadline")
    ledger.snapshot()  # One post-call code hash proof, not one per sample.
    metrics["sampled_disk_peak"] = peak
    result["fixture_metrics"] = metrics
    return result


def read_metadata(path, maximum=65536):
    import json
    from cognitive_castle.lance_exact_store import LanceExactStore

    file = Path(path)
    if any(p.is_symlink() for p in [file, *file.parents]):
        raise ValueError("metadata symlink")
    LanceExactStore._private(file)
    if file.stat().st_size > maximum:
        raise ValueError("bounded metadata file")
    with file.open("rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("bounded metadata file")
    return json.loads(raw)


def write_metadata(path, value, maximum=512 * 1024):
    import os
    from cognitive_castle.canonical_records import _json

    raw = _json(value).encode()
    if len(raw) > maximum:
        raise ValueError("bounded metadata output")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise ValueError("partial metadata output")
        os.fsync(fd)
    finally:
        os.close(fd)


def install_host_guard():
    import importlib.abc

    class NoHostEngines(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname.split(".")[0] in {
                "sqlite3",
                "lance",
                "pyarrow",
                "numpy",
                "torch",
                "transformers",
                "sentence_transformers",
                "spacy",
                "lancedb",
                "tensorflow",
                "mlx",
            }:
                raise ImportError("historical qualification host forbids engines/models")
            return None

    if {"sqlite3", "lance", "pyarrow", "numpy", "torch"}.intersection(sys.modules):
        raise ValueError("host already imported an engine")
    sys.meta_path.insert(0, NoHostEngines())

    def audit(event, args):
        if event.startswith("socket."):
            raise PermissionError("qualification host networking disabled")

    sys.addaudithook(audit)


def prepare_run(config, *, existing=False):
    import os
    import shutil
    from cognitive_castle import migration_codec
    from cognitive_castle.lance_exact_store import LanceExactStore

    run = Path(config["run_root"])
    registration = migration_codec.registration(
        config["source_descriptor"], str(run / "generation")
    )
    if existing or config["mode"] == "persisted":
        saved = read_metadata(run / "binding.json", 16384)
        if saved["registration"] != registration:
            raise ValueError("retained original registration changed")
        binding = saved["binding"]
        for path, identity in (
            (run / "generation", binding["parent_identity"]),
            (run / "generation/exact", binding["destination_identity"]),
        ):
            LanceExactStore._private(path, directory=True)
            info = path.lstat()
            if [info.st_dev, info.st_ino] != identity:
                raise ValueError("retained destination identity changed")
        basetemp = Path(config["pytest_basetemp"])
        if not basetemp.exists():
            reserve_stage(DiskLedger(config).snapshot(), "source")
            basetemp.mkdir(mode=0o700)
        LanceExactStore._private(basetemp, directory=True)
        return binding

    if os.path.lexists(run):
        raise ValueError("initial retained run root already exists")
    source = census(config["source_roots"], max_entries=128, max_depth=4)
    packet = census([config["packet_root"]], max_entries=288, max_depth=6)
    if (
        max(source["logical_bytes"], source["allocated_bytes"])
        + max(packet["logical_bytes"], packet["allocated_bytes"])
        + MIB
        > 512 * MIB
        or shutil.disk_usage(run.parent).free < 2 * 1024**3 + MIB
    ):
        raise ValueError("initial metadata reservation")
    basetemp = Path(config["pytest_basetemp"])
    if os.path.lexists(basetemp):
        raise ValueError("initial explicit basetemp already exists")
    for path in (
        run,
        run / "generation",
        run / "generation/exact",
        run / "generation/coordination",
        run / "qualifications",
        run / "evidence",
        run / "tmp",
        run / "tmp/coord-source",
        basetemp,
    ):
        path.mkdir(mode=0o700)
    binding = {
        "destination": str(run / "generation/exact"),
        "parent_identity": [(run / "generation").stat().st_dev, (run / "generation").stat().st_ino],
        "destination_identity": [
            (run / "generation/exact").stat().st_dev,
            (run / "generation/exact").stat().st_ino,
        ],
    }
    write_metadata(run / "binding.json", {"registration": registration, "binding": binding}, 16384)
    for name in ("events.jsonl", "peaks.jsonl"):
        fd = os.open(run / "evidence" / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
    fd = os.open(run / "evidence/first-refusal.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        if os.write(fd, b" " * 4096) != 4096:
            raise ValueError("precharged refusal slot")
    finally:
        os.close(fd)
    return binding


def host_temp(config):
    import os
    import tempfile
    from cognitive_castle.lance_exact_store import LanceExactStore

    path = Path(config["run_root"]) / "tmp/coord-source"
    LanceExactStore._private(path, directory=True)
    os.environ["TMPDIR"] = str(path)
    tempfile.tempdir = None


def result_path(config, name):
    return (
        Path(config["run_root"]) / "evidence" / (name + "-" + config["_config_hash"][:12] + ".json")
    )


def actual_tick(config, ledger, book, *, lost_publication=False, cold=False):
    import time
    from cognitive_castle.migration_coordinator import _fixture_tick_persisted
    from test_migration_coordinator import ObservedSession

    class ObservedPersistedSession(ObservedSession):
        @property
        def remaining(self):
            return self.session.remaining

    started = time.monotonic()
    with FixedSession(
        config, ledger, book, role="loss" if lost_publication else "cold" if cold else "stage"
    ) as session:
        observed = ObservedPersistedSession(
            session, loss="import_batch:1" if lost_publication else None
        )
        result = _fixture_tick_persisted(observed, book.registration)
        elapsed = time.monotonic() - started
        if elapsed >= 5 or session.remaining <= 0:
            return {"status": "unavailable", "reason": "fixture_shared_deadline"}, observed, elapsed
        return result, observed, elapsed


def actual_maintenance(config, ledger, book):
    import time

    # The immutable pin's metadata preflight consumes the same fixed first
    # read2s/write3s clock, never a fresh extension before native work.
    existed = (
        Path(config["run_root"]) / "qualifications" / f"maintenance-{book.ordinal:03}.json"
    ).exists()
    if config.get("continuation") and existed:
        raise ValueError("continuation has no maintenance reconciliation allowance")
    first_until = time.monotonic() + (2 if existed else 3)
    reserve_stage(ledger.snapshot(first_until), "read" if existed else "maintenance")
    book.start_maintenance()
    arguments = {"batch_id": book.last_ack, "source_binding": config["source_binding"]}
    if not book.maintenance["created"]:
        with FixedSession(config, ledger, book, not_after=first_until) as session:
            observed = session.exact(
                Path(config["run_root"]) / "generation/exact", "bulk_receipt", arguments
            )
        if observed.get("status") != "ok":
            return observed
        if book.maintenance_state(observed) == "after":
            book.finish_maintenance(observed)
            return {"status": "ok", "state": "existing_index_read"}
    with FixedSession(
        config, ledger, book, not_after=first_until if not existed else None
    ) as session:
        built = session.exact(Path(config["run_root"]) / "generation/exact", "build_id_index", {})
    if built.get("status") != "ok":
        return built
    # Separate fresh native worker + session verifies persisted coverage.
    with FixedSession(config, ledger, book) as session:
        observed = session.exact(
            Path(config["run_root"]) / "generation/exact", "bulk_receipt", arguments
        )
    if observed.get("status") == "ok":
        book.finish_maintenance(observed)
    return observed


def actual_queue_state(config, ledger, book, *, open_job=False):
    with FixedSession(config, ledger, book) as session:
        operation = "queue_migration_open" if open_job else "queue_migration_state"
        arguments = (
            {"registration": book.registration}
            if open_job
            else {"job_id": book.registration["job_id"]}
        )
        return session.store(Path(config["run_root"]) / "generation", operation, arguments)


def cold_run(config):
    import time

    install_host_guard()
    binding = prepare_run(config, existing=True)
    host_temp(config)
    ledger, book = DiskLedger(config), QualificationBook(config, binding)
    # Original coordinator opens the actual pending queue inside its5s lease.
    started = time.monotonic()
    result, observed, elapsed = actual_tick(config, ledger, book, cold=True)
    expected = {
        "queue_migration_open": 1,
        "source": 1,
        "import_batch_outcome": 1,
        "bulk_owner": 1,
        "queue_migration_ack": 1,
    }
    if result.get("status") == "ok" and observed.counts != expected:
        result = {"status": "unavailable", "reason": "fixture_cold_actual_call_sequence"}
    if result.get("status") == "ok" and book.geometry != {
        "version": 189,
        "physical_rows": 13696,
        "fragments": 171,
    }:
        result = {"status": "unavailable", "reason": "fixture_cold_actual_geometry"}
    from cognitive_castle.canonical_records import _hash, _json

    owner_hash = _hash(_json(config["owner_manifest"]))
    imports = [
        item
        for item in observed.session.actual_exact_metrics
        if item["operation"] == "import_batch_outcome"
    ]
    if result.get("status") == "ok" and (
        len(imports) != 1
        or any(
            item.get("owner_proofs") != 1
            or item.get("proof_pending_rows") != [0]
            or item.get("owner_stream_rows") != [13696]
            or item.get("batch_receipts") != 171
            or item.get("verified_owner_hashes") != [owner_hash]
            or item.get("geometry") != {"version": 189, "physical_rows": 13696, "fragments": 171}
            for item in imports
        )
    ):
        result = {"status": "unavailable", "reason": "fixture_cold_actual_full_owner_proofs"}
    summary = {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "result": result,
        "calls": observed.counts,
        "session_elapsed": elapsed,
        "process_phase_elapsed": time.monotonic() - started,
        "actual_import_proofs": imports,
        "owner_manifest_hash": owner_hash,
        "text_hash": config["owner_manifest"]["text_hash"],
        "text_bytes": 54525952,
        "members": 6656,
        "geometry": book.geometry,
        "source_eof_claim": False,
        "live_required": False,
    }
    if result.get("status") != "ok":
        ledger.refusal("cold", result.get("reason"))
        # Failure has no new result-growth authorization. Retain the original
        # refusal in the allocated4KiB slot and bounded host stdout only.
        return summary
    write_metadata(result_path(config, "cold-result"), summary, 16384)
    return summary


def observe_geometry(config, original, metrics, *args, **kwargs):
    actual = original(*args, **kwargs)
    if config["mode"] == "persisted" and actual != {
        "version": 189,
        "physical_rows": 13696,
        "fragments": 171,
    }:
        from cognitive_castle.lance_exact_store import _StoreError

        raise _StoreError("stale")
    metrics["geometry"] = actual
    return actual


def require_resume_boundary(book, state, loss):
    """Assert the actual final pending intent and retained successful publication."""
    from cognitive_castle.migration_codec import expected_receipt

    before = {"version": 188, "physical_rows": 13621, "fragments": 170}
    after = {"version": 189, "physical_rows": 13696, "fragments": 171}
    pending = state.get("pending")
    if (
        book.ordinal != 171
        or book.geometry != before
        or pending is None
        or pending != book.active
        or book.reference is None
        or pending.get("intent_id")
        != "fb5a90aaf74540004785b61f1066d34e81185cb28f3a35889b653afc408ab992"
        or book.last_ack != "4187eef5b0897f2b8f4424823e5b229fcfa447b3d981ff413c73e137aa4c20b6"
    ):
        raise ValueError("cold gate requires actual ACK170 and pending171")
    value, reference = book._read(Path(book.reference[0]), "append")
    if reference != book.reference or value["expected_states"] != [before, after]:
        raise ValueError("retained pending171 exact before/after pin")
    if (
        loss["intent_id"] != pending["intent_id"]
        or loss["phase"] != "publish"
        or loss["reason"] != "fixture_lost_response"
        or loss["actual_receipt"] != expected_receipt(pending)
    ):
        raise ValueError("retained final publication receipt differs")


def run_fixture(config):
    import subprocess
    import time

    install_host_guard()
    binding = prepare_run(config)
    host_temp(config)
    ledger, book = DiskLedger(config), QualificationBook(config, binding)
    state = actual_queue_state(config, ledger, book)
    if state.get("status") != "ok":
        return state
    loss_file = Path(config["run_root"]) / "evidence/publication-loss.json"
    require_resume_boundary(book, state, read_metadata(loss_file, 4096))
    if state["staging_cursor"] != config["final_cursor"]:
        raise ValueError("real final queue must remain pending")
    process_started = time.monotonic()
    process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            str(Path(__file__).absolute()),
            "--cold",
            config["_config_path"],
            config["_config_hash"],
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=7)
    except subprocess.TimeoutExpired:
        import os
        import psutil
        import signal

        try:
            children = psutil.Process(process.pid).children(recursive=True)
        except psutil.Error:
            children = []
        for child in reversed(children):
            try:
                child.kill()
            except psutil.Error:
                pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass
        process.wait(timeout=1)
        psutil.wait_procs(children, timeout=1)
        ledger.refusal("cold", "fixture_cold_host_deadline")
        return {"status": "unavailable", "reason": "fixture_cold_host_deadline"}
    # Fixed cold code prints<=7 compact stage lines plus<=16KiB summary, under
    # 32KiB. No original frame/body is printed.7s is a host transport/cleanup
    # bound around the unchanged inner5s session, not a larger native lease.
    if len(stdout.encode()) > 65536 or len(stderr.encode()) > 65536:
        raise ValueError("bounded cold host output")
    print(stdout, end="", flush=True)
    if process.returncode != 0:
        return {
            "status": "unavailable",
            "reason": "fixture_cold_host_refused",
            "returncode": process.returncode,
        }
    cold = read_metadata(result_path(config, "cold-result"), 16384)
    if cold.get("status") != "ok" or cold["session_elapsed"] >= 5:
        return cold
    # Refresh only evidence counters after the separate cold host; cursor and
    # counts come from a fresh real queue-state read, never from these logs.
    ledger, book = DiskLedger(config), QualificationBook(config, binding)
    state = actual_queue_state(config, ledger, book)
    if (
        state.get("status") != "ok"
        or state["pending"] is not None
        or state["counts"]["historical_occurrences"] != 1
        or state["counts"]["members"] != 6656
        or state["counts"]["audit_owners"]
        or state["counts"]["live_owners"]
        or state["owner_references"] != 1
        or state["semantic_pending"] != 1
        or state["state"] != "active"
        or book.geometry != {"version": 189, "physical_rows": 13696, "fragments": 171}
    ):
        raise ValueError("real final metadata ACK/parity")
    snapshot = ledger.snapshot()
    summary = {
        "status": "ok",
        "cold_process_elapsed": time.monotonic() - process_started,
        "cold": cold,
        "state": state,
        "geometry": book.geometry,
        "final_disk": snapshot,
        "source_eof_claim": False,
        "whole_Task1": "pending_root_review",
    }
    write_metadata(result_path(config, "final-result"), summary, 16384)
    return summary


def file_identity(info):
    return {
        name: getattr(info, "st_" + name)
        for name in ("dev", "ino", "size", "mtime_ns", "ctime_ns", "mode", "uid", "nlink")
    }


def verify_trusted_seal(config):
    """Fixed private root pin→exact seal→config/worker/helper, without a hash cycle."""
    import hashlib
    import json
    import os
    import stat

    if (
        Path(config["packet_root"]) != TRUSTED_PACKET_ROOT
        or Path(config["_config_path"]) != CONFIGURATION_PATH
    ):
        raise ValueError("fixed trusted launch location")
    pin_path = TRUSTED_PACKET_ROOT / PIN_NAME
    seal_path = TRUSTED_PACKET_ROOT / SEAL_NAME
    for path in (pin_path, seal_path):
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_mode & 0o277
            or info.st_size > 16384
            or any(p.is_symlink() for p in [path, *path.parents])
        ):
            raise ValueError("private read-only trusted launch pin")
    pin_raw, seal_raw = pin_path.read_bytes(), seal_path.read_bytes()
    pin, seal = json.loads(pin_raw), json.loads(seal_raw)
    if (
        pin.keys() != {"format", "configuration_hash", "seal_hash"}
        or pin["format"] != "ordered-history52-launch-pin-v1"
        or pin["configuration_hash"] != config["_config_hash"]
        or hashlib.sha256(seal_raw).hexdigest() != pin["seal_hash"]
    ):
        raise ValueError("trusted exact launch seal digest")
    if (
        seal.keys() != {"format", "configuration", "worker", "helper"}
        or seal["format"] != "ordered-history52-final-seal-v1"
    ):
        raise ValueError("fixed launch seal shape")
    attested = []
    for name, expected_path in (
        ("configuration", CONFIGURATION_PATH),
        ("worker", WORKER_PATH),
        ("helper", HELPER_PATH),
    ):
        item = seal[name]
        if (
            item.keys() != {"path", "bytes", "sha256"}
            or Path(item["path"]) != expected_path
            or type(item["bytes"]) is not int
            or not 1 <= item["bytes"] <= MIB
        ):
            raise ValueError("fixed exact raw code attestation")
        info = expected_path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_mode & 0o022
            or info.st_size != item["bytes"]
            or hashlib.sha256(expected_path.read_bytes()).hexdigest() != item["sha256"]
        ):
            raise ValueError("exact sealed config/worker/helper bytes changed")
        if name == "configuration" and item["sha256"] != config["_config_hash"]:
            raise ValueError("configuration claim changed")
        attested.append(
            {"path": str(expected_path), "identity": file_identity(info), "sha256": item["sha256"]}
        )
    for path, raw in ((pin_path, pin_raw), (seal_path, seal_raw)):
        attested.append(
            {
                "path": str(path),
                "identity": file_identity(path.lstat()),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    if any(Path(item["path"]) == WORKER_PATH for item in config["code_pins"]):
        raise ValueError("worker raw hash belongs in external seal, not circular config")
    return attested


if __name__ == "__main__":
    import json

    if len(sys.argv) != 4 or sys.argv[1] != "--cold" or not sys.flags.isolated:
        raise SystemExit("fixed cold fixture invocation required")
    try:
        _result = cold_run(load_frozen_config(sys.argv[2], sys.argv[3]))
    except Exception as _error:
        print(
            json.dumps(
                {"status": "unavailable", "reason": type(_error).__name__, "detail": str(_error)}
            ),
            flush=True,
        )
        raise SystemExit(1)
    print("LAYOUT52_COLD", json.dumps(_result, sort_keys=True), flush=True)
    raise SystemExit(0 if _result.get("status") == "ok" else 1)

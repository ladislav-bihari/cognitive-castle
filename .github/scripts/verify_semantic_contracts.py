"""Fixed CI-only pure regression check; no native or real-memory qualification."""

import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

BASELINE = "22ed33ff8d76164931bb4b6d6575fb85b419fc82"
CANDIDATE = "2a5f28d68a4aeba410fcd136378c6b4ca088ce6d"
ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "ci-layout-semantic-contract-results"
BASE = ROOT
TESTS = ['tests/test_migration_codec.py::test_intent_is_deterministic_body_free_and_uses_actual_batch_digest', 'tests/test_migration_codec.py::test_intent_strict_schema_binding_counts_and_classification', 'tests/test_migration_codec.py::test_source_response_frame_accounting_cannot_understate_wire_bytes', 'tests/test_migration_coordinator.py::test_coordinator_module_is_inactive_and_engine_free', 'tests/test_migration_coordinator.py::test_append_requires_checked_existing_replay_before_ack_and_reuses_export', 'tests/test_migration_coordinator.py::test_refusals_never_ack_or_build_indexes_or_start_new_sessions', 'tests/test_migration_coordinator.py::test_late_owner_success_cannot_authorize_ack_or_cleanup_lease', 'tests/test_migration_coordinator.py::test_pending_regeneration_changes_stop_before_destination_writes', 'tests/test_migration_coordinator.py::test_changed_original_live_receipt_and_owner_manifest_refuse_before_ack', 'tests/test_migration_coordinator.py::test_immutable_registration_rebind_conflicts_before_storage_or_admission', 'tests/test_migration_coordinator.py::test_orphan_refusal_return_retains_observed_orphan_counter', 'tests/test_semantic_queue.py::test_reference_contract_is_compact_strict_and_bounds_scope', 'tests/test_semantic_queue.py::test_host_store_refuses_unsupervised_and_has_no_native_imports', 'tests/test_semantic_store_worker.py::test_unsupervised_unknown_duplicate_and_oversized_frames_create_nothing']
CI_PATHS = {
    ".github/scripts/verify_grouped_owner.py",
    ".github/scripts/verify_semantic_contracts.py",
    ".github/scripts/verify_layout_native.py",
    ".github/scripts/layout-native-scope.json",
    ".github/workflows/grouped-owner-verification.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/version-guard.yml",
    ".github/workflows/research-info-theory.yml",
    ".github/workflows/publish.yml",
}


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True, timeout=10).strip()


def check_source():
    assert sys.platform == "darwin", "This scope includes owned macOS supervision contracts"
    assert git("rev-parse", "HEAD~1") == CANDIDATE
    assert set(git("diff", "--name-only", "HEAD~1", "HEAD").splitlines()) <= CI_PATHS
    active = {p.name for p in (ROOT / ".github/workflows").glob("*.yml")}
    assert active == {"grouped-owner-verification.yml"}, active


def prepare():
    check_source()
    assert not RESULTS.exists()
    RESULTS.mkdir(mode=0o700)
    (RESULTS / "scope.json").write_text(
        json.dumps(
            {
                "baseline": BASELINE,
                "candidate": CANDIDATE,
                "scope": "Pure regression only; native, macOS timing and installed memory remain unqualified",
                "outer_seconds_per_phase": 20,
                "aggregate_rss_mib": 256,
                "expected_candidate_cases": 23,
                "native_starts": 0,
            },
            indent=2,
        )
        + "\n"
    )


def launch(mode):
    import psutil

    check_source()
    assert mode in {"baseline", "candidate"}
    worktree = BASE if mode == "baseline" else ROOT
    assert subprocess.check_output(
        ["git", "-C", str(worktree), "rev-parse", "HEAD"], text=True, timeout=10
    ).strip() == git("rev-parse", "HEAD")
    basetemp = Path(os.environ["RUNNER_TEMP"]) / ("castle-contract-" + mode + "-pytest")
    xml = RESULTS / (mode + ".xml")
    assert not basetemp.exists() and not xml.exists()
    if psutil.virtual_memory().available < 1024**3:
        raise SystemExit("PRELAUNCH_PRESSURE: no test attempt")
    runtime = Path(tempfile.gettempdir())
    lockdir = runtime / ("castle-memory-" + str(os.getuid()))
    paths = [
        lockdir / "interactive.waiting",
        lockdir / "admission.lock",
        runtime / ("castle-safe-tests-" + str(os.getuid()) + ".lock"),
    ]
    fds = []
    try:
        for path in paths:
            if path.exists():
                fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
                fds.append(fd)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("PRELAUNCH_BUSY: no test attempt") from None
    finally:
        for fd in fds:
            os.close(fd)
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.chdir(worktree)
    argv = [
        sys.executable,
        "scripts/safe_brain_tests.py",
        "--timeout",
        "20",
        "--rss-mib",
        "256",
        *TESTS,
        "--noconftest",
        "-p",
        "no:cacheprovider",
        "--basetemp",
        str(basetemp),
        "--junitxml",
        str(xml),
        "-x",
        "-q",
    ]
    if mode == "baseline":
        argv.extend(["-k", "native_preparation_request_preserves_original_cursor"])
    os.execv(sys.executable, argv)


def verify(mode, returncode):
    check_source()
    assert mode in {"baseline", "candidate"}
    log = (RESULTS / (mode + ".log")).read_text()
    metrics = re.findall(
        r"\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed", log
    )
    assert len(metrics) == 1, "Missing clean resource completion; this is not regression evidence"
    peak, elapsed = map(float, metrics[0])
    assert peak <= 256 and elapsed <= 20, (peak, elapsed)
    root = ET.parse(RESULTS / (mode + ".xml")).getroot()
    cases = root.findall(".//testcase")
    failures, errors, skips = (
        root.findall(".//" + name) for name in ("failure", "error", "skipped")
    )
    assert not errors and not skips
    if mode == "baseline":
        assert returncode == 1 and len(cases) == len(failures) == 1
        assert (
            cases[0].get("name")
            == "test_native_preparation_request_preserves_original_cursor"
        )
        message = failures[0].get("message", "")
        assert "ValueError" in message
        assert "layout source request version" in message
        status = "EXPECTED_REGRESSION_CONFIRMED"
    else:
        assert returncode == 0 and len(cases) == 23 and not failures
        status = "PURE_CASES_PASSED"
    result = {
        "status": status,
        "cases": len(cases),
        "failures": len(failures),
        "errors": len(errors),
        "skips": len(skips),
        "rss_mib": peak,
        "elapsed_seconds": elapsed,
        "native_starts": 0,
        "scope": "Pure only; native/Mac timing, full corpus and activation remain unverified",
    }
    (RESULTS / (mode + "-result.json")).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    os.umask(0o077)
    assert os.environ.get("GITHUB_ACTIONS") == "true", "Remote CI only; never run on user's machine"
    mode = sys.argv[1]
    if mode == "prepare" and len(sys.argv) == 2:
        prepare()
    elif mode in {"baseline", "candidate"} and len(sys.argv) == 2:
        launch(mode)
    elif mode in {"verify-baseline", "verify-candidate"} and len(sys.argv) == 3:
        verify(mode.removeprefix("verify-"), int(sys.argv[2]))
    else:
        raise SystemExit("closed operation")

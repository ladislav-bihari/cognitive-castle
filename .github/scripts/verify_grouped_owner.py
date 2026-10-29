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
CANDIDATE = "d2b99c006c4982b3062ed6f08fb09442327adfa0"
ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "ci-vector-exposure-grants-green-results"
BASE = ROOT
TESTS = ['tests/test_vector_exposure_grants.py', 'tests/test_semantic_indexer.py::test_intent_is_body_free_deterministic_and_binds_scopes', 'tests/test_semantic_indexer.py::test_fixed_stage_caps_share_absolute_deadline', 'tests/test_semantic_indexer.py::test_indexer_recovers_committed_vector_before_any_inference', 'tests/test_semantic_indexer.py::test_refused_stage_preserves_position_and_never_launches_later_work', 'tests/test_semantic_indexer.py::test_stale_bindings_never_acknowledge', 'tests/test_semantic_indexer.py::test_query_helper_hydrates_under_same_session', 'tests/test_semantic_indexer.py::test_pipeline_host_imports_no_native_or_sqlite_runtime', 'tests/test_semantic_indexer.py::test_exact_receipt_survives_late_coordination_refusal_with_three_second_total', 'tests/test_semantic_indexer.py::test_session_releases_admission_after_preemption_and_cannot_launch_more_work', 'tests/test_semantic_indexer.py::test_query_validation_cannot_finish_after_shared_deadline', 'tests/test_semantic_indexer.py::test_corrupt_pending_intent_reports_phase_without_advancing', 'tests/test_lance_vector_store.py::test_publication_contract_rejects_changed_inference_and_stabilizes_float32', 'tests/test_lance_vector_store.py::test_host_native_imports_absent_and_unsupervised_store_refuses', 'tests/test_lance_vector_store.py::test_publication_rejects_duplicate_unit_records_within_page', 'tests/test_lance_vector_store.py::test_oversized_receipt_proof_rejects_before_storage']
CI_PATHS = {
    ".github/scripts/verify_grouped_owner.py",
    ".github/workflows/grouped-owner-verification.yml",
    ".github/workflows/ci.yml",
    ".github/workflows/version-guard.yml",
    ".github/workflows/research-info-theory.yml",
    ".github/workflows/publish.yml",
}


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True, timeout=10).strip()


def check_source():
    assert sys.platform == "linux", "This scope is pure Linux verification only"
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
                "expected_candidate_cases": 36,
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
    basetemp = Path(os.environ["RUNNER_TEMP"]) / ("castle-grouped-" + mode + "-pytest")
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
        assert returncode == 0 and len(cases) == 36 and not failures
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

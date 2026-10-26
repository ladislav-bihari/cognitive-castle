"""Fixed remote synthetic native scope. No user palace or production activation."""

import fcntl
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "ci-layout-native-results"
MANIFEST = ROOT / ".github/scripts/layout-native-scope.json"
SOURCE = "43bca59d9b6e4fcf87d340d491d739fd7fcf6f07"


def scope():
    assert os.environ.get("GITHUB_ACTIONS") == "true"
    assert sys.platform == "darwin" and platform.machine() == "arm64"
    m = json.loads(MANIFEST.read_text())
    assert platform.python_version() == m["python_version"]
    parent = subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD^"], text=True, timeout=10
    ).strip()
    assert parent == SOURCE == m["source_commit"]
    assert sum(s["starts"] for s in m["scopes"]) == 5
    assert sum(s["timeout"] for s in m["scopes"]) == 10
    for info in m["files"]:
        raw = (ROOT / info["path"]).read_bytes()
        assert len(raw) == info["bytes"] and hashlib.sha256(raw).hexdigest() == info["sha256"]
    for package, version in m["versions"].items():
        assert importlib.metadata.version(package) == version
    return m


def census():
    entries = []
    stack = [RESULTS]
    logical = allocated = 0
    while stack:
        path = stack.pop()
        info = path.lstat()
        assert (
            stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
        )
        entries.append(path)
        logical += info.st_size
        allocated += info.st_blocks * 512
        assert len(entries) <= 4096 and max(logical, allocated) <= 144 * 1024**2
        if stat.S_ISDIR(info.st_mode):
            stack.extend(path.iterdir())
    return entries, logical, allocated


def launch(name):
    m = scope()
    index = next(i for i, s in enumerate(m["scopes"]) if s["id"] == name)
    s = m["scopes"][index]
    for prior in m["scopes"][:index]:
        result = json.loads((RESULTS / prior["id"] / "result.json").read_text())
        assert result["status"] == "PASS" and result["starts"] == prior["starts"]
    base = ROOT / s["basetemp"]
    xml = RESULTS / name / "result.xml"
    assert not base.exists() and not xml.exists()
    census()
    assert shutil.disk_usage(RESULTS).free >= m["minimum_free_bytes"]
    try:
        pressure = subprocess.check_output(
            ["/usr/sbin/sysctl", "-n", "kern.memorystatus_vm_pressure_level"], timeout=0.5
        ).strip()
    except subprocess.TimeoutExpired:
        raise SystemExit("PRELAUNCH_PRESSURE_UNAVAILABLE: no test attempt") from None
    if pressure != b"1":
        raise SystemExit("PRELAUNCH_PRESSURE: no test attempt")
    runtime = Path(tempfile.gettempdir())
    locks = runtime / ("castle-memory-" + str(os.getuid()))
    paths = [
        locks / "interactive.waiting",
        locks / "admission.lock",
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
    os.chdir(ROOT)
    os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    args = [
        sys.executable,
        "scripts/safe_brain_tests.py",
        "--timeout",
        str(s["timeout"]),
        "--rss-mib",
        str(s["rss_mib"]),
        s["node"],
        "--noconftest",
        "-p",
        "no:cacheprovider",
        "--basetemp",
        str(base),
        "--junitxml",
        str(xml),
        "-x",
        "-q",
        "-s",
    ]
    os.execv(sys.executable, args)


def verify(name, code):
    m = scope()
    s = next(s for s in m["scopes"] if s["id"] == name)
    directory = RESULTS / name
    raw = (directory / "output.log").read_bytes()
    text = raw.decode()

    def events(prefix):
        return [
            json.loads(line[len(prefix) :]) for line in text.splitlines() if line.startswith(prefix)
        ]

    starts = events("LAYOUT_NATIVE_START ")
    stages = events("LAYOUT_NATIVE_STAGE ")
    ticks = events("LAYOUT_NATIVE_TICK ")
    metrics = re.findall(
        r"\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed", text
    )
    checks = {
        "exit": code == 0,
        "starts": len(starts) == s["starts"],
        "stages": len(stages) == len(starts),
        "call_order": [event["call"] for event in starts] == list(range(1, s["starts"] + 1)),
        "stage_order": [(event["call"], event["operation"]) for event in stages]
        == [(event["call"], event["operation"]) for event in starts],
        "metrics": len(metrics) == 1,
        "tick_count": len(ticks) == s["ticks"],
        "tick_deadlines": all(0 <= t["elapsed"] <= 5 for t in ticks),
    }
    if metrics:
        peak, elapsed = map(float, metrics[0])
        checks["outer_limits"] = peak <= s["rss_mib"] and elapsed <= s["timeout"]
    else:
        peak = elapsed = None
    xml = directory / "result.xml"
    if xml.exists():
        root = ET.parse(xml).getroot()
        cases = root.findall(".//testcase")
        checks["case"] = len(cases) == 1 and cases[0].get("name") == s["node"].split("::")[1]
        checks["assertions"] = not any(
            root.findall(".//" + kind) for kind in ["failure", "error", "skipped"]
        )
    else:
        checks["case"] = False
    checks["stage_metrics"] = all(len(stage["resources"]) == 1 for stage in stages)
    for stage in stages:
        kind = stage["operation"].split(":")[0]
        if kind.startswith(("queue_", "operator_")) or kind in {
            "source_fixture",
            "source_fixture_second",
            "source520",
            "underestimated_export",
            "fixture_lose_route",
            "fixture_resolve_absence",
        }:
            task, total = 128, 192
        elif kind in {"source_fixture_native", "layout_source"}:
            task, total = (512, 640) if name in {"native_source", "projection_prefix", "source_prefix"} else (128, 192)
        else:
            assert kind in {
                "layout_read",
                "layout_import_batch",
                "layout_verify_owners",
                "layout_maintain",
                "bulk_owner",
                "read_sources",
                "receipt",
            }, kind
            task, total = 192, 256
        checks["rss_" + str(stage["call"])] = all(
            0 < r["peak_task_rss_bytes"] <= task * 1024**2
            and r["peak_task_rss_bytes"] <= r["peak_total_rss_bytes"] <= total * 1024**2
            for r in stage["resources"]
        )
    _, logical, allocated = census()
    result = {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "scope": name,
        "exit_code": code,
        "starts": len(starts),
        "expected_starts": s["starts"],
        "checks": checks,
        "stages": stages,
        "ticks": ticks,
        "outer_rss_mib": peak,
        "outer_seconds": elapsed,
        "retained_logical_bytes": logical,
        "retained_allocated_bytes": allocated,
        "output_sha256": hashlib.sha256(raw).hexdigest(),
        "limits": "Remote synthetic only; failed ACK state unknown; no retry or cap change",
    }
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k not in {"stages", "ticks", "checks"}}))
    if result["status"] != "PASS":
        raise SystemExit(1)


def prepare():
    m = scope()
    assert not RESULTS.exists()
    RESULTS.mkdir(mode=0o700)
    for s in m["scopes"]:
        (RESULTS / s["id"]).mkdir(mode=0o700)
    (RESULTS / "scope.json").write_text(json.dumps(m, indent=2) + "\n")
    (RESULTS / "host.json").write_text(
        json.dumps(
            {
                "system": platform.platform(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "native_cases": 1,
                "source_commit": SOURCE,
                "real_palace_access": False,
            },
            indent=2,
        )
        + "\n"
    )


def archive():
    scope()
    entries, logical, allocated = census()
    path = ROOT / "layout-native-evidence.tar"
    assert not path.exists()
    # No symlink traversal: preserve pytest's link metadata, never linked files.
    with tarfile.open(path, mode="w", dereference=False) as tar:
        for entry in sorted(entries):
            tar.add(entry, arcname=str(entry.relative_to(ROOT)), recursive=False)
    print(
        json.dumps(
            {
                "archive": path.name,
                "entries": len(entries),
                "logical": logical,
                "allocated": allocated,
                "archive_bytes": path.stat().st_size,
            }
        )
    )


if __name__ == "__main__":
    os.umask(0o077)
    op = sys.argv[1]
    if op == "prepare" and len(sys.argv) == 2:
        prepare()
    elif op == "archive" and len(sys.argv) == 2:
        archive()
    elif op == "run" and len(sys.argv) == 3:
        launch(sys.argv[2])
    elif op == "verify" and len(sys.argv) == 4:
        verify(sys.argv[2], int(sys.argv[3]))
    else:
        raise SystemExit("closed operation")

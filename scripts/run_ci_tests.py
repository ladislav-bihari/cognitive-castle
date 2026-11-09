"""Run all CI tests while keeping bounded worker hosts free of legacy imports.

Legacy tests retain their shared fixtures. Bounded test files get fresh processes
without that conftest. Import-purity assertions also need a process without the
coverage plugin, which itself imports sqlite3. They still run unchanged; every
other case remains measured and the repository's combined coverage gate applies.
"""

import argparse
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


PREFIXES = (
    "test_memory_",
    "test_history_",
    "test_brain_",
    "test_capture_",
    "test_lance_",
    "test_layout_",
    "test_migration_",
    "test_semantic_",
    "test_partition",
    "test_native_",
    "test_sqlite_",
    "test_vector_",
    "test_bulk_",
)
EXTRA = {
    "test_fast_memory.py",
    "test_canonical_records.py",
    "test_bounded_json.py",
    "test_cancellable_pressure_probe.py",
    "test_pressure_policy_integration.py",
    "test_unified_routes.py",
    "test_live_publication_codec.py",
    "test_live_registration.py",
}


def partition(paths):
    legacy, bounded = [], []
    for path in paths:
        target = bounded if path.name.startswith(PREFIXES) or path.name in EXTRA else legacy
        target.append(path)
    return legacy, bounded


def import_guards(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    # Include dynamic sys imports and local helper chains conservatively.
    guarded = {
        name
        for name, node in functions.items()
        if any(
            isinstance(child, ast.Attribute) and child.attr == "modules" for child in ast.walk(node)
        )
    }
    references = {
        name: {child.id for child in ast.walk(node) if isinstance(child, ast.Name)}
        for name, node in functions.items()
    }
    while True:
        expanded = guarded | {name for name, names in references.items() if names & guarded}
        if expanded == guarded:
            return sorted(name for name in guarded if name.startswith("test_"))
        guarded = expanded


def commands(paths, group, *, coverage=True):
    legacy, bounded = partition(paths)
    base = [sys.executable, "-m", "pytest", "-v", "--durations=10"]
    measured = (
        [
            "-p",
            "pytest_cov.plugin",
            "--cov=cognitive_castle",
            "--cov-report=term-missing",
            "--cov-fail-under=0",
        ]
        if coverage
        else []
    )
    if group == "legacy":
        return [
            base
            + ["tests/", "--ignore=tests/benchmarks"]
            + ["--ignore=" + str(p) for p in bounded]
            + measured
        ]
    if group != "bounded":
        raise ValueError("unknown CI test group")
    result = []
    for path in bounded:
        command = base + [str(path), "--noconftest", "-p", "no:cacheprovider"]
        guards = import_guards(path) if coverage else []
        if guards:
            selection = " or ".join(guards)
            result.append(command + measured + ["--cov-append", "-k", "not (" + selection + ")"])
            result.append(command + ["-k", selection])
        else:
            result.append(command + measured + (["--cov-append"] if coverage else []))
    return result


def run(root, group, *, coverage=True):
    root = Path(root).resolve()
    paths = sorted(p.relative_to(root) for p in root.glob("tests/test_*.py"))
    # AST reads use the caller's root, while pytest paths remain relative for
    # coverage portability across all six platform/Python jobs.
    old_cwd = Path.cwd()
    try:
        os.chdir(root)
        selected = commands(paths, group, coverage=coverage)
    finally:
        os.chdir(old_cwd)
    results = root / "ci-test-results" / group
    results.mkdir(parents=True, exist_ok=False)
    scratch = (
        Path(tempfile.mkdtemp(prefix="castle-ci-bounded-", dir=os.environ.get("RUNNER_TEMP")))
        if group == "bounded"
        else None
    )
    if scratch is not None and scratch.resolve().is_relative_to(root):
        raise ValueError("private CI scratch must be outside the repository")
    outcomes = []
    for index, command in enumerate(selected):
        phase = results / str(index)
        phase.mkdir(mode=0o700)
        # pytest-cov<=6 can instrument descendants via COV_CORE_* at Python
        # startup, before plugin autoload is disabled. Each phase owns its own
        # coverage invocation; never inherit the enclosing test's injection.
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("COV_CORE_")
            and key not in {"COVERAGE_PROCESS_START", "COVERAGE_FILE"}
        }
        env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
        if group == "bounded":
            private = scratch / str(index)
            private.mkdir(mode=0o700)
            home = private / "home"
            home.mkdir(mode=0o700)
            env.update(HOME=str(home), USERPROFILE=str(home))
            command += ["--basetemp", str(private / "pytest")]
        command += ["--junitxml", str(phase / "result.xml")]
        print(json.dumps({"phase": index, "command": command}), flush=True)
        result = subprocess.run(command, cwd=root, env=env)
        # Pytest exit5 means the complementary selection has no cases. It is
        # acceptable only when its same-file counterpart actually passes.
        outcomes.append({"command": command, "returncode": result.returncode})
        (results / "outcomes.json").write_text(json.dumps(outcomes, indent=2) + "\n")
    errors = [r for r in outcomes if r["returncode"] not in {0, 5}]
    empty = [r for r in outcomes if r["returncode"] == 5]
    for result in empty:
        command = result["command"]
        if (
            group != "bounded"
            or "-k" not in command
            or not any(
                other["returncode"] == 0 and other["command"][5] == command[5] for other in outcomes
            )
        ):
            return 1
    return int(bool(errors) or not outcomes)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("group", choices=["legacy", "bounded"])
    args = parser.parse_args()
    raise SystemExit(run(Path(__file__).resolve().parents[1], args.group))

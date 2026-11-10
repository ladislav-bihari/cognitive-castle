"""The full CI suite must preserve engine-free worker-host contracts."""

import importlib.util
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

import pytest


ROOT = Path(__file__).resolve().parents[1]


def runner():
    path = ROOT / "scripts/run_ci_tests.py"
    assert path.is_file(), "CI needs a complete, isolated bounded-test partition"
    spec = importlib.util.spec_from_file_location("ci_test_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_partition_is_disjoint_exhaustive_and_keeps_legacy_backend_tests():
    paths = sorted(ROOT.glob("tests/test_*.py"))
    legacy, bounded = runner().partition(paths)
    assert set(legacy).isdisjoint(bounded)
    assert sorted(legacy + bounded) == paths
    assert ROOT / "tests/test_lancedb_identity_mismatch.py" in legacy
    assert ROOT / "tests/test_migration_coordinator.py" in bounded
    assert ROOT / "tests/test_memory_context.py" in bounded
    # Retain every file from the pre-existing CI isolation list as well.
    for name in (
        "test_memory_runtime.py",
        "test_memory_api_integration.py",
        "test_memory_context.py",
        "test_memory_coordinator.py",
        "test_unified_routes.py",
        "test_history_runtime.py",
        "test_history_native.py",
        "test_live_publication_codec.py",
        "test_live_registration.py",
    ):
        assert ROOT / "tests" / name in bounded


def test_each_bounded_file_has_its_own_pytest_process_without_global_fixtures():
    module = runner()
    paths = [Path("tests/test_semantic_alpha.py"), Path("tests/test_memory_beta.py")]
    commands = module.commands(paths, "bounded", coverage=False)
    assert len(commands) == len(paths)
    for command, path in zip(commands, paths):
        assert command[:3] == [sys.executable, "-m", "pytest"]
        assert "--noconftest" in command
        assert str(path) in command
        assert sum(str(p) in command for p in paths) == 1


def test_legacy_command_excludes_every_bounded_file():
    paths = [Path("tests/test_semantic_alpha.py"), Path("tests/test_legacy_beta.py")]
    (command,) = runner().commands(paths, "legacy", coverage=False)
    assert "--ignore=tests/test_semantic_alpha.py" in command
    assert "--ignore=tests/test_legacy_beta.py" not in command
    assert "--noconftest" not in command


def test_real_processes_do_not_share_sqlite_or_load_global_conftest(tmp_path):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "conftest.py").write_text("raise AssertionError('legacy fixture loaded')\n")
    (tests / "test_memory_alpha.py").write_text(
        "import sqlite3\ndef test_load():\n    assert sqlite3 is not None\n"
    )
    (tests / "test_semantic_beta.py").write_text(
        "import sys\ndef test_clean():\n    assert 'sqlite3' not in sys.modules\n"
    )
    assert runner().run(tmp_path, "bounded", coverage=False) == 0


def test_coverage_and_import_guards_use_complementary_fresh_processes(tmp_path):
    path = tmp_path / "test_migration_fixture.py"
    path.write_text(
        "import sys\ndef test_guard():\n    assert 'sqlite3' not in sys.modules\n"
        "def test_other():\n    assert True\n"
    )
    commands = runner().commands([path], "bounded", coverage=True)
    assert len(commands) == 2
    measured, strict = commands
    assert "--cov=cognitive_castle" in measured
    assert measured[measured.index("-k") + 1] == "not (test_guard)"
    assert not any(arg.startswith("--cov") for arg in strict)
    assert strict[strict.index("-k") + 1] == "test_guard"


@pytest.mark.parametrize("guard_style", ["direct", "helper"])
def test_real_coverage_retains_every_case_once_and_does_not_pollute_import_guard(
    tmp_path, monkeypatch, guard_style
):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "conftest.py").write_text("raise AssertionError('legacy fixture loaded')\n")
    package = tmp_path / "cognitive_castle"
    package.mkdir()
    (package / "__init__.py").write_text("VALUE = 42\n")
    guard = (
        "def test_guard():\n    assert 'sqlite3' not in sys.modules\n"
        if guard_style == "direct"
        else "def helper():\n    assert 'sqlite3' not in __import__('sys').modules\n"
        "def delegate():\n    helper()\n"
        "def test_guard():\n    delegate()\n"
    )
    (tests / "test_memory_mixed.py").write_text(
        "import sys,os\n"
        + guard
        + "    assert not any(k.startswith('COV_CORE_') for k in os.environ)\n"
        "def test_measured():\n    import cognitive_castle\n    assert cognitive_castle.VALUE == 42\n"
    )
    monkeypatch.setenv("COV_CORE_SOURCE", "must not reach child Python startup")
    assert runner().run(tmp_path, "bounded", coverage=True) == 0
    names = []
    for xml in (tmp_path / "ci-test-results").rglob("result.xml"):
        root = ET.parse(xml).getroot()
        assert not root.findall(".//failure") and not root.findall(".//error")
        names.extend(node.get("name") for node in root.findall(".//testcase"))
    assert sorted(names) == ["test_guard", "test_measured"]
    assert (tmp_path / ".coverage").read_bytes().startswith(b"SQLite format 3")
    import json

    outcomes = json.loads((tmp_path / "ci-test-results/bounded/outcomes.json").read_text())
    for outcome in outcomes:
        command = outcome["command"]
        assert not Path(command[command.index("--basetemp") + 1]).is_relative_to(tmp_path)


def test_an_empty_file_is_not_hidden_by_an_unrelated_passing_file(tmp_path):
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_memory_empty.py").write_text("# No tests: unexpected CI inventory\n")
    (tests / "test_semantic_ok.py").write_text("def test_ok():\n    assert True\n")
    assert runner().run(tmp_path, "bounded", coverage=False) == 1

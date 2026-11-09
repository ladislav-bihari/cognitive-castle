"""AST-only composition of current pressure admission and strict deadlines."""

import ast
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace as NS

import pytest


def pressure(value, elapsed):
    clock = [100.0]

    def probe(*args, **kwargs):
        clock[0] += elapsed
        return value

    ns = dict(
        json=json,
        time=NS(monotonic=lambda: clock[0]),
        sys=NS(platform="darwin"),
        subprocess=NS(
            check_output=probe,
            DEVNULL=subprocess.DEVNULL,
            SubprocessError=subprocess.SubprocessError,
            TimeoutExpired=subprocess.TimeoutExpired,
        ),
    )
    tree = ast.parse(
        (
            Path(__file__).resolve().parents[1] / "cognitive_castle/brain_reader_guardian.py"
        ).read_text()
    )
    names = {"_pressure_ok", "_record_pressure_failure"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == names
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "pressure-policy", "exec"), ns)
    return ns["_pressure_ok"]


@pytest.mark.parametrize(
    "value,accepted", [(b"2\n", True), (b"1\n", True), (b"4\n", False), (b"3\n", False)]
)
def test_current_pressure_policy_is_preserved(value, accepted):
    assert pressure(value, 0.001)(deadline=100.05) is accepted


@pytest.mark.parametrize("value", [b"1\n", b"2\n"])
@pytest.mark.parametrize("diagnostic", [None, {"site": "host_initial"}])
def test_late_admissible_pressure_never_authorizes_a_new_stage(value, diagnostic):
    assert pressure(value, 0.06)(deadline=100.05, diagnostic=diagnostic) is False

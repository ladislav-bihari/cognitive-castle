"""Pure AST-selected pressure diagnostics: no runtime import, process or DB access."""

import ast
import io
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]


def functions(file, names, namespace):
    tree = ast.parse((ROOT / "cognitive_castle" / file).read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in nodes} == set(names), "pressure attribution helper missing"
    exec(compile(ast.Module(body=nodes, type_ignores=[]), file, "exec"), namespace)
    return namespace


def guardian(value=b"1\n", elapsed=0.06, error=None):
    clock, calls = [100.0], []

    def probe(*args, **kwargs):
        calls.append((args, kwargs))
        clock[0] += elapsed
        if error is not None:
            raise error
        return value

    ns = {
        "json": json,
        "sys": SimpleNamespace(platform="darwin"),
        "time": SimpleNamespace(monotonic=lambda: clock[0]),
        "subprocess": SimpleNamespace(
            check_output=probe,
            DEVNULL=subprocess.DEVNULL,
            TimeoutExpired=subprocess.TimeoutExpired,
            SubprocessError=subprocess.SubprocessError,
        ),
    }
    functions(
        "brain_reader_guardian.py",
        {"_pressure_ok", "_pressure_reason", "_pressure_diag", "_record_pressure_failure"},
        ns,
    )
    return ns, clock, calls


def test_late_normal_keeps_false_and_records_exact_single_probe():
    ns, _, calls = guardian()
    detail = {"site": "host_initial"}
    assert (
        ns["_pressure_reason"](100.05, ns["_pressure_ok"], diagnostic=detail) == "worker_deadline"
    )
    assert len(calls) == 1 and 0.049 <= calls[0][1]["timeout"] <= 0.05
    assert detail["outcome"] == "late_normal" and detail["pressure"] == 1
    assert detail["elapsed_us"] >= 59999 and detail["remaining_us"] < 0
    assert ns["_pressure_diag"](detail) == detail
    assert len(json.dumps(detail).encode()) <= 512


def test_non_normal_guardian_keeps_half_second_timeout():
    ns, _, calls = guardian(b"4\n", 0.006)
    detail = {"site": "guardian_initial"}
    assert ns["_pressure_ok"](diagnostic=detail) is False
    assert len(calls) == 1 and calls[0][1]["timeout"] == 0.5
    assert detail["outcome"] == "non_normal" and detail["pressure"] == 4
    assert detail["remaining_us"] == -1 and detail["timeout_us"] == 500000


def test_timeout_does_not_invent_normal_or_echo_exception_output():
    ns, _, calls = guardian(
        error=subprocess.TimeoutExpired("private-command", 0.05, output=b"private")
    )
    detail = {"site": "host_periodic"}
    assert ns["_pressure_ok"](deadline=100.05, diagnostic=detail) is False
    assert len(calls) == 1 and detail["outcome"] == "timeout"
    assert "pressure" not in detail and b"private" not in json.dumps(detail).encode()


def test_normal_and_legacy_bool_calls_remain_unchanged():
    ns, _, calls = guardian(elapsed=0.001)
    detail = {"site": "guardian_periodic"}
    assert ns["_pressure_ok"](diagnostic=detail) is True
    assert detail == {"site": "guardian_periodic"}
    assert ns["_pressure_ok"]() is True and len(calls) == 2


def test_expired_deadline_refuses_without_an_extra_probe():
    ns, _, calls = guardian()
    detail = {"site": "host_initial"}
    assert ns["_pressure_ok"](deadline=99.0, diagnostic=detail) is False
    assert calls == [] and detail["outcome"] == "deadline_exhausted"
    assert detail["timeout_us"] == 0 and detail["remaining_us"] < 0


def test_first_failure_is_retained_and_os_error_is_body_free():
    ns, _, _ = guardian(error=OSError("private source body"))
    detail = {"site": "guardian_periodic"}
    assert ns["_pressure_ok"](diagnostic=detail) is False
    before = dict(detail)
    assert ns["_pressure_ok"](diagnostic=detail) is False and detail == before
    assert detail["outcome"] == "os_error" and "private" not in json.dumps(detail)


def test_fixed_diag_rejects_unknown_fields_types_and_unbounded_values():
    ns, _, _ = guardian()
    good = dict(
        site="host_initial", outcome="timeout", elapsed_us=50000, timeout_us=50000, remaining_us=0
    )
    assert ns["_pressure_diag"](good) == good
    for key, value in (
        ("body", "private"),
        ("site", "other"),
        ("outcome", "other"),
        ("elapsed_us", True),
        ("timeout_us", 500001),
        ("remaining_us", 2**80),
        ("pressure", "1"),
        ("pressure", True),
    ):
        assert ns["_pressure_diag"]({**good, key: value}) is None


def runtime_probe(ns):
    ns.update(interactive_waiting=lambda: False)

    def background(until, diagnostic, site, sample_resources=None):
        diagnostic["site"] = site
        normal = ns["_pressure_ok"](
            deadline=min(until, ns["time"].monotonic() + 0.5), diagnostic=diagnostic
        )
        return None if normal else "system_pressure_unavailable"

    ns["_background_pressure_refusal"] = background
    return functions(
        "memory_runtime.py",
        {"_unavailable", "_stage_pressure_reason", "_pressure_attribution", "_supervised_result"},
        ns,
    )


def test_host_periodic_attribution_and_foreground_mock_compatibility():
    ns, _, calls = guardian(b"4", 0.001)
    runtime_probe(ns)
    detail = {}
    reason = ns["_stage_pressure_reason"](True, 102.0, diagnostic=detail, site="host_periodic")
    value = ns["_unavailable"](reason, detail)
    assert value["reason"] == "system_pressure_unavailable"
    assert value["pressure_diag"]["site"] == "host_periodic" and len(calls) == 1
    ns["_pressure_ok"] = lambda **kwargs: False
    assert ns["_stage_pressure_reason"](False, 102.0) == "system_pressure_unavailable"


def test_waiter_wins_even_after_failed_pressure_probe():
    ns, _, _ = guardian()
    runtime_probe(ns)
    waiting = iter([False, True])
    ns["interactive_waiting"] = lambda: next(waiting)
    detail = {}
    reason = ns["_stage_pressure_reason"](True, 102.0, diagnostic=detail, site="host_initial")
    assert reason == "background_preempted"
    assert "pressure_diag" not in ns["_unavailable"](reason, detail)


def nested(file, parent, name, ns):
    tree = ast.parse((ROOT / "cognitive_castle" / file).read_text())
    owner = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == parent)
    node = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), file, "exec"), ns)
    return ns[name]


def test_guardian_host_coordinator_preserve_only_valid_pressure_metadata():
    ns, _, _ = guardian(b"4", 0.001)
    runtime_probe(ns)
    detail = {"site": "guardian_initial"}
    ns["_pressure_ok"](diagnostic=detail)
    stream = io.StringIO()
    ns.update(sys=SimpleNamespace(stdout=stream), pressure_diag=detail)
    ns["config"] = {"total_rss_limit": 192 * 1024**2}
    nested("brain_reader_guardian.py", "main", "unavailable", ns)("system_pressure_unavailable")
    value = json.loads(stream.getvalue())
    ns.update(peaks=[10, 20], started=100.0, background_preempt=False, until=None)
    value = nested("memory_runtime.py", "_supervise", "finish", ns)(value)
    writes = []
    ns.update(
        result={},
        state={},
        directory=Path("/not-opened"),
        _atomic=lambda p, v: writes.append((p.name, dict(v))),
        time=SimpleNamespace(monotonic=lambda: 100.1, time_ns=lambda: 123),
    )
    saved = nested("memory_coordinator.py", "_tick", "finish", ns)(value)
    assert saved["pressure_diag"] == detail and writes[-1][1]["pressure_diag"] == detail
    assert "resources" not in saved
    value["pressure_diag"] = {"body": "private"}
    assert "pressure_diag" not in nested("memory_runtime.py", "_supervise", "finish", ns)(value)


def test_background_finish_rejects_late_success_without_changing_other_results():
    ns, clock, _ = guardian()
    runtime_probe(ns)
    ns.update(peaks=[10, 20], started=100.0, background_preempt=True, until=100.05)
    clock[0] = 100.06
    finish = nested("memory_runtime.py", "_supervise", "finish", ns)
    value = finish({"status": "ok", "accepted": True, "complete": True, "value": 1})
    assert value["status"] == "unavailable" and value["reason"] == "worker_deadline"
    assert "accepted" not in value and "complete" not in value and "value" not in value
    assert (
        finish({"status": "unavailable", "reason": "worker_memory_limit"})["reason"]
        == "worker_memory_limit"
    )
    ns["background_preempt"] = False
    assert finish({"status": "ok"})["status"] == "ok"


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
    print(json.dumps({"passed": len(tests), "actual_probe_runtime_engine_DB_access": False}))


def test_warning_pressure_is_admitted():
    ns, _, _ = guardian(b"2\n", 0.001)
    assert ns["_pressure_ok"](diagnostic={"site": "guardian_initial"}) is True

"""Borrowed control transport must retain deadlines, limits and host import purity."""

from pathlib import Path
import subprocess
import sys
import pytest
from cognitive_castle.canonical_records import _hash, _json


def authority():
    value = dict(
        format="castle-capture-control-v1",
        directory=dict(path="/private/control", device=1, inode=2),
        database=dict(device=1, inode=3),
        max_bytes=3 * 1024**2,
    )
    return value, _hash(_json(value))


def client(session):
    from cognitive_castle.capture_control_runtime import CaptureControlClient

    value, binding = authority()
    return CaptureControlClient(session, value, expected_hash=binding)


@pytest.mark.parametrize(
    "operation,args,cap", [("coverage", {}, 2), ("source_prepare", dict(source_key="a" * 64), 3)]
)
def test_control_client_uses_existing_stage_with_correct_resource_profile(operation, args, cap):
    calls = []

    class Session:
        def _ready(self):
            return None

        def _run_stage(self, request, **kwargs):
            calls.append((request, kwargs))
            return dict(status="unavailable", reason="fixture")

    result = client(Session()).call(operation, args)
    assert result == dict(status="unavailable", reason="fixture") and len(calls) == 1
    request, config = calls[0]
    assert request["operation"] == operation and request["arguments"] == args
    assert request["authority_hash"] == authority()[1]
    assert (
        config["cap"] == cap and config["rss"] == 128 * 1024**2 and config["total"] == 192 * 1024**2
    )
    assert config["worker_command"] == [
        sys.executable,
        "-I",
        str(Path(__file__).resolve().parents[1] / "cognitive_castle/capture_control_worker.py"),
    ]


def test_control_client_refused_lease_never_launches_worker():
    class Session:
        def _ready(self):
            return dict(status="unavailable", reason="memory_busy")

        def _run_stage(self, *a, **kw):
            raise AssertionError("refusal launched worker")

    assert client(Session()).call("coverage", {}) == dict(
        status="unavailable", reason="memory_busy"
    )


def test_control_client_late_preparation_is_not_forwarded_to_publication():
    class Session:
        late = False

        def _ready(self):
            return dict(status="unavailable", reason="semantic_deadline") if self.late else None

        def _run_stage(self, *a, **kw):
            self.late = True
            return dict(status="prepared", preparation=dict(text="late words"))

    assert client(Session()).call("source_prepare", dict(source_key="a" * 64)) == dict(
        status="unavailable", reason="semantic_deadline"
    )


def test_control_client_invalid_arguments_fail_before_worker():
    class Session:
        def _ready(self):
            return None

        def _run_stage(self, *a, **kw):
            raise AssertionError("invalid operation launched worker")

    with pytest.raises(ValueError):
        client(Session()).call("initialize", {})


def test_control_client_import_and_construction_leave_host_free_of_engines():
    root = Path(__file__).resolve().parents[1]
    value, binding = authority()
    code = f"import sys;sys.path.insert(0,{str(root)!r});from cognitive_castle.capture_control_runtime import CaptureControlClient;CaptureControlClient(None,{value!r},expected_hash={binding!r});assert not {{'sqlite3','lance','lancedb','pyarrow','numpy','torch'}}.intersection(sys.modules)"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=3
    )
    assert result.returncode == 0, result.stderr

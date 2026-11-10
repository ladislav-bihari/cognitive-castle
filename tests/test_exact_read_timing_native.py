"""Read-only empirical timing, never a replacement for failed qualification."""

import hashlib
import json
import os
from pathlib import Path
import sys

from test_memory_selected_native import pytestmark  # noqa: F401


def test_archived_receipt_cold_read_timing_is_observed_without_mutation(tmp_path):
    from cognitive_castle.semantic_runtime import SemanticSession

    corpus = Path(os.environ["CASTLE_EXACT_TIMING_CORPUS"])
    root = corpus / "generation"
    handoff = json.loads((corpus / "model-handoff.json").read_text())
    assert handoff["source"] == "387b8c08191f399e240b2c054b4f429d13e1518f"

    def snapshot():
        files = {}
        total = 0
        for parent, dirs, names in os.walk(root, followlinks=False):
            assert not any((Path(parent) / name).is_symlink() for name in dirs + names)
            for name in names:
                path = Path(parent) / name
                total += path.stat().st_size
                assert total <= 4 * 1024**2 and len(files) < 1024
                files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
        return files

    before = snapshot()
    trace = tmp_path / "trace.jsonl"
    tmp_path.chmod(0o700)
    os.environ["CASTLE_EXACT_TIMING_SCRATCH"] = str(tmp_path)
    with SemanticSession(Path(__file__), interactive=False) as session:
        result = session._run_stage(
            dict(
                directory=str(root),
                operation="layout_read",
                arguments=dict(
                    operation="receipt",
                    arguments=dict(namespace="registered-model-fixture", key="3"),
                    envelope_hash=handoff["pin"],
                ),
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("exact_read_timing_fixture.py")),
                "timing",
                str(trace),
            ],
            cap=2,
            rss=192 * 1024**2,
            total=256 * 1024**2,
        )
    events = [json.loads(line) for line in trace.read_text().splitlines()]
    assert events and len(events) <= 64 and events[0]["event"] == "admitted"
    assert len(session.resources) == 1 and snapshot() == before
    assert result["status"] == "ok" or result == dict(
        status="unavailable", reason="worker_deadline"
    ), result
    if result["status"] == "ok":
        from test_registered_model_native import _fact

        prepared = _fact(3)
        assert result == dict(status="ok", digest=prepared.digest, receipt=prepared.receipt)
    evidence = dict(
        verdict="diagnostic_only_not_qualification",
        result=result,
        resources=session.resources,
        events=events,
    )
    (tmp_path / "timing-summary.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("EXACT_READ_TIMING " + json.dumps(evidence), flush=True)
    assert not {"sqlite3", "lance", "pyarrow", "numpy", "torch"}.intersection(sys.modules)

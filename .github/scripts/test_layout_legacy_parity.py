"""CI-only instrumentation; execute original selected legacy tests unchanged."""

import importlib
import json
import os
from pathlib import Path
import stat
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests"))
CASES = {
    "remember": ("test_lance_exact_store", "test_create_append_reopen_replay_and_conflict", 6),
    "pages": ("test_lance_exact_store", "test_unicode_nul_chunk_pages_and_hash_binding", 5),
    "provenance": (
        "test_lance_exact_store",
        "test_import_preserves_provenance_reuses_records_and_checks_replay",
        7,
    ),
    "live_receipt": (
        "test_bulk_exact_store",
        "test_migrated_ninth_live_chunk_uses_original_receipt",
        3,
    ),
    "record_reuse": (
        "test_bulk_exact_store",
        "test_visible_legacy_record_reused_without_exposure_or_kind_change",
        5,
    ),
}


def census(root):
    pending = [(root, 0)]
    count = logical = allocated = 0
    while pending:
        path, depth = pending.pop()
        info = path.lstat()
        directory = stat.S_ISDIR(info.st_mode)
        assert directory or stat.S_ISREG(info.st_mode)
        assert info.st_uid == os.getuid() and not info.st_mode & 0o077
        assert directory or info.st_nlink == 1
        assert depth <= 10
        count += 1
        logical += info.st_size
        allocated += info.st_blocks * 512
        assert count <= 2048 and max(logical, allocated) <= 128 * 1024**2
        if directory:
            pending.extend((child, depth + 1) for child in path.iterdir())


@pytest.mark.parametrize("case", list(CASES))
def test_legacy_parity(tmp_path, monkeypatch, case):
    assert sys.platform == "darwin" and os.environ.get("GITHUB_ACTIONS") == "true"
    from cognitive_castle import memory_runtime as runtime

    module, test, expected = CASES[case]
    original = runtime._supervise
    calls = []

    def observed(request, admission, **kwargs):
        assert kwargs["rss_limit"] == 192 * 1024**2
        assert kwargs["total_rss_limit"] == 256 * 1024**2
        assert kwargs["deadline"] in {2, 3}
        calls.append(request.get("operation", request.get("fixture")))
        assert len(calls) <= expected
        operation = "legacy_" + calls[-1]
        print(
            "LAYOUT_NATIVE_START " + json.dumps(dict(call=len(calls), operation=operation)),
            flush=True,
        )
        result = original(request, admission, **kwargs)
        print(
            "LAYOUT_NATIVE_STAGE "
            + json.dumps(
                dict(
                    call=len(calls),
                    operation=operation,
                    status=result.get("status"),
                    reason=result.get("reason"),
                    resources=[result.get("resources")],
                )
            ),
            flush=True,
        )
        assert isinstance(result.get("resources"), dict)
        return result

    monkeypatch.setattr(runtime, "_supervise", observed)
    census(tmp_path)
    getattr(importlib.import_module(module), test)(tmp_path)
    assert len(calls) == expected
    census(tmp_path)
    assert not {"sqlite3", "lance", "pyarrow", "numpy"}.intersection(sys.modules)

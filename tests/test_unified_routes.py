"""Ordinary public recall shares exact live/history sources and trusted policy."""

import hashlib
import json
import os
import subprocess
import sys

import pytest

from cognitive_castle.brain_api import BrainAPI
from cognitive_castle.fast_memory import MemoryStore

pytestmark = pytest.mark.skipif(os.name != "posix", reason="private storage requires POSIX")


def _install_workspace_indexes(store):
    from cognitive_castle.unified_memory import workspace_index_specs

    with store._connect(True) as conn:
        tables = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        indexes = {
            row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        }
        for spec in workspace_index_specs():
            if spec["table"] in tables and spec["name"] not in indexes:
                conn.execute(spec["sql"])


@pytest.fixture
def unified(tmp_path, monkeypatch):
    from cognitive_castle.memory_worker import _execute
    from cognitive_castle.unified_memory import history_store_path, register_history_store

    monkeypatch.setenv("CASTLE_BRAIN_HOST", "codex")
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    live = MemoryStore(palace / ".memory")
    assert (
        live.remember(
            "fresh",
            "sharedneedle fresh exact ž😀\r\n",
            {"workspace": "/work", "wing": "project", "room": "today", "role": "user"},
        )["status"]
        == "ok"
    )
    store_id = hashlib.sha256(b"historical-store").hexdigest()
    assert (
        register_history_store(
            palace, store_id, archive_id="archive", state="indexed", indexed_rows=2, total_rows=2
        )["status"]
        == "ok"
    )
    history = MemoryStore(history_store_path(palace, store_id))
    assert (
        history.remember(
            "old",
            "sharedneedle historical exact\t",
            {"workspace": "/work", "wing": "project", "room": "before", "role": "user"},
        )["status"]
        == "ok"
    )
    assert (
        history.remember("other", "sharedneedle another workspace", {"workspace": "/other"})[
            "status"
        ]
        == "ok"
    )
    _install_workspace_indexes(live)
    _install_workspace_indexes(history)
    api = BrainAPI(palace)
    api.policy_path.parent.mkdir(parents=True, mode=0o700)
    api.policy_path.write_text(json.dumps({"allow_local_sources": True}))
    api.policy_path.chmod(0o600)

    def local_runtime(palace, operation, arguments):
        return _execute({"palace": str(palace), "operation": operation, "arguments": arguments})

    monkeypatch.setattr("cognitive_castle.memory_runtime.run_memory", local_runtime)
    return palace, api, live, history


def test_python_brain_cli_mcp_share_common_default(unified, monkeypatch, capsys):
    import sys

    cli_stdout = sys.stdout
    from cognitive_castle import cli, mcp_server
    from cognitive_castle.searcher import search_memories

    # Import-time MCP stdio protection is intentional (#225). Initialize its
    # existing restore path before exercising the CLI in this same process.
    mcp_server._restore_stdout()
    monkeypatch.setattr(sys, "stdout", cli_stdout)
    palace, api, live, history = unified
    expected = {
        live.recall("sharedneedle")["sources"][0]["id"],
        history.recall("historical")["sources"][0]["id"],
    }
    brain = api.call("castle_brain_recall", {"query": "sharedneedle", "workspace": "/work"})
    python = search_memories("sharedneedle", str(palace), workspace="/work")
    monkeypatch.setenv("CASTLE_PALACE_PATH", str(palace))
    mcp = mcp_server.tool_search("sharedneedle", workspace="/work")
    assert {s["id"] for s in brain["sources"]} == expected
    assert {s["id"] for s in python["results"]} == expected
    assert {s["id"] for s in mcp["results"]} == expected
    assert python["coverage_detail"] == brain["coverage_detail"]
    args = cli.build_parser().parser.parse_args(
        ["--palace", str(palace), "search", "sharedneedle", "--workspace", "/work"]
    )
    assert args.mode is None
    cli.cmd_search(args)
    text = capsys.readouterr().out
    assert "fresh exact" in text and "historical exact" in text
    assert "another workspace" not in text


def test_scope_cursor_schema_and_canonical_exact_read(unified):
    palace, api, _, history = unified
    found = api.call(
        "castle_brain_recall",
        {"query": "sharedneedle", "wing": "project", "room": "before", "limit": 1},
    )
    assert len(found["sources"]) == 1
    item = found["sources"][0]
    page = api.call(
        "castle_memory_read",
        {"protocol_version": 1, "source_id": item["id"], "expected_hash": item["hash"]},
    )
    assert page["text"] == "sharedneedle historical exact\t"
    assert page["text"] == history.read_sources([item["id"]])["sources"][0]["text"]


def test_public_cursor_continuation_and_capture_change(unified):
    _, api, live, _ = unified
    arguments = {"query": "sharedneedle", "workspace": "/work", "limit": 1}
    first = api.call("castle_brain_recall", arguments)
    assert first["next_cursor"]
    following = api.call("castle_brain_recall", arguments | {"cursor": first["next_cursor"]})
    assert following["status"] == "ok"
    assert following["sources"][0]["id"] != first["sources"][0]["id"]
    assert (
        live.remember("concurrent", "sharedneedle newer", {"workspace": "/work"})["status"] == "ok"
    )
    stale = api.call("castle_brain_recall", arguments | {"cursor": first["next_cursor"]})
    assert stale["status"] == "stale_cursor" and stale["restart_required"]


def test_preview_budget_keeps_canonical_full_text_hash(unified):
    _, api, live, _ = unified
    text = "longneedle ž😀 " * 400
    receipt = live.remember("long", text)
    assert receipt["status"] == "ok"
    expected = live.read_sources(receipt["ids"])["sources"][0]
    result = api.call("castle_brain_recall", {"query": "longneedle"})
    source = result["sources"][0]
    assert source["hash"] == expected["provenance"]["text_hash"]
    assert source["text_bytes"] == len(expected["text"].encode())
    assert source["preview_truncated"]
    assert expected["text"].startswith(source["preview"])
    assert len(json.dumps(result, ensure_ascii=False).encode()) <= 8192


def test_selected_hashes_filter_before_limit(unified):
    from cognitive_castle.brain_loop import source_exposure_hash

    _, api, live, history = unified
    for n in range(24):
        assert live.remember(f"disallowed-{n}", "sharedneedle " * 20)["status"] == "ok"
    wanted = history.read_sources([history.recall("historical")["sources"][0]["id"]])["sources"][0]
    api.policy_path.write_text(
        json.dumps({"allowed_source_hashes": [source_exposure_hash(wanted)]})
    )
    found = api.call("castle_brain_recall", {"query": "sharedneedle", "limit": 1})
    assert [s["id"] for s in found["sources"]] == [wanted["id"]]


def test_missing_and_changed_host_policy_fail_closed(unified, monkeypatch):
    _, api, _, _ = unified
    api.policy_path.unlink()
    assert (
        api.call("castle_brain_recall", {"query": "sharedneedle"})["status"] == "exposure_blocked"
    )
    api.policy_path.write_text('{"allow_local_sources":true}')
    api.policy_path.chmod(0o600)
    from cognitive_castle import memory_runtime

    run = memory_runtime.run_memory

    def revoked(*a, **kw):
        result = run(*a, **kw)
        api.policy_path.write_text('{"allow_local_sources":false}')
        return result

    monkeypatch.setattr(memory_runtime, "run_memory", revoked)
    result = api.call("castle_brain_recall", {"query": "sharedneedle"})
    assert result["status"] == "exposure_blocked" and "sources" not in result


def test_explicit_legacy_modes_are_labeled_without_reinterpretation(unified, monkeypatch):
    from cognitive_castle import searcher

    palace, _, _, _ = unified
    seen = []

    def legacy(*a, **kw):
        seen.append(kw["mode"])
        return [{"id": "old-drawer", "text": "legacy only"}]

    monkeypatch.setattr(searcher, "_new_pipeline_search", legacy)
    for mode in ("fast", "standard", "boosted", "max"):
        result = searcher.search_memories("sharedneedle", str(palace), mode=mode)
        assert result["coverage"] == "legacy_index_only"
    assert seen == ["fast", "standard", "boosted", "max"]
    with pytest.raises(ValueError, match="legacy"):
        searcher.search_memories("sharedneedle", str(palace), info_weight=True)
    with pytest.raises(ValueError, match="legacy"):
        searcher.search_memories("sharedneedle", str(palace), max_distance=1)


def test_workspace_context_includes_history_and_keeps_work_state(unified):
    _, api, _, _ = unified
    result = api.call("castle_memory_context", {"protocol_version": 1, "workspace": "/work"})
    assert result["status"] == "ok"
    assert any("historical exact" in s["text"] for s in result["sources"])
    assert "work_state" in result
    assert "another workspace" not in json.dumps(result)
    assert len(json.dumps(result, ensure_ascii=False).encode()) <= 8192


def test_historical_context_reference_preserves_attributed_work_state(unified):
    _, api, _, history = unified
    identity = history.recall("historical")["sources"][0]["id"]
    updated = api.call(
        "castle_memory_context_update",
        {
            "protocol_version": 1,
            "workspace": "/work",
            "request_id": "work-1",
            "expected_revision": 0,
            "status": "completed",
            "source_ids": [identity],
        },
    )
    assert updated["status"] == "ok"
    result = api.call("castle_memory_context", {"protocol_version": 1, "workspace": "/work"})
    assert result["work_state"]["status"] == "completed"
    assert result["work_state"]["sources"][0]["source_id"] == identity
    assert any(s["source_id"] == identity for s in result["sources"])


def _registered_occurrence(history, *, workspace="/work"):
    from cognitive_castle.memory_history import _commit_row

    metadata = json.dumps({"workspace": workspace, "private_metadata": "ungranted detail"})
    row = {
        "fragment": 7,
        "row_address": 7 << 32,
        "offset": 1,
        "values": {
            "id": "registry-row",
            "text": "registryneedle exact text",
            "metadata_json": metadata,
        },
    }
    _, indexed, gaps = _commit_row(
        history,
        {"archive_id": "archive", "snapshot_id": "b" * 64, "version": 1},
        row,
        {"fragment": 0, "offset": 0},
    )
    assert (indexed, gaps) == (1, 0)
    _install_workspace_indexes(history)
    identity = history.recall("registryneedle")["sources"][0]["id"]
    return identity, metadata


def test_selected_text_hash_does_not_grant_metadata_or_its_descriptor(unified, monkeypatch):
    from cognitive_castle.brain_loop import source_exposure_hash
    from cognitive_castle.unified_memory import UnifiedMemoryStore
    from cognitive_castle import memory_runtime

    palace, api, _, history = unified
    identity, metadata = _registered_occurrence(history)
    source = UnifiedMemoryStore(palace).read_sources([identity])["sources"][0]
    descriptor = source["metadata"]
    api.policy_path.write_text(
        json.dumps({"allowed_source_hashes": [source_exposure_hash(source)]})
    )
    recall = api.call("castle_brain_recall", {"query": "registryneedle"})
    assert recall["sources"][0]["id"] == identity
    assert "metadata" not in recall["sources"][0]
    text = api.call(
        "castle_memory_read",
        {
            "protocol_version": 1,
            "source_id": identity,
            "expected_hash": source["provenance"]["text_hash"],
        },
    )
    assert text["text"] == source["text"]
    assert "metadata" not in text
    run = memory_runtime.run_memory
    calls = []

    def tracked(palace, operation, arguments):
        calls.append((operation, arguments))
        return run(palace, operation, arguments)

    monkeypatch.setattr(memory_runtime, "run_memory", tracked)
    for field in ("metadata", "columns"):
        blocked = api.call(
            "castle_memory_read",
            {
                "protocol_version": 1,
                "source_id": identity,
                "field": field,
                "expected_hash": descriptor[field + "_hash"],
            },
        )
        assert blocked["status"] == "exposure_blocked"
    assert calls == []
    api.policy_path.write_text('{"allow_local_sources": true}')
    permitted = api.call(
        "castle_memory_read",
        {
            "protocol_version": 1,
            "source_id": identity,
            "field": "metadata",
            "expected_hash": descriptor["metadata_hash"],
        },
    )
    assert permitted["status"] == "ok" and permitted["text"] == metadata


def test_long_historical_workspace_uses_registry_for_work_state(unified):
    _, api, _, history = unified
    workspace = "/work/" + "long-project-name/" * 6
    assert len(workspace.encode()) > 64
    identity, _ = _registered_occurrence(history, workspace=workspace)
    caller = history.read_sources([identity])["sources"][0]["provenance"]["caller"]
    assert "workspace" not in caller
    recalled = api.call("castle_brain_recall", {"query": "registryneedle", "workspace": workspace})
    assert recalled["sources"][0]["id"] == identity
    updated = api.call(
        "castle_memory_context_update",
        {
            "protocol_version": 1,
            "workspace": workspace,
            "request_id": "long-work-1",
            "expected_revision": 0,
            "status": "active",
            "source_ids": [identity],
        },
    )
    assert updated["status"] == "ok"
    restored = api.call("castle_memory_context", {"protocol_version": 1, "workspace": workspace})
    assert restored["status"] == "ok"
    assert restored["work_state"]["sources"][0]["source_id"] == identity
    assert restored["sources"][0]["source_id"] == identity
    mismatch = api.call(
        "castle_memory_context_update",
        {
            "protocol_version": 1,
            "workspace": "/another",
            "request_id": "wrong-work-1",
            "expected_revision": 0,
            "status": "active",
            "source_ids": [identity],
        },
    )
    assert mismatch["status"] == "scope_mismatch"


def test_cold_default_routes_import_no_native_or_models():
    code = """
import importlib.abc, sys
class Deny(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow','lancedb','lance','torch','transformers','sentence_transformers'}:
            raise AssertionError('forbidden ordinary import: '+fullname)
sys.meta_path.insert(0, Deny())
from cognitive_castle.searcher import search_memories
from cognitive_castle.cli import build_parser
from cognitive_castle import mcp_server
assert build_parser().parser.parse_args(['search','needle']).mode is None
assert search_memories('needle','/missing-palace')['status'] == 'exposure_blocked'
assert mcp_server.tool_search('needle')['status'] == 'exposure_blocked'
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr

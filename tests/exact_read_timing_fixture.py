"""Fixed read-only timing probe; only the archived synthetic receipt is allowed."""

import atexit
import builtins
import json
import os
from pathlib import Path
import sys
import time

_TRACE = {}


def main():
    started = time.perf_counter()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    assert len(sys.argv) == 3 and sys.argv[1] == "timing"
    trace = Path(sys.argv[2])
    scratch = Path(os.environ["CASTLE_EXACT_TIMING_SCRATCH"])
    assert trace.parent == scratch and scratch.resolve() == scratch
    assert scratch.stat().st_uid == os.getuid() and not scratch.stat().st_mode & 0o077
    request = json.loads(sys.stdin.buffer.read(512 * 1024 + 1))
    from cognitive_castle import lance_exact_worker as worker

    worker._install_guards()
    refusal = worker._admit_storage(request)
    if refusal is not None:
        return refusal
    fd = os.open(trace, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    events = 0

    def mark(event):
        nonlocal events
        if events < 64:
            events += 1
            os.write(
                fd,
                (
                    json.dumps(dict(event=event, seconds=time.perf_counter() - started)) + "\n"
                ).encode(),
            )

    _TRACE["mark"] = mark

    def final_trace():
        mark("atexit_end")
        os.close(fd)

    atexit.register(final_trace)
    mark("admitted")
    corpus = Path(os.environ["CASTLE_EXACT_TIMING_CORPUS"])
    assert (
        corpus.resolve() == corpus and corpus.name == "corpus" and corpus.parent.name == "fixtures"
    )
    value = json.loads((corpus / "model-handoff.json").read_text())
    assert value["source"] == "387b8c08191f399e240b2c054b4f429d13e1518f"
    assert request["directory"] == str(corpus / "generation")
    assert request["operation"] == "layout_read"
    assert request["arguments"] == dict(
        operation="receipt",
        arguments=dict(namespace="registered-model-fixture", key="3"),
        envelope_hash=value["pin"],
    )
    from cognitive_castle import (
        partitioned_exact_store as partition,
        lance_layout_directory as authority,
    )
    from cognitive_castle.lance_exact_store import LanceExactStore

    mark("pure_modules_ready")

    def wrap(owner, name, label):
        original = getattr(owner, name)

        def observed(*args, **kwargs):
            mark(label + ".begin")
            try:
                return original(*args, **kwargs)
            finally:
                mark(label + ".end")

        setattr(owner, name, observed)

    wrap(authority._NativeLayoutDirectory, "__init__", "authority")
    wrap(authority, "load_layout_qualification", "qualification")
    wrap(partition, "_temporary_charge", "temporary_geometry")
    wrap(partition._PartitionStore, "_paths", "paths")
    wrap(partition._PartitionStore, "_open", "dataset_open")
    wrap(partition._PartitionStore, "actual_pin", "actual_pin")
    wrap(LanceExactStore, "_verify_receipt", "receipt_verify")
    wrap(partition._PartitionStore, "_lookup_rows", "row_lookup")
    original_import = builtins.__import__
    imports = set()
    plugin_wrapped = False

    def traced_import(name, globals=None, locals=None, fromlist=(), level=0):
        nonlocal plugin_wrapped
        observed = (
            level == 0
            and name
            in {
                "lance",
                "pyarrow",
                "numpy",
                "lance_namespace",
                "lance_namespace_urllib3_client",
                "pydantic",
                "pydantic_core",
            }
            and name not in imports
        )
        if observed:
            imports.add(name)
            mark("import_" + name + ".begin")
        try:
            result = original_import(name, globals, locals, fromlist, level)
            loader = sys.modules.get("pydantic.plugin._loader")
            if not plugin_wrapped and loader is not None and hasattr(loader, "get_plugins"):
                plugin_wrapped = True
                get_plugins = loader.get_plugins
                plugin_measured = False

                def measured_plugins():
                    nonlocal plugin_measured
                    if plugin_measured:
                        return get_plugins()
                    plugin_measured = True
                    mark("plugin_discovery.begin")
                    try:
                        return get_plugins()
                    finally:
                        mark("plugin_discovery.end")

                loader.get_plugins = measured_plugins
            return result
        finally:
            if observed:
                mark("import_" + name + ".end")

    builtins.__import__ = traced_import
    try:
        mark("execute.begin")
        result = worker._execute(request)
        mark("execute.end")
        mark("execute.result_" + result.get("status", "invalid"))
        atexit.register(lambda: mark("atexit_begin"))
        return result
    finally:
        builtins.__import__ = original_import


if __name__ == "__main__":
    try:
        result = main()
    except Exception as exc:
        result = dict(status="unavailable", reason="timing_fixture_" + type(exc).__name__)
    sys.stdout.write(json.dumps(result))
    sys.stdout.flush()
    if "mark" in _TRACE:
        _TRACE["mark"]("stdout_flushed")

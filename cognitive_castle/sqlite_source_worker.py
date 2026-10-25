"""Private SQLite-only exporter executable; fixed guardian admission is required."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

MAX_FRAME = 512 * 1024


def main():
    os.umask(0o077)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_worker import _install_guards, _admit_storage, _sqlite_heap_limit

    _install_guards()
    # The legacy guard predates direct `lance` imports. Block them too.
    import importlib.abc

    class NoNative(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname.split(".")[0] in {"lance", "numpy", "pandas"}:
                raise ModuleNotFoundError("SQLite export forbids native backends")
            return None

    sys.meta_path.insert(0, NoNative())
    try:
        raw = sys.stdin.buffer.read(MAX_FRAME + 1)
        if len(raw) > MAX_FRAME:
            raise ValueError("request frame")
        request = json.loads(raw)
        if not isinstance(request, dict) or request.keys() not in (
            {"descriptor", "cursor", "_host_pid"},
            {"descriptor", "cursor", "layout", "_host_pid"},
        ):
            raise ValueError("request shape")
        result = _admit_storage(request)
        if result is None:
            _sqlite_heap_limit()
            if "layout" in request:
                from cognitive_castle.sqlite_export_codec import prepare_layout_request

                prepare_layout_request(request["descriptor"], request["cursor"], request["layout"])
            from cognitive_castle.sqlite_source_export import export_slice

            result = (
                export_slice(request["descriptor"], request["cursor"], layout=request["layout"])
                if "layout" in request
                else export_slice(request["descriptor"], request["cursor"])
            )
        output = json.dumps(result, allow_nan=False).encode("ascii")
        if len(output) > MAX_FRAME:
            raise ValueError("response frame")
    except Exception as exc:
        output = json.dumps(
            {"status": "unavailable", "reason": "source_worker_" + type(exc).__name__}
        ).encode("ascii")
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()

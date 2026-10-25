"""Fixed read-only native exporter executable, admitted by the existing guardian."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

MAX_FRAME = 512 * 1024


def _admit_source(request):
    import psutil
    from cognitive_castle.memory_history_worker import _admit_storage

    result = _admit_storage(request)
    if result is not None:
        return result
    try:
        guardian = psutil.Process(os.getppid())
        host = psutil.Process(request["_host_pid"])
        expected = str(Path(__file__).with_name("brain_reader_guardian.py"))
        command = guardian.cmdline()
        if (
            len(command) != 3
            or command[1:] != ["-I", expected]
            or os.getpgrp() != guardian.pid
            or os.getsid(0) != guardian.pid
            or os.getsid(guardian.pid) != guardian.pid
            or guardian.ppid() != request["_host_pid"]
            or guardian.uids().effective != os.getuid()
            or host.uids().effective != os.getuid()
        ):
            return {"status": "unavailable", "reason": "native_source_supervision_required"}
    except (psutil.Error, OSError):
        return {"status": "unavailable", "reason": "native_source_supervision_required"}
    return None


def main():
    os.umask(0o077)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from cognitive_castle.memory_history_worker import _install_guards

    _install_guards()

    def no_sqlite(event, args):
        if event == "sqlite3.connect":
            raise PermissionError("native source export forbids SQLite connections")

    sys.addaudithook(no_sqlite)
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
        if type(request["_host_pid"]) is not int or request["_host_pid"] <= 0:
            raise ValueError("host identity")
        result = _admit_source(request)
        if result is None:
            if "layout" in request:
                from cognitive_castle.native_export_codec import prepare_layout_request

                prepare_layout_request(request["descriptor"], request["cursor"], request["layout"])
            from cognitive_castle.native_source_export import export_slice

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
            {"status": "unavailable", "reason": "native_source_worker_" + type(exc).__name__}
        ).encode("ascii")
    sys.stdout.buffer.write(output)
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()

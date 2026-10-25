"""Inactive private source read, with fixed existing SQLite supervision."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

from . import memory_runtime
from .sqlite_export_codec import (
    empty_counts,
    frame_bytes,
    initial_cursor,
    source_binding,
    validate_cursor,
)


def source_request(descriptor, cursor=None, *, layout=None):
    binding = source_binding(descriptor)
    cursor = initial_cursor(binding, descriptor["store_id"]) if cursor is None else cursor
    validate_cursor(cursor, binding, descriptor["store_id"])
    if layout is not None:
        from .sqlite_export_codec import prepare_layout_request

        return prepare_layout_request(descriptor, cursor, layout)
    request = {"descriptor": descriptor, "cursor": cursor}
    frame_bytes(request)
    return request


def source_refusal(request, result):
    """Normalize supervisor failures without allowing cursor/completion claims."""
    cursor = request["cursor"]
    if "layout" in request:
        from .lance_layout_codec import SOURCE_RESULT
        from .canonical_records import _string

        status = result.get("status", "unavailable")
        reason = result.get("reason", "source_invalid_response")
        try:
            _string(status, 64, "source status")
            if status == "ok":
                raise ValueError("refusal status")
        except ValueError:
            status = "unavailable"
        if reason is not None:
            try:
                _string(reason, 1024, "source reason")
            except ValueError:
                reason = "source_invalid_response"
        wrapped = dict(
            format=SOURCE_RESULT,
            status=status,
            reason=reason,
            action=request["layout"]["action"],
            source_binding=cursor["source_binding"],
            cursor_before=cursor,
            cursor_after=cursor,
            plans=[],
            export=None,
        )
        if request["layout"]["action"] == "prepare":
            wrapped["preparation_hash"] = None
        frame_bytes(wrapped)
        return wrapped
    return dict(
        status=result.get("status", "unavailable"),
        reason=result.get("reason", "source_invalid_response"),
        source_binding=cursor["source_binding"],
        cursor_before=cursor,
        cursor_after=cursor,
        entries=[],
        finalizations=[],
        complete=False,
        phase_complete=False,
        phase=cursor["phase"],
        coverage="incomplete",
        counts=empty_counts(),
        frame_bytes=0,
        **({"resources": result["resources"]} if "resources" in result else {}),
    )


def source_result(request, result):
    if "layout" in request:
        from .lance_layout_codec import validate_layout_source_result

        if not isinstance(result, dict):
            return source_refusal(
                request, dict(status="unavailable", reason="source_invalid_response")
            )
        if result.get("status") != "ok":
            return source_refusal(request, result)
        try:
            validate_layout_source_result(
                result, request["layout"], request["descriptor"], request["cursor"]
            )
            frame_bytes(result)
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            return source_refusal(
                request, dict(status="unavailable", reason="source_invalid_response")
            )
        return result
    if "cursor_before" not in result:
        return source_refusal(request, result)
    frame_bytes({key: value for key, value in result.items() if key != "resources"})
    return result


def export_slice(descriptor, cursor=None):
    request = source_request(descriptor, cursor)

    def refusal(reason):
        return source_refusal(request, {"status": "unavailable", "reason": reason})

    if sys.platform != "darwin" or importlib.util.find_spec("psutil") is None:
        return refusal("platform_supervision_unavailable")
    if memory_runtime.interactive_waiting():
        return refusal("memory_busy")
    try:
        admission = memory_runtime._admission()
    except BlockingIOError:
        return refusal("memory_busy")
    except OSError:
        return refusal("admission_unavailable")
    try:
        if memory_runtime.interactive_waiting():
            return refusal("memory_busy")
        result = memory_runtime._supervise(
            request,
            admission,
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("sqlite_source_worker.py")),
            ],
            deadline=2,
            rss_limit=128 * 1024**2,
            total_rss_limit=192 * 1024**2,
            background_preempt=True,
        )
        return source_result(request, result)
    finally:
        os.close(admission)

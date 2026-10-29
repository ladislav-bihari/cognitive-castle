"""Inactive trusted semantic coordinator sharing the existing memory admission.

Only host preparation supplies the artifact manifest. The fixed worker and limits
are not public tool arguments. One session owns a single absolute deadline and
admission across all stages; callers must use the context manager.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys
import time

from . import memory_runtime
from .semantic_profile import MAX_FRAME, canonical_json, validate_vector
from .semantic_windows import input_binding, validate_page, validate_sources

TASK_RSS = 512 * 1024**2
TOTAL_RSS = 640 * 1024**2
DEADLINE = 5


class SemanticSession:
    def __init__(self, trusted_profile: Path, *, interactive=True):
        self._until = time.monotonic() + DEADLINE
        self._profile = Path(trusted_profile).absolute()
        self._interactive = interactive
        self._fd = self._waiter = None
        self._entered = False
        self._refusal = None
        self._issued = set()
        self.resources = []

    @property
    def remaining(self):
        return max(0, self._until - time.monotonic())

    def __enter__(self):
        if self._entered:
            raise ValueError("semantic session cannot be reused")
        self._entered = True
        if sys.platform != "darwin" or importlib.util.find_spec("psutil") is None:
            self._refusal = "platform_supervision_unavailable"
            return self
        try:
            if not self._interactive and memory_runtime.interactive_waiting():
                self._refusal = "memory_busy"
                return self
            try:
                self._fd = memory_runtime._admission()
            except BlockingIOError:
                if not self._interactive:
                    self._refusal = "memory_busy"
                    return self
                self._waiter = memory_runtime._waiting_marker()
                until = min(self._until, time.monotonic() + 0.5)
                while time.monotonic() < until:
                    try:
                        self._fd = memory_runtime._admission()
                        break
                    except BlockingIOError:
                        time.sleep(min(0.025, max(0, until - time.monotonic())))
                if self._fd is None:
                    self._refusal = "memory_busy"
            if not self._interactive and memory_runtime.interactive_waiting():
                self._refusal = "memory_busy"
        except OSError:
            self._refusal = "admission_unavailable"
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *args):
        for name in ("_fd", "_waiter"):
            fd = getattr(self, name)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)
        self._issued.clear()

    def _ready(self):
        if not self._entered:
            raise ValueError("semantic session requires context manager")
        if self._refusal:
            return {"status": "unavailable", "reason": self._refusal}
        if self._fd is None:
            return {"status": "unavailable", "reason": "semantic_session_closed"}
        if self.remaining <= 0:
            return {"status": "unavailable", "reason": "semantic_deadline"}
        if not self._interactive and memory_runtime.interactive_waiting():
            return {"status": "unavailable", "reason": "memory_busy"}
        return None

    def _run_stage(self, request, *, worker_command, cap=5, rss=TASK_RSS, total=TOTAL_RSS):
        """Internal extension point for a later fixed Lance stage under this lease."""
        refusal = self._ready()
        if refusal is not None:
            return refusal
        if len(canonical_json(request).encode("utf-8")) > MAX_FRAME:
            raise ValueError("semantic request limit")
        remaining = self.remaining
        if remaining <= 0:
            return {"status": "unavailable", "reason": "semantic_deadline"}
        result = memory_runtime._supervise(
            request,
            self._fd,
            worker_command=worker_command,
            deadline=min(remaining, cap),
            absolute_deadline=self._until,
            rss_limit=rss,
            total_rss_limit=total,
            background_preempt=not self._interactive,
        )
        if result.get("reason") == "background_preempted":
            self._refusal = "background_preempted"
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
        resources = result.pop("resources", None)
        if resources is not None:
            self.resources.append(resources)
        if result.get("status") == "ok" and self.remaining <= 0:
            return {"status": "unavailable", "reason": "semantic_deadline"}
        return result

    def _semantic(self, operation, arguments):
        return self._run_stage(
            {"operation": operation, "arguments": arguments},
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("semantic_worker.py")),
                "--profile",
                str(self._profile),
            ],
        )

    def tokenize(self, sources, continuation=None):
        refusal = self._ready()
        if refusal is not None:
            return refusal
        validate_sources(sources)
        result = self._semantic("tokenize", {"sources": sources, "continuation": continuation})
        if result.get("status") == "ok":
            try:
                validate_page(result)
                start = 0 if continuation is None else continuation["next_window"]
                if (
                    result["input_binding"] != input_binding(sources)
                    or result["start_window"] != start
                ):
                    raise ValueError("tokenizer response binding")
            except (ValueError, TypeError, KeyError):
                return {"status": "unavailable", "reason": "semantic_invalid_response"}
            if self.remaining <= 0:
                return {"status": "unavailable", "reason": "semantic_deadline"}
            self._issued.add(result["handoff_hash"])
        return result

    def infer(self, page):
        refusal = self._ready()
        if refusal is not None:
            return refusal
        validate_page(page)
        if page["handoff_hash"] not in self._issued:
            raise ValueError("unissued semantic handoff")
        result = self._semantic("infer", {"page": page, "expected_handoff": page["handoff_hash"]})
        if result.get("status") != "ok":
            return result
        try:
            fields = {
                "status",
                "profile",
                "handoff_hash",
                "input_binding",
                "start_window",
                "total_units",
                "next_continuation",
                "complete",
                "vectors",
                "runtime",
            }
            if set(result) != fields:
                raise ValueError("inference response signature")
            for key in fields - {"vectors", "runtime"}:
                if result[key] != page[key]:
                    raise ValueError("inference response binding")
            if not isinstance(result["vectors"], list) or len(result["vectors"]) != len(
                page["units"]
            ):
                raise ValueError("inference vector count")
            for row, unit in zip(result["vectors"], page["units"]):
                if (
                    not isinstance(row, dict)
                    or set(row) != {"unit_id", "vector"}
                    or row["unit_id"] != unit["unit_id"]
                ):
                    raise ValueError("inference vector identity")
                validate_vector(row["vector"])
        except (ValueError, TypeError, KeyError):
            return {"status": "unavailable", "reason": "semantic_invalid_response"}
        if self.remaining <= 0:
            return {"status": "unavailable", "reason": "semantic_deadline"}
        return result

    def exact(self, directory, operation, arguments):
        from .lance_exact_worker import _FIELDS

        if operation not in _FIELDS:
            raise ValueError("exact operation")
        return self._run_stage(
            {
                "directory": str(Path(directory).absolute()),
                "operation": operation,
                "arguments": arguments,
            },
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("lance_exact_worker.py")),
            ],
            cap=3
            if operation
            in {
                "remember",
                "import_sources",
                "import_batch",
                "build_id_index",
                "layout_import_batch",
                "layout_maintain",
            }
            else 2,
            rss=192 * 1024**2,
            total=256 * 1024**2,
        )

    def sqlite_source(self, descriptor, cursor=None):
        """One fixed source stage; admission remains owned by this session."""
        from .sqlite_source_runtime import source_request, source_result, source_refusal

        request = source_request(descriptor, cursor)
        result = self._run_stage(
            request,
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("sqlite_source_worker.py")),
            ],
            cap=2,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        result = source_result(request, result)
        if result.get("status") == "ok" and self.remaining <= 0:
            return source_refusal(request, {"status": "unavailable", "reason": "semantic_deadline"})
        return result

    def native_source(self, descriptor, cursor=None):
        """Fixed read-only native export under this session's absolute lease."""
        from .native_source_runtime import source_request, source_result, source_refusal

        request = source_request(descriptor, cursor)
        result = self._run_stage(
            request,
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("native_source_worker.py")),
            ],
            cap=2,
            rss=512 * 1024**2,
            total=640 * 1024**2,
        )
        result = source_result(request, result)
        if result.get("status") == "ok" and self.remaining <= 0:
            return source_refusal(request, {"status": "unavailable", "reason": "semantic_deadline"})
        return result

    def layout_source(self, descriptor, cursor, window):
        """Single fixed source stage; the registered wrapper carries no budgets."""
        from . import native_export_codec
        from .sqlite_source_runtime import source_result, source_refusal

        native = (
            isinstance(descriptor, dict) and descriptor.get("format") == native_export_codec.FORMAT
        )
        if native:
            from .native_source_runtime import source_request
        else:
            from .sqlite_source_runtime import source_request
        request = source_request(descriptor, cursor, layout=window)
        result = self._run_stage(
            request,
            worker_command=[
                sys.executable,
                "-I",
                str(
                    Path(__file__).with_name(
                        "native_source_worker.py" if native else "sqlite_source_worker.py"
                    )
                ),
            ],
            cap=2,
            rss=(512 if native else 128) * 1024**2,
            total=(640 if native else 192) * 1024**2,
        )
        result = source_result(request, result)
        if result.get("status") == "ok" and self.remaining <= 0:
            return source_refusal(request, {"status": "unavailable", "reason": "semantic_deadline"})
        return result

    def store(self, directory, operation, arguments):
        from .semantic_store_worker import _FIELDS

        if operation not in _FIELDS:
            raise ValueError("semantic store operation")
        queue = operation.startswith("queue_")
        read = operation in {
            "queue_status",
            "queue_migration_state",
            "queue_layout_routes",
            "vector_receipt",
            "vector_layout_receipt",
            "vector_nearest",
        }
        return self._run_stage(
            {
                "directory": str(Path(directory).absolute()),
                "operation": operation,
                "arguments": arguments,
            },
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("semantic_store_worker.py")),
            ],
            cap=2 if read else 3,
            rss=(128 if queue else 192) * 1024**2,
            total=(192 if queue else 256) * 1024**2,
        )


class LayoutSemanticSession(SemanticSession):
    """Trusted registered-generation adapter for the existing semantic driver.

    Exact hydration never falls back to the flat store. Selecting this internal
    session does not register, migrate or activate a production generation.
    """

    def __init__(self, trusted_profile: Path, generation: Path, *, interactive=True):
        self.generation = Path(generation)
        if not self.generation.is_absolute() or ".." in self.generation.parts:
            raise ValueError("registered semantic generation")
        super().__init__(trusted_profile, interactive=interactive)

    def _directory(self, directory, expected):
        value = Path(directory)
        if not value.is_absolute() or ".." in value.parts or value != expected:
            raise ValueError("registered semantic directory")

    def exact(self, directory, operation, arguments):
        self._directory(directory, self.generation / "exact")
        if operation not in {"read_sources", "receipt", "page"}:
            raise ValueError("registered exact read operation")
        return super().exact(
            self.generation,
            "layout_read",
            dict(operation=operation, arguments=arguments),
        )

    def store(self, directory, operation, arguments):
        self._directory(directory, self.generation)
        if operation in {"vector_publish", "vector_receipt"}:
            operation = operation.replace("vector_", "vector_layout_", 1)
        return super().store(directory, operation, arguments)

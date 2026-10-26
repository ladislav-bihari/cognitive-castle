"""Bounded projected windows for a pinned native row, inside the history worker.

Projection bounds returned values, not native decoder allocations. The existing
guardian, fixed IO/cache controls and installed-stack qualification remain required.
"""

from __future__ import annotations

import re

from .memory_history import IO_BUFFER_BYTES, MAX_FILES, MAX_NATIVE_BATCH_BYTES, _HistoryError

PAGE_CHARACTERS = 131072
_BYTES = "_castle_text_bytes"
_CHARACTERS = "_castle_text_characters"
_HASH = "_castle_text_sha256"
_PAGE = "_castle_text_page"


class NativeProjectedRow:
    """One immutable occurrence, with bounded materialization of each window."""

    def __init__(self, snapshot, version, cursor):
        import lance

        self.cursor = dict(cursor)
        self.dataset = lance.dataset(
            str(snapshot),
            version=version,
            metadata_cache_size_bytes=4 * 1024**2,
            index_cache_size_bytes=1024**2,
            read_params={"cache_repetition_index": False, "validate_on_decode": True},
        )
        if self.dataset.version != version:
            raise _HistoryError("changed")
        self._version = version
        names = self.dataset.schema.names
        if (
            any(name not in names for name in ("id", "text", "metadata_json"))
            or len(names) > 33
            or any(name in names for name in (_BYTES, _CHARACTERS, _HASH, _PAGE, "_rowaddr"))
        ):
            raise _HistoryError("unsupported")
        self.columns = [name for name in names if name not in {"text", "vector"}]
        self.fragments = sorted(self.dataset.get_fragments(), key=lambda f: f.fragment_id)
        if len(self.fragments) > MAX_FILES:
            raise _HistoryError("quota")
        self._header = None
        self._fragment = None
        self._bound_next = None

    def _scan(self, columns, fragment, offset):
        scanner = self.dataset.scanner(
            columns=columns,
            fragments=[fragment],
            offset=offset,
            limit=1,
            batch_size=1,
            strict_batch_size=True,
            batch_readahead=0,
            fragment_readahead=0,
            scan_in_order=True,
            with_row_address=True,
            io_buffer_size=IO_BUFFER_BYTES,
            use_scalar_index=False,
        )
        batches = iter(scanner.to_batches())
        try:
            batch = next(batches, None)
            if batch is None:
                return None
            if batch.num_rows != 1 or batch.nbytes > MAX_NATIVE_BATCH_BYTES:
                raise _HistoryError("native_batch_size")
            return {
                name: batch.column(batch.schema.get_field_index(name))[0].as_py()
                for name in [*columns, "_rowaddr"]
            }
        finally:
            close = getattr(batches, "close", None)
            if close is not None:
                close()

    def bind_header(self, header):
        """Reuse a validated durable stage header without repeating native work."""
        if not isinstance(header, dict) or set(header) != {
            "fragment",
            "offset",
            "row_address",
            "values",
            "text_bytes",
            "text_characters",
            "text_hash",
        }:
            raise _HistoryError("corrupt")
        for field in ("fragment", "offset", "row_address", "text_bytes", "text_characters"):
            if type(header[field]) is not int or not 0 <= header[field] < 2**63:
                raise _HistoryError("corrupt")
        fragment = next((f for f in self.fragments if f.fragment_id == header["fragment"]), None)
        same_fragment = header["fragment"] == self.cursor["fragment"]
        offset = self.cursor["offset"] if same_fragment else 0
        if (
            fragment is None
            or header["fragment"] < self.cursor["fragment"]
            or header["row_address"] >> 32 != header["fragment"]
            or (same_fragment and header["row_address"] <= self.cursor.get("row_address", -1))
            or header["offset"] != offset + 1
            or not header["text_characters"]
            <= header["text_bytes"]
            <= 4 * header["text_characters"]
            or not isinstance(header["text_hash"], str)
            or re.fullmatch(r"[a-f0-9]{64}", header["text_hash"]) is None
            or not isinstance(header["values"], dict)
            or set(header["values"]) != set(self.columns)
        ):
            raise _HistoryError("corrupt")
        self._header, self._fragment = dict(header), fragment
        # Keep the validated original following cursor independent of the
        # mutable header returned to a caller.
        self._bound_next = tuple(header[name] for name in ("fragment", "offset", "row_address"))

    def advance(self):
        """Read the next original occurrence on this same immutable Dataset."""
        if self.dataset is None or self._header is None or self._bound_next is None:
            raise _HistoryError("corrupt")
        if self.dataset.version != self._version:
            raise _HistoryError("changed")
        self.cursor = dict(zip(("fragment", "offset", "row_address"), self._bound_next))
        self._header = self._fragment = self._bound_next = None
        return self.header_or_none()

    def header(self):
        header = self.header_or_none()
        if header is None:
            raise _HistoryError("corrupt")
        return header

    def header_or_none(self):
        """Distinguish proven fragment exhaustion without changing header()."""
        if self._header is not None:
            return self._header
        # Preserve metadata through native column selection, not SQL strings:
        # Lance treats double quotes as literals and cannot project every valid
        # field name through SQL. Only the fixed text column needs expressions.
        columns = {
            _BYTES: "octet_length(`text`)",
            _CHARACTERS: "character_length(`text`)",
            _HASH: "sha256(`text`)",
        }
        for fragment in self.fragments:
            if fragment.fragment_id < self.cursor["fragment"]:
                continue
            offset = self.cursor["offset"] if fragment.fragment_id == self.cursor["fragment"] else 0
            result = self._scan(self.columns, fragment, offset)
            if result is None:
                continue
            statistics = self._scan(columns, fragment, offset)
            if statistics is None or statistics["_rowaddr"] != result["_rowaddr"]:
                raise _HistoryError("changed")
            digest = statistics[_HASH]
            if not isinstance(digest, bytes) or len(digest) != 32:
                raise _HistoryError("unsupported")
            header = {
                "fragment": fragment.fragment_id,
                "offset": offset + 1,
                "row_address": result.pop("_rowaddr"),
                "text_bytes": statistics[_BYTES],
                "text_characters": statistics[_CHARACTERS],
                "text_hash": digest.hex(),
                "values": result,
            }
            self.bind_header(header)
            return self._header
        return None

    def page(self, start, limit=PAGE_CHARACTERS):
        if (
            type(start) is not int
            or start < 0
            or type(limit) is not int
            or not 1 <= limit <= PAGE_CHARACTERS
        ):
            raise ValueError("projection page bounds")
        header = self.header()
        if start > header["text_characters"]:
            raise ValueError("projection page offset")
        remaining = header["text_characters"] - start
        count = min(limit, remaining)
        if count == 0:
            return ""
        # Lance4.0.1 rejects SUBSTRING AST syntax. These qualified scalar
        # functions preserve codepoints, including Unicode and embedded NUL.
        # Both forms return the same exact window. Choose the smaller
        # intermediate string so early windows do not copy almost the full row.
        prefix = start + count
        expression = (
            f"right(left(`text`, {prefix}), {count})"
            if prefix <= remaining
            else f"left(right(`text`, {remaining}), {count})"
        )
        result = self._scan({_PAGE: expression}, self._fragment, header["offset"] - 1)
        if result is None or result["_rowaddr"] != header["row_address"]:
            raise _HistoryError("changed")
        text = result[_PAGE]
        if not isinstance(text, str) or len(text) != count or len(text.encode()) > 4 * limit:
            raise _HistoryError("corrupt")
        return text

    def close(self):
        self._header = self._fragment = self.dataset = None
        self._bound_next = None
        self.fragments = []

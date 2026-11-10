"""Explicit operator-only background indexing, inactive until called.

Production semantic admission is not qualified. Diagnostic opt-in only permits
one bounded step against the trusted selected generation, never a daemon, model
download, fallback, or cutover. Per-stage selection checks detect replacement;
transactional selector draining remains a separate activation requirement.
"""

import math
from pathlib import Path
import time

from .memory_selection import (
    SelectionUnavailable,
    SelectionBusy,
    load_selection,
    selection_lease,
)
from .memory_selected_runtime import layout_hash
from .semantic_indexer import index_one
from .semantic_runtime import LayoutSemanticSession


class SelectedIndexSession(LayoutSemanticSession):
    """Revalidate the frozen operator selection before each supervised stage."""

    _defer_index_after_recovery = True

    def prepare_index(self, directory):
        return self.store(directory, "queue_prepare_index", dict(require_fairness=True))

    def __init__(self, palace, selected):
        self.palace = Path(palace)
        # The loaded snapshot belongs to this invocation, not to caller mutation.
        from copy import deepcopy

        self.selected = deepcopy(selected)
        super().__init__(
            Path(selected["profile"]["path"]),
            Path(selected["envelope"]["generation"]["path"]),
            interactive=False,
            envelope_hash=layout_hash(selected),
        )

    def _ready(self):
        refusal = super()._ready()
        if refusal is not None:
            return refusal
        try:
            current = load_selection(self.palace)
        except SelectionUnavailable:
            current = None
        if current != self.selected:
            return dict(status="unavailable", reason="canonical_selection_changed")
        # Metadata validation consumes the same lease and may observe a new
        # interactive waiter; do not grant a fresh deadline after validation.
        return super()._ready()


def index_tick(palace, *, diagnostic=False, max_seconds=5):
    """Run at most one resumable indexing step; do not count append as ACK.

    No caller-controlled workers, models, limits or generation arguments. The
    diagnostic flag never authorizes production use of the 512/640 MiB profile.
    """
    return _operator_tick(palace, diagnostic, max_seconds, prepare=False)


def prepare_index_schedule(palace, *, diagnostic=False, max_seconds=5):
    """Explicit operator preparation of at most eight body-free scheduling rows."""
    return _operator_tick(palace, diagnostic, max_seconds, prepare=True)


def _operator_tick(palace, diagnostic, max_seconds, *, prepare):
    if (
        type(diagnostic) is not bool
        or type(max_seconds) not in (int, float)
        or not math.isfinite(max_seconds)
        or not 0 < max_seconds <= 5
    ):
        raise ValueError("index tick requires a boolean diagnostic and 0 < max_seconds <= 5")
    if not diagnostic:
        return dict(status="unavailable", reason="production_semantic_profile_unqualified")
    until = time.monotonic() + max_seconds
    try:
        selected = load_selection(palace)
    except SelectionUnavailable:
        return dict(status="unavailable", reason="canonical_selection_unavailable")
    if selected is None:
        return dict(status="unavailable", reason="canonical_selection_required")
    if time.monotonic() >= until:
        return dict(status="unavailable", reason="semantic_deadline")
    try:
        with selection_lease(palace, selected):
            return _index_leased(palace, selected, until, prepare=prepare)
    except SelectionBusy:
        return dict(status="unavailable", reason="canonical_selection_busy")
    except SelectionUnavailable:
        return dict(status="unavailable", reason="canonical_selection_changed")


def _index_leased(palace, selected, until, *, prepare):
    with SelectedIndexSession(palace, selected) as session:
        session._until = min(session._until, until)
        refusal = session._ready()
        if refusal is not None:
            return refusal
        result = (
            session.store(session.generation, "queue_initialize_index_schedule", {})
            if prepare
            else index_one(session, session.generation)
        )
        refusal = session._ready()
        return refusal if refusal is not None else result

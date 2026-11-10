"""Selected routing contracts, without claiming native execution or activation."""

import time
import pytest
from cognitive_castle import live_publication
from cognitive_castle import semantic_runtime
from test_memory_selection import fixture


def api():
    from cognitive_castle import memory_selected_runtime

    return memory_selected_runtime


def session(monkeypatch):
    created = []

    class Session:
        def __init__(self, profile, *, interactive):
            self._until = time.monotonic() + 5
            self.profile, self.interactive = profile, interactive
            self.closed = False
            self.calls = []
            self.resources = []
            created.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

        def _ready(self):
            return (
                dict(status="unavailable", reason="semantic_deadline")
                if time.monotonic() >= self._until
                else None
            )

        def exact(self, generation, operation, arguments):
            self.calls.append((generation, operation, arguments))
            return dict(status="unavailable", reason="native_fixture_not_run")

    monkeypatch.setattr(semantic_runtime, "SemanticSession", Session)
    return created


def test_selected_read_binds_authority_and_original_deadline(monkeypatch, tmp_path):
    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)
    until = time.monotonic() + 2
    result = api().run_selected(
        palace,
        selected,
        "read_sources",
        dict(ids=["memory:" + "a" * 64]),
        until=until,
        background=False,
    )
    assert result == dict(status="unavailable", reason="native_fixture_not_run")
    assert len(made) == 1 and made[0].closed and made[0]._until <= until
    generation, operation, arguments = made[0].calls[0]
    assert str(generation) == selected["envelope"]["generation"]["path"]
    assert operation == "layout_read" and arguments["operation"] == "read_sources"
    assert arguments["envelope_hash"] == api().layout_hash(selected)


def test_selected_remember_uses_same_lease_and_preserves_caller_without_invented_project(
    monkeypatch, tmp_path
):
    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)

    def publish(lease, generation, publication, envelope_hash):
        assert lease is made[0]
        assert publication.canonical.records[0].text == " Ω\x00\r\n "
        assert publication.canonical.records[0].source()["provenance"]["caller"] == dict(
            note="caller"
        )
        scope = publication.intent["semantic"]["groups"][0]["references"][0]["scope"]
        assert scope == dict(workspace=None, wing="projects", room="conversations")
        assert envelope_hash == api().layout_hash(selected)
        return dict(status="in_progress", reason="fixture_pending")

    monkeypatch.setattr(live_publication, "publish", publish)
    result = api().run_selected(
        palace,
        selected,
        "remember",
        dict(request_id="request", text=" Ω\x00\r\n ", provenance=dict(note="caller")),
        until=time.monotonic() + 3,
        background=False,
    )
    assert result == dict(status="in_progress", reason="fixture_pending")
    assert len(made) == 1 and made[0].closed


def test_selected_marker_disappearing_after_read_does_not_authorize_legacy_return(
    monkeypatch, tmp_path
):
    palace, marker, selected = fixture(tmp_path)
    made = session(monkeypatch)

    def exact(self, *args):
        marker.unlink()
        return dict(status="ok", sources=[])

    monkeypatch.setattr(semantic_runtime.SemanticSession, "exact", exact)
    result = api().run_selected(
        palace,
        selected,
        "read_sources",
        dict(ids=["memory:" + "a" * 64]),
        until=time.monotonic() + 2,
        background=False,
    )
    assert result == dict(status="unavailable", reason="canonical_selection_changed")
    assert made[0].closed


@pytest.mark.parametrize("operation", ["recall", "restore_context", "discover_sources"])
def test_selected_unsupported_route_never_creates_lease_or_uses_old_backend(
    monkeypatch, tmp_path, operation
):
    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)
    result = api().run_selected(
        palace, selected, operation, {}, until=time.monotonic() + 2, background=True
    )
    assert result == dict(status="unavailable", reason="canonical_route_not_ready")
    assert made == []


@pytest.mark.parametrize(
    "operation,arguments,worker_operation,cap",
    [
        ("coverage", {}, "coverage", 2),
        ("source_page", {}, "source_page", 2),
        (
            "queue_source",
            dict(path="/source.jsonl", allowed_root="/", format="codex"),
            "source_queue",
            3,
        ),
        ("capture_one", dict(key="a" * 64), "source_prepare", 3),
    ],
)
def test_selected_control_uses_original_worker_limits_and_authority(
    tmp_path, monkeypatch, operation, arguments, worker_operation, cap
):
    palace, _, selected = fixture(tmp_path)
    made = session(monkeypatch)

    def stage(self, request, **kwargs):
        assert request["operation"] == worker_operation
        assert request["authority"] == selected["control_authority"]
        assert request["authority_hash"] == selected["control_hash"]
        assert (
            kwargs["cap"] == cap
            and kwargs["rss"] == 128 * 1024**2
            and kwargs["total"] == 192 * 1024**2
        )
        return dict(status="unavailable", reason="control_fixture_not_run")

    monkeypatch.setattr(semantic_runtime.SemanticSession, "_run_stage", stage, raising=False)
    assert api().run_selected(
        palace, selected, operation, arguments, until=time.monotonic() + 3, background=True
    ) == dict(status="unavailable", reason="control_fixture_not_run")
    assert len(made) == 1 and made[0].closed and made[0].interactive is False

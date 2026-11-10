"""Real scheduling/private progress files; transport replies only are synthetic.

Native publication and checkpoint truth are separately qualified by the selected
capture native case. These tests catch scheduling/counting, not storage proof.
"""

import copy
from cognitive_castle import memory_coordinator as capture
from cognitive_castle.brain_api import _root_registry


def setup(tmp_path, monkeypatch, *, fresh, forever=False):
    palace = (tmp_path / "palace").resolve()
    root = tmp_path / "originals"
    palace.mkdir(mode=0o700)
    root.mkdir(mode=0o700)
    _root_registry(palace, ("test", str(root), "codex"))
    capture.configure_capture(palace, enabled=True)
    rows = []
    for name, key in [("first", "1" * 64), ("second", "2" * 64)]:
        path = root / (name + ".jsonl")
        path.write_text("\n")
        rows.append(dict(source_key=key, path=str(path), format="codex", state="pending"))
    if forever:
        rows.pop()
    calls = []
    counts = {}

    def worker(path, operation, arguments, **kwargs):
        assert path == palace and kwargs["background"] is True and 0 < kwargs["max_seconds"] <= 5
        if operation == "source_page":
            selected = [
                r
                for r in rows
                if r["source_key"] > arguments.get("after", "")
                and (not arguments.get("pending_only") or r["state"] == "pending")
            ]
            return dict(
                status="ok",
                sources=copy.deepcopy(selected[: arguments.get("limit", 8)]),
                next_after=None,
            )
        assert operation == "capture_one"
        key = arguments["key"]
        calls.append(key)
        counts[key] = counts.get(key, 0) + 1
        if key == "1" * 64 and (forever or counts[key] == 1):
            return dict(status="in_progress", phase="verify_replay")
        next(r for r in rows if r["source_key"] == key)["state"] = "caught_up"
        return dict(status="ok", pending_bytes=0, records=1, coverage="complete")

    monkeypatch.setattr(capture, "run_memory", worker)
    directory = palace / ".memory"
    state = capture._progress(directory)
    state["fresh_keys"] = ["1" * 64] if fresh else []
    capture._atomic(directory / "capture-progress.json", state)
    return palace, calls


def test_fresh_pending_publication_continues_without_failure_or_losing_priority(
    tmp_path, monkeypatch
):
    palace, calls = setup(tmp_path, monkeypatch, fresh=True)
    result = capture.capture_tick(palace, discover=False)
    assert result["status"] == "ok" and result["failed_sources"] == 0
    assert result["pending_slices"] == 1 and result["successful_slices"] == 2
    assert calls.count("1" * 64) == 2 and calls.count("2" * 64) == 1
    assert capture._progress(palace / ".memory")["fresh_keys"] == []


def test_ordinary_pending_source_does_not_starve_next_source_and_resumes_next_tick(
    tmp_path, monkeypatch
):
    palace, calls = setup(tmp_path, monkeypatch, fresh=False)
    first = capture.capture_tick(palace, discover=False)
    assert first["status"] == "ok" and first["failed_sources"] == 0
    assert first["pending_slices"] == 1 and first["successful_slices"] == 1
    assert calls == ["1" * 64, "2" * 64]
    again = capture.capture_tick(palace, discover=False)
    assert again["status"] == "ok" and again["failed_sources"] == 0
    assert again["pending_slices"] == 0 and again["successful_slices"] == 1
    assert calls == ["1" * 64, "2" * 64, "1" * 64]


def test_repeated_pending_results_remain_bounded_and_durable(tmp_path, monkeypatch):
    palace, calls = setup(tmp_path, monkeypatch, fresh=True, forever=True)
    result = capture.capture_tick(palace, discover=False)
    assert result["status"] == "ok" and result["failed_sources"] == 0
    assert result["pending_slices"] == 32 and result["successful_slices"] == 0
    assert len(calls) == 32
    assert capture._progress(palace / ".memory")["fresh_keys"] == ["1" * 64]

"""Body-free recovery pages; commitments are not canonical publication proofs."""

import copy
import hashlib
import json
import pytest


def codec():
    from cognitive_castle import capture_recovery_codec

    return capture_recovery_codec


def reference(ordinal=1):
    return dict(
        id=f"{ordinal:064x}", binding="b" * 64, namespace="transcript", key=f"request-{ordinal}"
    )


def test_recovery_page_is_bounded_body_free_and_snapshots_references():
    refs = [reference(), reference(2)]
    page = codec().prepare_page("a" * 64, "", refs)
    refs[0]["key"] = "changed"
    assert page["references"][0]["key"] == "request-1"
    assert set(page) == {"format", "source_key", "after", "references", "revision", "binding"}
    assert set(page["references"][0]) == {"id", "binding", "namespace", "key"}
    assert codec().validate_page(page) == page


@pytest.mark.parametrize(
    "change",
    [
        "extra",
        "binding",
        "source",
        "cursor",
        "duplicate",
        "order",
        "too-many",
        "namespace",
        "oversized",
        "text",
        "revision",
    ],
)
def test_recovery_page_rejects_tampering_before_control_io(change):
    page = codec().prepare_page("a" * 64, "", [reference(), reference(2)])
    bad = copy.deepcopy(page)
    if change == "extra":
        bad["text"] = "original body"
    elif change == "binding":
        bad["binding"] = "c" * 64
    elif change == "source":
        bad["source_key"] = True
    elif change == "cursor":
        bad["after"] = "bad"
    elif change == "duplicate":
        bad["references"][1] = bad["references"][0]
    elif change == "order":
        bad["references"].reverse()
    elif change == "too-many":
        bad["references"].append(reference(3))
    elif change == "namespace":
        bad["references"][0]["namespace"] = "operator"
    elif change == "oversized":
        bad["references"][0]["key"] = "Ω" * 129
    elif change == "revision":
        bad["revision"] = True
    elif change == "text":
        bad["references"][0]["text"] = "original body"
    if change != "binding":
        core = {key: value for key, value in bad.items() if key != "binding"}
        bad["binding"] = hashlib.sha256(
            json.dumps(core, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    with pytest.raises(ValueError):
        codec().validate_page(bad)


def test_empty_recovery_page_has_explicit_empty_selection():
    page = codec().prepare_page("a" * 64, "c" * 64, [])
    assert page["references"] == [] and page["after"] == "c" * 64
    assert codec().validate_page(page) == page

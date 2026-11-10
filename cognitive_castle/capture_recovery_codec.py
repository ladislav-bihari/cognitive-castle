"""Pure body-free recovery references; page hashes are commitments, not grants."""

import re
from .canonical_records import _hash, _json, _string

FORMAT = "capture-recovery-page-v1"


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("recovery hash")
    return value


def prepare_page(source_key, after, references):
    _sha(source_key)
    if after != "":
        _sha(after)
    if not isinstance(after, str) or not isinstance(references, list) or len(references) > 2:
        raise ValueError("recovery window")
    copied = []
    for reference in references:
        if not isinstance(reference, dict) or reference.keys() != {
            "id",
            "binding",
            "namespace",
            "key",
        }:
            raise ValueError("recovery reference")
        _sha(reference["id"])
        _sha(reference["binding"])
        if reference["namespace"] != "transcript":
            raise ValueError("recovery namespace")
        _string(reference["key"], 128, "recovery request")
        if copied and copied[-1]["id"] >= reference["id"]:
            raise ValueError("recovery order")
        copied.append(dict(reference))
    core = dict(format=FORMAT, source_key=source_key, after=after, references=copied)
    return {**core, "binding": _hash(_json(core))}


def validate_page(value):
    if not isinstance(value, dict) or value.keys() != {
        "format",
        "source_key",
        "after",
        "references",
        "binding",
    }:
        raise ValueError("recovery page")
    expected = prepare_page(value["source_key"], value["after"], value["references"])
    if value != expected:
        raise ValueError("recovery page binding")
    return expected

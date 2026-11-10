"""Separate qualification of the actual installed production platform.

Explicit frozen-source grant is required. This does not qualify the macOS15
baseline; it executes the identical native assertions on the production tuple.
"""

import os
import platform
import re
import sys

import pytest

from test_lance_live_native import (
    _case_lost_append_response_resumes_exact_pending_batch,
    _case_publication_reopens_and_refuses_changed_request,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin"
    or platform.machine() != "arm64"
    or platform.mac_ver()[0] != "26.6.2"
    or platform.python_version() != "3.12.11"
    or re.fullmatch(
        r"[a-f0-9]{40}", os.environ.get("CASTLE_DIRECT_NATIVE_QUALIFICATION_SOURCE", "")
    )
    is None,
    reason="requires frozen production qualification grant and exact Mac26.6.2/arm64/Python3.12.11 tuple",
)


def test_production_direct_native_reopens_and_preserves_originals(tmp_path):
    _case_publication_reopens_and_refuses_changed_request(tmp_path)


def test_production_direct_native_recovers_lost_append_response(tmp_path):
    _case_lost_append_response_resumes_exact_pending_batch(tmp_path)

"""Inactive control client borrowing one caller-owned SemanticSession.

No lease, database, background job or root is created here. Trusted operator
state supplies authority; it does not grant disclosure of registered sources.
"""

from pathlib import Path
import sys
from .capture_control_codec import READS, validate_operation
from .capture_state_codec import validate_authority


class CaptureControlClient:
    def __init__(self, session, authority, *, expected_hash):
        self.session = session
        self.authority = validate_authority(authority, expected_hash=expected_hash)
        self.authority_hash = expected_hash

    def call(self, operation, arguments):
        refusal = self.session._ready()
        if refusal is not None:
            return refusal
        arguments = validate_operation(operation, arguments)
        result = self.session._run_stage(
            dict(
                authority=self.authority,
                authority_hash=self.authority_hash,
                operation=operation,
                arguments=arguments,
            ),
            worker_command=[
                sys.executable,
                "-I",
                str(Path(__file__).with_name("capture_control_worker.py")),
            ],
            cap=2 if operation in READS else 3,
            rss=128 * 1024**2,
            total=192 * 1024**2,
        )
        # Prepared data must also respect the shared deadline; it cannot escape
        # merely because the generic stage only recognizes successful receipts.
        refusal = self.session._ready()
        return refusal if refusal is not None else result

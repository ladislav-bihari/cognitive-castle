"""Fixed offline workers must not discover or execute Pydantic entry points."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("worker", ["exact", "queue", "vector"])
def test_offline_worker_never_enumerates_third_party_validator_plugins(worker):
    script = r"""
import importlib.util,importlib.metadata,runpy,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
if sys.argv[2]=='exact':
 from cognitive_castle.lance_exact_worker import _install_guards
 _install_guards()
else:
 from cognitive_castle.semantic_store_worker import _install_guards
 _install_guards(sys.argv[2])
def forbidden():
 raise AssertionError('offline worker attempted third-party plugin discovery')
importlib.metadata.distributions=forbidden
spec=importlib.util.find_spec('pydantic')
assert spec is not None
# Execute only the official pure loader file, without importing Pydantic/Core.
loader=runpy.run_path(str(Path(spec.origin).parent/'plugin/_loader.py'))
assert tuple(loader['get_plugins']())==()
assert not {'pydantic_core','lance','pyarrow','numpy','torch'}.intersection(sys.modules)
print('plugins-not-discovered')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(Path(__file__).resolve().parents[1]), worker],
        env=dict(os.environ, PYDANTIC_DISABLE_PLUGINS="host-custom-plugin"),
        capture_output=True,
        text=True,
        timeout=3,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "plugins-not-discovered"

"""Metadata-only selection fixtures do not qualify a DB, model or activation."""

from pathlib import Path
import subprocess
import sys
import pytest
from cognitive_castle.canonical_records import _hash, _json
from test_partitioned_exact_store import qualification_case


def api():
    from cognitive_castle import memory_selection

    return memory_selection


def identity(path):
    info = path.stat()
    return dict(device=info.st_dev, inode=info.st_ino)


def write(path, value):
    if path.exists():
        path.chmod(0o600)  # Explicit operator update of disposable fixture metadata.
    path.write_text(_json(value))
    path.chmod(0o400)


def select(marker, value):
    write(marker, value)
    write(
        marker.with_name("canonical-backend.json"),
        dict(
            format="castle-canonical-backend-v1",
            backend="lance",
            selection_hash=_hash(_json(value)),
        ),
    )


def fixture(tmp_path):
    palace = (tmp_path / "palace").resolve()
    palace.mkdir(mode=0o700)
    memory = palace / ".memory"
    memory.mkdir(mode=0o700)
    generation = tmp_path / "generation"
    generation.mkdir(mode=0o700)
    (generation / "exact").mkdir(mode=0o700)
    _, _, qualification, envelope = qualification_case()
    envelope["generation"] = dict(path=str(generation), **identity(generation))
    qualification["generation"] = envelope["generation"]
    envelope["qualification_hash"] = _hash(_json(qualification))
    write(generation / "exact/layout-qualification.json", qualification)
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    database = control / "control.sqlite3"
    database.write_bytes(b"")  # Physical identity fixture, deliberately no SQL schema.
    database.chmod(0o600)
    authority = dict(
        format="castle-capture-control-v1",
        directory=dict(path=str(control), **identity(control)),
        database=identity(database),
        max_bytes=3 * 1024**2,
    )
    write(control / "control-authority.json", authority)
    profile = tmp_path / "profile.json"
    write(profile, dict(fixture_only=True))
    value = dict(
        format="castle-canonical-selection-v1",
        backend="lance",
        envelope=envelope,
        control_authority=authority,
        control_hash=_hash(_json(authority)),
        profile=dict(path=str(profile), hash=_hash(profile.read_text())),
        wing="projects",
        room="conversations",
    )
    marker = memory / "canonical-selection.json"
    select(marker, value)
    return palace, marker, value


def test_absent_selection_does_not_create_storage(tmp_path):
    palace = tmp_path / "palace"
    palace.mkdir(mode=0o700)
    assert api().load_selection(palace) is None
    assert list(palace.iterdir()) == []


def test_declared_lance_backend_with_missing_selection_cannot_fall_back(tmp_path):
    palace, marker, value = fixture(tmp_path)
    write(
        marker.with_name("canonical-backend.json"),
        dict(
            format="castle-canonical-backend-v1",
            backend="lance",
            selection_hash=_hash(_json(value)),
        ),
    )
    marker.unlink()
    with pytest.raises(api().SelectionUnavailable):
        api().load_selection(palace)


@pytest.mark.parametrize("kind", ["absent", "file"])
def test_invalid_palace_cannot_be_reported_as_unconfigured_storage(tmp_path, kind):
    palace = tmp_path / "invalid-palace"
    if kind == "file":
        palace.write_text("not a palace")
    with pytest.raises(api().SelectionUnavailable):
        api().load_selection(palace)


def test_selection_validates_existing_pins_without_opening_engines(tmp_path):
    palace, _, value = fixture(tmp_path)
    selected = api().load_selection(palace)
    assert selected == value
    root = Path(__file__).resolve().parents[1]
    code = f"import sys;sys.path.insert(0,{str(root)!r});from cognitive_castle.memory_selection import load_selection;assert load_selection({str(palace)!r})['backend']=='lance';assert not {{'sqlite3','lance','lancedb','pyarrow','numpy','torch'}}.intersection(sys.modules)"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=3
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "change", ["backend", "extra", "control_hash", "profile_hash", "generation"]
)
def test_invalid_selected_authority_never_looks_like_absent_config(tmp_path, change):
    palace, marker, value = fixture(tmp_path)
    if change == "backend":
        value["backend"] = "sqlite"
    elif change == "extra":
        value["allow_local_sources"] = True
    elif change == "control_hash":
        value["control_hash"] = "f" * 64
    elif change == "profile_hash":
        value["profile"]["hash"] = "f" * 64
    else:
        value["envelope"]["generation"]["inode"] += 1
    marker.chmod(0o600)
    select(marker, value)
    with pytest.raises(api().SelectionUnavailable):
        api().load_selection(palace)


def test_replaced_control_database_refuses_even_with_same_contents(tmp_path):
    palace, _, value = fixture(tmp_path)
    database = Path(value["control_authority"]["directory"]["path"]) / "control.sqlite3"
    database.rename(database.with_name("original-preserved"))
    database.write_bytes(b"")
    database.chmod(0o600)
    with pytest.raises(api().SelectionUnavailable):
        api().load_selection(palace)


@pytest.mark.parametrize("change", ["symlink", "oversize", "permissions"])
def test_unsafe_marker_is_refused_without_writes_or_fallback(tmp_path, change):
    palace, marker, _ = fixture(tmp_path)
    if change == "symlink":
        target = marker.with_name("original-preserved")
        marker.rename(target)
        marker.symlink_to(target)
    else:
        marker.chmod(0o600)
        if change == "oversize":
            marker.write_bytes(b" " * (16384 + 1))
            marker.chmod(0o400)
    before = sorted(p.name for p in marker.parent.iterdir())
    with pytest.raises(api().SelectionUnavailable):
        api().load_selection(palace)
    assert sorted(p.name for p in marker.parent.iterdir()) == before

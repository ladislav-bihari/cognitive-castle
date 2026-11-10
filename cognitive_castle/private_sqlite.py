"""Shared private SQLite connection shell; the caller owns its explicit schema.

Extracted from MemoryStore without changing its privacy/resource/transaction
checks. This module selects neither a backend nor an ordinary memory route.
"""

from contextlib import contextmanager
import os
import sqlite3
import stat
import time


def private_path(path, directory=False):
    from .fast_memory import _Unavailable

    info = path.lstat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise _Unavailable("unsafe permissions or file type")
    if not directory and info.st_nlink != 1:
        raise _Unavailable("hard-linked storage")


@contextmanager
def private_connection(store, write=False):
    from .fast_memory import _Unavailable, _Missing, _Quota

    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "getuid"):
        raise _Unavailable("privacy primitives unavailable")
    # Do not resolve paths: every existing ancestor must reject symlinks.
    for path in reversed([store.directory, *store.directory.parents]):
        if path.is_symlink():
            raise _Unavailable("symlink ancestor")
    # Historical admission must precede creation and SQLite hot-journal
    # recovery. Read-only connections never reserve or expand capacity.
    budget = store._write_budget() if write else None
    if not store.directory.exists():
        if not write:
            raise _Missing("not initialized")
        store.directory.mkdir(mode=0o700)
    store._private(store.directory, True)
    for name in [
        store.db_path.name,
        store.db_path.name + "-journal",
        store.db_path.name + "-wal",
        store.db_path.name + "-shm",
    ]:
        path = store.directory / name
        if path.exists() or path.is_symlink():
            store._private(path)
    if write and sum(p.stat().st_size for p in store.directory.iterdir()) >= budget:
        raise _Quota()
    if not store.db_path.exists():
        if not write:
            raise _Missing("not initialized")
        fd = os.open(store.db_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(fd)
    conn = sqlite3.connect(
        store.db_path.as_uri() + ("?mode=rw" if write else "?mode=ro"), uri=True, timeout=0.05
    )
    conn.row_factory = sqlite3.Row
    deadline = time.monotonic() + (3 if write else 2)
    conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    try:
        conn.execute("PRAGMA cache_size=-4096")
        conn.execute("PRAGMA mmap_size=0")
        conn.execute("PRAGMA temp_store=FILE")
        if write:
            if conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0].lower() != "delete":
                raise _Unavailable("rollback journal unavailable")
            # Reserve room for rollback journal and disk-backed temporary work.
            # This deliberately admits less data than the total sidecar budget.
            page_size = conn.execute("PRAGMA page_size").fetchone()[0]
            max_pages = max(1, budget // (3 * page_size))
            if conn.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0] > max_pages:
                raise _Quota()
            conn.execute("BEGIN IMMEDIATE")
            store._initialize_schema(conn)
        yield conn
        if write:
            conn.commit()
    finally:
        conn.close()

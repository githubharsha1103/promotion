"""Cross-process file locks for local storage operations."""

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from filelock import FileLock, Timeout


@contextmanager
def locked(path: Path, timeout: float = 10.0) -> Iterator[None]:
    """Lock a sibling lock file while operating on ``path``."""

    lock_path = path.with_name(f".{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(lock_path), timeout=timeout):
            yield
    except Timeout as exc:
        raise TimeoutError(f"Timed out waiting for storage lock: {path}") from exc

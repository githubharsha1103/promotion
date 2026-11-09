"""Safe helpers for managing physical files in configured storage paths."""

import shutil
import uuid
from pathlib import Path


class FileManager:
    """Manage media and other physical files below an allowed root."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _resolve(self, relative_path: str | Path) -> Path:
        target = (self.root / relative_path).resolve()
        if target != self.root and self.root not in target.parents:
            raise ValueError("Path must remain inside the configured storage root")
        return target

    def save(self, source: str | Path, relative_path: str | Path | None = None) -> Path:
        source_path = Path(source)
        if not source_path.is_file():
            raise FileNotFoundError(source_path)
        destination = self._resolve(relative_path or f"{uuid.uuid4().hex}{source_path.suffix}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)
        return destination

    def path(self, relative_path: str | Path) -> Path:
        return self._resolve(relative_path)

    def delete(self, relative_path: str | Path, missing_ok: bool = True) -> bool:
        target = self._resolve(relative_path)
        try:
            target.unlink()
            return True
        except FileNotFoundError:
            if missing_ok:
                return False
            raise

    def exists(self, relative_path: str | Path) -> bool:
        return self._resolve(relative_path).is_file()

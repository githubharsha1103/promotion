"""Create and restore local filesystem backups."""

from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import tempfile
import zipfile


class BackupError(RuntimeError):
    """Raised when a backup cannot be created or restored safely."""


class BackupManager:
    """Archive configured local data directories without using a database."""

    def __init__(self, sources: dict[str, str | Path], backup_dir: str | Path):
        self.sources = {name: Path(path).resolve() for name, path in sources.items()}
        self.backup_dir = Path(backup_dir).resolve()

    def create(self, name: str | None = None) -> Path:
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive_name = name or f"backup-{timestamp}.zip"
        if not archive_name.endswith(".zip") or Path(archive_name).name != archive_name:
            raise ValueError("Backup name must be a simple .zip filename")
        destination = self.backup_dir / archive_name
        if destination.exists():
            raise FileExistsError(destination)
        fd, temporary_name = tempfile.mkstemp(prefix=".backup-", suffix=".tmp", dir=self.backup_dir)
        os.close(fd)
        try:
            with zipfile.ZipFile(temporary_name, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for label, source in self.sources.items():
                    if not source.exists():
                        continue
                    for file_path in source.rglob("*"):
                        if file_path.is_file():
                            archive.write(file_path, Path(label) / file_path.relative_to(source))
            Path(temporary_name).replace(destination)
        except (OSError, zipfile.BadZipFile) as exc:
            Path(temporary_name).unlink(missing_ok=True)
            raise BackupError(f"Could not create backup: {exc}") from exc
        return destination

    def restore(self, archive_path: str | Path, overwrite: bool = False) -> None:
        archive_path = Path(archive_path)
        try:
            with zipfile.ZipFile(archive_path, "r") as archive:
                members = archive.infolist()
                destinations: list[tuple[zipfile.ZipInfo, Path]] = []
                for member in members:
                    parts = Path(member.filename).parts
                    if not parts or parts[0] not in self.sources or Path(member.filename).is_absolute() or ".." in parts:
                        raise BackupError(f"Unsafe or unknown backup path: {member.filename}")
                    target = self.sources[parts[0]].joinpath(*parts[1:]).resolve()
                    root = self.sources[parts[0]]
                    if target != root and root not in target.parents:
                        raise BackupError(f"Backup path escapes destination: {member.filename}")
                    if member.is_dir():
                        continue
                    if target.exists() and not overwrite:
                        raise FileExistsError(target)
                    destinations.append((member, target))
                # Validate all paths and conflicts before writing any files.
                for member, target in destinations:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member, "r") as source, target.open("wb") as destination:
                        shutil.copyfileobj(source, destination)
        except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
            if isinstance(exc, BackupError):
                raise
            raise BackupError(f"Could not restore backup: {exc}") from exc

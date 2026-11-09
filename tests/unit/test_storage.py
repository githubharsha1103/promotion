import json
import multiprocessing
from pathlib import Path

import pytest

from app.storage.backup_manager import BackupManager
from app.storage.file_manager import FileManager
from app.storage.json_store import JsonStore, JsonStoreError, RecordNotFoundError
from app.storage.repositories import AccountRepository, BotRepository, CampaignRepository


def _create_record(path: str, index: int) -> None:
    JsonStore(path).create({"index": index})


def test_json_crud_and_restart_persistence(tmp_path: Path) -> None:
    store_path = tmp_path / "records" / "items.json"
    store = JsonStore(store_path)
    created = store.create({"name": "first"})
    assert len(created["id"]) == 32
    assert store.get(created["id"]) == created
    updated = store.update(created["id"], {"name": "updated"})
    assert updated["name"] == "updated"
    assert store.list() == [updated]
    assert JsonStore(store_path).get(created["id"]) == updated
    assert store.delete(created["id"]) is True
    assert store.delete(created["id"]) is False
    with pytest.raises(RecordNotFoundError):
        store.update(created["id"], {"name": "missing"})


def test_concurrent_writes_are_not_lost(tmp_path: Path) -> None:
    store_path = str(tmp_path / "items.json")
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=_create_record, args=(store_path, i)) for i in range(12)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    assert len(JsonStore(store_path).list()) == len(processes)


def test_atomic_write_leaves_valid_json_and_no_temp_files(tmp_path: Path) -> None:
    path = tmp_path / "records.json"
    store = JsonStore(path)
    store.create({"valid": True})
    assert json.loads(path.read_text(encoding="utf-8"))
    assert list(tmp_path.glob("*.tmp")) == []
    with pytest.raises(JsonStoreError):
        store.create({"not_json": object()})
    assert json.loads(path.read_text(encoding="utf-8"))


def test_backup_creation_and_restore(tmp_path: Path) -> None:
    records = tmp_path / "records"
    sessions = tmp_path / "sessions"
    backup_dir = tmp_path / "backups"
    JsonStore(records / "accounts.json").create({"phone": "masked"})
    sessions.mkdir(parents=True)
    (sessions / "account.session").write_bytes(b"session bytes")
    manager = BackupManager({"records": records, "sessions": sessions}, backup_dir)
    archive = manager.create("test-backup.zip")
    (records / "accounts.json").unlink()
    (sessions / "account.session").unlink()
    manager.restore(archive)
    assert JsonStore(records / "accounts.json").list()[0]["phone"] == "masked"
    assert (sessions / "account.session").read_bytes() == b"session bytes"


def test_file_manager_rejects_path_escape_and_manages_files(tmp_path: Path) -> None:
    root = tmp_path / "media"
    manager = FileManager(root)
    source = tmp_path / "input.bin"
    source.write_bytes(b"media")
    saved = manager.save(source, "images/example.bin")
    assert saved.read_bytes() == b"media"
    assert manager.exists("images/example.bin")
    assert manager.delete("images/example.bin")
    with pytest.raises(ValueError):
        manager.path("../outside")


def test_category_repositories_use_separate_json_files(tmp_path: Path) -> None:
    accounts = AccountRepository(tmp_path)
    campaigns = CampaignRepository(tmp_path)
    bots = BotRepository(tmp_path)
    accounts.create({"label": "account"})
    campaigns.create({"name": "campaign"})
    campaigns.tasks.create({"name": "task"})
    bots.create({"name": "bot session"})
    bots.configurations.create({"name": "bot config"})
    assert {path.name for path in tmp_path.glob("*.json")} == {
        "accounts.json", "campaigns.json", "campaign_tasks.json",
        "bot_sessions.json", "bot_configurations.json",
    }

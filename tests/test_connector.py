from typing import Any
import json
import asyncio
import pytest
from prs_connector_core.connector import BaseConnector

class TestConnector(BaseConnector):
    async def _connect_to_source(self) -> None:
        pass

    async def _read_tags(self) -> dict[str, Any]:
        """Абстрактный метод для чтения тегов из источника"""
        return {}

def test_connector_no_config_file():
    with pytest.raises(RuntimeError):
        TestConnector('nonexistent_config')

@pytest.mark.asyncio
async def test_connector_config_file(tmp_path):
    # Создаем временный конфигурационный файл
    config_file = tmp_path / "config.json"
    config_content = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "url": "mqtt://localhost"
    }
    config_file.write_text(json.dumps(config_content))

    # Создаем коннектор с указанием пути к временному файлу
    conn = TestConnector(str(config_file))

    assert str(conn._config_from_file.id) == "550e8400-e29b-41d4-a716-446655440000"
    assert conn._config_from_file.url == "mqtt://localhost"

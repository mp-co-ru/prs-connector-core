from typing import Any
import pytest
from unittest.mock import MagicMock
from prs_connector_core.connector import BaseConnector

class TestConnector(BaseConnector):
    async def _connect_to_source(self) -> None:
        pass

    async def _read_tags(self, tags: list[str]) -> dict[str, Any]:
        """Абстрактный метод для чтения тегов из источника"""
        return {}

def test_connector_no_config_file():
    with pytest.raises(RuntimeError):
        TestConnector('nonexistent_config')

def test_connector_invalid_config_file(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value=
        '{"id": "550e8400-e29b-41d4-a716-446655440000", "url": "mqtt://localhost"}'
    )

    conn = TestConnector()
    assert str(conn._config_from_file.id) == "550e8400-e29b-41d4-a716-446655440000"
    assert conn._config_from_file.url == "mqtt://localhost"

@pytest.mark.asyncio
async def test_save_queue_to_disk(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value=
        '{"id": "550e8400-e29b-41d4-a716-446655440000", "url": "mqtt://localhost"}'
    )
    mock_loop = MagicMock()

    mocker.patch("asyncio.get_event_loop", return_value=mock_loop)
    conn = TestConnector()
    save_data = {"test": "data"}
    conn._data_queue.put_nowait(save_data)
    await conn._shutdown()
    assert conn._data_queue.empty()  # Проверяем, что очередь пуста после завершения работы
    del conn

    conn = TestConnector()
    await conn._load_queue_from_disk()
    assert not conn._data_queue.empty()
    load_data = conn._data_queue.get_nowait()
    assert load_data == save_data

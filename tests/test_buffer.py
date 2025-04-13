import pytest
import json
from uuid import UUID
from prs_connector_core.buffer import BufferManager

@pytest.fixture
def buffer():
    return BufferManager(UUID("550e8400-e29b-41d4-a716-446655440000"))

@pytest.mark.asyncio
async def test_buffer_operations(buffer):
    test_data = {"tagId": "test", "data": 42}

    # Тест сохранения
    await buffer.save(test_data)

    # Тест загрузки
    loaded = await buffer.load()
    assert loaded[0] == test_data

    # Тест очистки
    await buffer.clear()
    assert await buffer.load() == []
import pytest
from prs_connector_core.buffer import BufferManager
from uuid import UUID

@pytest.mark.asyncio
async def test_batch_buffer_operations():
    buffer = BufferManager(UUID("550e8400-e29b-41d4-a716-446655440000"))
    test_packet = {
        "data": [
            {
                "tagId": "test1",
                "data": [{"x": 123, "y": 42, "q": None}]
            }
        ]
    }

    # Test save
    await buffer.save(test_packet)

    # Test load
    loaded = await buffer.load()
    assert loaded == [test_packet]

    # Test clear
    await buffer.clear()
    assert await buffer.load() == []
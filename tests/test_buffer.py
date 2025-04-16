import pytest
from unittest.mock import AsyncMock
from prs_connector_core.buffer import BufferManager
from uuid import UUID
from pathlib import Path

@pytest.mark.asyncio
async def test_load_with_nonexistent_file(mocker):
    # Mock Path.exists to return False
    mocker.patch("pathlib.Path.exists", return_value=False)

    # Initialize BufferManager
    con_id = "550e8400-e29b-41d4-a716-446655440000"
    buffer = BufferManager(UUID(con_id))

    # Call the load method
    result = await buffer.load()

    # Assert that the result is an empty list
    assert result == []

""",

        """
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_file_content, expected_result",
    [
        (
            [
                '{"data": [{"tagId": "test1", "data": [{"x": 123, "y": 42}]}]}\n',
                '{"data": [{"tagId": "test2", "data": [{"x": 456, "y": 78}]}]}\n',
            ],
            {
                "data": [
                    {"tagId": "test1", "data": [{"x": 123, "y": 42}]},
                    {"tagId": "test2", "data": [{"x": 456, "y": 78}]},
                ]
            }
        ),
        (
            [
                '{"data": [{"tagId": "test3", "data": [{"x": 789, "y": 101}]}]}\n',
            ],
            {
                "data": [
                    {"tagId": "test3", "data": [{"x": 789, "y": 101}]},
                ]
            },
        ),
        (
            [],
            {"data": []},
        ),
        (
            [
                '{"data": [{"tagId": "test1", "data": [{"x": 123, "y": 42}]}]}\n',
                '{"data": [{"tagId": "test1", "data": [{"x": 456, "y": 78}]}]}\n',
            ],
            {
                "data": [
                    {
                        "tagId": "test1",
                        "data": [
                            {"x": 123, "y": 42}, {"x": 456, "y": 78}
                        ]
                    }
                ]
            },
        )
    ]
)
async def test_load_with_existing_file(mocker, mock_file_content, expected_result):
    # Mock Path.exists to return True
    mocker.patch("pathlib.Path.exists", return_value=True)

    # Mock aiofiles.open with AsyncMock
    mock_file = AsyncMock()
    mock_file.__aenter__.return_value = mock_file
    mock_file.__aiter__.return_value = iter(mock_file_content)
    mocker.patch("aiofiles.open", return_value=mock_file)

    # Mock the clear method
    mocker.patch.object(BufferManager, "clear", return_value=None)

    # Initialize BufferManager
    con_id = "550e8400-e29b-41d4-a716-446655440000"
    buffer = BufferManager(UUID(con_id))

    # Call the load method
    result = await buffer.load()

    # Assert the result
    assert result == expected_result
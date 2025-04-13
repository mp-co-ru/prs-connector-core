import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from prs_connector_core.websocket_client import WebSocketClient
from prs_connector_core.config import ConnectorConfig, SSLConfig

@pytest.mark.asyncio
async def test_batch_data_sending():
    config = ConnectorConfig(
        id="550e8400-e29b-41d4-a716-446655440000",
        url="ws://localhost",
        ssl=None
    )

    client = WebSocketClient(config)
    test_data = {
        "data": [
            {"tagId": "test1", "data": [{"x": 123, "y": 42}]},
            {"tagId": "test2", "data": [{"x": 124, "y": 43}]}
        ]
    }

    with patch("websockets.connect") as mock_connect:
        mock_ws = AsyncMock()
        mock_connect.return_value = mock_ws

        await client.connect()
        await client.send_data(test_data)

        mock_ws.send.assert_awaited_once_with(json.dumps(test_data))
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from prs_connector_core.exceptions import ConnectionError, ConfigValidationError
from prs_connector_core.connector import BaseConnector
from prs_connector_core.config import ConnectorConfig

class TestConnector(BaseConnector):
    async def read_tag(self, tag_config):
        return 42

@pytest.fixture
def connector():
    return TestConnector(config_path="tests/test_config.json")

@pytest.mark.asyncio
async def test_connect_success(connector):
    with patch("prs_connector_core.websocket_client.WebSocketClient.connect") as mock_connect:
        await connector.connect_to_platform()
        mock_connect.assert_awaited_once()

@pytest.mark.asyncio
async def test_config_loading_error():
    with pytest.raises(ConfigValidationError):
        connector = TestConnector(config_path="invalid.json")
        await connector.connect_to_platform()

@pytest.mark.asyncio
async def test_tag_processing(connector):
    connector.platform_config = {
        "tags": [
            {
                "tagId": "tag1",
                "prsJsonConfigString": {"frequency": 1000},
                "attributes": {"prsMaxLineDev": 0}
            }
        ]
    }

    with patch.object(connector, 'send_to_platform') as mock_send:
        await connector._process_tag_group([connector.platform_config['tags'][0]])
        mock_send.assert_awaited()

@pytest.mark.asyncio
async def test_cached_config_usage():
    connector = TestConnector()

    with patch.object(connector.ws_client, 'connect', side_effect=ConnectionError()), \
         patch("pathlib.Path.exists", return_value=True), \
         patch("builtins.open", mock_open(read_data='{"tags": []}')) as mock_file:

        await connector.connect_to_platform()
        assert len(connector.platform_config.get('tags', [])) == 0
        mock_file.assert_called()

@pytest.mark.asyncio
async def test_acknowledgement_sending():
    connector = TestConnector()
    connector.platform_config = {"tags": []}

    with patch.object(connector.ws_client, 'send_data') as mock_send:
        await connector._send_acknowledgement()
        mock_send.assert_awaited_once_with({
            "action": "config_ack",
            "connector_id": str(connector.config.id),
            "status": "success",
            "message": "Configuration applied"
        })

@pytest.mark.asyncio
async def test_tag_group_processing():
    connector = TestConnector()
    connector.platform_config = {
        "tags": [
            {
                "tagId": "test1",
                "prsJsonConfigString": {"frequency": 1000},
                "attributes": {
                    "prsJSONata": "value * 2",
                    "prsMaxLineDev": 1
                }
            }
        ]
    }

    with patch.object(connector, 'read_tag', return_value=5), \
         patch.object(connector.ws_client, 'send_data') as mock_send:

        await connector._process_tag_group(connector.platform_config['tags'])

        # Проверка преобразования JSONata
        mock_send.assert_awaited_with({
            "tagId": "test1",
            "data": [{
                "x": pytest.approx(int(time.time()*1e6), rel=0.1),
                "y": 10,
                "q": None
            }]
        })
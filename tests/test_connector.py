import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from prs_connector_core.connector import BaseConnector
from prs_connector_core.config import ConnectorConfig

class TestConnector(BaseConnector):
    async def read_tag(self, tag_config):
        return 42

@pytest.fixture
def connector():
    return TestConnector(config_path="tests/test_config.json")

@pytest.mark.asyncio
async def test_group_data_processing(connector):
    connector.platform_config = {
        "tags": [
            {
                "tagId": "test1",
                "prsJsonConfigString": {"frequency": 1000},
                "attributes": {
                    "prsJSONata": "value * 2",
                    "prsMaxLineDev": 1
                }
            },
            {
                "tagId": "test2",
                "prsJsonConfigString": {"frequency": 1000},
                "attributes": {
                    "prsMaxLineDev": 0
                }
            }
        ]
    }

    with patch.object(connector, '_handle_data_delivery') as mock_send:
        await connector._process_group(1000, connector.platform_config['tags'])
        mock_send.assert_awaited_once()

        sent_packet = mock_send.call_args[0][0]
        assert len(sent_packet['data']) == 2
        assert all(tag['tagId'] in ['test1', 'test2'] for tag in sent_packet['data'])

@pytest.mark.asyncio
async def test_buffer_handling(connector):
    test_packet = {
        "data": [
            {
                "tagId": "test1",
                "data": [{"x": 123, "y": 42, "q": None}]
            }
        ]
    }

    with patch.object(connector.ws_client, 'is_connected', return_value=False), \
         patch.object(connector.buffer, 'save') as mock_save:
        await connector._handle_data_delivery(test_packet)
        mock_save.assert_awaited_once_with(test_packet)
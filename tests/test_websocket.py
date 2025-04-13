# tests/test_websocket.py
import pytest
import ssl
from unittest.mock import patch
from prs_connector_core.config import ConnectorConfig, SSLConfig
from prs_connector_core.websocket_client import WebSocketClient

@pytest.fixture
def ws_config():
    return ConnectorConfig(
        id="550e8400-e29b-41d4-a716-446655440000",
        url="ws://localhost",
        ssl=None
    )

@pytest.fixture
def wss_config():
    return ConnectorConfig(
        id="550e8400-e29b-41d4-a716-446655440000",
        url="wss://localhost",
        ssl=SSLConfig(
            certFile="cert.pem",
            keyFile="key.pem",
            certPassword="secret"
        )
    )

def test_ws_client_creation(ws_config):
    client = WebSocketClient(ws_config)
    assert client.ssl_context is None

def test_wss_client_creation(wss_config):
    client = WebSocketClient(wss_config)
    assert isinstance(client.ssl_context, ssl.SSLContext)

@pytest.mark.asyncio
async def test_ws_connection(ws_config):
    client = WebSocketClient(ws_config)
    with patch("websockets.connect") as mock_connect:
        await client.connect()
        mock_connect.assert_awaited_with(
            "ws://localhost/550e8400-e29b-41d4-a716-446655440000",
            ssl=None
        )
import pytest
from uuid import UUID
from prs_connector_core.config import ConnectorConfig, SSLConfig
from prs_connector_core.exceptions import ConfigValidationError

def test_valid_config():
    config = ConnectorConfig(
        id="550e8400-e29b-41d4-a716-446655440000",
        url="ws://localhost",
        ssl=None
    )
    assert isinstance(config.id, UUID)
    assert config.url == "ws://localhost"

def test_invalid_uuid():
    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id="invalid", url="ws://localhost")

def test_invalid_protocol():
    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id="550e8400-e29b-41d4-a716-446655440000", url="http://localhost")

def test_missing_ssl_for_wss():
    with pytest.raises(ConfigValidationError):
        ConnectorConfig(
            id="550e8400-e29b-41d4-a716-446655440000",
            url="wss://localhost"
        )

def test_ssl_config():
    ssl = SSLConfig(
        certFile="cert.pem",
        keyFile="key.pem",
        certPassword="secret"
    )
    config = ConnectorConfig(
        id="550e8400-e29b-41d4-a716-446655440000",
        url="wss://localhost",
        ssl=ssl
    )
    assert config.ssl.certFile == "cert.pem"
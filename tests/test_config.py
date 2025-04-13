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

def test_config_update_handling(connector):
    old_config = {"tags": [{"tagId": "test1", "attributes": {"prsJSONata": "value"}}]}
    new_config = {"tags": [{"tagId": "test1", "attributes": {"prsJSONata": "value*2"}}]}

    connector.platform_config = old_config
    connector._handle_config_update(new_config)

    assert connector.platform_config == new_config
    assert connector.data_handler.compiled_expressions["test1"] is not None
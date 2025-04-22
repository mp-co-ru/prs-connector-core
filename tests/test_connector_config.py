import pytest
from prs_connector_core.config import ConnectorConfig
from prs_connector_core.exceptions import ConfigValidationError
import uuid

def test_connector_config_id():
    id = "550e8400-e29b-41d4-a716-446655440000"
    config = ConnectorConfig(
        id=id,
        url="mqtt://localhost"
    )
    assert str(config.id) == id

    id = "550e8400-e29b-41d4-a716-44665544000"
    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id=id, url="mqtt://localhost")

def test_connector_config_protocol():
    config = ConnectorConfig(
        id=uuid.uuid4(),
        url="mqtt://localhost"
    )
    assert config.url.startswith("mqtt://")

    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id=uuid.uuid4(), url="http://localhost")

def test_connector_config_server():
    config = ConnectorConfig(
        id=uuid.uuid4(),
        url="mqtt://localhost"
    )
    assert config.url

    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id=uuid.uuid4(), url="mqtt://")

def test_connector_ssl():
    pass

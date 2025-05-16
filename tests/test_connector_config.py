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
        id=str(uuid.uuid4()),
        url="mqtt://localhost"
    )
    assert config.url.startswith("mqtt://")

    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id=str(uuid.uuid4()), url="http://localhost")

def test_connector_config_server():
    config = ConnectorConfig(
        id=str(uuid.uuid4()),
        url="mqtt://localhost"
    )
    assert config.url

    with pytest.raises(ConfigValidationError):
        ConnectorConfig(id=str(uuid.uuid4()), url="mqtt://")

def test_connector_config_from_file(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value=
        '{"id": "550e8400-e29b-41d4-a716-446655440000", "url": "mqtt://localhost"}'
    )

    conf = ConnectorConfig.from_file('config.json')
    assert str(conf.id) == "550e8400-e29b-41d4-a716-446655440000"
    assert conf.url == "mqtt://localhost"

    mocker.stopall()
    with pytest.raises(ConfigValidationError):
        ConnectorConfig.from_file('nonexistent_file')

def test_connector_config_ssl(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value=
        (
            '{"id": "550e8400-e29b-41d4-a716-446655440000", '
            '"url": "mqtts://localhost", '
            '"ssl": {"certFile": "cert.pem", "keyFile": "key.pem"} }'
        )
    )
    config = ConnectorConfig.from_file('config.json')
    assert config.ssl is not None
    assert config.ssl.certFile == "cert.pem"
    assert config.ssl.keyFile == "key.pem"

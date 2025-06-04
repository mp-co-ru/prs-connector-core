import pytest
import json
from pathlib import Path
from prs_connector_core.config import ConnectorConfig
from prs_connector_core.exceptions import ConfigValidationError
import uuid

def test_connector_config_ssl(tmp_path, monkeypatch):

    config_file = tmp_path / "config.json"
    config_content = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "url": "mqtts://localhost",
        "ssl": {"certFile": "cert.pem", "keyFile": "key.pem", "certsRequired": "CERTS_NONE"}
    }

    config_file.write_text(json.dumps(config_content))

    def mock_exists(path):
        # Разрешаем проверку для самого файла конфигурации
        if path == config_file:
            return True

        # Для сертификатов всегда возвращаем True
        return path.name in ("cert.pem", "key.pem")

    monkeypatch.setattr(Path, "exists", mock_exists)


    config = ConnectorConfig.from_file(str(config_file))
    assert config.ssl is not None
    assert config.ssl.certFile == "cert.pem"
    assert config.ssl.keyFile == "key.pem"

def test_connector_config_from_nonexistent_file(mocker):
    # Используем mock_open для эмуляции отсутствия файла
    mock_file = mocker.MagicMock()
    mock_file.exists.return_value = False
    mock_file.read_text.side_effect = FileNotFoundError("File not found")

    # Мокаем только конкретный путь
    mocker.patch("pathlib.Path", return_value=mock_file)

    with pytest.raises(ConfigValidationError):
        ConnectorConfig.from_file('nonexistent_file')

def test_connector_config_from_file(tmp_path, monkeypatch):

    config_file = tmp_path / "config.json"
    config_content = {"id": "550e8400-e29b-41d4-a716-446655440000", "url": "mqtt://localhost"}

    config_file.write_text(json.dumps(config_content))

    config = ConnectorConfig.from_file(str(config_file))


    assert str(config.id) == "550e8400-e29b-41d4-a716-446655440000"
    assert config.url == "mqtt://localhost"

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

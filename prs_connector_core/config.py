from pydantic import BaseModel, field_validator, model_validator, Field, ValidationError
from pathlib import Path
from jsonata import Jsonata
import uuid
import json
from urllib.parse import urlparse
from typing import Any
from typing_extensions import Self
from .exceptions import ConfigValidationError

class SSLConfig(BaseModel):
    certFile: str
    keyFile: str
    caFile: str = ""
    certsRequired: int | str = 'CERTS_NONE'

    @field_validator('certsRequired', mode='before')
    @classmethod
    def validate_id(cls, v: str) -> int:
        match v:
            case 'CERTS_NONE': return 0
            case 'CERTS_OPTIONAL': return 1
            case 'CERTS_REQUIRED': return 2
            case _:
                raise ConfigValidationError(field='certsRequired', details="certRequired должен быть `CERTS_NONE`, `CERTS_OPTIONAL` или `CERTS_REQUIRED`")

class LogConfig(BaseModel):
    level: str = "INFO"
    fileName: str = "logs/prs_connector.log"
    maxBytes: int = 10 * 1024 * 1024  # 10MB
    backupCount: int = 10

class ConnectorPrsJsonConfigStringFromPlatform(BaseModel):
    source: dict = {}
    log: LogConfig = LogConfig()

class TagPrsJsonConfigStringFromPlatform(BaseModel):
    source: dict = {}
    maxDev: float = 0
    JSONata: str | None = None
    frequency: float | None = 5

class ConnectorConfig(BaseModel):
    id: str
    url: str
    ssl: SSLConfig | None = None

    @field_validator('id', mode='before')
    @classmethod
    def validate_id(cls, v: str) -> str:
        try:
            uuid.UUID(str(v))
            return v
        except ValueError as e:
            raise ConfigValidationError(field='id', details="id должен быть в виде GUID")

    @field_validator('url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ('mqtt', 'mqtts'):
            raise ConfigValidationError(
                field='url',
                details="Протокол должен быть mqtt:// или mqtts://"
            )
        if parsed.netloc == '':
            raise ConfigValidationError(
                field='url',
                details="Отсутствует адрес брокера"
            )

        return v

    @model_validator(mode='after')
    def validate_ssl_requirements(self) -> Self:
        if not self.url:
            raise ConfigValidationError(
                field="url",
                details=f"Отсутствие поля 'url'."
            )
        parsed = urlparse(self.url)
        if parsed.scheme == 'mqtts':
            if not self.ssl:
                raise ConfigValidationError(
                    field='ssl',
                    details="SSL конфигурация обязательна для MQTTS"
                )

            for file_type in ('certFile', 'keyFile'):
                if not Path(getattr(self.ssl, file_type)).exists():
                    raise ConfigValidationError(
                        field=f'ssl.{file_type}',
                        details=f"Файл {getattr(self.ssl, file_type)} не найден"
                    )

        return self

    @classmethod
    def from_file(cls, config_file: str) -> Self:
        """Загрузка конфигурации из JSON-файла"""
        try:
            file = Path(config_file)
            if not file.exists():
                raise FileNotFoundError(f"Файл не найден: {config_file}")
            return cls.model_validate_json(file.read_text())

        except FileNotFoundError as e:
            raise ConfigValidationError(
                field='config_file',
                details=f"Файл конфигурации не найден: {config_file}"
            ) from e

        except json.JSONDecodeError as e:
            raise ConfigValidationError(
                field="config_file",
                details="Некорректный JSON формат"
            ) from e

        except ValidationError as e:
            raise ConfigValidationError(
                field="config_file",
                details=f"Ошибка валидации конфигурации: {e}"
            ) from e

class TagAttributes(BaseModel):
    # приходит от платформы
    prsActive: bool = Field(True)
    prsValueTypeCode: int = Field(..., ge=1, le=4)
    prsJsonConfigString: TagPrsJsonConfigStringFromPlatform

class PlatformConfig(BaseModel):
    prsActive: bool = True
    prsEntityTypeCode: int | None = None
    prsJsonConfigString: ConnectorPrsJsonConfigStringFromPlatform = ConnectorPrsJsonConfigStringFromPlatform()
    tags: dict[str, TagAttributes] = {}

    @field_validator('tags', mode='before')
    @classmethod
    def validate_tags_id(cls, v: dict[str, TagAttributes]) -> dict[str, TagAttributes]:

        for k in v.keys():
            try:
                uuid.UUID(str(k))
            except ValueError as e:
                raise ConfigValidationError(field='tags', details=f"id тега должен быть в виде GUID: {k}")

        return v

    @classmethod
    def _form_file_name(cls, connector_id: str) -> str:
        return f"platform_config_{connector_id}.json"

    @classmethod
    def from_file(cls, connector_id: str) -> Self:
        if (config_file := Path(cls._form_file_name(connector_id=connector_id))).exists():
            new_inst = cls.model_validate_json(config_file.read_text())
        else:
            new_inst = cls()
        new_inst.prsJsonConfigString.log.fileName = cls.default_log_file_name(connector_id)
        return new_inst

    @classmethod
    def default_log_file_name(cls, connector_id: str) -> str:
        return f"logs/prs_connector_{connector_id}.log"

    def save(self, connector_id: str) -> None:
        config_file = Path(self._form_file_name(connector_id=connector_id))
        data = self.model_dump_json(indent=2)
        config_file.write_text(data)
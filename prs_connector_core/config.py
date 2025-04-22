from pydantic import BaseModel, UUID4, field_validator, model_validator, Field, ValidationError
from pathlib import Path
import uuid
import json
from urllib.parse import urlparse
from typing_extensions import Self
from .exceptions import ConfigValidationError

class SSLConfig(BaseModel):
    certFile: str
    keyFile: str
    certPassword: str | None = None

class LogConfig(BaseModel):
    level: str = "INFO"
    fileName: str = "logs/connector.log"
    maxBytes: int = 10 * 1024 * 1024  # 10MB
    backupCount: int = 10

class ConfigStringPlatform(BaseModel):
    source: dict
    log: LogConfig

class ConnectorConfig(BaseModel):
    id: UUID4
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
            '''
            with open(config_file, "r") as f:
                config_data = json.load(f)
            return cls.model_validate(config_data)
            '''

            if (file := Path(config_file)).exists():
                return cls.model_validate_json(file.read_text())

        except FileNotFoundError as e:
            raise ConfigValidationError(
                field="config_file",
                details=f"Файл конфигурации не найден: {config_file}"
            ) from e

        except json.JSONDecodeError as e:
            raise ConfigValidationError(
                field="config_file",
                details="Некорректный JSON формат"
            ) from e

        except ValidationError as e:
            raise ConfigValidationError.from_pydantic(e) from e

class TagAttributes(BaseModel):
    prsMaxLineDev: float | None = None
    prsValueTypeCode: int = Field(..., ge=1, le=4)
    prsJsonConfigString: dict
    prsJSONata: str

class TagConfig(BaseModel):
    tagId: UUID4
    attributes: TagAttributes | None = None

class PlatformConfig(BaseModel):
    prsJsonConfigString: ConfigStringPlatform | None = None
    tags: list[TagConfig] = []

    @classmethod
    def from_file(cls, connector_id: UUID4) -> Self:
        if (config_file := Path(f"platform_{connector_id}.json")).exists():
            return cls.model_validate_json(config_file.read_text())
        return PlatformConfig()

    def save(self, connector_id: UUID4) -> None:
        Path(f"platform_{connector_id}.json").write_text(
            self.model_dump_json(indent=2, exclude_unset=True)
        )
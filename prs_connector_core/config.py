"""
Модуль для работы с конфигурацией коннектора
"""

import json
from pathlib import Path
from typing import Optional, Any
from uuid import UUID
from pydantic import BaseModel, field_validator, ValidationInfo

class SSLConfig(BaseModel):
    """Конфигурация SSL для подключения по WSS"""
    certFile: str
    keyFile: str
    certPassword: str

class ConnectorConfig(BaseModel):
    """Основная конфигурация коннектора"""
    id: UUID
    url: str
    ssl: Optional[SSLConfig] = None

    @field_validator('url')
    @classmethod
    def validate_url_protocol(cls, v: str) -> str:
        """Проверка корректности протокола в URL"""
        if not v.startswith(('ws://', 'wss://')):
            raise ValueError('URL должен использовать протокол WS или WSS')
        return v

    @field_validator('ssl')
    @classmethod
    def validate_ssl_requirements(
        cls,
        v: Optional[SSLConfig],
        info: ValidationInfo
    ) -> Optional[SSLConfig]:
        """Проверка необходимости SSL конфигурации"""
        url = info.data.get('url', '')
        if url.startswith('wss://') and v is None:
            raise ValueError(
                "Конфигурация SSL обязательна для протокола WSS"
            )
        return v

    @classmethod
    def from_file(cls, path: str | Path) -> 'ConnectorConfig':
        """Загрузка конфигурации из файла"""
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        return cls(**data)
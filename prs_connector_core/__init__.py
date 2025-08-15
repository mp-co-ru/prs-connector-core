"""
Базовый модуль для создания коннекторов Peresvet

Экспортирует основные классы и исключения:
- BaseConnector - базовый класс коннектора
- Конфигурационные модели
- Кастомные исключения
"""

from .connector import BaseConnector, TagGroupReaderConnector
from .config import (
    ConnectorConfig,
    PlatformConfig,
    LogConfig,
    SSLConfig
)
from .exceptions import (
    ConnectorBaseError,
    PlatformConnectionError,
    ConfigValidationError,
    DataProcessingError,
    PlatformConfigError
)
from .times import (
    ts,
    int_to_local_timestamp,
    ts_to_local_str,
    now_int
)
from typing_extensions import Self
__all__ = [
    'Self',
    'BaseConnector',
    'TagGroupReaderConnector',
    'ConnectorConfig',
    'PlatformConfig',
    'LogConfig',
    'SSLConfig',
    'ConnectorBaseError',
    'PlatformConnectionError',
    'ConfigValidationError',
    'DataProcessingError',
    'PlatformConfigError',
    'ts',
    'int_to_local_timestamp',
    'ts_to_local_str',
    'now_int'
]

__version__ = "0.3.2"
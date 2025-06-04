"""
Базовый модуль для создания коннекторов Peresvet

Экспортирует основные классы и исключения:
- BaseConnector - базовый класс коннектора
- Конфигурационные модели
- Кастомные исключения
"""

from .connector import BaseConnector
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
from typing_extensions import Self
__all__ = [
    'Self',
    'BaseConnector',
    'ConnectorConfig',
    'PlatformConfig',
    'LogConfig',
    'SSLConfig',
    'ConnectorBaseError',
    'PlatformConnectionError',
    'ConfigValidationError',
    'DataProcessingError',
    'PlatformConfigError'
]

__version__ = "0.1.0"
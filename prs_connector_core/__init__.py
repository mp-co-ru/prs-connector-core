"""
Базовый модуль для создания коннекторов Peresvet

Экспортирует основные классы и исключения:
- BaseConnector - базовый класс коннектора
- Конфигурационные модели
- Кастомные исключения
"""

from .connector import (
    BaseConnector,
    TagGroupReaderConnector,
    CN_Q_GOOD,
    CN_Q_UNLINK_COTTECTOR_TO_SOURCE,
    CN_Q_SOURCE_ERROR,
    main
)

from .config import (
    ConnectorConfig,
    PlatformConfig,
    LogConfig,
    SSLConfig,
    ConnectorPrsJsonConfigStringFromPlatform
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
    'ConnectorPrsJsonConfigStringFromPlatform',
    'ConfigValidationError',
    'DataProcessingError',
    'PlatformConfigError',
    'ts',
    'int_to_local_timestamp',
    'ts_to_local_str',
    'now_int',
    'CN_Q_GOOD',
    'CN_Q_UNLINK_COTTECTOR_TO_SOURCE',
    'CN_Q_SOURCE_ERROR',
    'main'
]

__version__ = "0.8.0"
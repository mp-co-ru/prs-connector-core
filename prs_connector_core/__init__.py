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
    CN_Q_UNLINK_CONNECTOR_TO_SOURCE,
    CN_Q_SOURCE_ERROR,
    CONNECTOR_CONFIG_ENV,
    resolve_config_file,
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
from importlib.metadata import version as _pkg_version, PackageNotFoundError

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
    'CN_Q_UNLINK_CONNECTOR_TO_SOURCE',
    'CN_Q_SOURCE_ERROR',
    'CONNECTOR_CONFIG_ENV',
    'resolve_config_file',
    'main'
]

try:
    from ._version import version as __version__
except Exception:
    try:
        __version__ = _pkg_version("prs_connector_core")
    except PackageNotFoundError:
        __version__ = "0.0.0.dev0"
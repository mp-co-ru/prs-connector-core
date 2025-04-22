from __future__ import annotations
import asyncio
import logging
import aiomqtt
from typing import Any
from abc import ABC, abstractmethod
from pathlib import Path
from .config import ConnectorConfig, LogConfig, PlatformConfig
from .data_handler import DataHandler
from .logger import setup_logger
from .exceptions import (
    PlatformConfigError,
    ConnectionError,
    ConfigValidationError
)

class BaseConnector(ABC):
    """Базовый класс коннектора платформы Peresvet"""

    def __init__(self, config_file: str = "config.json") -> None:
        # Инициализация конфигурации из файла
        # Параметры: id, url, ssl.
        try:
            self.config_from_file = ConnectorConfig.from_file(config_file)
        except ConfigValidationError as e:
            self._emergency_shutdown(f"Ошибка конфигурации: {e.details}")

        # Инициализация клиента MQTT
        self.mqtt_client : aiomqtt.Client = None

        # Инициализация конфигурации от платформы
        self.platfrom_config = PlatformConfig.from_file(self.config_from_file.id)

        # Настройка системы логирования
        self.logger = setup_logger(self.config_from_file.id, self.platfrom_config.prsJsonConfigString.log)

        self.logger.info("Инициализация базового коннектора")

        # Инициализация обработчика данных
        self.data_handler = DataHandler(self)

    def _emergency_shutdown(self, message: str) -> None:
        """Аварийное завершение работы при критических ошибках"""
        logger = logging.getLogger("prs_emergency")
        logger.error(message)
        raise RuntimeError(message)

    async def start(self) -> None:
        pass

    @abstractmethod
    async def connect_to_source(self) -> None:
        """Абстрактный метод для подключения к источнику данных"""
        raise NotImplementedError()

    @abstractmethod
    async def read_tags(self, tags: list[str]) -> dict[str, Any]:
        """Абстрактный метод для чтения тегов из источника"""
        raise NotImplementedError()
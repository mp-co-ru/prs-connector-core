from __future__ import annotations
import json
import ssl
import os
import asyncio
import signal
import logging
import aiomqtt
from collections import defaultdict
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

from jsonata import Jsonata
from .config import TagConfig, ConnectorConfig, LogConfig, PlatformConfig
from .logger import setup_logger
from .exceptions import (
    PlatformConfigError,
    ConnectionError,
    ConfigValidationError,
    DataProcessingError,
    JsonataError
)

class BaseConnector(ABC):
    """Базовый класс коннектора платформы Peresvet"""

    def __init__(self, config_file: str = "config.json") -> None:
        # Инициализация конфигурации из файла
        # Параметры: id, url, ssl.
        try:
            self._config_from_file : ConnectorConfig = ConnectorConfig.from_file(config_file)
        except ConfigValidationError as e:
            self._emergency_shutdown(f"Ошибка конфигурации: {e}")

        self._loop = asyncio.get_event_loop()

        # Инициализация клиента MQTT
        self._mqtt_client : aiomqtt.Client | None = None

        # Пул соединений к источнику данных
        self._source_pool = None

        # Инициализация конфигурации от платформы
        self._platfrom_config = PlatformConfig.from_file(self._config_from_file.id)

        # Настройка системы логирования
        self._logger = setup_logger(self._config_from_file.id, self._platfrom_config.prsJsonConfigString.log)

        self._logger.info("Инициализация базового коннектора")

        # теги, разбитые по группам
        # ключ - частота чтения
        self._tag_groups: defaultdict[float, list[TagConfig]] = defaultdict(list)
        # последние отосланные в платформу значения тегов
        self._last_values: dict[str, float | int | str | dict] = {}
        # кэш выражений jsonata
        self._jsonata_cache: dict[str, Jsonata] = {}
        # очередь данных для отправки в платформу
        self._data_queue: asyncio.Queue = asyncio.Queue()
        # блокировка для работы с очередью
        self._data_queue_lock: asyncio.Lock = asyncio.Lock()
        # имя временного файла буфера
        self._buf_tmp_file_name = f"backup_{self._config_from_file.id}.tmp"
        self._buf_final_file_name = f"backup_{self._config_from_file.id}.json"
        # флаг коннекта к платформе
        self._mqtt_connected = asyncio.Event()

        # Извлекаем параметры подключения
        parsed_url = urlparse(self._config_from_file.url)

        self._mqtt_parsed_url = {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 1883,  # Порт по умолчанию
            "user": parsed_url.username,
            "password": parsed_url.password,
            "tls": None
        }

        # SSL, если используется mqtts://
        if self._config_from_file.ssl:
            tls_params = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            # Загружаем CA сертификат для проверки сервера
            tls_params.load_verify_locations(cafile=self._config_from_file.ssl.caFile)

            # Загружаем клиентский сертификат и приватный ключ
            tls_params.load_cert_chain(
                certfile=self._config_from_file.ssl.certFile,
                keyfile=self._config_from_file.ssl.keyFile
            )

            # Требуем проверку сертификатов
            tls_params.verify_mode = ssl.VerifyMode(self._config_from_file.ssl.certsRequired)
            self._mqtt_parsed_url["tls"] = tls_params

    async def _shutdown(self):
        """Обработчик завершения работы"""
        self._logger.info(f"Получен сигнал завершения работы, сохраняем данные...")
        await self._save_queue_to_disk()
        self._loop.stop()

    async def _save_queue_to_disk(self):
        """Атомарное сохранение очереди в файл"""
        async with self._data_queue_lock:
            items = []
            while not self._data_queue.empty():
                try:
                    items.append(self._data_queue.get_nowait())
                    self._data_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            if items:
                # Сохраняем во временный файл, затем переименовываем
                with open(self._buf_tmp_file_name, "w") as f:
                    json.dump(items, f)
                os.replace(self._buf_tmp_file_name, self._buf_final_file_name)
                self._logger.info("Данные сохранены в файл.")

    async def _load_queue_from_disk(self):
        """Загрузка данных из файла в очередь"""
        try:
            if Path(self._buf_final_file_name).exists():
                with open(self._buf_final_file_name, "r") as f:
                    items = json.load(f)

                async with self._data_queue_lock:
                    for item in items:
                        await self._data_queue.put(item)

                os.remove(self._buf_final_file_name)
                self._logger.info("Данные загружены из файла.")
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _convert_value(self, value, type_code: int):
        match type_code:
            case 1: return int(value)
            case 2: return float(value)
            case 3: return str(value)
            case 4: return value
            case _ as code:
                raise DataProcessingError(
                    tag_id="system",
                    reason=f"Неподдерживаемый тип данных: {code}"
                )

    def _process_tag_data(self, tag: TagConfig, raw_value):
        try:
            value = raw_value
            # Если тег имеет выражение jsonata, то используем его
            if (expr := self._jsonata_cache.get(tag.tagId)):
                value = expr.evaluate(raw_value)
            return self._convert_value(value, tag.attributes.prsValueTypeCode)
        except Exception as e:
            raise JsonataError(
                tag_id=str(tag.tagId),
                reason=str(e)
            ) from e

    def _emergency_shutdown(self, message: str) -> None:
        """Аварийное завершение работы при критических ошибках"""
        logger = logging.getLogger("prs_emergency")
        logger.error(message)
        raise RuntimeError(message)

    async def run(self) -> None:

        for sig in [signal.SIGINT, signal.SIGTERM]:
            self._loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self._shutdown())
            )

        try:


            pass

        except aiomqtt.MqttError as e:
            print(f"Ошибка подключения: {e}")

    async def _connect_to_platform(self) -> None:
        """Подключение к платформе"""
        while True:
            try:
                self._mqtt_client = aiomqtt.Client(
                    hostname=self._mqtt_parsed_url["host"],
                    port=self._mqtt_parsed_url["port"],
                    username=self._mqtt_parsed_url["user"],
                    password=self._mqtt_parsed_url["password"],
                    tls_params=self._mqtt_parsed_url["tls"]
                )

            except aiomqtt.MqttError as e:
                self._mqtt_connected.clear()
                self._logger.error(f"Ошибка подключения к платформе: {e}")
                await asyncio.sleep(5)


    @abstractmethod
    async def connect_to_source(self) -> None:
        """Абстрактный метод для подключения к источнику данных"""
        raise NotImplementedError()

    @abstractmethod
    async def read_tags(self, tags: list[str]) -> dict[str, Any]:
        """Абстрактный метод для чтения тегов из источника"""
        raise NotImplementedError()
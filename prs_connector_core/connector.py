from __future__ import annotations
import json
import copy
import hashlib
import logging.handlers
import ssl
import os
import asyncio
from typing import Any
import signal
import logging
import aiomqtt
from collections import defaultdict
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

from jsonata import Jsonata
from .config import ConnectorConfig, LogConfig, PlatformConfig, TagAttributes
from .exceptions import (
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
        self._config_from_platfrom : PlatformConfig = PlatformConfig()
        # кэш тегов
        # содержит JSONata выражения и последние отправленные в платформу значения
        # имеет вид:
        # {
        #    "<tag_id>": {
        #       "JSONataExpr": Jsonata(),
        #       "last_value": ...
        #    }
        # }
        self._tag_cache = {}

        # группы тегов
        self._tag_groups: defaultdict = defaultdict(list)

        self._logger: logging.Logger = logging.getLogger(f"prs_connector_{self._config_from_file.id}")

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

        # топик, в который платформа будет посылать сообщения для коннектора
        self._mqtt_topic_messages_from_platform = f"connector/{self._config_from_file.id}"

        self._mqtt_parsed_url = {
            "host": parsed_url.hostname,
            "port": parsed_url.port or 1883,  # Порт по умолчанию
            "user": parsed_url.username,
            "password": parsed_url.password,
            "tls": None
        }

        try:
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
        except:
            self._emergency_shutdown("Ошибка загрузки сертификатов.")

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

        # обрабатываем конфигурацию
        cached_paltform_config : PlatformConfig = PlatformConfig()
        try:
            cached_paltform_config = PlatformConfig.from_file(self._config_from_file.id)
        except Exception as ex:
            self._emergency_shutdown(f"Ошибка чтения конфигурации из кэша: {ex}")

        await self._get_full_configuration_from_platform(cached_paltform_config.model_dump())

        for sig in [signal.SIGINT, signal.SIGTERM]:
            self._loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self._shutdown())
            )

        try:


            pass

        except aiomqtt.MqttError as e:
            self._logger.exception(f"Ошибка подключения: {e}")

    async def _get_full_configuration_from_platform(self, mes: dict):
        new_mes = {
            "data": {
                "prsJsonConfigString": mes["data"]["prsJsonConfigString"]
            }
        }
        await self._get_connector_configuration_from_platform(new_mes)

        new_mes = {
            "data": {
                "tags": mes["data"]["tags"]
            }
        }
        await self._tags_add_or_changed(new_mes)

    @classmethod
    def _hash_dict(cls, js: dict) -> bytes:
        # Делаем хэш словаря. Функция нужна для сравнений словарей.
        dict_str = json.dumps(js, sort_keys=True, ensure_ascii=False)
        dict_bytes = dict_str.encode("utf-8")
        hasher = hashlib.sha256()
        hasher.update(dict_bytes)
        # Возвращаем шестнадцатеричное представление хэша
        return hasher.digest()

    async def _get_connector_configuration_from_platform(self, mes: dict):
        config_changed = False

        old_log_config_hash = self._hash_dict(self._config_from_platfrom.prsJsonConfigString.log.model_dump())
        new_log_config_hash = self._hash_dict(mes["data"]["prsJsonConfigString"]["log"])
        if old_log_config_hash != new_log_config_hash:
            self._config_from_platfrom.prsJsonConfigString.log = LogConfig(**mes["data"]["prsJsonConfigString"]["log"])
            self._setup_logger()
            config_changed = True

        old_source_cofig_hash = self._hash_dict(self._config_from_platfrom.prsJsonConfigString.source)
        new_source_cofig_hash = self._hash_dict(mes["data"]["prsJsonConfigString"]["source"])
        if old_source_cofig_hash != new_source_cofig_hash:
            self._config_from_platfrom.prsJsonConfigString.source = copy.deepcopy(mes["data"]["prsJsonConfigString"]["source"])
            self._source_pool = await self._connect_to_source()
            config_changed = True

        if config_changed:
            self._config_from_platfrom.save(self._config_from_file.id)
            self._logger.info("Конфигурация коннектора изменена.")

    async def _tags_add_or_changed(self, mes: dict):
        existing_tags = self._config_from_platfrom.tags.keys()

        config_changed = False
        for tag_id, tag_data in mes["data"]["tags"].items():
            add_tag = False
            if tag_id in existing_tags:
                # если тег уже есть в списке...
                old_tag_hash = self._hash_dict(self._config_from_platfrom.tags[tag_id].model_dump())
                new_tag_hash = self._hash_dict(tag_data)
                if old_tag_hash != new_tag_hash:
                    add_tag = True
                    self._config_from_platfrom.tags.pop(tag_id)
                    for _, group_tags in self._tag_groups:
                        index = -1
                        try:
                            index = group_tags.index(tag_id)
                            group_tags.pop(index)
                            break
                        except:
                            pass
                    self._tag_cache.pop(tag_id)

            else:
                add_tag = True

            if add_tag:
                config_changed = True
                self._config_from_platfrom.tags[tag_id] = TagAttributes(**tag_data)
                self._create_tag_cache(tag_id)
                self._move_tag_to_group(tag_id)

        if config_changed:
            self._config_from_platfrom.save(self._config_from_file.id)
            self._logger.info("Конфигурация тегов изменена.")

    def _create_tag_cache(self, tag_id: str):
        self._tag_cache[tag_id] = {
            "last_value": None,
            "JSONataExpr": None
        }
        expr = None
        try:
            expr = self._config_from_platfrom.tags[tag_id].prsJsonConfigString.JSONata
            if expr:
                self._tag_cache[tag_id]["JSONataExpr"] = Jsonata(expr)
        except:
            self._logger.exception(f"Тег {tag_id}. Ошибка создания JSONata выражения '{expr}'")

    async def _tags_deleted(self, mes: dict):
        # удаление тегов из списка обрабатываемых коннектором
        for tag_id in mes["data"]["tags"]:
            self._config_from_platfrom.tags.pop(tag_id)
            self._tag_cache.pop(tag_id)
            for _, tag_group in self._tag_groups.items():
                if

    async def _message_handler(self):
        try:
            if self._mqtt_client:
                async for message in self._mqtt_client.messages:
                    message_data = json.loads(str(message.payload))
                    match message_data["action"]:
                        case "prsConnector.full_configuration":
                            await self._get_full_configuration_from_platform(message_data)
                        case "prsConnector.connector_configuration":
                            await self._get_connector_configuration_from_platform(message_data)
                        case "prsConnector.tags_configuration":
                            await self._tags_add_or_changed(message_data)
                        case "prsConnector.tags_deleted":
                            await self._tags_deleted(message_data)

        except:
            pass

    async def _connect_to_platform(self) -> None:
        """Подключение к платформе. При обрыве связи делаются попытки её восстановить."""
        while True:
            try:
                self._mqtt_client = aiomqtt.Client(
                    identifier=self._config_from_file.id,
                    protocol=aiomqtt.ProtocolVersion.V5,
                    hostname=self._mqtt_parsed_url["host"],
                    port=self._mqtt_parsed_url["port"],
                    username=self._mqtt_parsed_url["user"],
                    password=self._mqtt_parsed_url["password"],
                    tls_params=self._mqtt_parsed_url["tls"]
                )
                self._mqtt_connected.set()

                await self._get_full_configuration_from_platform()

                await asyncio.Future()

            except aiomqtt.MqttError as e:
                self._mqtt_connected.clear()
                self._logger.error(f"Ошибка подключения к платформе: {e}")
                await asyncio.sleep(5)

    def _setup_logger(self):
        self._logger.handlers.clear()
        self._logger = logging.getLogger(f"prs_connector_{self._config_from_file.id}")
        self._logger.setLevel(self._config_from_platfrom.prsJsonConfigString.log.level)

        formatter = logging.Formatter(
            '%(asctime)s :: [%(levelname)s] :: %(name)s :: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S.%f'
        )

        log_file = Path(self._config_from_platfrom.prsJsonConfigString.log.fileName)
        log_dir = log_file.parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            self._config_from_platfrom.prsJsonConfigString.log.fileName,
            maxBytes=self._config_from_platfrom.prsJsonConfigString.log.maxBytes,
            backupCount=self._config_from_platfrom.prsJsonConfigString.log.backupCount
        )
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

    @abstractmethod
    async def _connect_to_source(self) -> None:
        """Абстрактный метод для подключения к источнику данных"""
        raise NotImplementedError()

    @abstractmethod
    async def _read_tags(self, tags: list[str]) -> dict[str, Any]:
        """Абстрактный метод для чтения тегов из источника"""
        raise NotImplementedError()

    @abstractmethod
    def _move_tag_to_group(self, tag_id: str):
        """Абстрактный метод для помещения тега в группу"""
        raise NotImplementedError()
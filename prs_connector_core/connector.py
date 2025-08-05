from __future__ import annotations
import json
import copy
import hashlib
import logging.handlers
import ssl
import asyncio
from typing import Any
import signal
import logging
import aiofiles.os
import aiomqtt
import aiofiles
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

from jsonata import Jsonata
from .config import ConnectorConfig, LogConfig, PlatformConfig
from .exceptions import (
    ConfigValidationError,
    PlatformConnectionError
)
from .times import now_int

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

        # Инициализация конфигурации от платформы
        self._config_from_platfrom : PlatformConfig = PlatformConfig.from_file(self._config_from_file.id)

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

        self._logger : logging.Logger = None # type: ignore
        self._setup_logger()

        # очередь данных для отправки в платформу
        self._data_queue: asyncio.Queue = asyncio.Queue()
        # блокировка для работы с файлом буфера
        self._buf_file_lock: asyncio.Lock = asyncio.Lock()
        # имя файла буфера
        self._buf_file_name = f"backup_{self._config_from_file.id}.dat"
        # имя временного файла буфера
        self._tmp_buf_file_name = f"backup_{self._config_from_file.id}.tmp"
        # флаг коннекта к платформе
        self._mqtt_connected = asyncio.Event()

        # Извлекаем параметры подключения
        parsed_url = urlparse(self._config_from_file.url)

        # топик, в который платформа будет посылать сообщения для коннектора
        self._mqtt_topic_messages_from_platform = f"prs2conn/{self._config_from_file.id}"

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

        # обработка сообщений от платформы
        self._handle_messages_task = None
        # чтение данных тегов
        self._read_tags_task = None
        # работа с данными
        self._push_data_task = None
        # работа с буфером
        self._process_buffer_task = None

    async def _shutdown(self):
        """Обработчик завершения работы"""
        self._logger.info(f"Получен сигнал завершения работы, сохраняем данные...")

        # Отменяем все задачи
        tasks = [
            self._handle_messages_task,
            self._push_data_task,
            self._process_buffer_task,
            self._read_tags_task
        ]

        for task in tasks:
            if task and not task.done():
                task.cancel()

        # Ожидаем завершения задач
        await asyncio.gather(*tasks, return_exceptions=True)

        # Закрываем файловые дескрипторы
        if hasattr(self, '_buf_file') and self._buf_file:
            await self._buf_file.close()

    def _emergency_shutdown(self, message: str) -> None:
        """Аварийное завершение работы при критических ошибках"""
        logger = logging.getLogger("prs_emergency")
        logger.error(message)
        raise RuntimeError(message)

    async def _push_data(self):
        # берём из очереди сообщение и пытаемся отправить его в платформу
        # при неудаче сохраняем в буфер
        async def write_to_buf(mes):
            async with self._buf_file_lock:
                    await self._buf_file.write(mes) # type: ignore

        while True:
            mes = await self._data_queue.get()
            try:
                new_mes = self._process_tags_data(mes)

                if self._mqtt_connected.is_set():
                    await self._mqtt_client.publish(topic="prsTag/app_api/data_set/*", payload=new_mes, qos=1) # type: ignore
                else:
                    await write_to_buf(new_mes)
            except (aiomqtt.MqttError) as ex:
                await write_to_buf(mes)
                self._mqtt_connected.clear()

    async def _process_buffer(self):
        while True:
            await self._mqtt_connected.wait()

            async with self._buf_file_lock:
                stat = await aiofiles.os.stat(self._buf_file_name)
                if stat.st_size > 0:
                    # если размер буфера > 0
                    tmp_file = await aiofiles.open(self._tmp_buf_file_name, mode="+a")

                    queue_full = False
                    async for line in self._buf_file: # type: ignore
                        if queue_full or not self._mqtt_connected.is_set():
                            # если в процессе обработки буфера переполнилась очередь или прервалась связь с платформой,
                            # то все оставшиеся в буфере строки пишем во временный файл и потом
                            # переименовываем временный файл в файл буфера
                            await tmp_file.write(line)
                        try:
                            # TODO: ошибка! данные в очереди опять будут обрабатываться методом _process_tag_data!
                            self._data_queue.put_nowait(line)
                        except asyncio.QueueFull as _:
                            if not queue_full:
                                self._logger.exception("Очередь сообщений переполнена.")
                                queue_full = True
                                await tmp_file.write(line)

                    await self._buf_file.close() # type: ignore
                    await tmp_file.close()
                    await aiofiles.os.replace(self._tmp_buf_file_name, self._buf_file_name)
                    self._buf_file = await aiofiles.open(self._buf_file_name)

            await asyncio.sleep(10)

    def _process_tags_data(self, data: dict) -> dict:
        """Метод обрабатывает "сырые" данные.
        Логика работы.
        На вход метода приходит массив данных, в том виде, как описано в API.
        Если у значения нет метки времени, метод её добавит
        Каждое значение тега:
        1. Конвертируется с помощью JSONata
        2. Преобразуется к нужному типу
        3. Если разница между последним посланным в платформу значением и текущим больше указанного предела,
           то значение будет помещено в очередь данных.
        Для тегов с типами данных 2(str) и 4(json) сравнение происходит следующим образом:
        если maxDev = 0, то в очередь помещается каждое новое значение тега, если maxDev > 0, то в очередь
        новое значение помещается, только если отличается от последнего записанного.

        Args:
            data(dict) - словарь с массивом данных тегов:
                {
                    "data": [
                        {
                            "tagId": "...",
                            "data": []
                        }
                    ]
                }

        Returns:
            dict - обработанные данные
        """
        new_data = {
            "data": []
        }

        try:
            cur_time = now_int()
            for tag in data["data"]:
                tag_id = tag["tagId"]
                new_tag_data = {
                    "tagId": tag_id,
                    "data": []
                }
                jsonata_expr = self._tag_cache[tag_id]["JSONataExpr"]
                last_value = self._tag_cache[tag_id]["last_value"]
                value_type = self._config_from_platfrom.tags[tag_id].prsValueTypeCode
                max_dev = self._config_from_platfrom.tags[tag_id].prsJsonConfigString.maxDev
                for data_value in tag["data"]:
                    new_data_value = data_value
                    if jsonata_expr:
                        new_data_value[0] = jsonata_expr.evaluate(new_data_value[0])
                    if len(new_data_value) == 1:
                        new_data_value.append(cur_time)

                    match value_type:
                        case 0: new_data_value[0] = int(new_data_value[0])
                        case 1: new_data_value[0] = float(new_data_value[0])
                        case 2: new_data_value[0] = str(new_data_value[0])
                        case 4:
                            if isinstance(new_data_value[0], str):
                                try:
                                    new_data_value[0] = json.loads(new_data_value[0])
                                except:
                                    self._logger.exception(f"Тег '{tag_id}'. Ошибка конвертации значения '{new_data_value[0]}' к типу {value_type}")
                                    continue
                        case _ as code:
                            self._logger.exception(f"Тег '{tag_id}'. Ошибка конвертации значения '{new_data_value[0]}' к типу {value_type}")
                            continue

                    if (max_dev == 0 or \
                        last_value is None or \
                        value_type in [0, 1] and (max_dev <= abs(last_value - new_data_value[0])) or \
                        value_type == 2 and last_value != new_data_value[0] or \
                        value_type == 4 and not self._dicts_are_equal(last_value, new_data_value[0])):

                        new_tag_data["data"].append(new_data_value)
                        last_value = new_data_value[0]

                if new_tag_data["data"]:
                    self._tag_cache[tag_id]["last_value"] = last_value
                    new_data["data"].append(new_tag_data)

        except Exception as e:
            self._logger.exception(f"Ошибка обработки данных: {e}")

        return new_data

    async def run(self) -> None:

        for tag_id in self._config_from_platfrom.tags.keys():
            await self._create_tag_cache(tag_id)

        self._buf_file = await aiofiles.open(self._buf_file_name, mode="+a")

        for sig in [signal.SIGINT, signal.SIGTERM]:
            self._loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self._shutdown())
            )

        # обработка сообщений от платформы
        self._handle_messages_task = asyncio.create_task(self._handle_messages())
        # чтение данных тегов
        if self._config_from_platfrom.prsActive:
            self._read_tags_task = asyncio.create_task(self._read_tags())
        # работа с данными
        self._push_data_task = asyncio.create_task(self._push_data())
        # работа с буфером
        self._process_buffer_task = asyncio.create_task(self._process_buffer())

        while True:
            try:
                async with aiomqtt.Client(
                    identifier=self._config_from_file.id,
                    protocol=aiomqtt.ProtocolVersion.V5,
                    hostname=self._mqtt_parsed_url["host"],
                    port=self._mqtt_parsed_url["port"],
                    username=self._mqtt_parsed_url["user"],
                    password=self._mqtt_parsed_url["password"],
                    tls_params=self._mqtt_parsed_url["tls"]
                ) as client:
                    self._mqtt_client = client
                    await self._mqtt_client.subscribe(self._mqtt_topic_messages_from_platform)
                    self._mqtt_connected.set()
                    payload = {
                        "action": "getConfig",
                        "data": {
                            "id": self._config_from_file.id
                        }
                    }
                    await client.publish(
                        f"conn2prs/{self._config_from_file.id}",
                        payload=json.dumps(payload)
                    )

                    while self._mqtt_connected.is_set():
                        await asyncio.sleep(5)

                    self._logger.exception(f"Разрыв связи с платформой.")

            except aiomqtt.MqttError as e:
                self._mqtt_connected.clear()
                self._logger.exception(f"Разрыв связи с платформой: {e}")

    async def _get_full_configuration_from_platform(self, mes: dict):
        new_mes = {
            "data": {
                "prsActive": mes["data"]["prsActive"],
                "prsEntityTypeCode": mes["data"]["prsEntityTypeCode"],
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

    @classmethod
    def _dicts_are_equal(cls, d1: dict, d2: dict) -> bool:
        d1_hash = cls._hash_dict(d1)
        d2_hash = cls._hash_dict(d2)
        return d1_hash == d2_hash

    async def _get_connector_configuration_from_platform(self, mes: dict):
        config_changed = False

        if not self._dicts_are_equal(
               self._config_from_platfrom.prsJsonConfigString.log.model_dump(),
               mes["data"]["prsJsonConfigString"]["log"]):
            self._config_from_platfrom.prsJsonConfigString.log = LogConfig(**mes["data"]["prsJsonConfigString"]["log"])
            self._setup_logger()
            config_changed = True

        if not self._dicts_are_equal(
                self._config_from_platfrom.prsJsonConfigString.source,
                mes["data"]["prsJsonConfigString"]["source"]
            ):
            self._config_from_platfrom.prsJsonConfigString.source = copy.deepcopy(mes["data"]["prsJsonConfigString"]["source"])
            config_changed = True
            if self._read_tags_task and not self._read_tags_task.done():
                self._read_tags_task.cancel()
                await asyncio.gather(self._read_tags_task)
                self._read_tags_task = None

        if mes["data"]["prsActive"] and self._read_tags_task is None:
            self._read_tags_task = asyncio.create_task(self._read_tags())

        if mes["data"]["prsActive"] != self._config_from_platfrom.prsActive:
            self._config_from_platfrom.prsActive = mes["data"]["prsActive"]
            config_changed = True

        if mes["data"]["prsEntityTypeCode"] != self._config_from_platfrom.prsEntityTypeCode:
            self._config_from_platfrom.prsEntityTypeCode = mes["data"]["prsEntityTypeCode"]
            # TODO: необходимо вызывать метод _entity_type_code_changed, но его пока нет.
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
                    await self._remove_tag_cache(tag_id)

            else:
                add_tag = True

            if add_tag:
                config_changed = True
                self._config_from_platfrom.tags[tag_id] = tag_data
                await self._create_tag_cache(tag_id)

        if config_changed:
            self._config_from_platfrom.save(self._config_from_file.id)
            self._logger.info("Конфигурация тегов изменена.")

    async def _tags_deleted(self, mes: dict):
        # удаление тегов из списка обрабатываемых коннектором

        # аналогично методу _create_tag_cache, может быть переписан в классе-наследнике
        for tag_id in mes["data"]["tags"]:
            self._config_from_platfrom.tags.pop(tag_id)
            await self._remove_tag_cache(tag_id)

            self._logger.info(f"Тег {tag_id} удалён из списка.")

    async def _handle_messages(self):
        while True:
            try:
                await self._mqtt_connected.wait()

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
                            case "prsConnector.deleted":
                                await self._deleted(message_data)

            except aiomqtt.MqttError:
                self._mqtt_connected.clear()

    async def _deleted(self, message_data):
        self._config_from_platfrom.prsActive = False
        self._config_from_platfrom.save(self._config_from_file.id)
        self._logger.info(f"Коннектор удалён из иерархии.")
        await self._shutdown()

    def _setup_logger(self):
        self._logger = logging.getLogger(f"prs_connector_{self._config_from_file.id}")
        self._logger.handlers.clear()
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

    # ------------------------------------------------------------------------------------------------------------------
    # методы, которые можно переопределять в классах-наследниках

    async def _create_tag_cache(self, tag_id: str):
        # в случае, если требуется кэш другого вида, необходимо переопределить
        # данный метод в классе-наследнике,
        # при этом из переопределённого метода необходимо вызвать данный метод

        self._tag_cache[tag_id] = {
            "last_value": None,
            "JSONataExpr": None
        }
        expr = None
        try:
            expr = self._config_from_platfrom.tags[tag_id].prsJsonConfigString.JSONata
            if expr:
                self._tag_cache[tag_id]["JSONataExpr"] = Jsonata(expr)

            self._logger.info(f"Создан кэш для тега {tag_id}")
        except:
            self._logger.exception(f"Тег {tag_id}. Ошибка создания JSONata выражения '{expr}'")

    async def _remove_tag_cache(self, tag_id: str):
        # если при удалении из конфигурации тега необходимо выполнить дополнительные действия, то
        # то данный метод необходимо переопределить в классе-наследнике
        # и вызвать данный метод
        self._tag_cache.pop(tag_id)

    @abstractmethod
    async def _read_tags(self):
        """Абстрактный метод для чтения тегов из источника"""
        raise NotImplementedError()

    #--------------------------------------------------------------------------------------------------------------------

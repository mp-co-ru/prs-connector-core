from __future__ import annotations
import os
import json
import copy
import hashlib
import logging.handlers
import ssl
import asyncio
import signal
import logging
import aiofiles.os
import aiomqtt
import aiofiles
import time
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

from jsonata import Jsonata
from .config import (
    ConnectorConfig,
    LogConfig,
    PlatformConfig,
    ConnectorPrsJsonConfigStringFromPlatform,
    TagAttributes
)
from .exceptions import (
    ConfigValidationError
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

        self._loop = None

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

        self._canceled = False

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

        self._canceled = True

        for task in tasks:
            if task and not task.done():
                task.cancel()

        # Ожидаем завершения задач
        _, pending = await asyncio.wait(tasks, timeout=15.0)

        if pending:
            self._logger.exception(f"При остановке коннектора некоторые задачи не успели корректно завершиться: {pending}.")

        self._logger.info("Работа коннектора завершена.")

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
                async with aiofiles.open(self._buf_file_name, mode="+a") as buf_file:
                    s = json.dumps(mes)
                    await buf_file.write(f"{s}\n")


        while not self._canceled:
            mes = await self._data_queue.get()
            try:
                self._logger.debug(f"Новое сообщение с данными: {mes}.")
                # при помещении сообщения из буфера в очередь мы помечаем его как уже обработанное
                processed = mes.get("processed", False)
                if not processed:
                    new_mes = self._process_tags_data(mes)
                else:
                    new_mes = mes

                if new_mes["data"]:
                    if self._mqtt_connected.is_set():
                        self._logger.info(f"Отправка данных в платформу.")
                        await self._mqtt_client.publish( # type: ignore
                            topic="prsTag/app_api_client/data_set/*",
                            payload=json.dumps(new_mes),
                            retain=True
                        )
                    else:
                        await write_to_buf(new_mes)
                        self._logger.info(f"Данные сохранены в буфер.")
            except (aiomqtt.MqttError) as _:
                self._mqtt_connected.clear()
                await write_to_buf(mes)
                self._logger.info(f"Ошибка передачи данных: сохраняем в буфер.")
            except asyncio.CancelledError:
                return

    async def _process_buffer(self):
        try:
            while not self._canceled:
                if not self._mqtt_connected.is_set():
                    await asyncio.sleep(2)
                    continue

                async with self._buf_file_lock:
                        stat = await aiofiles.os.stat(self._buf_file_name)
                        if stat.st_size > 0:
                            # если размер буфера > 0
                            self._logger.info("Обработка буфера данных.")
                            async with aiofiles.open(self._tmp_buf_file_name, mode="+a") as tmp_file:
                                queue_full = False
                                async with aiofiles.open(self._buf_file_name) as buf_file:
                                    async for line in buf_file: # type: ignore
                                        if queue_full or not self._mqtt_connected.is_set():
                                            # если в процессе обработки буфера переполнилась очередь или прервалась связь с платформой,
                                            # то все оставшиеся в буфере строки пишем во временный файл и потом
                                            # переименовываем временный файл в файл буфера
                                            await tmp_file.write(f"{line}\n")
                                            self._logger.debug("Запись данных обратно в буфер.")
                                        else:
                                            try:
                                                js = json.loads(line)
                                                js["processed"] = True
                                                self._data_queue.put_nowait(js)
                                                self._logger.debug("Запись данных из буфера в очередь.")
                                            except asyncio.QueueFull as _:
                                                if not queue_full:
                                                    self._logger.exception("Очередь сообщений переполнена.")
                                                    queue_full = True
                                                    await tmp_file.write(line)
                            await aiofiles.os.replace(self._tmp_buf_file_name, self._buf_file_name)

        except asyncio.CancelledError:
            if await aiofiles.os.path.exists(self._tmp_buf_file_name):
                await aiofiles.os.replace(self._tmp_buf_file_name, self._buf_file_name)

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

                    if new_data_value[0] is not None:
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
                        new_data_value[0] is None and last_value is not None or \
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

        self._loop = asyncio.get_running_loop()

        for sig in [signal.SIGINT, signal.SIGTERM]:
            self._loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self._shutdown())
            )

        # создадим файл буфера
        async with aiofiles.open(self._buf_file_name, mode="+a") as _:
            pass

        for tag_id in self._config_from_platfrom.tags.keys():
            await self._create_tag_cache(tag_id)

        # обработка сообщений от платформы
        self._handle_messages_task = asyncio.create_task(self._handle_messages())
        # чтение данных тегов
        if self._config_from_platfrom.prsActive:
            self._read_tags_task = asyncio.create_task(self._read_tags())
        # работа с данными
        self._push_data_task = asyncio.create_task(self._push_data())
        # работа с буфером
        self._process_buffer_task = asyncio.create_task(self._process_buffer())

        try:
            while not self._canceled:
                try:
                    async with aiomqtt.Client(
                        identifier=self._config_from_file.id,
                        protocol=aiomqtt.ProtocolVersion.V5,
                        hostname=self._mqtt_parsed_url["host"],
                        port=self._mqtt_parsed_url["port"],
                        username=self._mqtt_parsed_url["user"],
                        password=self._mqtt_parsed_url["password"],
                        tls_params=self._mqtt_parsed_url["tls"],
                        timeout=5
                    ) as client:
                        self._mqtt_client = client
                        await self._mqtt_client.subscribe(self._mqtt_topic_messages_from_platform)
                        await asyncio.sleep(10)
                        self._mqtt_connected.set()
                        payload = {
                            "action": "getConfig",
                            "data": {
                                "id": self._config_from_file.id
                            }
                        }
                        await client.publish(
                            f"conn2prs/{self._config_from_file.id}",
                            payload=json.dumps(payload),
                            retain=True
                        )

                        self._logger.info("Связь с платформой установлена.")

                        while self._mqtt_connected.is_set() and not self._canceled:
                            await asyncio.sleep(3)

                except aiomqtt.MqttError as e:
                    self._logger.exception(f"Разрыв связи с платформой.")
                    self._mqtt_connected.clear()
                    if not self._canceled:
                        try:
                            await asyncio.sleep(5)
                        except asyncio.CancelledError:
                            break
                    else:
                        break

        except asyncio.CancelledError:
            pass

        except Exception as ex:
            self._logger.exception(f"Неопределённое исключение: {ex}.")

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

        checked_prsJsonConfigString = ConnectorPrsJsonConfigStringFromPlatform(**mes["data"]["prsJsonConfigString"])
        log_config = checked_prsJsonConfigString.model_dump()["log"]
        lc = mes["data"]["prsJsonConfigString"].get("log")
        if not lc or not lc.get("fileName"):
            log_config["fileName"] = PlatformConfig.default_log_file_name(self._config_from_file.id)

        if not self._dicts_are_equal(
               self._config_from_platfrom.prsJsonConfigString.log.model_dump(),
               log_config):
            self._config_from_platfrom.prsJsonConfigString.log = LogConfig(**log_config)
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

        if mes["data"]["prsActive"] and self._read_tags_task.done():
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

                tag_attrs = TagAttributes(**tag_data)
                new_tag_hash = self._hash_dict(tag_attrs.model_dump())
                if old_tag_hash != new_tag_hash:
                    add_tag = True
                    await self._remove_tag_cache(tag_id)
                    self._config_from_platfrom.tags.pop(tag_id)
            else:
                add_tag = True

            if add_tag:
                config_changed = True
                self._config_from_platfrom.tags[tag_id] = TagAttributes(**tag_data)
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
        while not self._canceled:
            try:
                await self._mqtt_connected.wait()
                if self._mqtt_client:
                    async for message in self._mqtt_client.messages:
                        json_str = message.payload.decode('utf8')
                        message_data = json.loads(json_str)

                        self._logger.info(f"Сообщение от платформы: {message_data['action']}.")

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
                            case "prsConnector.command":
                                await self._command(message_data)

            except aiomqtt.MqttError:
                self._mqtt_connected.clear()
            except asyncio.CancelledError:
                return
            except Exception as ex:
                self._logger.exception(f"Исключение {ex}.")

    async def _command(self, message_data):
        for line in message_data["data"]["command"]["lines"]:
            os.system(line)

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
            datefmt='%Y-%m-%d %H:%M:%S'
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

    async def _create_tag_cache(self, tag_id: str) -> bool:
        # в случае, если требуется кэш другого вида, необходимо переопределить
        # данный метод в классе-наследнике,
        # при этом из переопределённого метода необходимо вызвать данный метод

        if not self._config_from_platfrom.tags[tag_id].prsActive:
            # если тег неактивен, не создаём для него кэша
            return False

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
            return False

        return True

    async def _remove_tag_cache(self, tag_id: str):
        # если при удалении из конфигурации тега необходимо выполнить дополнительные действия, то
        # то данный метод необходимо переопределить в классе-наследнике
        # и вызвать данный метод
        try:
            self._tag_cache.pop(tag_id)
        except KeyError:
            pass

    @abstractmethod
    async def _read_tags(self):
        """Абстрактный метод для чтения тегов из источника"""
        raise NotImplementedError()

    #--------------------------------------------------------------------------------------------------------------------

class TagGroupReaderConnector(BaseConnector):
    """Класс коннектора, который читает данные из источника сам (не по подписке на события),
    при этом у каждого тега может быть своя частота чтения.
    Соответственно, у каждого тега в атрибуте prsJsonConfigString["source"] должен быть ключ frequency, тип - float,
    значение - частота чтения в секундах. Если указанного атрибута нет, значение по умолчанию принимается 5 сек.

    Коннектор формирует дополнительный кэш в атрибуте _frequency_groups:

    {
        <frequency>: {
            "tags": [<tag_id_1>, <tag_id_2>]
        }
    }

    Поэтому коннектор формирует группы тегов, при этом у каждой группы - своя частота чтения.

    Args:
        BaseConnector (_type_): _description_
    """

    def __init__(self, config_file: str = "config.json") -> None:
        super().__init__(config_file=config_file)

        self._tag_groups = defaultdict(self.default_tag_group)
        self._source_connected = asyncio.Event()

    def default_tag_group(self):
        return {"tags": []}

    async def _create_tag_cache(self, tag_id: str) -> bool:
        if await super()._create_tag_cache(tag_id):
            try:
                frequency = self._config_from_platfrom.tags[tag_id].prsJsonConfigString.frequency
                self._tag_groups[frequency]["tags"].append(tag_id)

                return True
            except:
                self._logger.exception(f"Создание кэша: указанного тега {tag_id} нет в списке.")
        return False

    async def _remove_tag_cache(self, tag_id: str):
        try:
            frequency = self._config_from_platfrom.tags[tag_id].prsJsonConfigString.frequency
        except:
            self._logger.exception(f"Удаление кэша: указанного тега {tag_id} нет в списке.")
            await super()._remove_tag_cache(tag_id)
            return

        try:
            self._tag_groups[frequency]["tags"].remove(tag_id)
        except ValueError:
            self._logger.exception(f"Тег {tag_id} не найден в соответствующей ему группе {frequency}.")

        if not len(self._tag_groups[frequency]["tags"]):
            self._tag_groups.pop(frequency)

        await super()._remove_tag_cache(tag_id)

    async def _periodic_task_for_group(self, frequency: float):
        try:
            while not self._canceled:
                start = time.time()
                await self._read_group(frequency=frequency)
                duration = time.time() - start
                await asyncio.sleep(frequency - duration)
        except asyncio.CancelledError:
            return

    @abstractmethod
    async def _read_group(self, frequency: float):
        raise NotImplementedError()

    async def _read_tags(self):
        tasks = []

        try:
            while not self._canceled:
                if await self._connect_to_source():
                    for frequency in self._tag_groups.keys():
                        tasks.append(asyncio.create_task(self._periodic_task_for_group(frequency=frequency)))
                    self._logger.info(f"Задачи чтения тегов созданы.")

                    while True:
                        if self._source_connected.is_set():
                            await asyncio.sleep(5)
                        else:
                            for task in tasks:
                                task.cancel()
                            _, pending = await asyncio.wait(tasks, timeout=3)
                            await self._close_source()
                else:
                    await asyncio.sleep(5)


        except asyncio.CancelledError:
            if tasks:
                for task in tasks:
                    task.cancel()
                _, pending = await asyncio.wait(tasks, timeout=3)
                if pending:
                    self._logger.exception(f"Задачи чтения тегов не завершились корректно.")
                    self._logger.info(f"Задачи чтения тегов остановлены.")
            await self._close_source()

    @abstractmethod
    async def _connect_to_source(self) -> bool:
        """Абстрактный метод соединения с источником"""
        raise NotImplementedError()

    @abstractmethod
    async def _close_source(self):
        """Абстрактный метод закрытия соединения с источником"""
        raise NotImplementedError()
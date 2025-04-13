"""
Базовый класс коннектора
"""

import argparse
import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Any
from pydantic import ValidationError
from .config import ConnectorConfig
from .websocket_client import WebSocketClient
from .data_handler import DataHandler
from .logger import configure_logger
from .buffer import BufferManager
from .exceptions import ConnectionError, ConfigValidationError, DataProcessingError
from abc import ABC, abstractmethod

class BaseConnector(ABC):
    """Абстрактный базовый класс для всех коннекторов"""

    def __init__(self, config_path: str = 'config.json'):
        self.args = self._parse_args(config_path)
        self.config = self._load_config(self.args.config)
        self.logger = configure_logger(self.config.id)
        self.ws_client = WebSocketClient(self.config)
        self.data_handler = DataHandler()
        self.buffer = BufferManager(self.config.id)
        self.platform_config: Dict[str, Any] = {}
        self.running = False
        self._scheduler_tasks: Dict[int, asyncio.Task] = {}
        self._cached_config_path = Path(f"{self.config.id}.json")

    def _parse_args(self, default_config: str) -> argparse.Namespace:
        """Парсинг аргументов командной строки"""
        parser = argparse.ArgumentParser()
        parser.add_argument('--config', default=default_config)
        return parser.parse_args()

    def _load_config(self, path: str) -> ConnectorConfig:
        """Загрузка конфигурации коннектора"""
        try:
            return ConnectorConfig.from_file(path)
        except (FileNotFoundError, ValidationError) as e:
            self.logger.error(f"Ошибка загрузки конфигурации: {e}")
            raise ConfigValidationError(f"Неверная конфигурация: {e}") from e

    async def connect_to_platform(self):
        """Установка и поддержание соединения с платформой"""
        try:
            await self.ws_client.connect()
            await self._perform_handshake()
        except ConnectionError as e:
            self.logger.error(f"Ошибка подключения: {e}")
            await self._use_cached_config()

    async def _perform_handshake(self):
        """Процедура подтверждения соединения"""
        try:
            self.platform_config = await self.ws_client.receive_config()
            self._save_platform_config()
            await self._send_acknowledgement()
        except ConnectionError as e:
            self.logger.error(f"Ошибка подтверждения соединения: {e}")
            await self._use_cached_config()

    async def _use_cached_config(self):
        """Использование сохранённой конфигурации"""
        config_path = self._cached_config_path

        if not config_path.exists():
            self.logger.error("Сохранённая конфигурация отсутствует")
            raise ConnectionError("Не удалось получить конфигурацию")

        try:
            with open(config_path, 'r') as f:
                self.platform_config = json.load(f)
                self.logger.info("Используется сохранённая конфигурация")

            if 'tags' not in self.platform_config:
                raise ValueError("Некорректный формат конфигурации")

        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Ошибка чтения конфигурации: {e}")
            raise ConfigValidationError("Неверная конфигурация") from e

    async def _send_acknowledgement(self):
        """Отправка подтверждения получения конфигурации"""
        ack_message = {
            "action": "config_ack",
            "connector_id": str(self.config.id),
            "status": "success",
            "message": "Конфигурация применена"
        }

        try:
            await self.ws_client.send_data(ack_message)
            self.logger.info("Подтверждение отправлено на платформу")
        except ConnectionError as e:
            self.logger.error(f"Ошибка отправки подтверждения: {e}")
            raise

    def _save_platform_config(self):
        """Сохранение конфигурации платформы на диск"""
        try:
            with open(self._cached_config_path, 'w') as f:
                json.dump(self.platform_config, f, indent=2)
            self.logger.info("Конфигурация платформы сохранена")
        except IOError as e:
            self.logger.error(f"Ошибка сохранения конфигурации: {e}")

    async def start(self):
        """Главный цикл работы коннектора"""
        self.running = True
        try:
            await self.connect_to_platform()
            await self._manage_schedulers()
            await self._monitor_connection()
        except KeyboardInterrupt:
            self.logger.info("Остановка по запросу пользователя")
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {e}")
        finally:
            await self._cleanup_resources()

    async def _manage_schedulers(self):
        """Управление задачами обработки тегов"""
        groups = self.data_handler.group_tags(self.platform_config.get('tags', []))
        await self._stop_schedulers()

        for freq, tags in groups.items():
            self._scheduler_tasks[freq] = asyncio.create_task(
                self._process_group(freq, tags)
            )

        self.logger.info(f"Активных групп обработки: {len(self._scheduler_tasks)}")

    async def _process_group(self, frequency: int, tags: list):
        """Цикл обработки группы тегов"""
        self.logger.debug(f"Запуск обработки группы с частотой {frequency} мс")
        while self.running:
            start_time = time.monotonic()
            await self._process_tags_batch(tags)
            elapsed = time.monotonic() - start_time
            await asyncio.sleep(max(0, frequency/1000 - elapsed))

    async def _process_tags_batch(self, tags: list):
        """Обработка пакета тегов"""
        for tag in tags:
            try:
                raw_value = await self.read_tag(tag)
                processed_value = self.data_handler.apply_jsonata(
                    raw_value,
                    tag['attributes'].get('prsJSONata', '$')
                )

                if self.data_handler.process_value(tag['tagId'], processed_value):
                    packet = self._create_data_packet(tag, processed_value)
                    await self._handle_data_delivery(packet)

            except DataProcessingError as e:
                self.logger.error(f"Ошибка обработки тега {tag['tagId']}: {e}")
            except Exception as e:
                self.logger.error(f"Критическая ошибка тега {tag['tagId']}: {e}")

    def _create_data_packet(self, tag: dict, value: Any) -> dict:
        """Формирование пакета данных"""
        return {
            "tagId": tag['tagId'],
            "data": [{
                "x": int(time.time() * 1e6),
                "y": value,
                "q": None
            }]
        }

    async def _handle_data_delivery(self, packet: dict):
        """Обработка доставки данных"""
        if await self.ws_client.is_connected():
            try:
                await self.ws_client.send_data(packet)
            except ConnectionError:
                await self.buffer.save(packet)
        else:
            await self.buffer.save(packet)

    async def _monitor_connection(self):
        """Мониторинг состояния соединения"""
        self.logger.info("Запуск монитора соединения")
        while self.running:
            try:
                if not await self.ws_client.is_connected():
                    await self._reconnect_with_backoff()
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Ошибка монитора соединения: {e}")
                await asyncio.sleep(10)

    async def _reconnect_with_backoff(self):
        """Повторное подключение с задержкой"""
        max_retries = 5
        base_delay = 1.0
        for attempt in range(max_retries):
            try:
                await self.connect_to_platform()
                if await self.ws_client.is_connected():
                    await self._flush_buffer()
                    return
                await asyncio.sleep(base_delay * (2 ** attempt))
            except ConnectionError as e:
                self.logger.warning(f"Попытка {attempt+1}/{max_retries}: {e}")

        self.logger.error("Достигнуто максимальное число попыток")
        raise ConnectionError("Не удалось восстановить соединение")

    async def _flush_buffer(self):
        """Отправка данных из буфера"""
        if await self.ws_client.is_connected():
            try:
                buffered = await self.buffer.load()
                for packet in buffered:
                    await self.ws_client.send_data(packet)
                self.logger.info(f"Отправлено буферизированных пакетов: {len(buffered)}")
            except Exception as e:
                self.logger.error(f"Ошибка отправки буфера: {e}")

    async def _cleanup_resources(self):
        """Очистка ресурсов"""
        self.logger.info("Завершение работы...")
        self.running = False
        await self._stop_schedulers()

        if await self.ws_client.is_connected():
            await self.ws_client.close()

        self.logger.info("Все ресурсы освобождены")

    async def _stop_schedulers(self):
        """Остановка всех задач обработки"""
        for task in self._scheduler_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._scheduler_tasks.clear()
        self.logger.info("Все задачи обработки остановлены")

    @abstractmethod
    async def read_tag(self, tag_config: Dict) -> Any:
        """Абстрактный метод чтения тега (реализуется в наследниках)"""
        pass
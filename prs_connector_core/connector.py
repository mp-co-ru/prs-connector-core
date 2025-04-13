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
        self._last_metrics_log = time.monotonic()

    async def _log_metrics(self):
        """Логирование метрик каждые 5 минут"""
        while self.running:
            await asyncio.sleep(300)  # 5 минут
            stats = self.data_handler.metrics.get_stats()
            self.logger.info(
                "Метрики производительности:\n"
                f"Обработано тегов: {stats['total_processed']}\n"
                f"Среднее время обработки: {stats['avg_processing_time']:.4f} сек\n"
                f"Среднее время JSONata: {stats['avg_jsonata_time']:.4f} сек\n"
                f"Среднее время отправки: {stats['avg_send_time']:.4f} сек\n"
                f"Процент ошибок: {stats['failure_rate']:.2%}"
            )
            self.data_handler.metrics.reset()

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
            asyncio.create_task(self._log_metrics())
            await self._monitor_connection()
        except KeyboardInterrupt:
            self.logger.info("Остановка по запросу пользователя")
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {e}")
        finally:
            await self._cleanup_resources()

    async def _handle_config_update(self, new_config: dict):
        """Обработка обновления конфигурации"""
        try:
            old_tags = {t['tagId']: t for t in self.platform_config.get('tags', [])}
            new_tags = {t['tagId']: t for t in new_config.get('tags', [])}

            # Определение измененных тегов
            changed_tags = [
                t for t in new_config['tags']
                if t['tagId'] not in old_tags or
                self.data_handler._calculate_hash(t['attributes']) !=
                self.data_handler.config_hashes.get(t['tagId'], '')
            ]

            if changed_tags:
                self.logger.info(f"Обнаружены изменения в {len(changed_tags)} тегах")
                await self._apply_config_changes(new_config, changed_tags)

        except Exception as e:
            self.logger.error(f"Ошибка обработки обновления конфигурации: {str(e)}")

    async def _apply_config_changes(self, new_config: dict, changed_tags: list):
        """Применение изменений конфигурации"""
        # Обновление основной конфигурации
        self.platform_config = new_config
        self._save_platform_config()

        # Перезапуск планировщиков с новой конфигурацией
        await self._manage_schedulers()
        self.logger.info("Конфигурация успешно обновлена")

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

            # Сбор данных для всех тегов группы
            data_batch = []
            timestamp = int(time.time() * 1e6)  # Общая временная метка

            for tag in tags:
                try:
                    tag_id = tag['tagId']
                    raw_value = await self.read_tag(tag)
                    processed_value = self.data_handler.apply_jsonata(raw_value, tag_id)

                    if self.data_handler.process_value(tag_id, processed_value):
                        data_batch.append({
                            "tagId": tag_id,
                            "data": [{
                                "x": timestamp,
                                "y": processed_value,
                                "q": None
                            }]
                        })

                except Exception as e:
                    self.logger.error(f"Ошибка обработки тега {tag_id}: {str(e)}")
                    self.data_handler.metrics.log_failure()

            # Отправка одним пакетом для всей группы
            if data_batch:
                await self._handle_data_delivery({"data": data_batch})

            elapsed = time.monotonic() - start_time
            await asyncio.sleep(max(0, frequency/1000 - elapsed))

    async def _process_tags_batch(self, tags: list):
        """Обработка пакета тегов с метриками"""
        for tag in tags:
            tag_id = tag['tagId']
            try:
                start_time = time.monotonic()  # Начало замера времени обработки

                # Чтение значения из источника
                raw_value = await self.read_tag(tag)

                # Применение JSONata преобразования
                processed_value = raw_value
                if self.data_handler.compiled_expressions.get(tag_id):
                    processed_value = self.data_handler.apply_jsonata(raw_value, tag_id)

                # Проверка необходимости отправки
                if self.data_handler.process_value(tag_id, processed_value):
                    send_start = time.monotonic()
                    packet = self._create_data_packet(tag, processed_value)
                    await self._handle_data_delivery(packet)
                    self.data_handler.metrics.log_send(time.monotonic() - send_start)

                # Фиксация общего времени обработки
                self.data_handler.metrics.log_processing(time.monotonic() - start_time)

            except DataProcessingError as e:
                self.logger.error(f"Ошибка обработки тега {tag_id}: {e}")
                self.data_handler.metrics.log_failure()
            except Exception as e:
                self.logger.error(f"Критическая ошибка тега {tag_id}: {e}")
                self.data_handler.metrics.log_failure()

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
        """Обработка доставки данных (переименованный метод)"""
        if await self.ws_client.is_connected():
            try:
                start = time.monotonic()
                await self.ws_client.send_data(packet)
                self.data_handler.metrics.log_send(time.monotonic() - start)
                self.logger.debug(f"Отправлен пакет с {len(packet['data'])} тегами")
            except ConnectionError:
                await self._buffer_packet(packet)
        else:
            await self._buffer_packet(packet)

    async def _buffer_packet(self, packet: dict):
        """Буферизация пакета"""
        await self.buffer.save(packet)
        self.logger.warning("Данные помещены в буфер из-за отсутствия соединения")

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
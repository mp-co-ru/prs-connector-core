"""
Обработка данных тегов
"""

from collections import defaultdict
from jsonata import Jsonata
from typing import Any, Dict, Optional
import hashlib
import time
import json
from .exceptions import DataProcessingError, JSONataError

class MetricsCollector:
    """Сбор метрик производительности"""
    def __init__(self):
        self.reset()

    def reset(self):
        self.total_processing_time = 0.0
        self.jsonata_time = 0.0
        self.send_time = 0.0
        self.total_tags = 0
        self.failed_tags = 0

    def log_processing(self, duration: float):
        self.total_processing_time += duration
        self.total_tags += 1

    def log_jsonata(self, duration: float):
        self.jsonata_time += duration

    def log_send(self, duration: float):
        self.send_time += duration

    def log_failure(self):
        self.failed_tags += 1

    def get_stats(self) -> dict:
        return {
            "total_processed": self.total_tags,
            "avg_processing_time": self.total_processing_time / max(self.total_tags, 1),
            "avg_jsonata_time": self.jsonata_time / max(self.total_tags, 1),
            "avg_send_time": self.send_time / max(self.total_tags, 1),
            "failure_rate": self.failed_tags / max(self.total_tags, 1)
        }

class DataHandler:
    def __init__(self):
        self.last_values = {}
        self.max_deviations = {}
        self.compiled_expressions: Dict[str, Jsonata] = {}
        self.config_hashes: Dict[str, str] = {}
        self.metrics = MetricsCollector()

    def _calculate_hash(self, config: dict) -> str:
        """Вычисление хеша конфигурации тега"""
        return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()

    def group_tags(self, tags: list) -> dict:
        """Группировка тегов с валидацией и компиляцией выражений"""
        groups = defaultdict(list)

        for tag in tags:
            try:
                tag_id = tag['tagId']
                config = tag['attributes']

                # Валидация JSONata выражения
                expr_str = config.get('prsJSONata')
                if expr_str:
                    try:
                        # Проверка синтаксиса выражения
                        Jsonata(expr_str)
                    except Exception as e:
                        raise DataProcessingError(
                            f"Некорректное JSONata выражение в теге {tag_id}: {str(e)}"
                        ) from e

                # Проверка изменений конфигурации
                new_hash = self._calculate_hash(config)
                if self.config_hashes.get(tag_id) != new_hash:
                    self._update_expression(tag_id, expr_str)
                    self.config_hashes[tag_id] = new_hash

                # Группировка по частоте
                freq = tag['prsJsonConfigString'].get('frequency', 1000)
                groups[freq].append(tag)
                self.max_deviations[tag_id] = config.get('prsMaxLineDev', 0)

            except Exception as e:
                self.metrics.log_failure()
                raise DataProcessingError(
                    f"Ошибка обработки тега {tag_id}: {str(e)}"
                ) from e

        return groups

    def _update_expression(self, tag_id: str, expr_str: Optional[str]):
        """Обновление скомпилированного выражения"""
        if expr_str:
            try:
                self.compiled_expressions[tag_id] = Jsonata(expr_str)
            except JSONataError as e:
                raise DataProcessingError(
                    f"Ошибка компиляции JSONata для тега {tag_id}: {str(e)}"
                ) from e
        else:
            self.compiled_expressions[tag_id] = None

    def process_value(self, tag_id: str, value: Any) -> bool:
        """Проверка необходимости отправки значения"""
        start_time = time.monotonic()
        try:
            max_dev = self.max_deviations.get(tag_id, 0)
            last = self.last_values.get(tag_id)

            if max_dev == 0 or last is None:
                self.last_values[tag_id] = value
                return True

            if abs(value - last) >= max_dev:
                self.last_values[tag_id] = value
                return True

            return False
        finally:
            self.metrics.log_processing(time.monotonic() - start_time)

    def apply_jsonata(self, value: Any, tag_id: str) -> Any:
        """Применение JSONata с замером времени"""
        start_time = time.monotonic()
        try:
            expr = self.compiled_expressions.get(tag_id)
            if not expr:
                return value

            result = expr.evaluate(value)
            self.metrics.log_jsonata(time.monotonic() - start_time)
            return result
        except Exception as e:
            self.metrics.log_failure()
            raise JSONataError(
                f"Ошибка выполнения JSONata для тега {tag_id}: {str(e)}"
            ) from e
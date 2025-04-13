"""
Обработка данных тегов
"""

from typing import Any
from collections import defaultdict
from jsonata import Jsonata
from .exceptions import DataProcessingError

class DataHandler:
    def __init__(self):
        self.last_values = {}
        self.max_deviations = {}

    def group_tags(self, tags: list) -> dict:
        """Группировка тегов по частоте опроса"""
        groups = defaultdict(list)
        for tag in tags:
            freq = tag['prsJsonConfigString'].get('frequency', 5000)
            groups[freq].append(tag)
            self.max_deviations[tag['tagId']] = tag['attributes'].get('prsMaxLineDev', 0)
        return groups

    def process_value(self, tag_id: str, value: Any) -> bool:
        """Проверка необходимости отправки значения"""
        max_dev = self.max_deviations.get(tag_id, 0)
        last = self.last_values.get(tag_id)

        if max_dev == 0 or last is None:
            self.last_values[tag_id] = value
            return True

        if abs(value - last) >= max_dev:
            self.last_values[tag_id] = value
            return True

        return False

    @staticmethod
    def apply_jsonata(value: Any, expression: str) -> Any:
        """Применение JSONata выражения"""
        try:
            # Создаем экземпляр парсера JSONata
            expr = Jsonata(expression)  # Используем класс Jsonata
            return expr.evaluate(value)

        except Exception as e:
            error_msg = (
                f"Ошибка обработки данных JSONata. Выражение: '{expression}'. "
                f"Входные данные: {value}. Ошибка: {str(e)}"
            )
            raise DataProcessingError(error_msg) from e
"""
Обработка данных тегов
"""

from typing import Any, Dict
from collections import defaultdict
from jsonata import Jsonata
from .exceptions import DataProcessingError

class DataHandler:
    def __init__(self):
        self.last_values = {}
        self.max_deviations = {}
        self.compiled_expressions: Dict[str, Jsonata] = {}

    def group_tags(self, tags: list) -> dict:
        """Группировка тегов по частоте опроса"""
        groups = defaultdict(list)
        for tag in tags:
            try:
                freq = tag['prsJsonConfigString'].get('frequency', 1000)
                groups[freq].append(tag)
                self.max_deviations[tag['tagId']] = tag['attributes'].get('prsMaxLineDev', 0)

                # Компиляция выражения при инициализации
                expr_str = tag['attributes'].get('prsJSONata')
                if expr_str:
                    self.compiled_expressions[tag['tagId']] = Jsonata(expr_str)
                else:
                    self.compiled_expressions[tag['tagId']] = None

            except Exception as e:
                raise DataProcessingError(
                    f"Ошибка инициализации тега {tag.get('tagId', 'unknown')}: {str(e)}"
                ) from e
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

    def apply_jsonata(self, value: Any, tag_id: str) -> Any:
        """Применение предварительно скомпилированного JSONata выражения"""
        if tag_id not in self.compiled_expressions:
            return value

        expr = self.compiled_expressions[tag_id]
        if not expr:
            return value

        try:
            return expr.evaluate(value)
        except Exception as e:
            raise DataProcessingError(
                f"Ошибка выполнения JSONata для тега {tag_id}: {str(e)}"
            ) from e
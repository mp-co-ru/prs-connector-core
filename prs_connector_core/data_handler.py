from jsonata import Jsonata
from collections import defaultdict
from .config import TagConfig
from .exceptions import DataProcessingError, JsonataError

class DataHandler:
    def __init__(self, connector):
        self.connector = connector
        self.tag_groups: defaultdict[int, list[TagConfig]] = defaultdict(list)
        self.last_values: dict[str, float] = {}
        self.jsonata_cache: dict[str, Jsonata] = {}

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
            if (expr := self.jsonata_cache.get(tag.attributes.prsJSONata)):
                return self._convert_value(
                    expr.evaluate(raw_value),
                    tag.attributes.prsValueTypeCode
                )
            return self._convert_value(raw_value, tag.attributes.prsValueTypeCode)
        except Exception as e:
            raise JsonataError(
                tag_id=str(tag.tagId),
                reason=e
            ) from e
import sys
import asyncio
from time import time
# включаем в проект пакет для низкоуровневой работы с протоколом InduX
from py_indux import InduXClient, InduXException
# из пакета с базовым классом коннектора
# импортируем сам базовый класс и функцию,
# возвращающую текущую метку времени
from prs_connector_core import BaseConnector, ts, main

class InduXConnector(BaseConnector):

    # В общем случае, это единственный метод, который мы должны переопределить у базового класса.
    # Метод запускается базовым классом в виде отдельной задачи
    # каждый раз, когда от платформы приходит исправленная конфигурация
    # (не считая изменений, касающихся тегов, для них - отдельные сообщения).
    # Таким образом, метод _read_tags останавливается и после обновления конфигурации запускается вновь
    async def _read_tags(self):

        async def get_data():
            # Для упрощения предположим, что коннектор читает все теги одним циклом
            # с минимальной частотой, указанной в конфигурации привязанных тегов.
            interval = min(
                tag.prsJsonConfigString.frequency or 5
                for tag in self._config_from_platfrom.tags.values()
            )
            while True:

                # Зафиксируем время начала выполнения чтения.
                start_time = time()

                # Заготовка для пакета данных.
                data = {"data": []}
                # Время в платформе хранится как количество микросекунд, прошедших, начиная
                # с 01-01-1970 00:00:00 UTC.
                # Функция ts() текущую метку времени в указанном формате.
                # Предполагаем, что источник данных не возвращает метку времени вместе со значением,
                # поэтому мы должны присвоить её сами.
                current_time = ts()
                # пробегаем циклом по всем тегам в конфигурации
                for tag_id, tag_config in self._config_from_platfrom.tags:

                    # читаем значение каждого тега из конфигурации
                    value = await self._client.read(tag_config.prsJsonConfigString.source["name"])
                    # формируем пакет данных
                    data["data"].append(

                        {
                            "tagId": tag_id,
                            "data": [

                                [value, current_time]

                            ]

                        }

                    )

                # Помещаем сформированный пакет данных в очередь сообщений.
                self._data_queue.put_nowait(data)

                self._logger.info("Цикл чтения данных завершён.")

                # Считаем, сколько времени заняло чтение данных.
                elapsed = time() - start_time
                wait_time = max(0, interval - elapsed)
                # Делаем задержку между циклами чтения.
                await asyncio.sleep(wait_time)

        while True:
            try:
                # Соединяемся с источником данных
                source = self._config_from_platfrom.prsJsonConfigString.source
                self._client = InduXClient(

                    port=source["port"],
                    baudrate=source["baudrate"],
                    bytesize=source["bytesize"],
                    parity=source["parity"],
                    stopbits=source["stopbits"],
                    timeout=source["timeout"]

                )

                await self._client.connect()

                self._logger.info("Соединение с источником установлено.")

                # Запускаем цикл чтения данных.
                await get_data()
            except InduXException as ex:
                self._logger.exception(f"Ошибка чтения данных: {ex}")

                # При ошибке чтения записываем в каждый тег значение None
                # с кодом качества = 102, что обозначает разрыв связи коннектора
                # с источником данных.
                data = {"data": []}
                current_time = ts()
                for tag_id, tag_config in self._config_from_platfrom.tags:

                    data["data"].append(

                        {"tagId": tag_id, "data": [[None, current_time, 102]]}

                    )

                self._data_queue.put_nowait(data)

if __name__ == "__main__":
    main(InduXConnector)
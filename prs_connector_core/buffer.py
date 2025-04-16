"""
Буферизация данных при потере соединения
"""

import json
from pathlib import Path
from uuid import UUID
import aiofiles

class BufferManager:
    def __init__(self, connector_id: UUID):
        self.buffer_dir = Path("buffer")
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self.file_path = self.buffer_dir / f"{connector_id}.dat"

    async def save(self, packet: dict):
        """Сохранение целых пакетов"""
        if not packet.get('data'):
            return

        async with aiofiles.open(self.file_path, "a") as f:
            await f.write(json.dumps(packet) + "\n")

    async def load(self) -> dict:
        """Загрузка целых пакетов"""
        if not self.file_path.exists():
            return []

        data = {}
        packets = {"data": []}
        async with aiofiles.open(self.file_path, "r") as f:
            async for line in f:
                data_in_line = json.loads(line)
                for data_item in data_in_line["data"]:
                    data_ar = data.setdefault(data_item["tagId"], [])
                    data_ar.extend(data_item["data"])

            for key, item in data.items():
                packets["data"].append({
                    "tagId": key,
                    "data": item
                })

        await self.clear()
        return packets

    async def clear(self):
        """Очистка буфера"""
        if self.file_path.exists():
            self.file_path.unlink()
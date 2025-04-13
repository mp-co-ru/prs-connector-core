"""
Буферизация данных при потере соединения
"""

import json
from pathlib import Path
from uuid import UUID
import aiofiles

class BufferManager:
    def __init__(self, connector_id: UUID):
        self.buffer_dir = Path("buffer") / str(connector_id)
        self.buffer_dir.mkdir(parents=True, exist_ok=True)

    async def save(self, packet: dict):
        """Сохранение целых пакетов"""
        if not packet.get('data'):
            return

        file_path = self.buffer_dir / "pending.dat"
        async with aiofiles.open(file_path, "a") as f:
            await f.write(json.dumps(packet) + "\n")

    async def load(self) -> list:
        """Загрузка целых пакетов"""
        file_path = self.buffer_dir / "pending.dat"
        if not file_path.exists():
            return []

        packets = []
        async with aiofiles.open(file_path) as f:
            async for line in f:
                packets.append(json.loads(line))

        await self.clear()
        return packets

    async def clear(self):
        """Очистка буфера"""
        file_path = self.buffer_dir / "pending.json"
        if file_path.exists():
            file_path.unlink()
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

    async def save(self, data: dict):
        """Сохранение данных в буфер"""
        file_path = self.buffer_dir / "pending.json"
        async with aiofiles.open(file_path, 'a') as f:
            await f.write(json.dumps(data) + '\n')

    async def load(self) -> list:
        """Загрузка данных из буфера"""
        file_path = self.buffer_dir / "pending.json"
        if not file_path.exists():
            return []

        data = []
        async with aiofiles.open(file_path) as f:
            async for line in f:
                data.append(json.loads(line))

        # Очистка буфера после загрузки
        await self.clear()
        return data

    async def clear(self):
        """Очистка буфера"""
        file_path = self.buffer_dir / "pending.json"
        if file_path.exists():
            file_path.unlink()
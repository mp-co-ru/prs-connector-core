"""
Клиент WebSocket для связи с платформой
"""

import websockets
import ssl
import json
from typing import Optional, Dict, Any
from .config import ConnectorConfig
from .exceptions import ConnectionError

class WebSocketClient:
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.connection = None
        self.ssl_context = self._create_ssl_context()

    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Создание SSL контекста для WSS"""
        if self.config.url.startswith('wss://'):
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.load_cert_chain(
                self.config.ssl.certFile,
                keyfile=self.config.ssl.keyFile,
                password=self.config.ssl.certPassword
            )
            return context
        return None

    async def connect(self):
        """Установка соединения"""
        try:
            self.connection = await websockets.connect(
                f"{self.config.url}/{self.config.id}",
                ssl=self.ssl_context
            )
        except Exception as e:
            raise ConnectionError(f"Ошибка подключения: {e}") from e

    async def receive_config(self) -> Dict[str, Any]:
        """Получение конфигурации от платформы"""
        if not self.connection:
            raise ConnectionError("Соединение не установлено")

        try:
            message = await self.connection.recv()
            return json.loads(message)
        except Exception as e:
            raise ConnectionError(f"Ошибка получения данных: {e}") from e

    async def send_data(self, data: Dict):
        """Отправка данных на платформу"""
        try:
            await self.connection.send(json.dumps(data))
        except Exception as e:
            raise ConnectionError(f"Ошибка отправки данных: {e}") from e

    async def close(self):
        """Закрытие соединения"""
        if self.connection:
            await self.connection.close()
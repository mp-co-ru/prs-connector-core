"""
Кастомные исключения для коннекторов Peresvet
"""

class ConnectorBaseError(Exception):
    """Базовое исключение для всех ошибок коннектора"""
    def __init__(self, message: str = "Ошибка коннектора"):
        super().__init__(message)
        self.message = message

class PlatformConnectionError(ConnectorBaseError):
    """Ошибка соединения с платформой"""
    def __init__(self, target: str):
        message = f"Не удалось подключиться к {target}"
        super().__init__(message)

class SourceConnectionError(ConnectorBaseError):
    """Ошибка соединения с источником данных"""
    def __init__(self, target: str):
        message = f"Не удалось подключиться к {target}"
        super().__init__(message)

class ConfigValidationError(ConnectorBaseError):
    """Ошибка валидации конфигурации"""
    def __init__(self, field: str, details: str):
        message = f"Некорректная конфигурация в поле {field}: {details}"
        super().__init__(message)

class DataProcessingError(ConnectorBaseError):
    """Ошибка обработки данных тега"""
    def __init__(self, tag_id: str, reason: str):
        message = f"Ошибка обработки тега {tag_id}: {reason}"
        super().__init__(message)

class PlatformConfigError(ConnectorBaseError):
    """Ошибка конфигурации платформы"""
    def __init__(self, reason: str):
        message = f"Некорректная конфигурация платформы: {reason}"
        super().__init__(message)

class JsonataError(ConnectorBaseError):
    """Ошибка выражения jsonata"""
    def __init__(self, tag_id: str, reason: str):
        message = f"Ошибка обработки jsonata '{reason}' для тега {tag_id}"
        super().__init__(message)
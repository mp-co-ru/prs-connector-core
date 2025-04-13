"""
Пользовательские исключения
"""

class ConnectionError(Exception):
    """Ошибка соединения с платформой"""
    pass

class ConfigValidationError(Exception):
    """Ошибка валидации конфигурации"""
    pass

class DataProcessingError(Exception):
    """Ошибка обработки данных"""
    pass

class ConfigValidationError(Exception):
    """Ошибка валидации конфигурации (наследуем от базового исключения)"""
    pass

class DataProcessingError(Exception):
    """Ошибка обработки данных"""
    pass
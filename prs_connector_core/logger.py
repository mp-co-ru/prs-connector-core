"""
Настройка логирования
"""

import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from uuid import UUID

def configure_logger(connector_id: UUID) -> logging.Logger:
    """Настройка логгера коннектора"""
    logger = logging.getLogger(f'prs_connector_{connector_id}')
    logger.setLevel(logging.INFO)

    # Формат сообщений
    formatter = logging.Formatter(
        '%(asctime)s :: [%(levelname)s] :: %(name)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Ротация логов по времени и размеру
    log_file = Path(f"logs/connector_{connector_id}.log")
    log_file.parent.mkdir(exist_ok=True)

    handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=100*1024*1024,  # 100 MB
        backupCount=10,
        encoding='utf-8'
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
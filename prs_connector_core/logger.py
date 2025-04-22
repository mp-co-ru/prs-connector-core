import logging
from logging.handlers import RotatingFileHandler
from .config import LogConfig

def setup_logger(connector_id: str, config: LogConfig) -> logging.Logger:
    logger = logging.getLogger(f"prs_connector_{connector_id}")
    logger.setLevel(config.level)

    formatter = logging.Formatter(
        '%(asctime)s :: [%(levelname)s] :: %(name)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%f'
    )

    file_handler = RotatingFileHandler(
        config.fileName.format(connector_id=connector_id),
        maxBytes=config.maxBytes,
        backupCount=config.backupCount
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
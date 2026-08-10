"""
Sphinx-расширение: подключает общие страницы документации из prs-connector-core.

Использование в docs/source/conf.py дочернего коннектора::

    extensions = [
        # ...
        "prs_connector_core.sphinx_shared",
    ]

В toctree master-документа добавьте::

    Запуск коннектора в Docker<_prs_connector_core/docker_launch>

Каталог ``_prs_connector_core`` создаётся рядом с conf.py при сборке документации
(копии RST из ``prs_connector_core.share.docs``).
"""

from __future__ import annotations

import shutil
from importlib.resources import files
from pathlib import Path

SHARED_DOCS_DIRNAME = "_prs_connector_core"


def shared_docs_dir() -> Path:
    """Каталог с общими RST/MD внутри установленного пакета."""
    return Path(str(files("prs_connector_core.share").joinpath("docs")))


def sync_shared_docs(dest: Path) -> Path:
    """
    Копирует общие страницы документации в ``dest / _prs_connector_core``.

    Returns:
        Путь к каталогу с скопированными файлами.
    """
    source = shared_docs_dir()
    target = dest / SHARED_DOCS_DIRNAME
    target.mkdir(parents=True, exist_ok=True)

    for item in source.iterdir():
        if item.is_file() and item.suffix in {".rst", ".md"}:
            shutil.copy2(item, target / item.name)

    return target


def _on_config_inited(app, config) -> None:  # noqa: ARG001
    conf_dir = Path(app.confdir)
    sync_shared_docs(conf_dir)


def setup(app):
    app.connect("config-inited", _on_config_inited)
    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }

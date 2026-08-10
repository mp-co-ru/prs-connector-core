"""
Копирование общих шаблонов deployment и страниц документации в проект коннектора.

Примеры::

    python -m prs_connector_core scaffold deployment
    python -m prs_connector_core scaffold deployment --dest /path/to/connector
    python -m prs_connector_core scaffold docs --dest docs/source
    python -m prs_connector_core scaffold readme --dest README.docker.md
"""

from __future__ import annotations

import argparse
import shutil
import sys
from importlib.resources import files
from pathlib import Path


def _share_root() -> Path:
    return Path(str(files("prs_connector_core.share")))


def scaffold_deployment(dest: Path) -> list[Path]:
    """Копирует Docker-шаблоны в ``dest/deployment/docker/``."""
    source = _share_root() / "deployment" / "docker"
    target = dest / "deployment" / "docker"
    target.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for item in source.iterdir():
        if item.is_file():
            out = target / item.name
            shutil.copy2(item, out)
            written.append(out)
    return written


def scaffold_docs(dest: Path) -> list[Path]:
    """Копирует общие RST/MD страницы в ``dest`` (обычно ``docs/source``)."""
    source = _share_root() / "docs"
    dest.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    for item in source.iterdir():
        if item.is_file() and item.suffix in {".rst", ".md"}:
            out = dest / item.name
            shutil.copy2(item, out)
            written.append(out)
    return written


def scaffold_readme(dest: Path) -> Path:
    """
    Копирует Markdown-фрагмент про Docker.

    ``dest`` — путь к файлу (например ``README.docker.md``) или к каталогу
    (тогда создаётся ``docker_launch.md`` внутри).
    """
    source = _share_root() / "docs" / "docker_launch.md"
    if dest.is_dir() or str(dest).endswith("/"):
        dest.mkdir(parents=True, exist_ok=True)
        out = dest / "docker_launch.md"
    else:
        dest.parent.mkdir(parents=True, exist_ok=True)
        out = dest
    shutil.copy2(source, out)
    return out


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m prs_connector_core",
        description="Утилиты prs-connector-core для проектов-коннекторов",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sc = sub.add_parser(
        "scaffold",
        help="Скопировать шаблоны deployment или страницы документации",
    )
    sc.add_argument(
        "what",
        choices=("deployment", "docs", "readme"),
        help="deployment — Docker-шаблоны; docs — RST/MD для Sphinx; "
        "readme — Markdown-фрагмент про Docker",
    )
    sc.add_argument(
        "--dest",
        type=Path,
        default=Path("."),
        help="Каталог или файл назначения (по умолчанию: текущий каталог)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command != "scaffold":
        parser.error(f"неизвестная команда: {args.command}")

    dest: Path = args.dest.resolve()

    if args.what == "deployment":
        written = scaffold_deployment(dest)
        print(f"Скопированы Docker-шаблоны в {dest / 'deployment' / 'docker'}:")
        for path in written:
            print(f"  {path}")
        print(
            "\nПо умолчанию используется network_mode: host "
            "(LAN оборудования + MQTT на хосте)."
        )
        return 0

    if args.what == "docs":
        written = scaffold_docs(dest)
        print(f"Скопированы страницы документации в {dest}:")
        for path in written:
            print(f"  {path}")
        print(
            "\nДобавьте в toctree: docker_launch "
            "или подключите расширение prs_connector_core.sphinx_shared."
        )
        return 0

    if args.what == "readme":
        out = scaffold_readme(dest)
        print(f"Markdown-фрагмент записан: {out}")
        print("Вставьте содержимое в README.md проекта коннектора или дайте ссылку.")
        return 0

    parser.error(f"неизвестный тип scaffold: {args.what}")
    return 2


if __name__ == "__main__":
    sys.exit(main())

[![Лицензия Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-green.svg)
[![Coverage Status](https://coveralls.io/repos/github/mp-co-ru/prs-connector-core/badge.svg?branch=dev)](https://coveralls.io/github/mp-co-ru/prs-connector-core?branch=dev)
[![PyPI](https://img.shields.io/pypi/v/prs-connector-core.svg)](https://www.pypi.org/project/prs-connector-core)

# prs-connector-core

Базовый модуль для создания коннекторов платформы [Пересвет](https://github.com/mp-co-ru/peresvet).

Коннектор - программа, которая может собирать данные с какого-либо источника и отправлять их в платформу. Также коннектор может отсеивать лишние данные и проводить обработку полученных данных с помощью языка [JSONata](https://jsonata.org/).

## Описание проекта

`prs-connector-core` — это Python-модуль, реализующий общую функциональность для коннекторов платформы Пересвет.
Модуль предоставляет базовые классы, на основе которых можно создавать коннекторы для конкретных источников данных (OPC, Modbus, базы данных и др.).

**Базовая функциональность и основные возможности:**

- Установление связи с платформой Пересвет по протоколу MQTT
- Получение от платформы конфигурации:
    - настройка связи с источником данных,
    - параметры логирования,
    - список тегов, по которым необходимо получать данные
- Поддержка защищённого канала связи с SSL/TLS
- Преобразование данных через JSONata
- Отсеивание лишних данных
- Локальное кэширование конфигурации при потере связи с платформой
- Буферизация данных при потере связи с платформой
- Ротация логов

## Работа с проектом

> [!WARNING]
> Работа тестировалась на операционной системе Ubuntu.

### Требования
- Python 3.12 или новее
- pip 21.3+
- pipenv
- pyenv

### Работа над исходным кодом

Клонируйте репозиторий и выполните команды в консоли:
```bash
git clone git@github.com:mp-co-ru/prs-connector-core.git
cd prs-connector-core
pipenv install
```

#### Запуск тестов
```bash
$ pytest
```

#### Сборка пакета
```bash
$ python -m build
```

### Запуск нескольких экземпляров коннектора

Проекты коннекторов, использующие `prs_connector_core.main()`, могут запускать
несколько экземпляров на одной машине. Каждый экземпляр должен получать свой
JSON-файл конфигурации с уникальным `id`:

```bash
python connector.py --config /etc/prs-connectors/modbus-line-1.json
PRS_CONNECTOR_CONFIG=/etc/prs-connectors/modbus-line-2.json python connector.py
```

Для постоянного запуска нескольких экземпляров добавлены шаблоны:

- `deployment/systemd/prs-connector@.service.example`;
- `deployment/docker/compose.example.yml`;
- `deployment/docker/Dockerfile.example`.

Подробная процедура описана в `docs/source/multi_instance_launch.rst`.

### Версионирование

Версия пакета **не хранится в коде вручную**, а вычисляется при сборке из **имени git-тега** с помощью [setuptools-scm](https://github.com/pypa/setuptools_scm).

- **В репозитории** создаются теги вида `v0.9.0`, `v1.0.0` и т.п. При сборке (`python -m build`) setuptools-scm по текущему коммиту и тегам определяет версию и генерирует файл `prs_connector_core/_version.py` (он в `.gitignore` и не коммитится).
- **В коде** версия доступна как `prs_connector_core.__version__`: она берётся из сгенерированного `_version.py`, при его отсутствии — из метаданных установленного пакета, иначе используется `"0.0.0.dev0"` (например, при разработке без тега).
- **Релизы**: при публикации Release в GitHub с тегом (например `v0.9.0`) workflow собирает пакет с этой версией и публикует его в PyPI.
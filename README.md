# prs-connector-core

Базовый модуль для создания коннекторов платформы Peresvet

[![Лицензия Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-green.svg)
[![Coverage](https://coveralls.io/repos/github/vovaman/repo/badge.svg?branch=dev)](https://coveralls.io/github/vovaman/prs-connector-core)

## Описание проекта

`prs-connector-core` — это Python-модуль, реализующий общую функциональность для коннекторов платформы Peresvet. Модуль предоставляет:

- Базовые классы для работы с промышленными протоколами (OPC DA, Modbus, OPC UA и др.)
- Унифицированную систему конфигурации
- Подключение к платформе через WebSocket
- Управление тегами и группами опроса
- Логирование и буферизацию данных
- Автоматическое восстановление соединения

**Основные возможности:**
- Поддержка WSS с SSL/TLS
- Группировка тегов по частоте опроса
- Преобразование данных через JSONata
- Локальное кэширование конфигурации
- Ротация логов

## Установка

### Требования
- Python 3.12 или новее
- pip 21.3+

### Инструкция

1. Клонируйте репозиторий:
```bash
git clone https://github.com/your-account/prs-connector-core.git
cd prs-connector-core
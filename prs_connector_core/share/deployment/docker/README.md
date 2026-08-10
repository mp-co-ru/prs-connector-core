# Запуск нескольких коннекторов через Docker Compose

Файлы `Dockerfile.example` и `compose.example.yml` — типовой запуск нескольких
экземпляров одного коннектора из одного Docker-образа.

По умолчанию используется **`network_mode: host`**: контейнер видит LAN хоста
(оборудование) и может подключаться к MQTT на `127.0.0.1` или к удалённой
платформе. Это рекомендуемый режим для всех коннекторов на `prs-connector-core`.

Каждый сервис получает свой файл конфигурации через `PRS_CONNECTOR_CONFIG`.

## Подготовка

Скопируйте примеры в репозиторий конкретного коннектора:

```bash
python -m prs_connector_core scaffold deployment
```

или вручную из git-репозитория / установленного пакета:

```bash
cp deployment/docker/Dockerfile.example Dockerfile
cp deployment/docker/compose.example.yml compose.yml
mkdir -p configs state
```

`Dockerfile.example` предполагает стартовый файл `/app/connector.py`. Если в
проекте другой файл запуска, измените последнюю строку:

```dockerfile
CMD ["python", "/app/my_connector.py"]
```

## Добавление нового экземпляра

1. Создайте конфигурацию `configs/modbus-line-1.json`.
2. Убедитесь, что `id` в конфигурации уникален среди запущенных коннекторов.
3. Добавьте сервис в `compose.yml`:

```yaml
services:
  connector-modbus-line-1:
    <<: *prs-connector
    environment:
      PRS_CONNECTOR_CONFIG: /configs/modbus-line-1.json
      TZ: Europe/Moscow
    volumes:
      - ./configs:/configs:ro
      - ./state/modbus-line-1:/state
```

4. Запустите сервис:

```bash
docker compose up -d --build connector-modbus-line-1
```

Для следующего экземпляра добавьте новый JSON-файл и новую секцию сервиса с другим
именем и отдельным каталогом `./state/<instance>`.

Рабочий каталог контейнера `/state` хранит файлы, которые создаёт базовый класс:
`platform_config_<connector_id>.json`, `backup_<connector_id>.dat`, временный буфер
и каталог `logs`.

## Документация

Полное описание и способ включения страницы в Sphinx дочернего проекта —
в `docs/source` пакета (`docker_launch`) и в
`python -m prs_connector_core scaffold docs`.

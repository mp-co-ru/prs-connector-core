# Запуск нескольких коннекторов через Docker Compose

Файлы `Dockerfile.example` и `compose.example.yml` показывают типовой запуск
нескольких экземпляров одного коннектора из одного Docker-образа. Каждый сервис
получает свой файл конфигурации через переменную окружения `PRS_CONNECTOR_CONFIG`.

## Подготовка

В репозитории конкретного коннектора скопируйте примеры:

```bash
cp deployment/docker/Dockerfile.example Dockerfile
cp deployment/docker/compose.example.yml compose.yml
mkdir -p configs state
```

`Dockerfile.example` предполагает, что стартовый файл коннектора находится по пути
`/app/connector.py`. Если в вашем проекте другой файл запуска, измените последнюю
строку:

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

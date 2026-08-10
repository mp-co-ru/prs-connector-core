## Запуск коннектора в Docker

Базовый пакет `prs-connector-core` задаёт единый способ запуска коннекторов в Docker.
Проекты-наследники используют те же соглашения: **сеть хоста**, `PRS_CONNECTOR_CONFIG`,
отдельный каталог состояния на экземпляр.

### Почему `network_mode: host`

Коннекторы должны достучаться до оборудования в LAN хоста и до MQTT-платформы
(локальной на `127.0.0.1:1883` или удалённой). Режим `--network host` даёт контейнеру
сетевой стек хоста без NAT и проброса портов.

Это **рекомендуемый режим по умолчанию** для всех коннекторов на базе `prs-connector-core`.

### Контракт запуска

Путь к стартовому JSON (через `prs_connector_core.main()`), по приоритету:

1. `--config` / `-c`
2. позиционный аргумент
3. переменная окружения `PRS_CONNECTOR_CONFIG`
4. `config.json` в текущем каталоге

Обычно задают `PRS_CONNECTOR_CONFIG` и монтируют конфиги как `/configs:ro`.
Рабочий каталог контейнера — `/state`.

### Шаблоны

```bash
python -m prs_connector_core scaffold deployment
```

Либо скопируйте из репозитория `prs-connector-core`:

- `deployment/docker/Dockerfile.example`
- `deployment/docker/compose.example.yml`

В Compose укажите `network_mode: host` и для каждого экземпляра — свой JSON и `./state/<instance>`.

### Пример `docker run`

```bash
docker run -d \
  --name my-prs-connector-line-1 \
  --restart always \
  --network host \
  --workdir /state \
  -e PRS_CONNECTOR_CONFIG=/configs/line-1.json \
  -e TZ=Europe/Moscow \
  -v "$(pwd)/configs:/configs:ro" \
  -v "$(pwd)/state/line-1:/state" \
  my-prs-connector:latest
```

### Документация Sphinx дочернего проекта

```python
# docs/source/conf.py
extensions = [..., "prs_connector_core.sphinx_shared"]
```

В `index.rst` (toctree): `Запуск коннектора в Docker<_prs_connector_core/docker_launch>`

Либо: `python -m prs_connector_core scaffold docs --dest docs/source`

# Запуск нескольких коннекторов через systemd

Шаблон `prs-connector@.service.example` позволяет запускать на одной машине несколько
экземпляров одного и того же коннектора. Каждый экземпляр получает свой файл
конфигурации и отдельный рабочий каталог.

## Установка шаблона

```bash
sudo cp deployment/systemd/prs-connector@.service.example /etc/systemd/system/prs-connector@.service
sudo systemctl daemon-reload
```

В шаблоне по умолчанию ожидается:

- исполняемый Python: `/opt/prs-connector/.venv/bin/python`;
- стартовый файл коннектора: `/opt/prs-connector/connector.py`;
- конфигурации экземпляров: `/etc/prs-connectors/<instance>.json`;
- рабочие каталоги: `/var/lib/prs-connectors/<instance>`.

Если путь к Python или стартовому файлу отличается, создайте env-файл
`/etc/prs-connectors/<instance>.env`:

```ini
CONNECTOR_PYTHON=/opt/my-connector/.venv/bin/python
CONNECTOR_ENTRYPOINT=/opt/my-connector/my_connector.py
```

## Добавление нового экземпляра

1. Создайте конфигурацию, например `/etc/prs-connectors/modbus-line-1.json`.
2. Убедитесь, что поле `id` в JSON соответствует нужному коннектору в платформе
   Пересвет и не повторяется у другого запущенного экземпляра.
3. Запустите сервис:

```bash
sudo systemctl enable --now prs-connector@modbus-line-1.service
```

Для второго экземпляра достаточно добавить новый файл конфигурации и запустить сервис
с другим именем:

```bash
sudo cp /etc/prs-connectors/modbus-line-1.json /etc/prs-connectors/modbus-line-2.json
sudo editor /etc/prs-connectors/modbus-line-2.json
sudo systemctl enable --now prs-connector@modbus-line-2.service
```

Проверка состояния и логов:

```bash
systemctl status prs-connector@modbus-line-1.service
journalctl -u prs-connector@modbus-line-1.service -f
```

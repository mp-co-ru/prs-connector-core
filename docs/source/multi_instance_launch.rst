Запуск нескольких экземпляров коннектора
---------------------------------------

На одном компьютере можно запускать несколько экземпляров одного и того же
коннектора. Каждый экземпляр должен получать свой стартовый JSON-файл
конфигурации с уникальным ``id`` коннектора в платформе Пересвет.

Базовый класс уже разделяет рабочие файлы по ``id``:

* MQTT-топики содержат ``<connector_id>``;
* кэш конфигурации платформы сохраняется в ``platform_config_<connector_id>.json``;
* буфер данных сохраняется в ``backup_<connector_id>.dat``;
* лог по умолчанию пишется в ``logs/prs_connector_<connector_id>.log``.

Поэтому для запуска нескольких экземпляров достаточно стартовать несколько
процессов коннектора и передать каждому процессу свой файл конфигурации.

Выбор файла конфигурации
~~~~~~~~~~~~~~~~~~~~~~~~

Функция ``prs_connector_core.main()`` поддерживает три способа указать конфиг:

.. code-block:: bash

    python connector.py /etc/prs-connectors/modbus-line-1.json
    python connector.py --config /etc/prs-connectors/modbus-line-1.json
    PRS_CONNECTOR_CONFIG=/etc/prs-connectors/modbus-line-1.json python connector.py

Приоритет выбора:

#. аргумент ``--config``;
#. позиционный аргумент;
#. переменная окружения ``PRS_CONNECTOR_CONFIG``;
#. файл ``config.json`` в текущем каталоге.

Позиционный аргумент сохранён для обратной совместимости со старым запуском.
Переменная окружения удобна для systemd и Docker, где путь к конфигурации часто
задаётся вне команды запуска.

Вариант systemd
~~~~~~~~~~~~~~~

В репозитории есть шаблон:
``deployment/systemd/prs-connector@.service.example``.

Скопируйте его в systemd:

.. code-block:: bash

    sudo cp deployment/systemd/prs-connector@.service.example /etc/systemd/system/prs-connector@.service
    sudo systemctl daemon-reload

По умолчанию шаблон ожидает:

* стартовый файл коннектора: ``/opt/prs-connector/connector.py``;
* Python окружение: ``/opt/prs-connector/.venv/bin/python``;
* файлы конфигурации: ``/etc/prs-connectors/<instance>.json``;
* рабочий каталог экземпляра: ``/var/lib/prs-connectors/<instance>``.

Если пути отличаются, создайте файл ``/etc/prs-connectors/<instance>.env``:

.. code-block:: ini

    CONNECTOR_PYTHON=/opt/my-connector/.venv/bin/python
    CONNECTOR_ENTRYPOINT=/opt/my-connector/my_connector.py

Добавление нового коннектора:

.. code-block:: bash

    sudo editor /etc/prs-connectors/modbus-line-1.json
    sudo systemctl enable --now prs-connector@modbus-line-1.service

Для второго экземпляра создайте второй JSON-файл и запустите второй сервис:

.. code-block:: bash

    sudo editor /etc/prs-connectors/modbus-line-2.json
    sudo systemctl enable --now prs-connector@modbus-line-2.service

Имя после ``@`` используется как имя экземпляра и как имя JSON-файла без
расширения. Например, ``prs-connector@modbus-line-2.service`` читает
``/etc/prs-connectors/modbus-line-2.json``.

Вариант Docker Compose
~~~~~~~~~~~~~~~~~~~~~~

В репозитории есть примеры:

* ``deployment/docker/Dockerfile.example``;
* ``deployment/docker/compose.example.yml``.

Один Docker-образ можно использовать для нескольких сервисов:

.. code-block:: yaml

    x-prs-connector: &prs-connector
      image: my-prs-connector:latest
      restart: unless-stopped
      working_dir: /state

    services:
      connector-modbus-line-1:
        <<: *prs-connector
        environment:
          PRS_CONNECTOR_CONFIG: /configs/modbus-line-1.json
        volumes:
          - ./configs:/configs:ro
          - ./state/modbus-line-1:/state

      connector-modbus-line-2:
        <<: *prs-connector
        environment:
          PRS_CONNECTOR_CONFIG: /configs/modbus-line-2.json
        volumes:
          - ./configs:/configs:ro
          - ./state/modbus-line-2:/state

Добавление нового экземпляра:

#. создать новый файл ``configs/<instance>.json``;
#. добавить новый сервис в ``compose.yml`` с ``PRS_CONNECTOR_CONFIG`` на этот файл;
#. запустить ``docker compose up -d --build <service_name>``.

Отдельный каталог ``./state/<instance>`` нужен, чтобы кэш платформенной
конфигурации, буфер и логи каждого экземпляра хранились независимо.

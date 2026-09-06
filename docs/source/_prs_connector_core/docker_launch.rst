Запуск коннектора в Docker
--------------------------

Базовый пакет ``prs-connector-core`` задаёт единый способ запуска коннекторов
в Docker. Проекты-наследники используют те же соглашения: сеть хоста,
``PRS_CONNECTOR_CONFIG``, отдельный каталог состояния на экземпляр.

Почему ``network_mode: host``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Коннекторы платформы Пересвет почти всегда должны одновременно:

* достучаться до оборудования в LAN хоста (PLC, контроллеры, преобразователи);
* подключиться к MQTT-платформе — локальной на ``127.0.0.1:1883`` или удалённой.

Режим ``--network host`` / ``network_mode: host`` даёт контейнеру сетевой стек
хоста: отдельный NAT и проброс портов не нужны, ``127.0.0.1`` внутри контейнера
совпадает с хостом.

Это **рекомендуемый режим по умолчанию** для всех коннекторов на базе
``prs-connector-core``. Режим bridge с ``host.docker.internal`` допустим только
как осознанное исключение (например, если политика площадки запрещает host
network).

Контракт запуска
~~~~~~~~~~~~~~~~

Внутри контейнера процесс стартует через ``prs_connector_core.main()``.
Путь к стартовому JSON задаётся одним из способов (по приоритету):

#. аргумент ``--config`` / ``-c``;
#. позиционный аргумент;
#. переменная окружения ``PRS_CONNECTOR_CONFIG``;
#. файл ``config.json`` в текущем каталоге.

Для Docker обычно задают ``PRS_CONNECTOR_CONFIG`` и монтируют каталог конфигов
как ``/configs:ro``. Рабочий каталог контейнера — ``/state``: туда базовый класс
пишет ``platform_config_<id>.json``, буфер и логи.

Шаблоны в репозитории / пакете
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

В git-репозитории ``prs-connector-core`` и в установленном пакете есть примеры:

* ``deployment/docker/Dockerfile.example``;
* ``deployment/docker/compose.example.yml``.

Скопировать их в проект коннектора можно вручную или командой:

.. code-block:: bash

    python -m prs_connector_core scaffold deployment

``Dockerfile.example`` предполагает стартовый файл ``/app/connector.py``.
Если точка входа другая, измените ``CMD``:

.. code-block:: dockerfile

    CMD ["python", "/app/src/my_connector.py"]

Docker Compose
~~~~~~~~~~~~~~

Пример якоря сервиса (сеть хоста обязательна):

.. code-block:: yaml

    x-prs-connector: &prs-connector
      build:
        context: .
        dockerfile: deployment/docker/Dockerfile.example
      image: my-prs-connector:latest
      restart: unless-stopped
      network_mode: host
      working_dir: /state

    services:
      connector-line-1:
        <<: *prs-connector
        environment:
          PRS_CONNECTOR_CONFIG: /configs/line-1.json
          TZ: Europe/Moscow
        volumes:
          - ./configs:/configs:ro
          - ./state/line-1:/state

      connector-line-2:
        <<: *prs-connector
        environment:
          PRS_CONNECTOR_CONFIG: /configs/line-2.json
          TZ: Europe/Moscow
        volumes:
          - ./configs:/configs:ro
          - ./state/line-2:/state

Запуск:

.. code-block:: bash

    mkdir -p configs state
    docker compose up -d --build

Несколько экземпляров на одном хосте
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

При ``network_mode: host`` все контейнеры разделяют сеть хоста. Это нормально
для коннекторов: они выступают MQTT-клиентами и клиентами оборудования и
обычно не слушают фиксированные порты на хосте.

Для каждого экземпляра нужны:

* свой JSON с уникальным ``id``;
* свой каталог ``./state/<instance>``;
* свой сервис в Compose (или отдельный ``docker run --name ...``).

Запуск через ``docker run``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

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

Продуктовые дистрибутивы
~~~~~~~~~~~~~~~~~~~~~~~~

Отдельные коннекторы могут поставлять архив с ``./run_connector.sh``, офлайн-колёсами
и зеркалом базовых образов. Скрипт установки должен сохранять тот же контракт:
``--network host``, ``PRS_CONNECTOR_CONFIG``, тома ``/configs`` и ``/state``.

Включение этой страницы в документацию дочернего коннектора
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Если у проекта коннектора есть Sphinx-документация, добавьте расширение и пункт
оглавления — страница подтянется из установленного ``prs-connector-core``:

.. code-block:: python

    # docs/source/conf.py
    extensions = [
        # ...
        "prs_connector_core.sphinx_shared",
    ]

.. code-block:: rst

    # docs/source/index.rst — в toctree:
    Запуск коннектора в Docker<_prs_connector_core/docker_launch>

Либо скопируйте RST в дерево исходников документации:

.. code-block:: bash

    python -m prs_connector_core scaffold docs --dest docs/source

.. prs-connector-core documentation master file, created by
   sphinx-quickstart on Wed Apr 23 20:26:28 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

prs-connector-core
==================

**prs-connector-core** - пакет для языка Python.

Главный класс, экспортируемый пакетом - ``BaseConnector``.
Этот класс реализует функциональность, общую для всех коннекторов платформы Пересвет_.

.. _Пересвет: https://github.com/mp-co-ru/peresvet


.. toctree::
   :maxdepth: 2
   :caption: Содержание:

   Описание<description>
   Регистрация коннектора в платформе<registration>
   Логика работы коннектора<work_logic>
   Формат сообщений<message_format>
   Разработка нового коннектора<create_new_connector>
   Класс BaseConnector<baseconnector_class>
   Класс TagGroupReaderConnector<taggroupreaderconnector_class>

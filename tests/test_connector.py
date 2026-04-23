import json
import asyncio
from uuid import uuid4
from types import SimpleNamespace
from typing import Any, cast

import pytest
import aiomqtt

from prs_connector_core.config import TagAttributes, TagPrsJsonConfigStringFromPlatform
from prs_connector_core.connector import BaseConnector, TagGroupReaderConnector, CN_Q_UNLINK_CONNECTOR_TO_SOURCE


class TestConnector(BaseConnector):
    async def _read_tags(self):
        return None


class DummyTagGroupConnector(TagGroupReaderConnector):
    def __init__(self, config_file: str = "config.json") -> None:
        super().__init__(config_file=config_file)
        self.connect_result = True
        self.read_group_calls = []
        self.close_source_calls = 0

    async def _connect_to_source(self) -> bool:
        return self.connect_result

    async def _close_source(self):
        self.close_source_calls += 1
        self._source_connected.clear()

    async def _read_group(self, frequency: float):
        self.read_group_calls.append(frequency)


class DummyJsonata:
    def __init__(self, result):
        self._result = result

    def evaluate(self, _):
        return self._result


def _make_connector(tmp_path, monkeypatch) -> TestConnector:
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "url": "mqtt://localhost",
    }))
    return TestConnector(str(config_file))


def _register_tag(
    conn: TestConnector,
    tag_id: str,
    value_type: int,
    max_dev: float = 0,
):
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=value_type,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=max_dev),
    )
    conn._tag_cache[tag_id] = {"JSONataExpr": None, "lastValue": None}


def _make_tag_group_connector(tmp_path, monkeypatch) -> DummyTagGroupConnector:
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps({
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "url": "mqtt://localhost",
    }))
    return DummyTagGroupConnector(str(config_file))


def test_connector_no_config_file():
    with pytest.raises(RuntimeError):
        TestConnector("nonexistent_config")


def test_connector_config_file(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    assert str(conn._config_from_file.id) == "550e8400-e29b-41d4-a716-446655440000"
    assert conn._config_from_file.url == "mqtt://localhost"


def test_process_tags_data_float_conversion_and_new_order(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=1, max_dev=0)

    payload = {"data": [{"tagId": tag_id, "data": [[123456, "12.5", 100]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": [{"tagId": tag_id, "data": [[123456, 12.5, 100]]}]}
    assert conn._tag_cache[tag_id]["lastValue"] == [123456, 12.5, 100]


def test_process_tags_data_value_only_gets_timestamp_in_first_position(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=1, max_dev=0)
    monkeypatch.setattr("prs_connector_core.connector.now_int", lambda: 222222)

    payload = {"data": [{"tagId": tag_id, "data": [["7.5"]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": [{"tagId": tag_id, "data": [[222222, 7.5]]}]}


def test_process_tags_data_uses_jsonata_on_value_index(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=2, max_dev=0)
    conn._tag_cache[tag_id]["JSONataExpr"] = DummyJsonata(result="converted")

    payload = {"data": [{"tagId": tag_id, "data": [[1000, "raw", 100]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": [{"tagId": tag_id, "data": [[1000, "converted", 100]]}]}


def test_process_tags_data_respects_max_dev_for_numeric_values(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=1, max_dev=5)
    conn._tag_cache[tag_id]["lastValue"] = [1000, 10.0, 100]

    payload = {"data": [{"tagId": tag_id, "data": [[1001, 12.0, 100], [1002, 16.0, 100]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": [{"tagId": tag_id, "data": [[1002, 16.0, 100]]}]}


def test_process_tags_data_emits_on_quality_change(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=1, max_dev=10)
    conn._tag_cache[tag_id]["lastValue"] = [1000, 10.0, 100]

    payload = {"data": [{"tagId": tag_id, "data": [[1001, 10.0, 103]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": [{"tagId": tag_id, "data": [[1001, 10.0, 103]]}]}


def test_reset_tag_cache_last_sent_values_allows_resend_after_platform_restart(tmp_path, monkeypatch):
    """После сброса кэша стабильное значение снова проходит дедупликацию (как «первое»)."""
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=0, max_dev=10)
    conn._tag_cache[tag_id]["lastValue"] = [1, 549, 0]
    payload = {"data": [{"tagId": tag_id, "data": [[2, 549, 0]]}]}
    assert conn._process_tags_data(payload) == {"data": []}
    conn._reset_tag_cache_last_sent_values()
    result = conn._process_tags_data(payload)
    assert result == {"data": [{"tagId": tag_id, "data": [[2, 549, 0]]}]}


def test_process_tags_data_json_compares_dict_content_not_key_order(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    _register_tag(conn, tag_id, value_type=4, max_dev=1)
    conn._tag_cache[tag_id]["lastValue"] = [1000, {"a": 1, "b": 2}, 100]

    payload = {"data": [{"tagId": tag_id, "data": [[1001, {"b": 2, "a": 1}, 100]]}]}
    result = conn._process_tags_data(payload)

    assert result == {"data": []}


@pytest.mark.asyncio
async def test_create_tag_cache_skips_inactive_tag(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=False,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(),
    )

    created = await conn._create_tag_cache(tag_id)
    assert created is False
    assert tag_id not in conn._tag_cache


@pytest.mark.asyncio
async def test_create_tag_cache_with_jsonata(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(JSONata="$sum([1,2])"),
    )

    created = await conn._create_tag_cache(tag_id)
    assert created is True
    assert conn._tag_cache[tag_id]["lastValue"] is None
    assert conn._tag_cache[tag_id]["JSONataExpr"] is not None


@pytest.mark.asyncio
async def test_remove_tag_cache(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._tag_cache[tag_id] = {"lastValue": [1, 2, 100], "JSONataExpr": None}

    await conn._remove_tag_cache(tag_id)
    assert tag_id not in conn._tag_cache


def test_dict_helpers_are_order_independent():
    d1 = {"a": 1, "b": 2}
    d2 = {"b": 2, "a": 1}
    d3 = {"a": 1, "b": 3}

    assert BaseConnector._hash_dict(d1) == BaseConnector._hash_dict(d2)
    assert BaseConnector._dicts_are_equal(d1, d2) is True
    assert BaseConnector._dicts_are_equal(d1, d3) is False


@pytest.mark.asyncio
async def test_get_connector_configuration_updates_and_restarts_reading(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)

    setup_logger_called = {"value": 0}
    save_called = {"value": 0}

    async def long_read_tags():
        await asyncio.sleep(10)

    conn._read_tags_task = asyncio.create_task(asyncio.sleep(10))
    monkeypatch.setattr(conn, "_read_tags", long_read_tags)
    monkeypatch.setattr(conn, "_setup_logger", lambda: setup_logger_called.__setitem__("value", setup_logger_called["value"] + 1))
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: save_called.__setitem__("value", save_called["value"] + 1),
    )

    mes = {
        "data": {
            "prsActive": True,
            "prsEntityTypeCode": 7,
            "prsJsonConfigString": {
                "source": {"host": "127.0.0.1"},
                "log": {"level": "DEBUG", "maxBytes": 1234, "backupCount": 2},
            },
        }
    }

    await conn._get_connector_configuration_from_platform(mes)

    assert conn._config_from_platfrom.prsEntityTypeCode == 7
    assert conn._config_from_platfrom.prsJsonConfigString.source == {"host": "127.0.0.1"}
    assert conn._config_from_platfrom.prsActive is True
    assert setup_logger_called["value"] == 1
    assert save_called["value"] == 1
    assert conn._read_tags_task is not None

    conn._read_tags_task.cancel()
    await asyncio.gather(conn._read_tags_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_get_connector_configuration_without_changes_does_not_save(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)

    setup_logger_called = {"value": 0}
    save_called = {"value": 0}
    monkeypatch.setattr(conn, "_setup_logger", lambda: setup_logger_called.__setitem__("value", setup_logger_called["value"] + 1))
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: save_called.__setitem__("value", save_called["value"] + 1),
    )

    mes = {
        "data": {
            "prsActive": conn._config_from_platfrom.prsActive,
            "prsEntityTypeCode": conn._config_from_platfrom.prsEntityTypeCode,
            "prsJsonConfigString": {
                "source": conn._config_from_platfrom.prsJsonConfigString.source,
                "log": conn._config_from_platfrom.prsJsonConfigString.log.model_dump(),
            },
        }
    }

    await conn._get_connector_configuration_from_platform(mes)
    assert setup_logger_called["value"] == 0
    assert save_called["value"] == 0


@pytest.mark.asyncio
async def test_tags_add_or_changed_full_list_adds_and_removes_tags(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    existing_tag = str(uuid4())
    removed_tag = str(uuid4())
    new_tag = str(uuid4())

    conn._config_from_platfrom.tags[existing_tag] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=0),
    )
    conn._config_from_platfrom.tags[removed_tag] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=0),
    )

    removed = []
    refreshed = {"value": 0}
    saved = {"value": 0}

    async def fake_remove_tag(tag_id):
        removed.append(tag_id)
        conn._config_from_platfrom.tags.pop(tag_id, None)

    async def fake_create_tag_cache(_tag_id):
        return True

    async def fake_refresh():
        refreshed["value"] += 1

    monkeypatch.setattr(conn, "_remove_tag", fake_remove_tag)
    monkeypatch.setattr(conn, "_create_tag_cache", fake_create_tag_cache)
    monkeypatch.setattr(conn, "_refresh_read_tags", fake_refresh)
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: saved.__setitem__("value", saved["value"] + 1),
    )

    mes = {
        "data": {
            "tags": {
                existing_tag: {
                    "prsActive": True,
                    "prsValueTypeCode": 1,
                    "prsJsonConfigString": {"maxDev": 0},
                },
                new_tag: {
                    "prsActive": True,
                    "prsValueTypeCode": 2,
                    "prsJsonConfigString": {"maxDev": 0},
                },
            }
        }
    }

    await conn._tags_add_or_changed(mes, full_list=True)

    assert removed == [removed_tag]
    assert set(conn._config_from_platfrom.tags.keys()) == {existing_tag, new_tag}
    assert refreshed["value"] == 1
    assert saved["value"] == 1


@pytest.mark.asyncio
async def test_tags_deleted_removes_tags_and_saves(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_1 = str(uuid4())
    tag_2 = str(uuid4())
    conn._config_from_platfrom.tags[tag_1] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=0),
    )
    conn._config_from_platfrom.tags[tag_2] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=0),
    )

    removed_cache = []
    saved = {"value": 0}

    async def fake_remove_tag_cache(tag_id):
        removed_cache.append(tag_id)

    monkeypatch.setattr(conn, "_remove_tag_cache", fake_remove_tag_cache)
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: saved.__setitem__("value", saved["value"] + 1),
    )

    await conn._tags_deleted({"data": {"tags": [tag_1]}})

    assert removed_cache == [tag_1]
    assert tag_1 not in conn._config_from_platfrom.tags
    assert tag_2 in conn._config_from_platfrom.tags
    assert saved["value"] == 1


@pytest.mark.asyncio
async def test_command_executes_all_lines(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    executed = []
    monkeypatch.setattr("prs_connector_core.connector.os.system", lambda cmd: executed.append(cmd) or 0)

    await conn._command({"data": {"command": {"lines": ["echo 1", "echo 2"]}}})
    assert executed == ["echo 1", "echo 2"]


@pytest.mark.asyncio
async def test_deleted_deactivates_connector_saves_and_shutdown(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)

    saved = {"value": 0}
    shutdown_called = {"value": 0}

    async def fake_shutdown():
        shutdown_called["value"] += 1

    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: saved.__setitem__("value", saved["value"] + 1),
    )
    monkeypatch.setattr(conn, "_shutdown", fake_shutdown)

    await conn._deleted({"data": {}})

    assert conn._config_from_platfrom.prsActive is False
    assert saved["value"] == 1
    assert shutdown_called["value"] == 1


class DummyMqttClient:
    def __init__(self):
        self.calls = []

    async def publish(self, **kwargs):
        self.calls.append(kwargs)


@pytest.mark.asyncio
async def test_push_data_publishes_when_connected_and_data_present(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    dummy_client = DummyMqttClient()
    cast(Any, conn)._mqtt_client = dummy_client
    conn._mqtt_connected.set()
    conn._canceled = False

    conn._process_tags_data = lambda data: {"data": [{"tagId": "t", "data": [[1, 2, 100]]}]}
    await conn._data_queue.put({"data": [{"tagId": "t", "data": [[1, 2, 100]]}]})

    task = asyncio.create_task(conn._push_data())
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(dummy_client.calls) == 1
    assert dummy_client.calls[0]["topic"] == "prsTag/app_api_client/data_set/*"
    assert dummy_client.calls[0]["retain"] is True


@pytest.mark.asyncio
async def test_push_data_writes_to_buffer_when_disconnected(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._mqtt_connected.clear()
    conn._canceled = False
    conn._buf_file_name = str(tmp_path / "backup_test.dat")

    conn._process_tags_data = lambda data: {"data": [{"tagId": "t", "data": [[1, 2, 100]]}]}
    await conn._data_queue.put({"data": [{"tagId": "t", "data": [[1, 2, 100]]}]})

    task = asyncio.create_task(conn._push_data())
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    written = (tmp_path / "backup_test.dat").read_text()
    assert '"tagId": "t"' in written


@pytest.mark.asyncio
async def test_process_buffer_moves_lines_to_queue_with_processed_flag(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._canceled = False
    conn._mqtt_connected.set()
    conn._buf_file_name = str(tmp_path / "backup_test.dat")
    conn._tmp_buf_file_name = str(tmp_path / "backup_test.tmp")

    (tmp_path / "backup_test.dat").write_text('{"data":[{"tagId":"x","data":[[1,2,100]]}]}\n')

    pushed = []

    class QueueStub:
        def put_nowait(self, js):
            pushed.append(js)
            conn._canceled = True

    cast(Any, conn)._data_queue = QueueStub()

    await conn._process_buffer()

    assert len(pushed) == 1
    assert pushed[0]["processed"] is True


@pytest.mark.asyncio
async def test_refresh_read_tags_cancels_previous_task_and_starts_new(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)

    async def old_task():
        await asyncio.sleep(10)

    async def new_read_tags():
        await asyncio.sleep(10)

    conn._read_tags_task = asyncio.create_task(old_task())
    old_ref = conn._read_tags_task
    monkeypatch.setattr(conn, "_read_tags", new_read_tags)

    await conn._refresh_read_tags()

    assert old_ref.cancelled()
    assert conn._read_tags_task is not None
    assert conn._read_tags_task is not old_ref

    conn._read_tags_task.cancel()
    await asyncio.gather(conn._read_tags_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_handle_messages_dispatches_actions(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._canceled = False
    conn._mqtt_connected.set()

    called = []

    async def full(_): called.append("full")
    async def conn_cfg(_): called.append("connector")
    async def tags_cfg(_): called.append("tags")
    async def tags_del(_): called.append("deleted_tags")
    async def deleted(_): called.append("deleted")
    async def command(_): called.append("command")

    monkeypatch.setattr(conn, "_get_full_configuration_from_platform", full)
    monkeypatch.setattr(conn, "_get_connector_configuration_from_platform", conn_cfg)
    monkeypatch.setattr(conn, "_tags_add_or_changed", tags_cfg)
    monkeypatch.setattr(conn, "_tags_deleted", tags_del)
    monkeypatch.setattr(conn, "_deleted", deleted)
    monkeypatch.setattr(conn, "_command", command)

    actions = [
        "prsConnector.full_configuration",
        "prsConnector.connector_configuration",
        "prsConnector.tags_configuration",
        "prsConnector.tags_deleted",
        "prsConnector.deleted",
        "prsConnector.command",
    ]

    class FakeMessages:
        def __init__(self, payloads):
            self.payloads = payloads
            self.idx = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.idx >= len(self.payloads):
                conn._canceled = True
                raise StopAsyncIteration
            payload = self.payloads[self.idx]
            self.idx += 1
            # Топик должен начинаться с prs2conn, иначе коннектор отправит сообщение в _process_message
            return SimpleNamespace(payload=payload, topic="prs2conn/" + conn._config_from_file.id)

    payloads = [json.dumps({"action": a, "data": {}}).encode("utf8") for a in actions]
    cast(Any, conn)._mqtt_client = SimpleNamespace(messages=FakeMessages(payloads))

    await conn._handle_messages()

    assert called == ["full", "connector", "tags", "deleted_tags", "deleted", "command"]


@pytest.mark.asyncio
async def test_tag_group_create_cache_adds_tag_to_frequency_group(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(frequency=2.5, maxDev=0),
    )

    created = await conn._create_tag_cache(tag_id)

    assert created is True
    assert tag_id in conn._tag_cache
    assert conn._tag_groups[2.5]["tags"] == [tag_id]


@pytest.mark.asyncio
async def test_tag_group_remove_cache_removes_group_and_cancels_task(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    frequency = 1.0

    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(frequency=frequency, maxDev=0),
    )
    conn._tag_cache[tag_id] = {"lastValue": None, "JSONataExpr": None}
    conn._tag_groups[frequency]["tags"] = [tag_id]
    conn._tag_groups[frequency]["task"] = asyncio.create_task(asyncio.sleep(10))

    await conn._remove_tag_cache(tag_id)

    assert tag_id not in conn._tag_cache
    assert frequency not in conn._tag_groups


@pytest.mark.asyncio
async def test_tag_group_remove_cache_unknown_tag_keeps_running(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    unknown_tag = str(uuid4())
    conn._tag_cache[unknown_tag] = {"lastValue": None, "JSONataExpr": None}

    await conn._remove_tag_cache(unknown_tag)

    assert unknown_tag not in conn._tag_cache


@pytest.mark.asyncio
async def test_periodic_task_for_group_calls_read_group(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn._canceled = False
    conn._source_connected.set()

    async def fake_read_group(frequency):
        conn.read_group_calls.append(frequency)
        conn._canceled = True

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(conn, "_read_group", fake_read_group)
    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)

    await conn._periodic_task_for_group(0.5)

    assert conn.read_group_calls == [0.5]


@pytest.mark.asyncio
async def test_read_tags_connect_false_sleeps_and_retries(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn.connect_result = False
    conn._canceled = False
    sleeps = []

    async def fast_sleep(seconds):
        sleeps.append(seconds)
        conn._canceled = True

    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)

    await conn._read_tags()

    assert sleeps == [5]


@pytest.mark.asyncio
async def test_read_tags_disconnect_pushes_bad_quality_data_and_closes_source(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn.connect_result = True
    conn._canceled = False
    conn._source_connected.clear()
    conn._tag_groups[1.0]["tags"] = ["tag-1"]
    conn._tag_cache["tag-1"] = {"lastValue": None, "JSONataExpr": None}

    queue_payloads = []

    class QueueStub:
        def put_nowait(self, item):
            queue_payloads.append(item)

    cast(Any, conn)._data_queue = QueueStub()

    async def fake_close_source():
        conn.close_source_calls += 1
        conn._canceled = True

    async def fast_sleep(_):
        return None

    monkeypatch.setattr(conn, "_close_source", fake_close_source)
    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("prs_connector_core.connector.now_int", lambda: 777)

    await conn._read_tags()

    assert conn.close_source_calls == 1
    assert len(queue_payloads) == 1
    assert queue_payloads[0] == {
        "data": [{
            "tagId": "tag-1",
            "data": [[777, None, CN_Q_UNLINK_CONNECTOR_TO_SOURCE]],
        }]
    }


@pytest.mark.asyncio
async def test_shutdown_cancels_running_tasks(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)

    async def long_task():
        await asyncio.sleep(10)

    conn._handle_messages_task = asyncio.create_task(long_task())
    conn._push_data_task = asyncio.create_task(long_task())
    conn._process_buffer_task = asyncio.create_task(long_task())
    conn._read_tags_task = asyncio.create_task(long_task())

    await conn._shutdown()

    assert conn._handle_messages_task.cancelled()
    assert conn._push_data_task.cancelled()
    assert conn._process_buffer_task.cancelled()
    assert conn._read_tags_task.cancelled()


def test_signal_handlers_set_canceled_flag(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._canceled = False

    conn._handle_signal_win(None, None)
    assert conn._canceled is True

    conn._canceled = False
    conn._handle_signal_unix()
    assert conn._canceled is True


@pytest.mark.asyncio
async def test_run_success_connect_flow(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._config_from_platfrom.prsActive = False

    class FakeClientCM:
        def __init__(self):
            self.subscriptions = []
            self.publishes = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def subscribe(self, topic):
            self.subscriptions.append(topic)

        async def publish(self, topic, payload, retain):
            self.publishes.append((topic, payload, retain))

    fake_client = FakeClientCM()
    shutdown_called = {"value": 0}

    async def fake_shutdown():
        shutdown_called["value"] += 1

    async def fast_sleep(seconds):
        if seconds == 3:
            conn._canceled = True
        return None

    monkeypatch.setattr("prs_connector_core.connector.aiomqtt.Client", lambda **kwargs: fake_client)
    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("prs_connector_core.connector.signal.signal", lambda *_: None)
    monkeypatch.setattr("prs_connector_core.connector.sys.platform", "win32")
    monkeypatch.setattr(conn, "_shutdown", fake_shutdown)

    await conn.run()

    assert shutdown_called["value"] == 1
    assert fake_client.subscriptions == [conn._mqtt_topic_messages_from_platform]
    assert len(fake_client.publishes) == 1
    assert fake_client.publishes[0][0] == f"conn2prs/{conn._config_from_file.id}"


@pytest.mark.asyncio
async def test_run_retries_when_client_context_exit_raises_oserror(tmp_path, monkeypatch):
    """aiomqtt при закрытии клиента может выбросить из __aexit__ не MqttError — цикл run() должен переподключаться."""
    conn = _make_connector(tmp_path, monkeypatch)
    conn._config_from_platfrom.prsActive = False

    class FirstClientExplodesOnExit:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            raise OSError(111, "Connection refused")

        async def subscribe(self, topic):
            return None

        async def publish(self, topic, payload, retain):
            return None

    class GoodClient:
        def __init__(self):
            self.subscriptions: list[str] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def subscribe(self, topic):
            self.subscriptions.append(topic)

        async def publish(self, topic, payload, retain):
            return None

    instances: list[object] = []

    def client_factory(**kwargs):
        if len(instances) == 0:
            c: object = FirstClientExplodesOnExit()
        else:
            c = GoodClient()
        instances.append(c)
        return c

    shutdown_called = {"value": 0}

    async def fake_shutdown():
        shutdown_called["value"] += 1

    inner_sleeps = {"n": 0}

    async def fast_sleep(seconds):
        if seconds == 3:
            inner_sleeps["n"] += 1
            if inner_sleeps["n"] == 1:
                conn._mqtt_connected.clear()
            elif inner_sleeps["n"] >= 2:
                conn._canceled = True
        return None

    monkeypatch.setattr("prs_connector_core.connector.aiomqtt.Client", client_factory)
    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("prs_connector_core.connector.signal.signal", lambda *_: None)
    monkeypatch.setattr("prs_connector_core.connector.sys.platform", "win32")
    monkeypatch.setattr(conn, "_shutdown", fake_shutdown)

    await conn.run()

    assert len(instances) >= 2
    assert shutdown_called["value"] == 1


@pytest.mark.asyncio
async def test_run_handles_mqtt_error_and_retries(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._config_from_platfrom.prsActive = False

    class FailingClientCM:
        async def __aenter__(self):
            raise aiomqtt.MqttError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    shutdown_called = {"value": 0}

    async def fake_shutdown():
        shutdown_called["value"] += 1

    async def fast_sleep(seconds):
        if seconds == 5:
            conn._canceled = True
        return None

    monkeypatch.setattr("prs_connector_core.connector.aiomqtt.Client", lambda **kwargs: FailingClientCM())
    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", fast_sleep)
    monkeypatch.setattr("prs_connector_core.connector.signal.signal", lambda *_: None)
    monkeypatch.setattr("prs_connector_core.connector.sys.platform", "win32")
    monkeypatch.setattr(conn, "_shutdown", fake_shutdown)

    await conn.run()

    assert shutdown_called["value"] == 1


@pytest.mark.asyncio
async def test_get_full_configuration_dispatches_to_two_handlers(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    called_connector = []
    called_tags = []

    async def fake_connector_cfg(mes):
        called_connector.append(mes)

    async def fake_tags_cfg(mes, full_list=False):
        called_tags.append((mes, full_list))

    monkeypatch.setattr(conn, "_get_connector_configuration_from_platform", fake_connector_cfg)
    monkeypatch.setattr(conn, "_tags_add_or_changed", fake_tags_cfg)

    payload = {
        "data": {
            "prsActive": False,
            "prsEntityTypeCode": 5,
            "prsJsonConfigString": {"source": {"x": 1}, "log": {}},
            "tags": {"tag-1": {"prsActive": True, "prsValueTypeCode": 1, "prsJsonConfigString": {}}},
        }
    }
    await conn._get_full_configuration_from_platform(payload)

    assert called_connector[0] == {
        "data": {
            "prsActive": False,
            "prsEntityTypeCode": 5,
            "prsJsonConfigString": {"source": {"x": 1}, "log": {}},
        }
    }
    assert called_tags[0][0] == {"data": {"tags": payload["data"]["tags"]}}
    assert called_tags[0][1] is True


@pytest.mark.asyncio
async def test_get_connector_configuration_active_toggle_to_false(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._config_from_platfrom.prsActive = True
    conn._read_tags_task = asyncio.create_task(asyncio.sleep(10))
    save_called = {"value": 0}
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: save_called.__setitem__("value", save_called["value"] + 1),
    )

    mes = {
        "data": {
            "prsActive": False,
            "prsEntityTypeCode": conn._config_from_platfrom.prsEntityTypeCode,
            "prsJsonConfigString": {
                "source": conn._config_from_platfrom.prsJsonConfigString.source,
                "log": conn._config_from_platfrom.prsJsonConfigString.log.model_dump(),
            },
        }
    }

    await conn._get_connector_configuration_from_platform(mes)
    assert conn._config_from_platfrom.prsActive is False
    assert save_called["value"] == 1


@pytest.mark.asyncio
async def test_tags_add_or_changed_updates_changed_existing_tag(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(maxDev=0),
    )
    removed = []
    refreshed = {"value": 0}
    saved = {"value": 0}

    async def fake_remove_tag(tag_id):
        removed.append(tag_id)
        conn._config_from_platfrom.tags.pop(tag_id, None)

    async def fake_create(_):
        return True

    async def fake_refresh():
        refreshed["value"] += 1

    monkeypatch.setattr(conn, "_remove_tag", fake_remove_tag)
    monkeypatch.setattr(conn, "_create_tag_cache", fake_create)
    monkeypatch.setattr(conn, "_refresh_read_tags", fake_refresh)
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: saved.__setitem__("value", saved["value"] + 1),
    )

    await conn._tags_add_or_changed({
        "data": {
            "tags": {
                tag_id: {
                    "prsActive": True,
                    "prsValueTypeCode": 2,
                    "prsJsonConfigString": {"maxDev": 0},
                }
            }
        }
    })

    assert removed == [tag_id]
    assert conn._config_from_platfrom.tags[tag_id].prsValueTypeCode == 2
    assert refreshed["value"] == 1
    assert saved["value"] == 1


@pytest.mark.asyncio
async def test_tags_add_or_changed_create_cache_failure_does_not_save(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    refreshed = {"value": 0}
    saved = {"value": 0}

    async def fake_create(_):
        return False

    async def fake_refresh():
        refreshed["value"] += 1

    monkeypatch.setattr(conn, "_create_tag_cache", fake_create)
    monkeypatch.setattr(conn, "_refresh_read_tags", fake_refresh)
    monkeypatch.setattr(
        "prs_connector_core.connector.PlatformConfig.save",
        lambda self, _id: saved.__setitem__("value", saved["value"] + 1),
    )

    await conn._tags_add_or_changed({
        "data": {
            "tags": {
                tag_id: {
                    "prsActive": True,
                    "prsValueTypeCode": 1,
                    "prsJsonConfigString": {"maxDev": 0},
                }
            }
        }
    })

    assert refreshed["value"] == 0
    assert saved["value"] == 0


@pytest.mark.asyncio
async def test_handle_messages_mqtt_error_clears_connection_flag(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._mqtt_connected.set()
    conn._canceled = False

    class ErrorMessages:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise aiomqtt.MqttError("oops")

    cast(Any, conn)._mqtt_client = SimpleNamespace(messages=ErrorMessages())

    task = asyncio.create_task(conn._handle_messages())
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert not conn._mqtt_connected.is_set()


@pytest.mark.asyncio
async def test_handle_messages_generic_exception_branch(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._mqtt_connected.set()
    conn._canceled = False

    class ErrorMessages:
        def __aiter__(self):
            return self

        async def __anext__(self):
            conn._canceled = True
            raise ValueError("boom")

    cast(Any, conn)._mqtt_client = SimpleNamespace(messages=ErrorMessages())
    await conn._handle_messages()
    assert conn._canceled is True
    assert not conn._mqtt_connected.is_set()


@pytest.mark.asyncio
async def test_create_tag_cache_returns_false_when_jsonata_init_fails(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(JSONata="$bad("),
    )

    def bad_jsonata(_):
        raise Exception("bad expr")

    monkeypatch.setattr("prs_connector_core.connector.Jsonata", bad_jsonata)
    created = await conn._create_tag_cache(tag_id)
    assert created is False


@pytest.mark.asyncio
async def test_tag_group_create_cache_missing_tag_after_super_returns_false(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())

    async def fake_super_create(self, _tag_id):
        return True

    monkeypatch.setattr(BaseConnector, "_create_tag_cache", fake_super_create)
    created = await conn._create_tag_cache(tag_id)
    assert created is False


@pytest.mark.asyncio
async def test_tag_group_remove_cache_logs_when_tag_not_in_group(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    tag_id = str(uuid4())
    frequency = 3.0
    conn._config_from_platfrom.tags[tag_id] = TagAttributes(
        prsActive=True,
        prsValueTypeCode=1,
        prsJsonConfigString=TagPrsJsonConfigStringFromPlatform(frequency=frequency, maxDev=0),
    )
    conn._tag_groups[frequency]["tags"] = []
    conn._tag_cache[tag_id] = {"lastValue": None, "JSONataExpr": None}

    await conn._remove_tag_cache(tag_id)
    assert frequency not in conn._tag_groups
    assert tag_id not in conn._tag_cache


@pytest.mark.asyncio
async def test_periodic_task_for_group_cancelled_branch(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn._source_connected.set()
    conn._canceled = False

    async def slow_read_group(frequency):
        await asyncio.sleep(10)

    monkeypatch.setattr(conn, "_read_group", slow_read_group)
    task = asyncio.create_task(conn._periodic_task_for_group(1.0))
    await asyncio.sleep(0.05)
    task.cancel()
    result = await asyncio.gather(task, return_exceptions=True)
    assert result[0] is None


@pytest.mark.asyncio
async def test_read_tags_source_connected_sleep_then_cancelled_branch(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn.connect_result = True
    conn._canceled = False
    conn._source_connected.set()
    conn._tag_groups[1.0]["tags"] = ["tag-1"]

    async def cancel_sleep(_):
        raise asyncio.CancelledError

    monkeypatch.setattr("prs_connector_core.connector.asyncio.sleep", cancel_sleep)

    await conn._read_tags()
    assert conn.close_source_calls == 1


@pytest.mark.asyncio
async def test_read_tags_generic_exception_branch(tmp_path, monkeypatch):
    conn = _make_tag_group_connector(tmp_path, monkeypatch)
    conn._canceled = False

    async def broken_connect():
        conn._canceled = True
        raise RuntimeError("err")

    monkeypatch.setattr(conn, "_connect_to_source", broken_connect)
    await conn._read_tags()
    assert conn._canceled is True


@pytest.mark.asyncio
async def test_process_buffer_exception_branch_exits_when_canceled(tmp_path, monkeypatch):
    conn = _make_connector(tmp_path, monkeypatch)
    conn._mqtt_connected.set()
    conn._canceled = False

    async def broken_stat(_):
        conn._canceled = True
        raise RuntimeError("stat failed")

    monkeypatch.setattr("prs_connector_core.connector.aiofiles.os.stat", broken_stat)
    await conn._process_buffer()
    assert conn._canceled is True

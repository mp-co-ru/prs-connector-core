from prs_connector_core.data_handler import DataHandler

def test_tag_grouping():
    handler = DataHandler()
    tags = [
        {"prsJsonConfigString": {"frequency": 1000}},
        {"prsJsonConfigString": {"frequency": 5000}},
        {"prsJsonConfigString": {"frequency": 1000}}
    ]

    groups = handler.group_tags(tags)
    assert len(groups[1000]) == 2
    assert len(groups[5000]) == 1

def test_value_processing():
    handler = DataHandler()
    tag = {"tagId": "test", "attributes": {"prsMaxLineDev": 1}}

    assert handler.process_value("test", 5) is True  # Первое значение
    assert handler.process_value("test", 5.5) is True  # Разница 0.5 >= 1? Нет
    assert handler.process_value("test", 6.6) is True  # Разница 1.6 >= 1

def test_jsonata_processing():
    handler = DataHandler()
    result = handler.apply_jsonata(
        {"value": 42},
        "value * 2"
    )
    assert result == 84
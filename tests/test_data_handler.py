import pytest
from prs_connector_core.data_handler import DataHandler, MetricsCollector
from prs_connector_core.exceptions import DataProcessingError

@pytest.fixture
def data_handler():
    return DataHandler()

def test_group_tags_with_valid_jsonata(data_handler):
    tags = [
        {
            "tagId": "tag1",
            "prsJsonConfigString": {"frequency": 1000},
            "attributes": {
                "prsJSONata": "value * 2",
                "prsMaxLineDev": 1
            }
        }
    ]

    groups = data_handler.group_tags(tags)
    assert 1000 in groups
    assert "tag1" in data_handler.compiled_expressions

def test_group_tags_with_invalid_jsonata(data_handler):
    tags = [
        {
            "tagId": "tag1",
            "prsJsonConfigString": {"frequency": 1000},
            "attributes": {
                "prsJSONata": "invalid $jsonata",
                "prsMaxLineDev": 1
            }
        }
    ]

    with pytest.raises(DataProcessingError):
        data_handler.group_tags(tags)

def test_metrics_collection():
    metrics = MetricsCollector()
    metrics.log_processing(0.1)
    metrics.log_jsonata(0.05)
    metrics.log_send(0.02)
    metrics.log_failure()

    stats = metrics.get_stats()
    assert stats["total_processed"] == 1
    assert stats["avg_processing_time"] == 0.1
    assert stats["failure_rate"] == 0.25
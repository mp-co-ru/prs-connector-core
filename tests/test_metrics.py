from prs_connector_core.data_handler import MetricsCollector

def test_metrics_reset():
    """Тестирование сброса метрик"""
    metrics = MetricsCollector()
    metrics.log_processing(0.1)
    metrics.log_jsonata(0.05)
    metrics.log_send(0.02)
    metrics.log_failure()
    metrics.reset()

    stats = metrics.get_stats()
    assert stats["total_processed"] == 0
    assert stats["failure_rate"] == 0
    assert stats["avg_processing_time"] == 0
    assert stats["avg_jsonata_time"] == 0
    assert stats["avg_send_time"] == 0
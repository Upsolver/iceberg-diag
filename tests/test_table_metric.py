import pytest

from icebergdiag.metrics.table_metric import IntMetric, DurationMetric, SizeMetric, MetricName, TableMetric


@pytest.mark.parametrize("before, after, expected_before, expected_after", [
    (5, 10, "5", "10"),
    (1, 0, "1", "0")
])
def test_int_metric_values(before, after, expected_before, expected_after):
    metric = IntMetric(MetricName.FILE_COUNT, before, after)
    assert metric.get_before_value() == expected_before
    assert metric.get_after_value() == expected_after


@pytest.mark.parametrize("milliseconds_before, milliseconds_after, expected_before, expected_after, improvement", [
    (5500, 4500, "5.5s", "4.5s", "18.18%"),
    (9, 0, "<0.01s", "0s", "0.00%"),
    (10, 1, "0.01s", "<0.01s", "90.00%"),
    (3600000, 7200000, "1h 0m 0s", "2h 0m 0s", "-100.00%"),
    (125000, 75000, "2m 5s", "1m 15s", "40.00%"),
    (60000, 120000, "1m 0s", "2m 0s", "-100.00%")
])
def test_duration_metric_values(milliseconds_before, milliseconds_after, expected_before, expected_after, improvement):
    metric = DurationMetric(MetricName.FULL_SCAN_OVERHEAD, milliseconds_before, milliseconds_after)
    assert metric.get_before_value() == expected_before
    assert metric.get_after_value() == expected_after
    assert metric.get_improvement_value() == improvement


@pytest.mark.parametrize("bytes_before, bytes_after, expected_before, expected_after", [
    (1234, 2345, "1.21 KB", "2.29 KB"),
    (1500, 3500, "1.46 KB", "3.42 KB"),
    (1048576, 3145728, "1.00 MB", "3.00 MB"),
    (2123456, 5123456, "2.03 MB", "4.89 MB"),
    (1073741824, 3221225472, "1.00 GB", "3.00 GB"),
    (1149237760, 2382364672, "1.07 GB", "2.22 GB"),
    (1099511627776, 2199023255552, "1.00 TB", "2.00 TB"),
    (1234567890123, 2345678901234, "1.12 TB", "2.13 TB"),
    (456, 789, "456.00 B", "789.00 B"),
    (98765, 54321, "96.45 KB", "53.05 KB")
])
def test_size_metric_values(bytes_before, bytes_after, expected_before, expected_after):
    metric = SizeMetric(MetricName.TOTAL_TABLE_SIZE, bytes_before, bytes_after)
    assert metric.get_before_value() == expected_before
    assert metric.get_after_value() == expected_after


def test_improvement_calculation():
    metric = IntMetric(MetricName.FILE_COUNT, 10, 5)
    assert metric.get_improvement_value() == "50.00%"


@pytest.mark.parametrize("before, after, expected_improvement", [
    (100, 50, "50.00%"),
    (100, 150, "-50.00%"),
    (100, 100, "0.00%"),
    (100, 0, "100.00%"),
    (0, 0, "0.00%"),
    (1000000, 500000, "50.00%"),
    (12345678, 1234567, "90.00%"),
    (456, 107, "76.54%"),
    (100, None, "")
])
def test_improvement_cases(before, after, expected_improvement):
    metric = IntMetric(MetricName.FILE_COUNT, before, after)
    assert metric.get_improvement_value() == expected_improvement


@pytest.mark.parametrize("metric_name, metric_type, before, after", [
    (MetricName.FULL_SCAN_OVERHEAD, DurationMetric, 1000, 500),
    (MetricName.WORST_SCAN_OVERHEAD, DurationMetric, 2000, 1500),
    (MetricName.FILE_COUNT, IntMetric, 10, 5),
    (MetricName.WORST_FILE_COUNT, IntMetric, 20, 15),
    (MetricName.AVG_FILE_SIZE, SizeMetric, 1024, 512),
    (MetricName.WORST_AVG_FILE_SIZE, SizeMetric, 2048, 1024),
    (MetricName.TOTAL_TABLE_SIZE, SizeMetric, 4096, 2048),
    (MetricName.LARGEST_PARTITION_SIZE, SizeMetric, 8192, 4096),
    (MetricName.TOTAL_PARTITIONS, IntMetric, 3, 2)
])
def test_create_metrics(metric_name, metric_type, before, after):
    metric = TableMetric.create_metric(metric_name, before, after)
    assert isinstance(metric, metric_type)
    assert metric.name == metric_name
    assert metric.before == before
    assert metric.after == after


def test_create_metric_error_handling():
    with pytest.raises(ValueError):
        TableMetric.create_metric("unknown_metric", 10, 20)

import pytest
from pyiceberg.manifest import DataFileContent
from pyiceberg.typedef import Record

from icebergdiag.metrics.table_metric import MetricName
from icebergdiag.metrics.table_metrics import MetricsCalculator


class DataFile:
    def __init__(self, partition, file_size_in_bytes, data_file=False):
        self.partition = partition
        self.file_size_in_bytes = file_size_in_bytes
        if data_file:
            self.content = DataFileContent.DATA
        else:
            self.content = DataFileContent.EQUALITY_DELETES


@pytest.fixture(scope="function")
def scan():
    partitions = ["partition1", "partition2", "partition3"]
    scan_example = []

    for i in range(1, 301):
        partition = Record(partition=partitions[i % len(partitions)])
        file_size = (12 + (i % 13)) * 1024 * 1024
        if i % 3 == 0:
            delete_files = [DataFile(partition, 10 * 1024 * 1024), DataFile(partition, 5 * 1024 * 1024)]  # 15 MB total
        elif i % 3 == 1:
            delete_files = [DataFile(partition, 20 * 1024 * 1024)]  # 20 MB total
        else:
            delete_files = [DataFile(partition, 5 * 1024 * 1024), DataFile(partition, 5 * 1024 * 1024),
                            DataFile(partition, 10 * 1024 * 1024)]  # 20 MB total

        scan_example.extend([DataFile(partition, file_size, True)] + delete_files)
    # Total data files: 900
    return scan_example


@pytest.fixture(scope="function")
def computed_metrics(scan):
    manifest_files_count = 10
    return MetricsCalculator.compute_metrics(scan, manifest_files_count)


def test_file_count(computed_metrics):
    file_count_metric = next(m for m in computed_metrics if m.name == MetricName.FILE_COUNT)
    assert file_count_metric.before == 900
    assert file_count_metric.after == 9
    assert file_count_metric.improvement == 99.0


def test_worst_file_count(computed_metrics):
    worst_file_count = next(m for m in computed_metrics if m.name == MetricName.WORST_FILE_COUNT)
    assert worst_file_count.before == 400
    assert worst_file_count.after == 3
    assert worst_file_count.improvement == 99.25


def test_full_scan_overhead(computed_metrics):
    full_scan_overhead = next(m for m in computed_metrics if m.name == MetricName.FULL_SCAN_OVERHEAD)
    assert full_scan_overhead.before == 1810
    assert full_scan_overhead.after == 180
    assert full_scan_overhead.improvement == 90.05524861878453


def test_worst_scan_overhead(computed_metrics):
    worst_scan_overhead = next(m for m in computed_metrics if m.name == MetricName.WORST_SCAN_OVERHEAD)
    assert worst_scan_overhead.before == 800
    assert worst_scan_overhead.after == 60
    assert worst_scan_overhead.improvement == 92.5


def test_avg_file_size(computed_metrics):
    avg_file_size = next(m for m in computed_metrics if m.name == MetricName.AVG_FILE_SIZE)
    assert avg_file_size.before == 18856891.733333334


def test_total_table_size(computed_metrics):
    total_table_size = next(m for m in computed_metrics if m.name == MetricName.TOTAL_TABLE_SIZE)
    assert total_table_size.before == 11424235520


def test_largest_partition(computed_metrics):
    total_partition = next(m for m in computed_metrics if m.name == MetricName.LARGEST_PARTITION_SIZE)
    assert total_partition.before == 3982491648


def test_total_partitions(computed_metrics):
    total_partitions = next(m for m in computed_metrics if m.name == MetricName.TOTAL_PARTITIONS)
    assert total_partitions.before == 3

from collections import defaultdict
from typing import Iterable, List, Dict, Tuple

from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.typedef import Record

from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import TableMetric, MetricName


class TableMetrics:
    def __init__(self, table: Table, metrics: List[TableMetric]):
        self.table = table
        self.metrics = metrics


FETCH_SIZE = 32 * 1024 * 1024
MAX_GROUP_BYTE_SIZE = 500 * 1024 * 1024
MAX_FILES_PER_GROUP = 500
MILLISECONDS_PER_SCAN = 10


class PartitionMetrics:
    """
    Class to hold metrics for each partition.
    """

    def __init__(self):
        self.file_count = 0
        self.total_size = 0
        self.scan_overhead = 0

    def average_file_size(self):
        """Returns the average file size for the partition."""
        return self.total_size / self.file_count if self.file_count > 0 else 0

    def is_empty(self):
        """Checks if the partition is empty (no files)."""
        return self.file_count == 0


class MetricsCalculator:
    """
    Helper class to calculate table metrics.
    """

    @staticmethod
    def compute_metrics(files: Iterable[DataFile]) -> List[TableMetric]:
        """Computes various metrics for the table."""
        metrics = {name: 0 for name in MetricName}
        partition_files = defaultdict(list)
        partition_metrics = defaultdict(PartitionMetrics)

        for file in files:
            partition = MetricsCalculator.deterministic_repr(file.partition)
            file_size = file.file_size_in_bytes
            read_cost = MetricsCalculator.calc_read_file_cost(file_size)
            overhead = read_cost * MILLISECONDS_PER_SCAN

            metrics[MetricName.FILE_COUNT] += 1
            metrics[MetricName.TOTAL_TABLE_SIZE] += file_size
            metrics[MetricName.FULL_SCAN_OVERHEAD] += overhead

            partition_metric = partition_metrics[partition]
            partition_metric.file_count += 1
            partition_metric.total_size += file_size
            partition_metric.scan_overhead += overhead
            partition_files[partition].append(file)

        metrics, worst_partitions = MetricsCalculator._update_avg_and_worst_metrics(metrics, partition_metrics)
        after_metrics = MetricsCalculator._compute_after_metrics(partition_files, worst_partitions)

        return [TableMetric.create_metric(name, value, after_metrics.get(name)) for name, value in metrics.items()]

    @staticmethod
    def deterministic_repr(record: Record) -> str:
        """Generate a deterministic string representation of a Record."""
        items = sorted(record.__dict__.items())
        item_strs = [f"{key}={repr(value)}" for key, value in items if not key.startswith('_')]
        return f"{record.__class__.__name__}[{', '.join(item_strs)}]"

    @staticmethod
    def _update_avg_and_worst_metrics(
            metrics: Dict[MetricName, int],
            partition_metrics: Dict[str, PartitionMetrics]
    ) -> Tuple[Dict[MetricName, int], Dict[MetricName, str]]:
        """
        Updates metrics with average and worst-case values based on the consolidated partition data.
        """
        total_files = metrics[MetricName.FILE_COUNT]
        metrics[MetricName.AVG_FILE_SIZE] = (metrics[MetricName.TOTAL_TABLE_SIZE] / total_files) if total_files else 0

        worst_partitions_info = {
            MetricName.WORST_FILE_COUNT: max(partition_metrics.items(), key=lambda x: x[1].file_count),
            MetricName.WORST_SCAN_OVERHEAD: max(partition_metrics.items(), key=lambda x: x[1].scan_overhead)
        }

        metrics[MetricName.WORST_FILE_COUNT] = worst_partitions_info[MetricName.WORST_FILE_COUNT][1].file_count
        metrics[MetricName.WORST_SCAN_OVERHEAD] = worst_partitions_info[MetricName.WORST_SCAN_OVERHEAD][1].scan_overhead
        metrics[MetricName.WORST_AVG_FILE_SIZE] = min(
            (p.average_file_size() for p in partition_metrics.values() if not p.is_empty()), default=0)
        metrics[MetricName.LARGEST_PARTITION_SIZE] = max((p.total_size for p in partition_metrics.values()), default=0)

        worst_partitions = {k: v[0] for k, v in worst_partitions_info.items()}

        return metrics, worst_partitions

    @staticmethod
    def _compute_after_metrics(partition_file_sizes: Dict[str, List[DataFile]],
                               worst_partitions: Dict[MetricName, str]) -> Dict[MetricName, int]:
        """Computes metrics after partitioning."""
        after = {
            MetricName.FILE_COUNT: 0,
            MetricName.WORST_FILE_COUNT: 0,
            MetricName.FULL_SCAN_OVERHEAD: 0,
            MetricName.WORST_SCAN_OVERHEAD: 0
        }

        partition_new_sizes = {}
        for partition, files in partition_file_sizes.items():
            data_files_sizes = [file.file_size_in_bytes for file in files if file.content == DataFileContent.DATA]
            partition_new_sizes[partition] = MetricsCalculator.build_partition_groups(data_files_sizes,
                                                                                      MAX_GROUP_BYTE_SIZE,
                                                                                      MAX_FILES_PER_GROUP)

        for partition_name, new_partition in partition_new_sizes.items():
            files_count = len(new_partition)
            after[MetricName.FILE_COUNT] += files_count
            partition_cost = sum(MetricsCalculator.calc_read_file_cost(sum(group)) for group in new_partition)
            partition_overhead = partition_cost * MILLISECONDS_PER_SCAN
            after[MetricName.FULL_SCAN_OVERHEAD] += partition_overhead

            if partition_name == worst_partitions[MetricName.WORST_FILE_COUNT]:
                after[MetricName.WORST_FILE_COUNT] += files_count
            if partition_name == worst_partitions[MetricName.WORST_SCAN_OVERHEAD]:
                after[MetricName.WORST_SCAN_OVERHEAD] += partition_overhead

        return after

    @staticmethod
    def _create_metric_instances(before_metrics: Dict[MetricName, int],
                                 after_metrics: Dict[MetricName, int]) -> List[TableMetric]:
        """
        Create instances of TableMetric from the metrics dictionary.

        Args:
            before_metrics (Dict[MetricName, int]): Dictionary of 'before' metrics.
            after_metrics (Dict[MetricName, int]): Dictionary of 'after' metrics.

        Returns:
            List[TableMetric]: A list of TableMetric instances.
        """
        metric_instances = []
        for metric_name, before_value in before_metrics.items():
            after_value = after_metrics.get(metric_name)
            metric_instance = TableMetric.create_metric(metric_name, before_value, after_value)
            metric_instances.append(metric_instance)

        return metric_instances

    @staticmethod
    def build_partition_groups(partition_files_sizes: List[int],
                               max_bytes_per_group: int,
                               max_files: int) -> List[List[int]]:
        """Builds groups of partition files based on size constraints."""
        sorted_sizes = sorted(partition_files_sizes)
        result, current_group = [], []
        current_size_bytes = 0

        for file_size in sorted_sizes:
            if len(current_group) >= max_files or current_size_bytes > max_bytes_per_group:
                result.append(current_group)
                current_group = []
                current_size_bytes = 0
            current_group.append(file_size)
            current_size_bytes += file_size

        if current_group:
            result.append(current_group)

        return result

    @staticmethod
    def calc_read_file_cost(bytes_size: int) -> int:
        """
        Calculate the cost of reading a file based on its size.

        Args:
            bytes_size (int): Size of the file in bytes.

        Returns:
            int: The calculated read file cost.
        """
        return bytes_size // FETCH_SIZE + 2

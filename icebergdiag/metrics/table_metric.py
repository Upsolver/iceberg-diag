from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional

from icebergdiag.utils import OrderedEnum

T = TypeVar('T')


class MetricName(OrderedEnum):
    FULL_SCAN_OVERHEAD = "Full Scan Overhead"
    WORST_SCAN_OVERHEAD = "Worst Partition Scan Overhead"
    FILE_COUNT = "Total File Count"
    WORST_FILE_COUNT = "Worst Partition File Count"
    AVG_FILE_SIZE = "Avg Data File Size"
    WORST_AVG_FILE_SIZE = "Worst Partition Avg Data File Size"
    TOTAL_TABLE_SIZE = "Total Table Size"
    LARGEST_PARTITION_SIZE = "Largest Partition Size"
    TOTAL_PARTITIONS = "Total Partitions"


class MetricConfig:
    def __init__(self, metric_type, local_mode_supported, show_improvement):
        self.metric_type = metric_type
        self.local_mode_supported = local_mode_supported
        self.show_improvement = show_improvement


class TableMetric(ABC, Generic[T]):
    """
    Abstract base class for a table metric.

    Attributes:
        name (MetricName): The name of the metric.
        before (T): The value of the metric before a certain operation.
        after (Optional[T]): The value of the metric after the operation.
        improvement (Optional[float]): The calculated improvement.
    """

    def __init__(self, name: MetricName,
                 before: T,
                 after: Optional[T] = None,
                 display_in_local: bool = True,
                 display_improvement: bool = True):
        self.name = name
        self.before = before
        self.after = after
        self.improvement = self._calculate_improvement() if after is not None else None
        self.display_in_local = display_in_local
        self.display_improvement = display_improvement

    def __eq__(self, other):
        return self.name == other.name and self.before == other.before and self.after == self.after

    @abstractmethod
    def get_before_value(self) -> str:
        pass

    @abstractmethod
    def get_after_value(self) -> str:
        pass

    def get_improvement_value(self) -> str:
        if not self.display_improvement:
            return ""
        return f"{self._calculate_improvement():.2f}%" if self.improvement is not None else ""

    def _calculate_improvement(self) -> float:
        if self.before == 0 and self.after == 0:
            return 0
        return (1 - self.after / self.before) * 100 if self.before != 0 else float('inf')

    @staticmethod
    def create_metric(metric_name: MetricName, before_value: T, after_value: Optional[T] = None) -> 'TableMetric':
        """
        Create a metric object based on the metric name.

        Args:
            metric_name (MetricName): The name of the metric to create.
            before_value (T): The 'before' value for the metric.
            after_value (Optional[T]): The 'after' value for the metric.

        Returns:
            TableMetric: An instance of a subclass of TableMetric.
        """

        metric_map = {
            MetricName.FULL_SCAN_OVERHEAD: MetricConfig(DurationMetric, True, True),
            MetricName.WORST_SCAN_OVERHEAD: MetricConfig(DurationMetric, True, True),
            MetricName.FILE_COUNT: MetricConfig(IntMetric, True, True),
            MetricName.WORST_FILE_COUNT: MetricConfig(IntMetric, True, True),
            MetricName.AVG_FILE_SIZE: MetricConfig(SizeMetric, True, False),
            MetricName.WORST_AVG_FILE_SIZE: MetricConfig(SizeMetric, False, False),
            MetricName.TOTAL_TABLE_SIZE: MetricConfig(SizeMetric, True, True),
            MetricName.LARGEST_PARTITION_SIZE: MetricConfig(SizeMetric, True, True),
            MetricName.TOTAL_PARTITIONS: MetricConfig(IntMetric, True, True)
        }
        if metric_name in metric_map:
            metric_config = metric_map[metric_name]
            return metric_config.metric_type(metric_name, before_value, after_value,
                                             metric_config.local_mode_supported, metric_config.show_improvement)
        else:
            raise ValueError(f"Unknown metric name: {metric_name}")


class IntMetric(TableMetric[int]):
    """
    Metric class for whole number values.
    """

    def get_before_value(self) -> str:
        return f"{self.before}"

    def get_after_value(self) -> str:
        return f"{self.after if self.after is not None else ''}"


class DurationMetric(TableMetric[int]):
    """
    Metric class for duration values, assumed to be in milliseconds.
    """

    def get_before_value(self) -> str:
        return self._format_duration(self.before)

    def get_after_value(self) -> str:
        return self._format_duration(self.after) if self.after is not None else ""

    def get_improvement_value(self) -> str:
        if self.before < 10 and self.after < 10:
            return "0.00%"
        return super().get_improvement_value()


    @staticmethod
    def _format_duration(milliseconds: int) -> str:
        """
        Format a duration from milliseconds to a human-readable string.
        """
        total_seconds = milliseconds / 1000
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = total_seconds % 60
        if hours > 0:
            return f"{hours}h {minutes}m {int(seconds)}s"
        elif minutes > 0:
            return f"{minutes}m {int(seconds)}s"
        else:
            if 0 < seconds < 0.01:
                return "<0.01s"
            return f"{seconds:.2f}".rstrip('0').rstrip('.') + "s"


class SizeMetric(TableMetric[float]):
    """
    Metric class for size values, assumed to be in bytes.
    """

    def get_before_value(self) -> str:
        return self._format_size(self.before)

    def get_after_value(self) -> str:
        return self._format_size(self.after) if self.after is not None else ""

    @staticmethod
    def _format_size(size: float) -> str:
        """
        Format a size from bytes to a human-readable string with units.
        """
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        unit_index = 0

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        return f"{size:.2f} {units[unit_index]}"

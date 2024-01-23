from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, TypeVar, Optional

T = TypeVar('T')


class MetricName(Enum):
    FULL_SCAN_OVERHEAD = "Full Scan Overhead"
    WORST_SCAN_OVERHEAD = "Worst Partition Scan Overhead"
    FILE_COUNT = "Number of Files"
    WORST_FILE_COUNT = "Worst Partition Number of Files"
    AVG_FILE_SIZE = "Avg File Size"
    WORST_AVG_FILE_SIZE = "Worst Partition Avg File Size"
    TOTAL_TABLE_SIZE = "Total Table Size"
    LARGEST_PARTITION_SIZE = "Largest Partition Size"


class TableMetric(ABC, Generic[T]):
    """
    Abstract base class for a table metric.

    Attributes:
        name (MetricName): The name of the metric.
        before (T): The value of the metric before a certain operation.
        after (Optional[T]): The value of the metric after the operation.
        improvement (Optional[float]): The calculated improvement.
    """
    def __init__(self, name: MetricName, before: T, after: Optional[T] = None):
        self.name = name
        self.before = before
        self.after = after
        self.improvement = self._calculate_improvement() if after is not None else None

    @abstractmethod
    def get_before_value(self) -> str:
        pass

    @abstractmethod
    def get_after_value(self) -> str:
        pass

    def get_improvement_value(self) -> str:
        return f"{self._calculate_improvement()-100:.2f}%" if self.improvement is not None else ""

    def _calculate_improvement(self) -> float:
        return (self.before / self.after) * 100 if self.before != 0 else float('inf')

    @staticmethod
    def create_metric(metric_name: MetricName, before_value: T, after_value: Optional[T]) -> 'TableMetric':
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
            MetricName.FULL_SCAN_OVERHEAD: DurationMetric,
            MetricName.WORST_SCAN_OVERHEAD: DurationMetric,
            MetricName.FILE_COUNT: IntMetric,
            MetricName.WORST_FILE_COUNT: IntMetric,
            MetricName.AVG_FILE_SIZE: SizeMetric,
            MetricName.WORST_AVG_FILE_SIZE: SizeMetric,
            MetricName.TOTAL_TABLE_SIZE: SizeMetric,
            MetricName.LARGEST_PARTITION_SIZE: SizeMetric,
        }
        if metric_name in metric_map:
            return metric_map[metric_name](metric_name, before_value, after_value)
        else:
            raise ValueError(f"Unknown metric name: {metric_name}")


class IntMetric(TableMetric[int]):
    """
    Metric class for whole number values.
    """
    def get_before_value(self) -> str:
        return f"{self.before}"

    def get_after_value(self) -> str:
        return f"{self.after or ''}"


class DurationMetric(TableMetric[int]):
    """
    Metric class for duration values, assumed to be in milliseconds.
    """
    def get_before_value(self) -> str:
        return self._format_duration(self.before)

    def get_after_value(self) -> str:
        return self._format_duration(self.after) if self.after is not None else ""

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
            formatted_seconds = f"{seconds:.2f}".rstrip('0').rstrip('.') if seconds > 0 else str(int(seconds))
            return f"{formatted_seconds}s"


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

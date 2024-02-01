from typing import List, Dict, Any, Tuple
from collections import namedtuple

from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import MetricName, TableMetric
from icebergdiag.metrics.table_metrics import TableMetrics
from icebergdiag.utils import NestedDictAccessor


def format_metrics(prefix: str, *metrics: str) -> Tuple[str, ...]:
    return tuple(f"{prefix}.{metric}" for metric in metrics)


TARGET_FILE_COUNT = "targetFilesCount"
TARGET_SIZE = "targetSizeBytes"
FILES_COUNT_METRICS = ["totalFilesCount", TARGET_FILE_COUNT]
DATA_FILE_COUNT = 'totalDataFileCount'
DATA_FILE_SIZE = 'totalDataFileSizeBytes'
SIZE_METRICS = ["totalSizeBytes", TARGET_SIZE]
AVG_METRICS = [DATA_FILE_COUNT, TARGET_FILE_COUNT, DATA_FILE_SIZE, TARGET_SIZE]
SCAN_METRICS = ["currentScanOverheadMillis", "targetScanOverheadMillis"]
DIAGNOSTICS_METRICS_MAP = {
    MetricName.FULL_SCAN_OVERHEAD:
        format_metrics("table", *SCAN_METRICS),
    MetricName.WORST_SCAN_OVERHEAD:
        format_metrics("worstOverheadPartition", *SCAN_METRICS),
    MetricName.FILE_COUNT:
        format_metrics("table", *FILES_COUNT_METRICS),
    MetricName.WORST_FILE_COUNT:
        format_metrics("worstFilesCountPartition", *FILES_COUNT_METRICS),
    MetricName.TOTAL_TABLE_SIZE:
        format_metrics("table", *SIZE_METRICS),
    MetricName.LARGEST_PARTITION_SIZE:
        format_metrics("largestPartition", *SIZE_METRICS)
}


class DiagnosticsResponse:
    def __init__(self, metrics: List[TableMetrics], errors: List[Dict[str, str]]):
        self.metrics = metrics
        self.errors = errors

    def extract_errors(self) -> List[Tuple[Table, str]]:
        return [
            (Table.from_full_name(error.get("table", "")), error.get("error", "Unknown Error"))
            for error in self.errors
            if error.get("table")
        ]


def calculate_average_metric(metric: MetricName,
                             metric_path_prefix: str,
                             data: NestedDictAccessor) -> TableMetric:
    def calculate_average(count, size):
        return size / count if count != 0 else 0

    AvgMetrics = namedtuple('AvgMetrics', ['before_count', 'after_count', 'before_size', 'after_size'])

    formatted_metrics = format_metrics(metric_path_prefix, *AVG_METRICS)
    extracted_metrics = AvgMetrics(*[data[metric_path] for metric_path in formatted_metrics])

    before_avg = calculate_average(extracted_metrics.before_count, extracted_metrics.before_size)
    after_avg = calculate_average(extracted_metrics.after_count, extracted_metrics.after_size)
    return TableMetric.create_metric(metric, before_avg, after_avg)


def parse_response(data: Dict[str, Any]) -> DiagnosticsResponse:
    metrics = []
    for result in data['analysisResults']:
        res = NestedDictAccessor(result)
        table = Table(*res["table.name"].split("."))
        table_metrics: list[TableMetric] = []
        for metric, metrics_path in DIAGNOSTICS_METRICS_MAP.items():
            current_value = res[metrics_path[0]]
            target_value = res[metrics_path[1]]
            table_metric = TableMetric.create_metric(metric, current_value, target_value)
            table_metrics.append(table_metric)
        avg_metrics = [
            calculate_average_metric(MetricName.AVG_FILE_SIZE, "table", res),
            calculate_average_metric(MetricName.WORST_AVG_FILE_SIZE, "worstAvgFileSizePartition", res)
        ]
        table_metrics.extend(avg_metrics)
        total_partitions = TableMetric.create_metric(MetricName.TOTAL_PARTITIONS, res["table.totalPartitionsCount"])
        table_metrics.append(total_partitions)
        metrics.append(TableMetrics(table, sorted(table_metrics, key=lambda x: x.name)))
    return DiagnosticsResponse(metrics, data['errors'])

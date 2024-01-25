from icebergdiag.diagnostics.requester import DiagnosticsRequester
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import DurationMetric, IntMetric, SizeMetric, MetricName


def test_request_metrics(mocker):
    mock_response = {
        "analysisResults": [
            {
                "table": {
                    "name": "schema.table",
                    "totalSizeBytes": 12783164,
                    "targetSizeBytes": 11398256,
                    "currentScanOverheadMillis": 22,
                    "targetScanOverheadMillis": 2,
                    "totalFilesCount": 11,
                    "targetFilesCount": 1,
                    "totalPartitionsCount": 1
                },
                "largestPartition": {
                    "name": "",
                    "totalSizeBytes": 12783164,
                    "targetSizeBytes": 11398256,
                    "currentScanOverheadMillis": 22,
                    "targetScanOverheadMillis": 2,
                    "totalFilesCount": 11,
                    "targetFilesCount": 1
                },
                "worstOverheadPartition": {
                    "name": "",
                    "totalSizeBytes": 12783164,
                    "targetSizeBytes": 11398256,
                    "currentScanOverheadMillis": 22,
                    "targetScanOverheadMillis": 2,
                    "totalFilesCount": 11,
                    "targetFilesCount": 1
                },
                "worstFilesCountPartition": {
                    "name": "",
                    "totalSizeBytes": 12783164,
                    "targetSizeBytes": 11398256,
                    "currentScanOverheadMillis": 22,
                    "targetScanOverheadMillis": 2,
                    "totalFilesCount": 11,
                    "targetFilesCount": 1
                },
                "worstAvgFileSizePartition": {
                    "name": "",
                    "totalSizeBytes": 12783164,
                    "targetSizeBytes": 11398256,
                    "currentScanOverheadMillis": 22,
                    "targetScanOverheadMillis": 2,
                    "totalFilesCount": 11,
                    "targetFilesCount": 1
                }
            }
        ],
        "errors": []
    }

    mocker.patch.object(DiagnosticsRequester, '_post', return_value=mock_response)

    requester = DiagnosticsRequester()

    session_info = {"session": "test_session"}
    table = Table(database="schema", table_name="table")

    result = requester.request_metrics(session_info, [table])
    expected_metrics = [
            DurationMetric(MetricName.FULL_SCAN_OVERHEAD, 22, 2, True, True),
            DurationMetric(MetricName.WORST_SCAN_OVERHEAD, 22, 2, True, True),
            IntMetric(MetricName.FILE_COUNT, 11, 1, True, True),
            IntMetric(MetricName.WORST_FILE_COUNT, 11, 1, True, True),
            SizeMetric(MetricName.AVG_FILE_SIZE, 0.09090909090909091, 0.8916615636003731, True, False),
            SizeMetric(MetricName.WORST_AVG_FILE_SIZE, 0.09090909090909091, 0.8916615636003731, False, False ),
            SizeMetric(MetricName.TOTAL_TABLE_SIZE, 12783164, 11398256, True, True),
            SizeMetric(MetricName.LARGEST_PARTITION_SIZE, 12783164, 11398256, True, True),
            IntMetric(MetricName.TOTAL_PARTITIONS, 1, None, True, True)
    ]
    result_metric = result.metrics[0]
    assert result_metric.table == table
    assert result_metric.metrics == expected_metrics

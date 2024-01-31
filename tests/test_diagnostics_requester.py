from icebergdiag.diagnostics.requester import DiagnosticsRequester
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import DurationMetric, IntMetric, SizeMetric, MetricName


def test_request_metrics(mocker):
    mock_response = {
            "analysisResults": [
                {
                    "table":
                        {
                            "name": "schema.table",
                            "totalSizeBytes": 447765484092,
                            "targetSizeBytes": 500000000,
                            "currentScanOverheadMillis": 14787,
                            "targetScanOverheadMillis": 14785,
                            "totalFilesCount": 970,
                            "targetFilesCount": 5,
                            "totalPartitionsCount": 27,
                            "totalDataFileCount": 950,
                            "totalDataFileSizeBytes": 447765480000
                        },
                    "largestPartition":
                        {
                            "name": "partition_date=2023-12-18",
                            "totalSizeBytes": 24449273386,
                            "targetSizeBytes": 24449273386,
                            "currentScanOverheadMillis": 804,
                            "targetScanOverheadMillis": 804,
                            "totalFilesCount": 52,
                            "targetFilesCount": 1
                        },
                    "worstOverheadPartition":
                        {
                            "name": "partition_date=2023-12-27",
                            "totalSizeBytes": 15661932448,
                            "targetSizeBytes": 15661932448,
                            "currentScanOverheadMillis": 516,
                            "targetScanOverheadMillis": 514,
                            "totalFilesCount": 34,
                            "targetFilesCount": 3
                        },
                    "worstFilesCountPartition":
                        {
                            "name": "partition_date=2023-12-18",
                            "totalSizeBytes": 24449273386,
                            "targetSizeBytes": 24449273386,
                            "currentScanOverheadMillis": 804,
                            "targetScanOverheadMillis": 804,
                            "totalFilesCount": 52,
                            "targetFilesCount": 1
                        },
                    "worstAvgFileSizePartition":
                        {
                            "name": "partition_date=2023-12-27",
                            "totalSizeBytes": 15661932448,
                            "targetSizeBytes": 15661932448,
                            "currentScanOverheadMillis": 516,
                            "targetScanOverheadMillis": 514,
                            "totalFilesCount": 34,
                            "totalDataFileCount": 33,
                            "totalDataFileSizeBytes": 15661932440,
                            "targetFilesCount": 3
                        }
                }
            ],
            "errors": [

            ]
        }

    mocker.patch.object(DiagnosticsRequester, '_post', return_value=mock_response)

    requester = DiagnosticsRequester()

    session_info = {"session": "test_session"}
    table = Table(database="schema", table_name="table")

    result = requester.request_metrics(session_info, [table])
    expected_metrics = [
            DurationMetric(MetricName.FULL_SCAN_OVERHEAD, 14787, 14785, True, True),
            DurationMetric(MetricName.WORST_SCAN_OVERHEAD, 516, 514, True, True),
            IntMetric(MetricName.FILE_COUNT, 970, 5, True, True),
            IntMetric(MetricName.WORST_FILE_COUNT, 52, 1, True, True),
            SizeMetric(MetricName.AVG_FILE_SIZE, 471332084.2105263, 100000000.0, True, False),
            SizeMetric(MetricName.WORST_AVG_FILE_SIZE, 474604013.3333333, 5220644149.333333, False, False),
            SizeMetric(MetricName.TOTAL_TABLE_SIZE, 447765484092, 500000000, True, True),
            SizeMetric(MetricName.LARGEST_PARTITION_SIZE, 24449273386, 24449273386, True, True),
            IntMetric(MetricName.TOTAL_PARTITIONS, 27, None, True, True)
    ]
    result_metric = result.metrics[0]
    assert result_metric.table == table
    assert result_metric.metrics == expected_metrics

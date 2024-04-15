import logging
from typing import Dict, Any, List

import requests

from icebergdiag.diagnostics.response import DiagnosticsResponse, parse_response
from icebergdiag.exceptions import RequestHandlingError, ParsingResponseError
from icebergdiag.metrics.table import Table

logger = logging.getLogger(__name__)


class DiagnosticsRequester:
    METRICS_URL = "https://iceberg-auditor.upsolver.com/v2/wizards/optimizer/cli-analyze"

    def __init__(self, url: str = METRICS_URL):
        self.url = url

    def request_metrics(self, session_info: Dict[str, Any],
                        tables: List[Table]) -> DiagnosticsResponse:
        requested_tables = [table.full_table_name() for table in tables]
        logger.debug(f"Preparing to send metrics request for tables: {', '.join(requested_tables)}")
        result = self._post({**session_info, "tables": requested_tables})
        parsed_result = self._parse_response(result, requested_tables)

        logger.debug(f"Received and parsed metrics for tables successfully.")
        return parsed_result

    def _post(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = requests.post(self.url, json=data)
            response.raise_for_status()
            logger.debug("HTTP POST request successful, parsing response.")
            return response.json()
        except requests.RequestException as e:
            raise RequestHandlingError(data["tables"], e)

    @staticmethod
    def _parse_response(data: Dict[str, Any], tables: List[str]) -> DiagnosticsResponse:
        logger.debug("Parsing response data")
        try:
            response = parse_response(data)
            logger.debug(f"Response parsed successfully")
            return response
        except Exception as e:
            raise ParsingResponseError(data, tables, e)

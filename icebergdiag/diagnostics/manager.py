import fnmatch
from itertools import chain
from typing import List, Iterable, Dict, Any, Optional, Tuple

import boto3
import botocore.exceptions as boto3_exceptions
from botocore.client import BaseClient
from botocore.config import Config
from pyiceberg.catalog import load_glue, Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.io import PY_IO_IMPL, FSSPEC_FILE_IO
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table as IcebergTable, _open_manifest
from pyiceberg.utils.concurrent import ExecutorFactory

from icebergdiag.exceptions import ProfileNotFoundError, EndpointConnectionError, \
    IcebergDiagnosticsError, DatabaseNotFound, TableMetricsCalculationError, SSOAuthenticationError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metrics import TableMetrics, MetricsCalculator

CATALOG_CONFIG = {PY_IO_IMPL: FSSPEC_FILE_IO}


class IcebergDiagnosticsManager:
    def __init__(self, profile: str, region: Optional[str] = None):
        self.profile = profile
        self.region = region
        self._initialize_catalog()

    def _initialize_catalog(self):
        try:
            self._validate()
            self.catalog = load_glue(name="glue",
                                     conf={"profile_name": self.profile, "region_name": self.region, **CATALOG_CONFIG})
            self.glue_client = IcebergDiagnosticsManager._get_glue_client(self.catalog)
            self.session = boto3.Session(profile_name=self.profile, region_name=self.region)
        except boto3_exceptions.ProfileNotFound:
            raise ProfileNotFoundError(self.profile)
        except boto3_exceptions.EndpointConnectionError:
            raise EndpointConnectionError(self.region)
        except boto3_exceptions.SSOError as e:
            raise SSOAuthenticationError(self.profile, e) from e
        except Exception as e:
            raise IcebergDiagnosticsError(f"An unexpected error occurred: {e}")

    def _validate(self):
        try:
            session = boto3.Session(profile_name=self.profile, region_name=self.region)
            temp_config = Config(retries={'max_attempts': 1})
            temp_glue_client = session.client('glue', config=temp_config)
            temp_glue_client.get_databases(MaxResults=1)
        except Exception as e:
            raise e

    def list_databases(self) -> List[str]:
        databases = self.catalog.list_namespaces()
        return sorted([db[0] for db in databases])

    def list_tables(self, database: str) -> List[str]:
        try:
            return self._fetch_and_filter_tables(database)
        except self.glue_client.exceptions.EntityNotFoundException as e:
            raise DatabaseNotFound(database) from e

    def get_matching_tables(self, database: str, search_pattern: str) -> List[str]:
        all_tables = self.list_tables(database)
        return fnmatch.filter(all_tables, search_pattern)

    def calculate_metrics(self, table: Table) -> TableMetrics:
        try:
            return TableDiagnostics(self.catalog, table).get_metrics()
        except Exception as e:
            raise TableMetricsCalculationError(table, e)

    def _fetch_and_filter_tables(self, database: str) -> List[str]:
        next_token = None
        iceberg_tables = []
        while True:
            params = {'DatabaseName': database}
            if next_token:
                params['NextToken'] = next_token

            response = self.glue_client.get_tables(**params)
            tables = response.get('TableList', [])
            iceberg_tables.extend([tbl['Name'] for tbl in tables if IcebergDiagnosticsManager._is_iceberg_table(tbl)])

            next_token = response.get("NextToken")
            if not next_token:
                break

        return sorted(iceberg_tables)

    @staticmethod
    def _get_glue_client(catalog: Catalog) -> BaseClient:
        if isinstance(catalog, GlueCatalog):
            return catalog.glue
        else:
            raise TypeError("Expected GlueCatalog, got Catalog")

    @staticmethod
    def _is_iceberg_table(table_properties: Dict) -> bool:
        parameters = table_properties['Parameters']
        return parameters.get('table_type') == 'ICEBERG'

    def get_session_info(self) -> Dict[str, Any]:

        credentials = self.session.get_credentials()
        session_info = {
            "accessKey": credentials.access_key,
            "secretKey": credentials.secret_key,
            "region": self.session.region_name,
        }
        if credentials.token:
            session_info["tokenSession"] = credentials.token

        return session_info


class TableDiagnostics:
    def __init__(self, catalog: Catalog, table: Table):
        self.table = table
        self.catalog = catalog

    def get_metrics(self) -> TableMetrics:
        metrics = MetricsCalculator.compute_metrics(*self._get_manifest_files())
        return TableMetrics(self.table, metrics)

    def _load_table(self) -> IcebergTable:
        return self.catalog.load_table(self.table.full_table_name())

    def _get_manifest_files(self) -> Tuple[Iterable[DataFile], int]:
        """Returns a list of all data files in manifest entries.

        Returns:
            Iterable of DataFile objects.
        """
        def no_filter(_):
            return True

        table = self._load_table()
        scan = table.scan()
        snapshot = scan.snapshot()
        if not snapshot:
            return iter([]), 0

        io = table.io
        manifests = snapshot.manifests(io)
        executor = ExecutorFactory.get_or_create()
        all_data_files = []
        for manifest_entry in chain(
                *executor.map(
                    lambda manifest: _open_manifest(io, manifest, no_filter, no_filter),
                    manifests
                )):
            all_data_files.append(manifest_entry.data_file)

        return all_data_files, len(manifests)

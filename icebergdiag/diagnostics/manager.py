import fnmatch
import logging
import traceback
from itertools import chain
from typing import List, Iterable, Dict, Any, Optional, Tuple

import boto3
import botocore.exceptions as boto3_exceptions
from botocore.client import BaseClient
from botocore.config import Config
from pyiceberg.catalog import load_glue, Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table as IcebergTable, _open_manifest
from pyiceberg.utils.concurrent import ExecutorFactory

from icebergdiag.exceptions import ProfileNotFoundError, EndpointConnectionError, \
    IcebergDiagnosticsError, DatabaseNotFound, TableMetricsCalculationError, SSOAuthenticationError, \
    SessionInitializationError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metrics import TableMetrics, MetricsCalculator

logger = logging.getLogger(__name__)


class IcebergDiagnosticsManager:
    def __init__(self, profile: str, region: Optional[str] = None):
        logger.debug(f"Initializing with profile={profile}, region={region if region else 'default'}")
        self.profile = profile
        self.region = region
        self._initialize_catalog()

    def _initialize_catalog(self):
        logger.debug("Starting catalog initialization")
        try:
            self._validate()
            self.session = boto3.Session(profile_name=self.profile, region_name=self.region)
            credentials = self.session.get_credentials().get_frozen_credentials()
            self.catalog = load_glue(name="glue",
                                     conf={"profile_name": self.profile,
                                           "region_name": self.session.region_name,
                                           "aws_access_key_id": credentials.access_key,
                                           "aws_secret_access_key": credentials.secret_key,
                                           "aws_session_token": credentials.token,
                                           "s3.access-key-id": credentials.access_key,
                                           "s3.secret-access-key": credentials.secret_key,
                                           "s3.session-token": credentials.token,
                                           "s3.region": self.session.region_name,
                                           })
            self.glue_client = IcebergDiagnosticsManager._get_glue_client(self.catalog)
            logger.debug("Glue Catalog initialized successfully")
        except boto3_exceptions.ProfileNotFound:
            raise ProfileNotFoundError(self.profile)
        except boto3_exceptions.EndpointConnectionError:
            raise EndpointConnectionError(self.region)
        except boto3_exceptions.SSOError as e:
            raise SSOAuthenticationError(self.profile, e) from e
        except boto3_exceptions.BotoCoreError as e:
            raise SessionInitializationError(self.profile, e)
        except Exception as e:
            raise IcebergDiagnosticsError(f"An unexpected error occurred: {e}")

    def _validate(self):
        logger.debug("Validating session")
        try:
            session = boto3.Session(profile_name=self.profile, region_name=self.region)
            temp_config = Config(retries={'max_attempts': 1})
            temp_glue_client = session.client('glue', config=temp_config)
            temp_glue_client.get_databases(MaxResults=1)
            logger.debug("Session validated successfully")
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
        logger.debug(f"Searching for tables in database '{database}' with pattern '{search_pattern}'")
        all_tables = self.list_tables(database)
        return fnmatch.filter(all_tables, search_pattern)

    def calculate_metrics(self, table: Table) -> TableMetrics:
        logger.debug(f"Calculating metrics for table: '{table}'", )
        try:
            return TableDiagnostics(self.catalog, table).get_metrics()
        except Exception as e:
            logger.debug(f"Failed to Calculate metrics: {''.join(traceback.format_exception(e))}")
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
        logger.debug(f"Loading table {self.table.full_table_name()}")
        return self.catalog.load_table(self.table.full_table_name())

    def _get_manifest_files(self) -> Tuple[Iterable[DataFile], int]:
        logger.debug(f"Getting manifest files for table '{self.table}'")
        """Returns a list of all data files in manifest entries.

        Returns:
            Iterable of DataFile objects.
        """

        def no_filter(_):
            return True

        table = self._load_table()
        logger.debug(f"Scanning table: {self.table.full_table_name()}")
        scan = table.scan()
        logger.debug(f"Loading snapshot for table {self.table.full_table_name()}")
        snapshot = scan.snapshot()
        if not snapshot:
            return iter([]), 0

        io = table.io
        logger.debug(f"Opening manifests files for table {self.table.full_table_name()}")
        manifests = snapshot.manifests(io)
        executor = ExecutorFactory.get_or_create()
        all_data_files = []
        for manifest_entry in chain(
                *executor.map(
                    lambda manifest: _open_manifest(io, manifest, no_filter, no_filter),
                    manifests
                )):
            all_data_files.append(manifest_entry.data_file)

        logger.debug(f"All data loaded successfully for table {self.table.full_table_name()}")
        return all_data_files, len(manifests)

from typing import List, Optional, Dict, Any

import requests

from icebergdiag.metrics.table import Table


class IcebergDiagnosticsError(Exception):
    """Base class for all exceptions raised by the Iceberg Diagnostics Manager."""

    def __init__(self, message: str = "An error occurred in Iceberg Diagnostics Manager"):
        super().__init__(message)


class ProfileNotFoundError(IcebergDiagnosticsError):
    """Exception raised when the specified AWS profile does not exist."""

    def __init__(self, profile: Optional[str]):
        profile_msg = f"The AWS profile '{profile}' does not exist." if profile is not None else "No AWS profile found."
        super().__init__(profile_msg)


class SSOAuthenticationError(IcebergDiagnosticsError):
    """Exception raised when the SSO session is expired or invalid."""

    def __init__(self, profile: Optional[str], original_exception: Exception):
        message = f"There was an issue with the SSO for profile '{profile}': {original_exception}" if profile else str(
            original_exception)
        super().__init__(message)


class NoRegionError(IcebergDiagnosticsError):
    """Exception raised when no AWS region is specified."""

    def __init__(self):
        super().__init__("No AWS region specified.")


class EndpointConnectionError(IcebergDiagnosticsError):
    """Exception raised when connection to AWS endpoint fails."""

    def __init__(self, region: Optional[str]):
        region_message = f"region '{region}'" if region is not None else "default region"
        super().__init__(f"Could not connect to AWS in the {region_message}.")


class SessionInitializationError(IcebergDiagnosticsError):
    """Exception raised when an AWS session fails to initialize."""

    def __init__(self, profile: Optional[str], original_error: Exception):
        profile_part = f"with profile '{profile}'" if profile else "with default profile"
        message = f"Failed to initialize AWS session {profile_part}: {original_error}"
        super().__init__(message)


class UnexpectedError(IcebergDiagnosticsError):
    """Exception raised for any unexpected errors."""

    def __init__(self, message: str):
        super().__init__(f"An unexpected error occurred: {message}")


class DatabaseNotFound(IcebergDiagnosticsError):
    """Exception raised for querying non-existent Database."""

    def __init__(self, database: str):
        super().__init__(f"Database does not exist: : {database}")


class TableMetricsCalculationError(IcebergDiagnosticsError):
    """Exception raised when calculating metrics failed"""

    def __init__(self, table: Table, original_exception: Exception):
        self.table = table
        self.original_exception = original_exception
        super().__init__(f"Failed to calculate metrics for table '{table}': {original_exception}")


class RequestHandlingError(IcebergDiagnosticsError):
    """Exception raised for errors during remote diagnostics request."""

    def __init__(self, table_names: List[str], error: requests.RequestException):
        if isinstance(error, requests.HTTPError):
            message = str(error).split(" for url:")[0]
        else:
            message = f'An error occurred during the request for tables {table_names}: {error.__class__.__name__}'
        super().__init__(message)


class ParsingResponseError(IcebergDiagnosticsError):
    def __init__(self, data: Dict[str, Any], table_names: List[str], error: Exception):
        super().__init__(f"Failed to parse diagnostics response {data} for tables: {table_names}: {error}")

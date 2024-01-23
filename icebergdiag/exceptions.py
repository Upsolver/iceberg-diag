class IcebergDiagnosticsError(Exception):
    """Base class for all exceptions raised by the Iceberg Diagnostics Manager."""

    def __init__(self, message: str = "An error occurred in Iceberg Diagnostics Manager"):
        super().__init__(message)


class ProfileNotFoundError(IcebergDiagnosticsError):
    """Exception raised when the specified AWS profile does not exist."""

    def __init__(self, profile: str):
        super().__init__(f"The AWS profile '{profile}' does not exist.")


class NoRegionError(IcebergDiagnosticsError):
    """Exception raised when no AWS region is specified."""

    def __init__(self):
        super().__init__("No AWS region specified.")


class EndpointConnectionError(IcebergDiagnosticsError):
    """Exception raised when connection to AWS endpoint fails."""

    def __init__(self, region: str):
        super().__init__(f"Could not connect to AWS in the region '{region}'.")


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

    def __init__(self, table, original_exception):
        self.table = table
        self.original_exception = original_exception
        super().__init__(f"Failed to calculate metrics for table '{table}': {original_exception}")

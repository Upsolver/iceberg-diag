import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import List, Callable, Any, Tuple

from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
from rich.table import Table as RichTable

from icebergdiag.diagnostics.manager import IcebergDiagnosticsManager
from icebergdiag.diagnostics.requester import DiagnosticsRequester
from icebergdiag.diagnostics.response import DiagnosticsResponse
from icebergdiag.exceptions import TableMetricsCalculationError, IcebergDiagnosticsError, RequestHandlingError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metrics import TableMetrics
from icebergdiag.metrics.table_metrics_displayer import TableMetricsDisplayer, RunMode


def configure_logging(verbose=False):
    logging.basicConfig(
        level="ERROR",
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
    )
    if verbose:
        logging.getLogger().setLevel(logging.INFO)
        os.environ['S3FS_LOGGING_LEVEL'] = 'DEBUG'
        debug_loggers = ['icebergdiag', 'pyiceberg']
        for logger_name in debug_loggers:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)


logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='iceberg-diag',
        description='Iceberg Diagnostics Tool')
    parser.add_argument('--profile', type=str, help='AWS profile name')
    parser.add_argument('--region', type=str, help='AWS region')
    parser.add_argument('--database', type=str, help='Database name')
    parser.add_argument('--table-name', type=str, help="Table name or glob pattern (e.g., '*', 'tbl_*')")
    parser.add_argument('--remote', action='store_true', help='Use remote diagnostics')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logs')
    return parser.parse_args()


def stderr_print(stderr_message: str) -> None:
    Console(stderr=True).print(stderr_message, style="yellow")


def display_list(lst: List[str], heading: str) -> None:
    console = Console()

    table = RichTable(show_header=True, box=box.SIMPLE_HEAD)
    table.add_column(heading)
    for item in lst:
        table.add_row(item)

    console.print(table)


def list_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str) -> None:
    tables = run_with_progress(diagnostics_manager.list_tables, "Fetching Iceberg tables...", database)
    display_list(tables, "Tables")
    error_message = (
        "Use --table-name to get diagnostics on the Iceberg table, "
        "you can use a glob pattern to receive diagnostics on multiple tables in one command"
    )
    stderr_print(error_message)


def list_databases(diagnostics_manager: IcebergDiagnosticsManager) -> None:
    databases = run_with_progress(diagnostics_manager.list_databases, "Fetching databases...")
    display_list(databases, "Databases")
    stderr_print("Use --database to get the list of tables")


def run_with_progress(task_function, message, *args, **kwargs):
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
        task_id = progress.add_task(f"[cyan]{message}", total=None)
        result = task_function(*args, **kwargs)
        progress.update(task_id, completed=100)
        return result


def print_table_count_message(table_count, table_pattern):
    console = Console(highlight=False)
    if table_count == 0:
        console.print(f"[bold yellow]No tables matching the pattern '{table_pattern}' were found.[/bold yellow]\n")
    else:
        table_word = "table" if table_count == 1 else "tables"
        wait_message = "please allow a few minutes for the process to complete."
        console.print(f"[green]Analyzing {table_count} {table_word}, {wait_message}[/green]\n")


def fetch_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str, table_pattern: str) -> list[str]:
    tables = run_with_progress(diagnostics_manager.get_matching_tables, "Fetching tables...",
                               database, table_pattern)
    print_table_count_message(len(tables), table_pattern)
    if not tables:
        exit(0)
    return tables


def process_tables(
        diagnostics_manager: IcebergDiagnosticsManager,
        database: str,
        table_pattern: str,
        metric_function: Callable[[Table], Any],
        result_handler: Callable[[TableMetricsDisplayer, Any, List[Tuple[Table, str]]], None]
) -> None:
    table_names = fetch_tables(diagnostics_manager, database, table_pattern)
    tables = [Table(database, name) for name in table_names]
    failed_tables: List[Tuple[Table, str]] = []

    with Progress(SpinnerColumn(spinner_name="line"),
                  TextColumn("{task.description}"),
                  BarColumn(complete_style="progress.percentage"),
                  MofNCompleteColumn(),
                  transient=True) as progress:
        displayer = TableMetricsDisplayer(progress.console)
        with ThreadPoolExecutor(max_workers=10) as executor:
            task = progress.add_task("[cyan]Processing...", total=len(tables))
            futures = {executor.submit(metric_function, table): table for table in tables}
            for future in as_completed(futures):
                try:
                    table_result = future.result()
                    result_handler(displayer, table_result, failed_tables)
                except (TableMetricsCalculationError, RequestHandlingError) as e:
                    failed_tables.append((futures[future], str(e)))

                progress.update(task, advance=1)

    if failed_tables:
        logging.error("Failed to process the following tables:")
        for table, error in failed_tables:
            logging.error(f"Table: {table}, Error: {error}")


def generate_table_metrics(diagnostics_manager: IcebergDiagnosticsManager, database: str, table_pattern: str) -> None:
    def metric_function(table: Table) -> TableMetrics:
        return diagnostics_manager.calculate_metrics(table)

    def result_handler(displayer: TableMetricsDisplayer, table_result: TableMetrics, _) -> None:
        displayer.display_table_metrics(table_result, RunMode.LOCAL)

    process_tables(diagnostics_manager, database, table_pattern, metric_function, result_handler)

    stderr_print(
        "For a comprehensive analysis including all metrics and size reduction calculations, use the --remote option."
    )


def request_table_metrics(diagnostics_manager: IcebergDiagnosticsManager,
                          database: str,
                          table_pattern: str) -> None:
    session_info = diagnostics_manager.get_session_info()
    requester = DiagnosticsRequester()
    func = partial(requester.request_metrics, session_info)

    def metric_function(table: Table) -> DiagnosticsResponse:
        return func([table])

    def result_handler(displayer: TableMetricsDisplayer, table_result: DiagnosticsResponse,
                       failed_tables: List[Tuple[Table, str]]) -> None:
        displayer.display_metrics(table_result.metrics, RunMode.REMOTE)
        failed_tables.extend(table_result.extract_errors())

    process_tables(diagnostics_manager, database, table_pattern, metric_function, result_handler)


def cli_runner() -> None:
    args = parse_arguments()
    configure_logging(args.verbose)
    try:
        diagnostics_manager = run_with_progress(IcebergDiagnosticsManager, "Initializing...",
                                                profile=args.profile, region=args.region)

        if args.database is None:
            list_databases(diagnostics_manager)
        elif args.table_name is None:
            list_tables(diagnostics_manager, args.database)
        elif args.remote:
            request_table_metrics(diagnostics_manager, args.database, args.table_name)
        else:
            generate_table_metrics(diagnostics_manager, args.database, args.table_name)

    except IcebergDiagnosticsError as e:
        logger.error(e)
        exit(1)
    except KeyboardInterrupt:
        stderr_print("Analysis aborted. Exiting...")
        exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    cli_runner()

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
from rich.table import Table as RichTable

from icebergdiag.diagnostics import IcebergDiagnosticsManager, IcebergDiagnosticsError
from icebergdiag.exceptions import TableMetricsCalculationError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metrics_displayer import TableMetricsDisplayer

logging.basicConfig(
    level="ERROR",
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=False, show_path=False)]
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='iceberg-diag',
        description='Iceberg Diagnostics Tool')
    parser.add_argument('--profile', type=str, help='AWS profile name')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--database', type=str, help='Database name')
    parser.add_argument('--table-name', type=str, help="Table name or glob pattern (e.g., '*', 'tbl_*')")
    parser.add_argument('--remote', action='store_true', help='Use remote diagnostics')
    parser.add_argument('--no-tracking', action='store_true', help='Disable tracking')
    return parser.parse_args()


def stderr_print(stderr_message: str) -> None:
    Console(file=sys.stderr).print(stderr_message, style="yellow")


def display_list(lst: List[str], heading: str) -> None:
    console = Console()

    table = RichTable(show_header=True, box=box.SIMPLE_HEAD)
    table.add_column(heading)
    for item in lst:
        table.add_row(item)

    console.print(table)


def list_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str) -> None:
    tables = diagnostics_manager.list_tables(database)
    display_list(tables, "Tables")
    error_message = (
        "Use --table-name to get diagnostics on the iceberg table, "
        "you can use a glob pattern to receive diagnostics on multiple tables in one command"
    )
    stderr_print(error_message)


def list_databases(diagnostics_manager: IcebergDiagnosticsManager) -> None:
    databases = diagnostics_manager.list_databases()
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
        console.print(f"[green]Found {table_count} {table_word}, analyzing...[/green]\n")


def fetch_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str, table_pattern: str) -> list[str]:
    tables = run_with_progress(diagnostics_manager.get_matching_tables, "Fetching tables...",
                               database, table_pattern)
    print_table_count_message(len(tables), table_pattern)
    return tables


def generate_table_metrics(diagnostics_manager: IcebergDiagnosticsManager, database: str, table_pattern: str) -> None:
    table_names = fetch_tables(diagnostics_manager, database, table_pattern)
    tables = [Table(database, name) for name in table_names]
    table_count = len(tables)
    failed_tables = []
    with Progress(SpinnerColumn(spinner_name="line"),
                  TextColumn("{task.description}"),
                  BarColumn(complete_style="progress.percentage"),
                  MofNCompleteColumn(),
                  transient=True) as progress:
        displayer = TableMetricsDisplayer(progress.console)
        with ThreadPoolExecutor(max_workers=table_count) as executor:
            task = progress.add_task("[cyan]Processing...", total=table_count)
            futures = {executor.submit(diagnostics_manager.calculate_metrics, table): table for table in tables}

            for future in as_completed(futures):
                try:
                    table_result = future.result()
                    displayer.display_table_metrics(table_result)
                except TableMetricsCalculationError as e:
                    failed_tables.append((futures[future], str(e)))
                finally:
                    progress.update(task, advance=1)
    stderr_print("use --remote to get size reduction calculation")

    if failed_tables:
        logging.error("Failed to process the following tables:")
        for table, error in failed_tables:
            logging.error(f"Table: {table}, Error: {error}")


def cli_runner() -> None:
    logger = logging.getLogger(__name__)
    args = parse_arguments()
    try:
        diagnostics_manager = run_with_progress(IcebergDiagnosticsManager, "Initializing...",
                                                profile=args.profile, region=args.region)

        if args.database is None:
            list_databases(diagnostics_manager)
        elif args.table_name is None:
            list_tables(diagnostics_manager, args.database)
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

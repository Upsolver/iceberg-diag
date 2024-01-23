from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from .table_metrics import TableMetrics


class TableMetricsDisplayer:

    def __init__(self, console: Console):
        self.console = console

    def display_metrics(self, tables: list[TableMetrics]) -> None:
        for table in tables:
            self.display_table_metrics(table)

    def display_table_metrics(self, table: TableMetrics) -> None:
        output_table = TableMetricsDisplayer._create_output_table(table.table_name)
        for metric in table.metrics:
            output_table.add_row(metric.name.value,
                                 metric.get_before_value(),
                                 metric.get_after_value(),
                                 metric.get_improvement_value())

        panel = Panel(output_table, box=box.MINIMAL)
        self.console.print(panel)

    @staticmethod
    def _create_output_table(title: str) -> Table:
        output_table = Table(show_header=True,
                             title=f"[bold]{title}[/]")
        output_table.add_column("Metric", justify="left")
        output_table.add_column("Before", justify="right")
        output_table.add_column("After", justify="right")
        output_table.add_column("Improvement", justify="right")
        return output_table

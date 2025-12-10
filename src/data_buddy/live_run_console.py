from typing import Dict, Tuple, List
from contextlib import contextmanager
from collections import Counter

from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .metric_query import MetricQuery, QueryStatus


@contextmanager
def live_run_console():
    with Live("", auto_refresh=False) as live:
        yield LiveConsole(live)


class LiveConsole:
    def __init__(self, rich_live: Live):
        self.rich_live = rich_live

    def print(self, *args, **kwargs):
        self.rich_live.console.print(*args, **kwargs)

    def log(self, *args, **kwargs):
        self.rich_live.console.log(*args, **kwargs)

    def update(
        self,
        num_successful,
        num_failed,
        num_queries,
        in_progress: List[MetricQuery],
    ):
        if in_progress:
            # use a table so we wrap each individual line
            table = Table(box=None, show_header=False, expand=True)
            table.add_column("", no_wrap=True, overflow="ellipsis")
            for q in sorted(in_progress, key=lambda m: m.timeout_at):
                table.add_row(
                    Text.from_markup(
                        f"[dim]#{q.query_id}[/dim] "
                        f"[blue]{q.query_detail}[/blue] "
                        f"{q.fq_table_name} {q.column_name}"
                    )
                )
            for _ in range(max(0, 10 - len(in_progress))):
                table.add_row("")

            self.rich_live.update(Panel(
                renderable=table,
                title_align="left",
                subtitle_align="left",
                title=f"In Progress ({len(in_progress)})",
                subtitle=f"{num_successful + num_failed} / {num_queries} Completed ({num_failed} Errors)",
            ), refresh=True)
        else:
            self.rich_live.update("", refresh=True)


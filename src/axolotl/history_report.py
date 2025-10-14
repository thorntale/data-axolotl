import re
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.padding import Padding
import asciichartpy

from .display_utils import pretty_table_name


class HistoryReport:
    console = Console()

    def __init__(self, metric_set):
        self.metric_set = metric_set

    def print(self):
        for table in sorted(self.metric_set.get_tracked_tables()):
            self._print_table_header(table)
            for tracker in self.metric_set.get_metric_trackers_for_table(table):
                self.console.print(Padding(
                    (
                        f"{tracker.pretty_name}: [bright_green]{tracker.value_formatter(tracker.get_current_value())}"
                    ),
                    (0, 2),
                ))
                if all(
                    v.metric_value is None
                    or isinstance(v.metric_value, (int, float))
                    for v in tracker.values
                ):
                    self._print_numeric_chart(tracker)

                # self.console.print(tracker.get_current_value())

    def _print_table_header(self, table: str):
        self.console.print(Panel.fit(
            f"[bold blue]{escape(pretty_table_name(table))}[/bold blue] - Table Metrics\n"
            f"[blue]{escape(table)}[/blue]",
        ))

    def _print_numeric_chart(self, tracker: MetricTracker):
        values = [v.metric_value or float("nan") for v in tracker.values]
        class LabelFormatter:
            def format(self, v):
                suffix = ""
                value = v

                if abs(v) >= 1e12:
                    value, suffix = v / 1e12, "T"
                elif abs(v) >= 1e9:
                    value, suffix = v / 1e9, "B"
                elif abs(v) >= 1e6:
                    value, suffix = v / 1e6, "M"
                elif abs(v) >= 1e3:
                    value, suffix = v / 1e3, "k"

                # try decreasing precision until it fits
                for p in [3, 2, 1, 0]:
                    s = re.sub(
                        r"\.0+$",
                        "",
                        str(round(value, p)),
                    ) + suffix
                    # s = f"{value:.{p}f}{suffix}"
                    if len(s) <= 5:
                        return '    ' + s.rjust(5)
                return '    ' + f"{round(value)}{suffix}".rjust(5)

        print(asciichartpy.plot(values, {
            'height': 10,
            'format': LabelFormatter(),
        }))


def intersperse(arr, sep):
    result = [sep] * (len(arr) * 2 - 1)
    result[0::2] = arr
    return result

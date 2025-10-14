from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.padding import Padding
import asciichartpy


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
                        for p in range(3, -1, -1):
                            s = f"{value:.{p}f}{suffix}"
                            if len(s) <= 5:
                                return '    ' + s.rjust(5)
                        return '    ' + f"{round(value)}{suffix}".rjust(5)

                print(asciichartpy.plot(values, {
                    'height': 10,
                    'format': LabelFormatter(),
                }))
                # self.console.print(tracker.get_current_value())

    def _print_table_header(self, table: str):
        self.console.print(Panel.fit(
            f"[bold blue]{escape(table.split('.')[-1].title())}[/bold blue] - Table Metrics\n"
            f"[blue]{escape(table)}[/blue]",
        ))


def intersperse(arr, sep):
    result = [sep] * (len(arr) * 2 - 1)
    result[0::2] = arr
    return result

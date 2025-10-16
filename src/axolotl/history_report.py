import re
import itertools
from typing import Optional, Any, Dict

from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.padding import Padding
from rich.text import Text

from .display_utils import pretty_table_name
from .trackers import MetricTracker
from .trackers import ChartMode
from .line_chart import Chart


rainbow_colors = [
    "\033[0;31m",
    "\033[1;33m",
    '\033[0;32m',
    "\033[0;36m",
    "\033[0;34m",
]

class HistoryReport:
    console = Console()

    def __init__(self, metric_set):
        self.metric_set = metric_set

    def print(self):
        for table in sorted(self.metric_set.get_tracked_tables()):
            self._print_table_header(table)
            for tracker in self.metric_set.get_metric_trackers_for_table(table):
                self._print_tracker(tracker)

            columns = self.metric_set.get_tracked_columns(table)
            for _, column in columns:
                self._print_column_header(table, column)
                for tracker in self.metric_set.get_metric_trackers_for_column(table, column):
                    self._print_tracker(tracker)

    def _print_table_header(self, table: str):
        self.console.print(Panel.fit(
            f"[bold blue]{escape(pretty_table_name(table))}[/bold blue] - Table Metrics\n"
            f"[blue]{escape(table)}[/blue]",
            border_style='blue',
        ))

    def _print_column_header(self, table: str, column: str):
        self.console.print(Panel.fit(
            f"[bold green]{escape(column.title())}[/bold green] - Column Metrics\n"
            f"[blue]{escape(table)}[/blue].[green]{escape(column)}[/green]",
            border_style='green',
        ))

    def _print_tracker(self, tracker: MetricTracker):
        self.console.print(Padding(
            f"{tracker.pretty_name}: [bright_green]{tracker.value_formatter(tracker.get_current_value())}",
            (0, 2),
        ))
        if tracker.chart_mode == ChartMode.Standard:
            if all(
                v.metric_value is None
                or isinstance(v.metric_value, (int, float))
                for v in tracker.values
            ):
                self._print_numeric_chart(tracker)
        elif tracker.chart_mode == ChartMode.NumericPercentiles:
            self._print_percentile_chart(tracker)
        elif tracker.chart_mode == ChartMode.NumericHistogram:
            self._print_histogram_chart(tracker)
        elif tracker.chart_mode == ChartMode.HasChanged:
            self._print_has_changed_chart(tracker)

    def _print_numeric_chart(self, tracker: MetricTracker):
        chart = Chart(
            # left_axis_title=tracker.pretty_name,
            # right_axis_title='Δ',
            # include_zero_right=True,
        )
        # chart.add_plot(
        #     [None] + tracker.get_all_detlas(),
        #     side='right',
        #     color='\033[0;34m',
        # )
        chart.add_plot(
            [v.metric_value for v in tracker.values],
            label_end=True,
        )
        self._print_chart(chart.render())

    def _normalize_percentile_value(self, pcts: Optional[Dict[str, Any]]) -> Optional[Dict[int, Any]]:
        if not pcts:
            return None
        return {
            int(k[0:-1]): v  # remove the p suffix from the key
            for k, v in pcts.items()
        }

    def _print_percentile_chart(self, tracker: MetricTracker):
        vals_over_time = [
            self._normalize_percentile_value(val.metric_value)
            for val in tracker.values
        ]
        # order matters here; later plots are drawn on top
        plot_keys_and_colors = {
            50:  rainbow_colors[2],
            10:  rainbow_colors[1],
            90:  rainbow_colors[3],
            0:   rainbow_colors[0],
            100: rainbow_colors[4],
        }
        chart = Chart(
            # left_axis_title=tracker.pretty_name,
        )
        for key, color in plot_keys_and_colors.items():
            chart.add_plot(
                [
                    val[key] if val else None
                    for val in vals_over_time
                ],
                color=color,
                label_end=f"{key}p",
            )
        self._print_chart(chart.render())
        # self._print_percentile_curve(tracker)

    # def _print_percentile_curve(self, tracker: MetricTracker):
    #     val = self._normalize_percentile_value(tracker.get_current_value())
    #     if not val:
    #         return
    #     xs = range(0, 101, 2)
    #     ys = [
    #         max(((k, v) for k, v in val.items() if k <= pct), key=lambda t: t[0])[1]
    #         for pct in xs
    #     ]
    #     chart = Chart()
    #     chart.add_plot(ys)
    #     self._print_chart(chart.render())

    def _print_histogram_chart(self, tracker: MetricTracker):
        """ estimates a histogram from the percentile values """
        val = tracker.get_current_value()
        if not val:
            return

        ordered_values = [
            val[k]
            for k in sorted(val.keys(), key=float)
            for _ in range(0, 3)  # make bars wider
        ]

        DISP_H = 10
        chart = Chart(include_zero=True)
        chart.add_plot(ordered_values, bar_like=True)
        self._print_chart(chart.render())

    def _print_has_changed_chart(self, tracker: MetricTracker):
        vals = [v.metric_value for v in tracker.values]
        mode = [
            ' ' if prev == curr == None else
            '\033[2m○\033[0m' if prev == curr else
            '●'
            for prev, curr
            in itertools.pairwise([None] + vals)
        ]
        self._print_chart(''
            + "    Δ \033[2m╢ \033[0m" + ''.join(mode) + '\n'
            + "      \033[2m╚═" + '═' * len(mode) + '\033[0m'
        )

    def _print_chart(self, chart_ansi):
        self.console.print(
            Padding(
                Text.from_ansi(chart_ansi),
                (0, 4),
            ),
            highlight=False,
        )


def intersperse(arr, sep):
    result = [sep] * (len(arr) * 2 - 1)
    result[0::2] = arr
    return result

class ChartLabelFormatter:
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

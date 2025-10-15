import re
import itertools

from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.padding import Padding
import asciichartpy

from .display_utils import pretty_table_name
from .trackers import MetricTracker
from .trackers import ChartMode


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

    def _print_numeric_chart(self, tracker: MetricTracker):
        values = [v.metric_value or float("nan") for v in tracker.values]

        print(asciichartpy.plot(values, {
            'height': 10,
            'format': ChartLabelFormatter(),
        }))

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
            50: asciichartpy.green,
            10: asciichartpy.yellow,
            90: asciichartpy.cyan,
            # 5: asciichartpy.white,
            # 95: asciichartpy.white,
            0: asciichartpy.red,
            100: asciichartpy.blue,
        }
        plots = [
            [
                val[p] if val else float('nan')
                for val
                in vals_over_time
            ]
            for p in plot_keys_and_colors.keys()
        ]
        print(asciichartpy.plot(plots, {
            'height': 10,
            'format': ChartLabelFormatter(),
            'colors': list(plot_keys_and_colors.values())
            # 'format': LabelFormatter(),
        }))
        # self._print_percentile_histogram(tracker)

    def _print_percentile_histogram(self, tracker: MetricTracker):
        """ estimates a histogram from the percentile values """
        val = self._normalize_percentile_value(tracker.get_current_value())
        if not val:
            return
        ranges = list(itertools.pairwise(sorted(val.keys())))
        # it's possible for a bucket to be 0-width
        #   ex: 0th percentile == 5th percentile
        # so in that case we expand the bucket slightly.
        min_possible_width = ((val[100] - val[0]) * 0.01) or 1
        # tuples of (percent of rows, width of bucket)
        quantity_and_widths = [
            (100.0 * (end - start), max(min_possible_width, val[end] - val[start]))
            for start, end in ranges
        ]
        total_width = sum(w for q, w in quantity_and_widths)
        height_and_widths = [
            (q / w, w)
            for q, w in quantity_and_widths
        ]
        max_height = max(h for h, w in height_and_widths)

        DISP_H = 10
        DISP_W = 100

        # y -> x -> char
        grid = [
            [' '] * (DISP_W + 10)
            for y in range(0, DISP_H)
        ]
        left = 0
        for _h, _w in height_and_widths:
            h = round(_h / max_height * DISP_H)
            w = round(_w / total_width * DISP_W)
            for x in range(left, left + w):
                for y in range(0, h):
                    grid[y][x] = '#'
            left += w

        print('Percentile Hist:')
        print('\n'.join(''.join(row) for row in grid))
        # print(val)

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

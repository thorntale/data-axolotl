from pathlib import Path
from typing import Optional
from typing import Dict
from typing import List
from typing import Set
from collections import defaultdict
import sys

from rich.console import Console
from rich.panel import Panel
from rich.markup import escape
from rich.padding import Padding

from .trackers import AlertSeverity
from .display_utils import pretty_table_name

from .trackers import MetricTracker, MetricAlert, MetricKey
from .connectors.identifiers import FqTable

class AlertReport:
    console = Console()

    def __init__(self, metric_set):
        self.metric_set = metric_set

    def get_items(self, level: Set[AlertSeverity]) -> List[MetricAlert]:
        return [
            a
            for a in self.metric_set.get_all_alerts()
            if a.severity in level
        ]

    def print(
        self,
        save_path: Optional[Path],
        level: Set[AlertSeverity] = {AlertSeverity.Major, AlertSeverity.Minor},
    ) -> None:
        if save_path:
            try:
                save_path.mkdir(parents=True, exist_ok=True)
                with open(save_path / "alerts.txt", "w") as f:
                    self.console = Console(file=f, width=120)
                    self._print(level)
            finally:
                self.console = Console()
        else:
            self._print(level)

    def _print(
        self,
        level: Set[AlertSeverity],
    ) -> None:
        all_alerts = self.metric_set.get_all_alerts()

        # severity -> table -> column|None -> alert
        grouped_alerts: Dict[AlertSeverity, Dict[FqTable, Dict[str|None, List[MetricAlert]]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for a in all_alerts:
            grouped_alerts[a.alert_severity][a.key.target_table][a.key.target_column] += [a]

        for severity in AlertSeverity:
            if severity not in level:
                continue
            alerts_by_table_column = grouped_alerts[severity]
            num_alerts = sum(
                1
                for alerts_by_column in alerts_by_table_column.values()
                for alerts in alerts_by_column.values()
                for alert in alerts
            )
            self.console.print(Panel.fit(
                f"[bold red]{severity.value}[/bold red] [dim]({num_alerts} metrics)",
                border_style="red",
            ), highlight=False)
            if not num_alerts:
                self.console.print(Padding("[dim]No Alerts\n", (0, 2)))

            for table, alerts_by_column in sorted(alerts_by_table_column.items()):
                self.console.print(Padding(
                    f"[bold blue]{escape(pretty_table_name(table))}[/bold blue] [dim]({escape(str(table))}):",
                    (0, 2),
                ), highlight=False)
                for alert in alerts_by_column[None]:
                    self.console.print(Padding(
                        self._format_alert(alert, '  '),
                        (0, 4),
                    ), highlight=False)

                for column, alerts in alerts_by_column.items():
                    if column is None:
                        continue

                    self.console.print(Padding(
                        f"[bold green]{escape(column.title())}[/bold green] [dim]({escape(column)}):",
                        (0, 4),
                    ), highlight=False)

                    for alert in alerts:
                        self.console.print(Padding(
                            self._format_alert(alert),
                            (0, 6),
                        ), highlight=False)
                self.console.print()

    def _format_alert(self, alert: MetricAlert, extra_spacing='') -> str:
        name = alert.pretty_name + ':'
        DIFF_SIZE = 15
        return (
            "- "
            + escape(f"{name:<20s} ")
            + extra_spacing
            + f"[dim]{escape(alert.prev_value_formatted.rjust(DIFF_SIZE))} → [/dim]"
            + f"[magenta]{escape(alert.current_value_formatted.ljust(DIFF_SIZE))}[/magenta]"
            + (' ' * (
                DIFF_SIZE * 2 + 3
                - max(DIFF_SIZE, len(alert.prev_value_formatted))
                - max(DIFF_SIZE, len(alert.current_value_formatted))
            ))
            + f" ({escape(alert.change_formatted)})"
        )

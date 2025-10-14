from collections import defaultdict

from rich.console import Console
from rich.panel import Panel
from rich.markup import escape
from rich.padding import Padding

from .trackers import AlertSeverity
from .display_utils import pretty_table_name


class AlertReport:
    console = Console()

    def __init__(self, metric_set):
        self.metric_set = metric_set

    def print(self):
        all_alerts = self.metric_set.get_all_alerts()

        self.console.print(all_alerts)

        # severity -> table -> column|None -> alert
        grouped_alerts = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for a in all_alerts:
            grouped_alerts[a.alert_severity][a.key.target_table][a.key.target_column] += [a]

        for severity in AlertSeverity:
            alerts_by_table_column = grouped_alerts[severity]
            num_alerts = sum(
                1
                for alerts_by_column in alerts_by_table_column.values()
                for alerts in alerts_by_column.values()
                for alert in alerts
            )
            self.console.print(Panel.fit(
                f"[bold blue]{severity.value}[/bold blue] [bright_black]({num_alerts} alerts)"
            ))
            if not num_alerts:
                self.console.print(Padding("[bright_black]No Alerts", (0, 2)))

            for table, alerts_by_column in sorted(alerts_by_table_column.items()):
                self.console.print(Padding(
                    f"[bold blue]{escape(pretty_table_name(table))}[/bold blue] [bright_black]({escape(table)}):",
                    (0, 2),
                ))
                for alert in alerts_by_column[None]:
                    self.console.print(Padding(
                        self._format_alert(alert),
                        (0, 4),
                    ), highlight=False)

                for column, alerts in alerts_by_column.items():
                    if column is None:
                        continue

                    self.console.print(Padding(
                        f"[bold green]{escape(column.title())}[/bold green] [bright_black]({escape(column)}):",
                        (0, 4),
                    ))

                    for alert in alerts:
                        self.console.print(Padding(
                            self._format_alert(alert),
                            (0, 6),
                        ), highlight=False)

    def _format_alert(self, alert: MetricAlert) -> str:
        name = alert.pretty_name + ':'
        return (
            "- "
            + escape(f"{name:<15s} ")
            + f"[bright_black]{escape(alert.prev_value_formatted)} → [/bright_black]"
            + f"[magenta]{escape(alert.current_value_formatted)}[/magenta]"
            + (' ' * (30 - len(alert.prev_value_formatted + alert.current_value_formatted)))
            + f" ({escape(alert.change_formatted)})"
        )

from pathlib import Path
from typing import Optional
from typing import List
from typing_extensions import Annotated
from rich.markup import escape
from rich.console import Console
import time
from importlib.metadata import version
import itertools
import traceback
import typer

from tabulate import tabulate
from .metric_set import MetricSet
from .history_report import HistoryReport
from .alert_report import AlertReport
from .config import load_config, SnowflakeConnectionConfig
from .connectors.identifiers import IncludeDirective
from .live_run_console import live_run_console
from .generate_config_interactive import generate_config_interactive
from .timeouts import Timeout
from .trackers import AlertSeverity
from .config import DataAxolotlConfig
from . import api


app = typer.Typer()

def version_callback(value: bool):
    if value:
        typer.echo(version('data-axolotl'))
        raise typer.Exit()

@app.callback()
def version_arg(
    version: Annotated[bool, typer.Option(
        '--version',
        help="Show the current version and exit.",
        callback=version_callback,
        is_eager=True,
    )] = False,
):
    pass

def _get_config_from_path(config_path: Optional[Path]) -> DataAxolotlConfig:
    console = Console()

    if not config_path:
        config_path = Path("./config.yaml")
        if not config_path.is_file():
            # no config provided, and no config at default location.
            success = generate_config_interactive(config_path)
            if not success:
                console.print(f"Config generation aborted. Please specify a valid '--config-path', or place a config at './config.yaml'.")
                raise typer.Exit(code=1)
    else:
        if not config_path.is_file():
            # config provided, but does not exist.
            console.print(f"Could not find config file [red]{escape(str(config_path))}[/red].\nPlease specify a valid '--config-path', or run with no config path to generate a config interactively.")
            raise typer.Exit(code=1)

    return load_config(config_path)


config_path_arg = typer.Option(help="The run configuration file. Defaults to './config.yaml'")

@app.command()
def run(
    config_path: Annotated[Optional[Path], config_path_arg] = None,
    list_only: Annotated[Optional[bool], typer.Option(
        '--list-only',
        help="List which metrics would be generated then exit",
    )] = False,
):
    """
    Execute a new run. Takes a --config path/to/config.yaml
    """
    config = _get_config_from_path(config_path)
    api.run(config, list_only or False)

@app.command()
def list(
    config_path: Annotated[Optional[Path], config_path_arg] = None,
):
    """
    Show past runs.
    """
    config = _get_config_from_path(config_path)

    runs = api.list_runs(config)

    print(
        tabulate(
            [
                [
                    run.run_id,
                    run.started_at and run.started_at.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    run.finished_at.strftime("%Y-%m-%d %H:%M:%S %Z")
                    if run.finished_at else '--',
                    run.successful or 'False',
                ]
                for run in runs
            ],
            headers=[
                "id",
                "Started At",
                "Finished At",
                "Successful",
            ],
        )
    )


@app.command()
def rm_run(
    id: int,
    config_path: Annotated[Optional[Path], config_path_arg] = None,
):
    """
    Remove a past run from the state db.

    Args:
        id: The ID of the run to remove
    """
    config = _get_config_from_path(config_path)
    if api.rm_run(config, run_id=id):
        typer.echo(f"Removed run {id}")
    else:
        typer.echo(f"Run {id} does not exist")


target_arg = typer.Argument(
    help="""
        Generate a report for only matching objects. Supports: db, db.schema, db.schema.table, db.schema.table:metric, db.schema.table.column, db.schema.table.column:metric
    """
)
run_id_arg = typer.Option(
    '--run',
    help="The run to use. Defaults to latest successful run."
)
save_arg = typer.Option(
    '--save',
    help="Save report artifacts at the specified location."
)
changed_arg = typer.Option(
    '--changed',
    help="Include change-level alerts."
)
all_alerts_arg = typer.Option(
    '--all',
    help="Include change alerts and unchanged alerts."
)

def _get_metric_set(
    include: List[IncludeDirective] = [],
    run_id: Optional[int] = None,
    config_path: Optional[Path] = None,
) -> MetricSet:
    config = _get_config_from_path(config_path)
    try:
        return api.get_metrics(
            config,
            run_id,
            include,
        )
    except ValueError as ve:
        print(str(ve))
        raise typer.Exit(code=1)

@app.command()
def report(
    config_path: Annotated[Optional[Path], config_path_arg] = None,
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
    changed: Annotated[Optional[bool], changed_arg] = False,
    all_alerts: Annotated[Optional[bool], all_alerts_arg] = False,
):
    """
    Do only the report generation step of run.
    """
    level = (
        { AlertSeverity.Major, AlertSeverity.Minor, AlertSeverity.Changed, AlertSeverity.Unchanged }
        if all_alerts
        else { AlertSeverity.Major, AlertSeverity.Minor, AlertSeverity.Changed }
        if changed
        else { AlertSeverity.Major, AlertSeverity.Minor }
    )
    parsed_targets = [IncludeDirective.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id, config_path)
    AlertReport(metric_set).print(save, level)
    HistoryReport(metric_set).print(save)

@app.command()
def alerts(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
    changed: Annotated[Optional[bool], changed_arg] = False,
    all_alerts: Annotated[Optional[bool], all_alerts_arg] = False,
    config_path: Annotated[Optional[Path], config_path_arg] = None,
):
    """ Show alerts """
    parsed_targets = [IncludeDirective.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id, config_path)
    level = (
        { AlertSeverity.Major, AlertSeverity.Minor, AlertSeverity.Changed, AlertSeverity.Unchanged }
        if all_alerts
        else { AlertSeverity.Major, AlertSeverity.Minor, AlertSeverity.Changed }
        if changed
        else { AlertSeverity.Major, AlertSeverity.Minor }
    )
    AlertReport(metric_set).print(save, level)

@app.command()
def history(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
    config_path: Annotated[Optional[Path], config_path_arg] = None,
):
    """ Show data history """
    parsed_targets = [IncludeDirective.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id, config_path)
    HistoryReport(metric_set).print(save)

def main():
    app()

if __name__ == "__main__":
    main()

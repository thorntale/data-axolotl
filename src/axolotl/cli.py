from pathlib import Path
from typing import Optional
from typing import List
from typing_extensions import Annotated
from rich.markup import escape
from rich.console import Console
import time
from importlib.metadata import version

import typer
from tabulate import tabulate
from .connectors.state_dao import StateDAO
from .metric_set import MetricSet
from .history_report import HistoryReport
from .alert_report import AlertReport
from .config import load_config, SnowflakeConnectionConfig
from .connectors.identifiers import IncludeDirective
from .live_run_console import live_run_console
from .generate_config_interactive import generate_config_interactive
from .timeouts import Timeout
from .trackers import AlertSeverity
from .config import AxolotlConfig
import traceback


app = typer.Typer()

def version_callback(value: bool):
    if value:
        typer.echo(version('axolotl'))
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

def _get_config(config_path: Optional[Path]) -> AxolotlConfig:
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
    config_path: Annotated[Optional[Path], config_path] = None,
    list_only: Annotated[Optional[bool], typer.Option(
        '--list-only',
        help="List which metrics would be generated then exit",
    )] = False,
):
    """
    Execute a new run. Takes a --config path/to/config.yaml
    """
    console = Console()
    config = _get_config(config_path)
    state = config.get_state_dao()

    #t_run_start = time.monotonic()
    #run_timeout_at = t_run_start + config.run_timeout_seconds

    with live_run_console() as console:
        with state.make_run() as run_id:
            run_timeout = Timeout(timeout_seconds=config.run_timeout_seconds, detail=f"run_id: {run_id}")
            run_timeout.start()
            console.print(f"Starting run #{run_id}...")

            for conn_config in config.connections.values():
                with conn_config.get_conn(run_id, console) as conn:
                    if list_only:
                        for m in conn.list_only(run_timeout):
                            console.print(m)
                    else:
                        for m in conn.snapshot(run_timeout):
                            try:
                                state.record_metric(m)
                            except Exception as e:
                                print(f"Error recording metric: {e}")
                                print(traceback.format_exc())
                                raise


@app.command()
def list(
    config_path: Annotated[Optional[Path], config_path] = None,
):
    """
    Show past runs.
    """
    state = _get_config(config_path).get_state_dao()
    runs = sorted(state.get_all_runs(), key=lambda r: r.run_id)

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
    config_path: Annotated[Optional[Path], config_path] = None,
):
    """
    Remove a past run from the state db.

    Args:
        id: The ID of the run to remove
    """
    state = _get_config(config_path).get_state_dao()
    runs = state.get_all_runs()
    if id in [run.run_id for run in runs]:
        state.delete_run(id)
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
    includes: List[IncludeDirective] = [],
    run_id: Optional[int] = None,
    config_path: Optional[Path] = None,
):
    state = _get_config(config_path).get_state_dao()

    run_id = run_id or state.get_latest_successful_run_id()
    if run_id is None:
        print("No run found")
        raise typer.Exit(code=1)

    runs = state.get_all_runs()
    if run_id not in [r.run_id for r in runs]:
        print(f"Run {run_id} does not exist.")
        raise typer.Exit(code=1)
    if run_id not in [r.run_id for r in runs if r.successful]:
        print(f"Run {run_id} was not successful.")
        raise typer.Exit(code=1)

    filtered_runs = [
        r for r in runs
        if r.successful and r.run_id <= run_id
    ]

    metrics = [
        m for m in state.get_metrics(
            run_id_lte=run_id,
            only_successful=True,
        )
        if not includes or m.matches_includes(includes)
    ]

    return MetricSet(filtered_runs, metrics)

@app.command()
def report(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
    changed: Annotated[Optional[bool], changed_arg] = False,
    all_alerts: Annotated[Optional[bool], all_alerts_arg] = False,
    config_path: Annotated[Optional[Path], config_path] = None,
):
    """
    Do only the report generation step of run.
    """
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
    HistoryReport(metric_set).print(save)

@app.command()
def alerts(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
    changed: Annotated[Optional[bool], changed_arg] = False,
    all_alerts: Annotated[Optional[bool], all_alerts_arg] = False,
    config_path: Annotated[Optional[Path], config_path] = None,
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
    config_path: Annotated[Optional[Path], config_path] = None,
):
    """ Show data history """
    parsed_targets = [IncludeDirective.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id, config_path)
    HistoryReport(metric_set).print(save)

def main():
    app()

if __name__ == "__main__":
    main()

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
from .state_connection import get_conn
from .snowflake_connection import SnowflakeConn
from .state_dao import StateDAO
from .metric_set import MetricSet
from .history_report import HistoryReport
from .alert_report import AlertReport
from .config import load_config, SnowflakeConnectionConfig, IncludeDerictive
from .live_run_console import live_run_console
from .generate_config_interactive import generate_config_interactive
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

@app.command()
def run(
    config_path: Annotated[Optional[Path], typer.Option(help="The run configuration file. Defaults to './config.yaml'")] = None,
    list_only: Annotated[Optional[bool], typer.Option(
        '--list-only',
        help="List which metrics would be generated then exit",
    )] = False,
):
    """
    Execute a new run. Takes a --config path/to/config.yaml
    """
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

    state_conn = get_conn()
    state = StateDAO(state_conn)

    config = load_config(config_path)
    t_run_start = time.monotonic()
    run_timeout_at = t_run_start + config.run_timeout_seconds

    with live_run_console() as console:
        with state.make_run() as run_id:
            console.print(f"Starting run #{run_id}...")

            for conn_config in config.connections.values():
                if isinstance(conn_config, SnowflakeConnectionConfig):
                    with SnowflakeConn(config, conn_config, run_id, console) as snowflake_conn:
                        if list_only:
                            for m in snowflake_conn.list_only(run_timeout_at):
                                console.print(m)
                        else:
                            for m in snowflake_conn.snapshot(run_timeout_at):
                                try:
                                    state.record_metric(m)
                                except Exception as e:
                                    print(f"Error recording metric: {e}")
                                    print(traceback.format_exc())
                                    raise
                else:
                    raise TypeError('Unknown connection config type')


@app.command()
def list():
    """
    Show past runs.
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)

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
def rm_run(id: int):
    """
    Remove a past run from the state db.

    Args:
        id: The ID of the run to remove
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)
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

def _get_metric_set(
    includes: List[IncludeDerictive] = [],
    run_id: Optional[int] = None,
):
    state_conn = get_conn()
    state = StateDAO(state_conn)

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
):
    """
    Do only the report generation step of run.
    """
    parsed_targets = [IncludeDerictive.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id)
    AlertReport(metric_set).print(save)
    HistoryReport(metric_set).print(save)

@app.command()
def alerts(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
):
    """ Show alerts """
    parsed_targets = [IncludeDerictive.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id)
    AlertReport(metric_set).print(save)

@app.command()
def history(
    target: Annotated[List[str], target_arg] = [],
    run_id: Annotated[Optional[int], run_id_arg] = None,
    save: Annotated[Optional[Path], save_arg] = None,
):
    """ Show data history """
    parsed_targets = [IncludeDerictive.from_string(t) for t in target]
    metric_set = _get_metric_set(parsed_targets, run_id)
    HistoryReport(metric_set).print(save)

def main():
    app()

if __name__ == "__main__":
    main()

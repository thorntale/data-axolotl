from typing import Optional
from typing_extensions import Annotated

import typer
from tabulate import tabulate
from .state_connection import (
    get_conn,
)
from .snowflake_connection import (
    SnowflakeConn,
    SnowflakeOptions,
)
from .state_dao import StateDAO
from .metric_set import MetricSet
from .history_report import HistoryReport
from .alert_report import AlertReport

app = typer.Typer()


@app.command()
def run(
    account: Annotated[str, typer.Option(prompt=True)],
    user: Annotated[str, typer.Option(prompt=True)],
    password: Annotated[str, typer.Option(prompt=True)],
    database: Annotated[str, typer.Option(prompt=True)],
    warehouse: Annotated[str, typer.Option(prompt=True)],
    table_schema: Annotated[str, typer.Option(prompt=True)],
):
    """
    Execute a new run.
    """

    typer.echo("Running...")
    state_conn = get_conn()
    state = StateDAO(state_conn)

    options = SnowflakeOptions(
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse,
        table_schema=table_schema,
    )

    with state.make_run() as run_id:
        typer.echo(f"Running {run_id}...")
        snowflake_conn = SnowflakeConn(options, run_id)

        for metric_list in snowflake_conn.snapshot(run_id):
            for m in metric_list:
                try:
                    state.record_metric(m)
                except Exception as e:
                    print(f"Error recording metric: {e}")
                    raise

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
                    run.started_at and run.started_at.strftime("%d-%m-%Y %H:%M:%S %Z"),
                    run.finished_at
                    and run.finished_at.strftime("%d-%m-%Y %H:%M:%S %Z"),
                    run.successful,
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


@app.command()
def report(
    run_id: Annotated[Optional[int], typer.Option(help="The run id to make a report on. Defaults to latest successful run.")] = None,
    alerts: Annotated[bool, typer.Option(help="Include the alerting report")] = True,
    history: Annotated[bool, typer.Option(help="Include the history report")] = True,
):
    """
    Do only the report generation step of run.

    Args:
        run_options: Optional run options to configure report generation
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)

    run_id = run_id or state.get_latest_successful_run_id()
    if run_id is None:
        print('No run found')
        raise typer.Exit(code=1)

    runs = state.get_all_runs()
    if not run_id in [r.run_id for r in runs]:
        print(f"Run {run_id} does not exist.")
        raise typer.Exit(code=1)
    if not run_id in [r.run_id for r in runs if r.successful]:
        print(f"Run {run_id} was not successful.")
        raise typer.Exit(code=1)

    metrics = state.get_metrics(
        run_id_lte=run_id,
        only_successful=True,
    )
    filtered_runs = [
        r for r in runs
        if r.successful and r.run_id <= run_id
    ]

    metric_set = MetricSet(filtered_runs, metrics)

    if alerts:
        AlertReport(metric_set).print()

    if history:
        HistoryReport(metric_set).print()

def main():
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()

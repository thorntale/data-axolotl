import typer
from tabulate import tabulate
from typing import Optional
from .state_connection import (
    get_conn,
)
from .snowflake_connection import (
    SnowflakeConn,
    SnowflakeOptions,
)
from typing_extensions import Annotated
from .state_dao import StateDAO

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
    snowflake_conn = SnowflakeConn(options)

    with state.make_run() as run_id:
        typer.echo(f"Running {run_id}...")
        # metrics = snowflake_conn.get_table_level_metrics(run_id)
        try:
            metrics = snowflake_conn.snapshot(run_id)
        except Exception as e:
            print(f"Error: {e}")
            raise

        for m in metrics:
            try:
                state.record_metric(m)
            except Exception as e:
                print(f"Error recording metric: {e}")
                raise

        # print(metrics)
        # scan_database(snowflake_conn, options, state, run_id)

        # TODO: Implement run logic


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
    run_options: Optional[str] = typer.Argument(
        None, help="Run options for report generation"
    )
):
    """
    Do only the report generation step of run.

    Args:
        run_options: Optional run options to configure report generation
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)

    typer.echo(f"Generating report with options: {run_options}")
    # TODO: Implement report logic
    pass


def main():
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()

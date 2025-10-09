import typer
from typing import Optional
from .state_connection import get_conn, get_snowflake_conn, SnowflakeOptions
from typing_extensions import Annotated

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
    state_conn = get_conn()

    typer.echo("Running...")

    options = SnowflakeOptions(
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse,
        table_schema=table_schema,
    )

    get_snowflake_conn(options)

    # TODO: Implement run logic
    pass


@app.command()
def list():
    """
    Show past runs.
    """
    state_conn = get_conn()
    typer.echo("Listing past runs...")
    # TODO: Implement list logic
    pass


@app.command()
def rm_run(id: str):
    """
    Remove a past run from the state db.

    Args:
        id: The ID of the run to remove
    """
    state_conn = get_conn()
    typer.echo(f"Removing run: {id}")
    # TODO: Implement rm-run logic
    pass


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
    typer.echo(f"Generating report with options: {run_options}")
    # TODO: Implement report logic
    pass


def main():
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()

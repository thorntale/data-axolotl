import typer
from typing import Optional
from .state_connection import get_conn
from .state_connection import get_snowflake_conn
from .state_dao import StateDAO

app = typer.Typer()


@app.command()
def run():
    """
    Execute a new run.
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)
    external_conn = get_snowflake_conn()

    with state.make_run() as run_id:
        typer.echo(f"Running {run_id}...")
        # TODO: Implement run logic

@app.command()
def list():
    """
    Show past runs.
    """
    state_conn = get_conn()
    state = StateDAO(state_conn)

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
    state = StateDAO(state_conn)

    typer.echo(f"Removing run: {id}")
    # TODO: Implement rm-run logic
    pass


@app.command()
def report(
    run_options: Optional[str] = typer.Argument(None, help="Run options for report generation")
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

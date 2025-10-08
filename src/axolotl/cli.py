# SPDX-FileCopyrightText: 2025-present John Stimac <johnstimac111@gmail.com>
#
# SPDX-License-Identifier: MIT

import typer
from typing import Optional

app = typer.Typer()


@app.command()
def run():
    """
    Execute a new run.
    """
    typer.echo("Running...")
    # TODO: Implement run logic
    pass


@app.command()
def list():
    """
    Show past runs.
    """
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
    typer.echo(f"Generating report with options: {run_options}")
    # TODO: Implement report logic
    pass


def main():
    """Entry point for the CLI application."""
    app()


if __name__ == "__main__":
    main()

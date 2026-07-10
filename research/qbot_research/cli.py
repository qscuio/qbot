from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(help="Independent qbot research worker scaffold.")


@app.callback()
def main() -> None:
    """Bootstrap the research CLI group."""


@app.command("train-all")
def train_all(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            dir_okay=False,
            help="Path to the research worker configuration file.",
        ),
    ],
) -> None:
    typer.echo(
        f"train-all scaffold only; parsed config {config}. "
        "No training ran. Task 3 will implement this command."
    )

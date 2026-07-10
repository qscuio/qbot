from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from qbot_research.datasets import build_dataset, normalize_horizon

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
        "No training ran."
    )


@app.command("build-dataset")
def build_dataset_command(
    horizon: Annotated[
        str,
        typer.Option(
            "--horizon",
            help="Dataset horizon to export.",
        ),
    ],
    as_of: Annotated[
        str,
        typer.Option(
            "--as-of",
            help="Point-in-time data cutoff date.",
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output-dir",
            file_okay=False,
            help="Directory where the partitioned dataset should be written.",
        ),
    ],
) -> None:
    try:
        normalized_horizon = normalize_horizon(horizon)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--horizon") from error

    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter("expected YYYY-MM-DD", param_hint="--as-of") from error

    manifest = build_dataset(
        horizon=normalized_horizon,
        as_of=as_of_date,
        output_dir=output_dir,
    )
    typer.echo(
        f"built dataset {manifest.dataset_version} "
        f"rows={manifest.row_count} files={len(manifest.files)}"
    )

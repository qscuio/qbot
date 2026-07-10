import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Mapping, cast
from uuid import UUID

import typer

from qbot_research.datasets import build_dataset, normalize_horizon, validate_publishable_horizon
from qbot_research.export import ExportMetadata, export_pattern_version, validate_model_export_horizon

app = typer.Typer(help="Independent qbot research worker scaffold.")


@app.callback()
def main() -> None:
    """Bootstrap the research CLI group."""


@app.command("train-all")
def train_all(
    as_of: Annotated[
        str,
        typer.Option(
            "--as-of",
            help="Point-in-time training cutoff date.",
        ),
    ],
    plan_json: Annotated[
        Path | None,
        typer.Option(
            "--plan-json",
            dir_okay=False,
            help="Explicit export plan JSON. Required until full orchestration exists.",
        ),
    ] = None,
    output_json: Annotated[
        Path | None,
        typer.Option(
            "--output-json",
            dir_okay=False,
            help="Optional path for the exported pattern-version rows.",
        ),
    ] = None,
) -> None:
    as_of_date = _parse_date(as_of, "--as-of")
    if plan_json is None:
        typer.echo("train-all requires --plan-json with explicit export inputs. No training ran.")
        raise typer.Exit(2)

    plan_payload = _read_json_object(plan_json)
    export_items = _object_list_field(plan_payload, "exports")
    exported_payloads: list[dict[str, Any]] = []
    for item in export_items:
        horizon = validate_model_export_horizon(_string_field(item, "horizon"))
        metadata = ExportMetadata.from_payload(
            _mapping_field(item, "metadata"),
            horizon=horizon,
            dataset_version=_string_field(item, "dataset_version"),
        )
        _validate_export_metadata_cutoff(metadata, as_of_date)
        exported = export_pattern_version(
            candidate_payload=_mapping_field(item, "candidate"),
            validation_payload=_mapping_field(item, "validation"),
            metadata=metadata,
            typical_positive_examples=_mapping_list_field(item, "typical_positive_examples"),
            failed_examples=_mapping_list_field(item, "failed_examples"),
        )
        exported_payloads.append(exported.payload())

    _emit_json({"exports": exported_payloads}, output_json)


@app.command("train")
def train(
    horizon: Annotated[
        str,
        typer.Option(
            "--horizon",
            help="Model export horizon. Only week and month are allowed.",
        ),
    ],
    dataset_version: Annotated[
        str,
        typer.Option(
            "--dataset-version",
            help="Dataset version used for this training/export run.",
        ),
    ],
    candidate_json: Annotated[
        Path,
        typer.Option(
            "--candidate-json",
            dir_okay=False,
            help="JSON file containing the PatternModelPayload candidate.",
        ),
    ],
    validation_json: Annotated[
        Path,
        typer.Option(
            "--validation-json",
            dir_okay=False,
            help="JSON file containing the validation payload and candidate_status.",
        ),
    ],
    metadata_json: Annotated[
        Path,
        typer.Option(
            "--metadata-json",
            dir_okay=False,
            help="JSON file containing explicit export metadata except horizon and dataset_version.",
        ),
    ],
    positive_examples_json: Annotated[
        Path,
        typer.Option(
            "--positive-examples-json",
            dir_okay=False,
            help="JSON list of typical positive examples.",
        ),
    ],
    failed_examples_json: Annotated[
        Path,
        typer.Option(
            "--failed-examples-json",
            dir_okay=False,
            help="JSON list of failed examples.",
        ),
    ],
    output_json: Annotated[
        Path | None,
        typer.Option(
            "--output-json",
            dir_okay=False,
            help="Optional path for the exported pattern-version row payload.",
        ),
    ] = None,
) -> None:
    try:
        model_horizon = validate_model_export_horizon(horizon)
    except ValueError as error:
        typer.echo(str(error))
        raise typer.Exit(2) from error

    metadata = ExportMetadata.from_payload(
        _read_json_object(metadata_json),
        horizon=model_horizon,
        dataset_version=dataset_version,
    )
    exported = export_pattern_version(
        candidate_payload=_read_json_object(candidate_json),
        validation_payload=_read_json_object(validation_json),
        metadata=metadata,
        typical_positive_examples=_read_json_mapping_list(positive_examples_json),
        failed_examples=_read_json_mapping_list(failed_examples_json),
    )
    _emit_json(exported.payload(), output_json)


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
    ] = Path("data"),
) -> None:
    try:
        normalized_horizon = normalize_horizon(horizon)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--horizon") from error

    try:
        publishable_horizon = validate_publishable_horizon(normalized_horizon)
    except ValueError as error:
        raise typer.BadParameter(str(error), param_hint="--horizon") from error

    try:
        as_of_date = date.fromisoformat(as_of)
    except ValueError as error:
        raise typer.BadParameter("expected YYYY-MM-DD", param_hint="--as-of") from error

    manifest = build_dataset(
        horizon=publishable_horizon,
        as_of=as_of_date,
        output_dir=output_dir,
    )
    typer.echo(
        f"built dataset {manifest.dataset_version} "
        f"rows={manifest.row_count} files={len(manifest.files)}"
    )


def _read_json_object(path: Path) -> dict[str, object]:
    payload = _read_json_payload(path)
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"{path} must contain a JSON object")
    return cast(dict[str, object], payload)


def _read_json_mapping_list(path: Path) -> list[Mapping[str, object]]:
    payload = _read_json_payload(path)
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise typer.BadParameter(f"{path} must contain a JSON object list")
    return [cast(Mapping[str, object], item) for item in payload]


def _read_json_payload(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        message = error.strerror or str(error)
        raise typer.BadParameter(f"could not read JSON file {path}: {message}") from error
    except json.JSONDecodeError as error:
        raise typer.BadParameter(f"malformed JSON in {path}: {error.msg}") from error


def _parse_date(value: str, param_hint: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise typer.BadParameter("expected YYYY-MM-DD", param_hint=param_hint) from error


def _mapping_field(payload: Mapping[str, object], field_name: str) -> Mapping[str, object]:
    value = payload.get(field_name)
    if not isinstance(value, dict):
        raise typer.BadParameter(f"{field_name} must be a JSON object")
    return cast(Mapping[str, object], value)


def _string_field(payload: Mapping[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise typer.BadParameter(f"{field_name} must be a non-empty string")
    return value


def _object_list_field(payload: Mapping[str, object], field_name: str) -> list[Mapping[str, object]]:
    value = payload.get(field_name)
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise typer.BadParameter(f"{field_name} must be a JSON object list")
    return [cast(Mapping[str, object], item) for item in value]


def _mapping_list_field(payload: Mapping[str, object], field_name: str) -> list[Mapping[str, object]]:
    return _object_list_field(payload, field_name)


def _validate_export_metadata_cutoff(metadata: ExportMetadata, as_of: date) -> None:
    if metadata.trained_until > as_of:
        raise typer.BadParameter(
            "trained_until must be on or before --as-of",
            param_hint="--plan-json",
        )
    if metadata.available_at_cutoff.date() > as_of:
        raise typer.BadParameter(
            "available_at_cutoff date must be on or before --as-of",
            param_hint="--plan-json",
        )


def _emit_json(payload: Mapping[str, Any], output_json: Path | None) -> None:
    encoded = json.dumps(payload, default=_json_default, sort_keys=True)
    if output_json is None:
        typer.echo(encoded)
        return
    output_json.write_text(f"{encoded}\n", encoding="utf-8")
    typer.echo(f"wrote export payload {output_json}")


def _json_default(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

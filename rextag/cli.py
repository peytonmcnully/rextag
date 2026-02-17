"""CLI entry point for rextag pipeline."""

import tempfile
from pathlib import Path

import click

from rextag.config import load_config
from rextag.extract import download_from_gcs, unzip_geodatabase, list_layers, extract_layer_to_jsonl
from rextag.load import upload_to_gcs, load_jsonl_to_bigquery
from rextag.schema import build_bq_schema


@click.group()
def main():
    """rextag - Geodatabase to BigQuery ELT pipeline."""
    pass


def run_extract(config_path: Path, source_name: str | None = None):
    """Run extraction for all (or one) configured sources."""
    config = load_config(config_path)
    sources = config.sources

    if source_name:
        sources = [s for s in sources if s.name == source_name]
        if not sources:
            raise click.ClickException(f"Source '{source_name}' not found in config")

    for source in sources:
        click.echo(f"Processing source: {source.name} ({source.uri})")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Download
            zip_path = tmpdir / f"{source.name}.gdb.zip"
            click.echo(f"  Downloading {source.uri}...")
            download_from_gcs(source.uri, zip_path)

            # Unzip
            click.echo("  Extracting geodatabase...")
            gdb_path = unzip_geodatabase(zip_path, tmpdir / "extracted")

            # List and process all layers
            layers = list_layers(gdb_path)
            click.echo(f"  Found {len(layers)} layers: {', '.join(layers)}")

            for layer in layers:
                click.echo(f"  Converting layer: {layer}")
                jsonl_path = tmpdir / f"{layer}.jsonl"

                # Extract to JSONL
                count = extract_layer_to_jsonl(gdb_path, layer, jsonl_path, source.name)
                click.echo(f"    Wrote {count} features")

                # Upload to GCS
                gcs_uri = config.staging_gcs_path(source.name, layer)
                click.echo(f"    Uploading to {gcs_uri}")
                upload_to_gcs(jsonl_path, gcs_uri)

                # Load into BigQuery
                table_id = config.staging_table_id(source.name, layer)
                click.echo(f"    Loading into {table_id}")
                import fiona
                with fiona.open(gdb_path, layer=layer) as collection:
                    schema = build_bq_schema(collection.schema)
                load_jsonl_to_bigquery(gcs_uri, table_id, schema=schema)

                click.echo(f"    Done: {layer}")

        click.echo(f"Completed: {source.name}")


def run_list(source_uri: str):
    """List layers in a geodatabase from GCS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        zip_path = tmpdir / "source.gdb.zip"

        click.echo(f"Downloading {source_uri}...")
        download_from_gcs(source_uri, zip_path)

        gdb_path = unzip_geodatabase(zip_path, tmpdir / "extracted")
        layers = list_layers(gdb_path)

        click.echo(f"\nLayers in {source_uri}:")
        for layer in layers:
            click.echo(f"  - {layer}")


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path), default="config.yml")
@click.option("--source", "source_name", default=None, help="Extract a single source by name")
def extract(config_path: Path, source_name: str | None):
    """Extract geodatabases from GCS and load into BigQuery staging."""
    run_extract(config_path, source_name)


@main.command("list")
@click.option("--source", required=True, help="GCS URI of a geodatabase zip file")
def list_cmd(source: str):
    """List layers in a geodatabase."""
    run_list(source)

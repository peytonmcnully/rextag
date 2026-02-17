"""CLI entry point for rextag pipeline."""

import tempfile
from pathlib import Path

import click

from rextag.config import load_config
from rextag.extract import (
    download_from_gcs,
    list_blobs,
    parse_data_drop,
    has_geometry,
    unzip_geodatabase,
    list_layers,
    extract_layer_to_jsonl,
)
from rextag.load import upload_to_gcs
from rextag.scan import inspect_geodatabase, generate_dbt_files


@click.group()
def main():
    """rextag - Geodatabase to BigQuery ELT pipeline."""
    pass


def run_scan(
    prefix: str,
    output_dir: Path,
    staging_bucket: str,
    staging_prefix: str,
):
    """Scan all zips under a GCS prefix, discover schemas, generate dbt files."""
    click.echo(f"Scanning {prefix}")
    zip_uris = list_blobs(prefix, suffix=".zip")
    click.echo(f"Found {len(zip_uris)} zip files")

    for zip_uri in zip_uris:
        filename = zip_uri.rsplit("/", 1)[-1]
        dataset_name = filename.replace(".zip", "").lower()

        click.echo(f"\n  Dataset: {dataset_name} ({filename})")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            zip_path = tmpdir / filename

            click.echo("    Downloading...")
            download_from_gcs(zip_uri, zip_path)

            click.echo("    Extracting...")
            gdb_path = unzip_geodatabase(zip_path, tmpdir / "extracted")

            click.echo("    Inspecting layers...")
            dataset = inspect_geodatabase(gdb_path, dataset_name)

            for layer in dataset.layers:
                ext = layer.file_extension
                geom_str = layer.geometry_type or "no geometry"
                n_cols = len(layer.fiona_schema["properties"])
                click.echo(f"      {layer.name}: {geom_str}, {n_cols} fields -> .{ext}")

            click.echo("    Generating dbt files...")
            out_path = generate_dbt_files(dataset, output_dir, staging_bucket, staging_prefix)
            click.echo(f"    Written to {out_path}")

    click.echo(f"\nScan complete. Review generated files in {output_dir}")


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

        data_drop = parse_data_drop(source.uri)
        if data_drop is None:
            raise click.ClickException(
                f"Could not parse data_drop from URI: {source.uri}. "
                "Expected format: .../data_drop=VALUE/..."
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            zip_path = tmpdir / f"{source.name}.zip"
            click.echo(f"  Downloading {source.uri}...")
            download_from_gcs(source.uri, zip_path)

            click.echo("  Extracting geodatabase...")
            gdb_path = unzip_geodatabase(zip_path, tmpdir / "extracted")

            layers = list_layers(gdb_path)
            click.echo(f"  Found {len(layers)} layers: {', '.join(layers)}")

            import fiona

            for layer in layers:
                click.echo(f"  Converting layer: {layer}")

                with fiona.open(gdb_path, layer=layer) as collection:
                    ext = "geojsonl" if has_geometry(collection.schema) else "jsonl"

                local_path = tmpdir / f"data.{ext}"
                count = extract_layer_to_jsonl(gdb_path, layer, local_path, source.name)
                click.echo(f"    Wrote {count} features ({ext})")

                gcs_uri = config.hive_staging_path(source.name, layer, data_drop, ext)
                click.echo(f"    Uploading to {gcs_uri}")
                upload_to_gcs(local_path, gcs_uri)

                click.echo(f"    Done: {layer}")

        click.echo(f"Completed: {source.name}")


def run_list(source_uri: str):
    """List layers in a geodatabase from GCS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        zip_path = tmpdir / "source.zip"

        click.echo(f"Downloading {source_uri}...")
        download_from_gcs(source_uri, zip_path)

        gdb_path = unzip_geodatabase(zip_path, tmpdir / "extracted")
        layers = list_layers(gdb_path)

        click.echo(f"\nLayers in {source_uri}:")
        for layer in layers:
            click.echo(f"  - {layer}")


@main.command()
@click.option("--prefix", required=True, help="GCS prefix to scan for zip files")
@click.option("--output-dir", type=click.Path(path_type=Path), required=True, help="Directory to write generated dbt files")
@click.option("--staging-bucket", required=True, help="GCS bucket for staged data")
@click.option("--staging-prefix", default="staged/", help="GCS prefix under bucket for staged data")
def scan(prefix: str, output_dir: Path, staging_bucket: str, staging_prefix: str):
    """Scan geodatabases in GCS and generate dbt source definitions."""
    run_scan(prefix, output_dir, staging_bucket, staging_prefix)


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True, path_type=Path), default="config.yml")
@click.option("--source", "source_name", default=None, help="Extract a single source by name")
def extract(config_path: Path, source_name: str | None):
    """Extract geodatabases from GCS to hive-partitioned staging paths."""
    run_extract(config_path, source_name)


@main.command("list")
@click.option("--source", required=True, help="GCS URI of a geodatabase zip file")
def list_cmd(source: str):
    """List layers in a geodatabase."""
    run_list(source)

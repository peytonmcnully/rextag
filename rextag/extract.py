"""Download and read geodatabase files from GCS."""

import re
import zipfile
from pathlib import Path

import fiona
from google.cloud import storage


def download_from_gcs(gcs_uri: str, dest: Path) -> None:
    """Download a file from GCS to a local path.

    Args:
        gcs_uri: Full GCS URI like gs://bucket/path/to/file.gdb.zip
        dest: Local destination file path
    """
    parts = gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_path = parts[1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))


def unzip_geodatabase(zip_path: Path, dest_dir: Path) -> Path:
    """Unzip a .gdb.zip file and return the path to the .gdb directory.

    Args:
        zip_path: Path to the .gdb.zip file
        dest_dir: Directory to extract into

    Returns:
        Path to the extracted .gdb directory
    """
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)

    gdb_dirs = list(dest_dir.glob("*.gdb"))
    if not gdb_dirs:
        raise ValueError(f"No .gdb directory found in {zip_path}")
    return gdb_dirs[0]


def list_layers(gdb_path: Path) -> list[str]:
    """List all layer names in a geodatabase.

    Args:
        gdb_path: Path to the .gdb directory

    Returns:
        List of layer name strings
    """
    return fiona.listlayers(gdb_path)


def list_blobs(gcs_prefix: str, suffix: str | None = None) -> list[str]:
    """List blob URIs under a GCS prefix."""
    parts = gcs_prefix.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    uris = []
    for blob in blobs:
        uri = f"gs://{bucket_name}/{blob.name}"
        if suffix is None or blob.name.endswith(suffix):
            uris.append(uri)
    return uris


def parse_data_drop(uri: str) -> str | None:
    """Extract the data_drop value from a GCS URI containing data_drop=VALUE."""
    match = re.search(r"data_drop=([^/]+)", uri)
    return match.group(1) if match else None


def has_geometry(fiona_schema: dict) -> bool:
    """Check if a Fiona schema has a geometry column."""
    geom_type = fiona_schema.get("geometry")
    return geom_type is not None and str(geom_type) != "None"


def extract_layer_to_jsonl(
    gdb_path: Path,
    layer_name: str,
    output_path: Path,
    source_file: str,
) -> int:
    """Extract a single layer from a geodatabase to a JSONL file.

    Args:
        gdb_path: Path to the .gdb directory
        layer_name: Name of the layer to extract
        output_path: Path to write the JSONL output file
        source_file: Name of the source file (for metadata)

    Returns:
        Number of features written
    """
    from rextag.convert import convert_features

    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0

    with fiona.open(gdb_path, layer=layer_name) as collection:
        if has_geometry(collection.schema) and collection.crs:
            crs = collection.crs.get("init", "EPSG:4326") if isinstance(collection.crs, dict) else str(collection.crs)
            if not crs or crs.strip() == "":
                crs = "EPSG:4326"
        else:
            crs = "EPSG:4326"
        lines = convert_features(collection, crs=crs, source_file=source_file, layer_name=layer_name)

        with open(output_path, "w") as f:
            for line in lines:
                f.write(line + "\n")
                count += 1

    return count

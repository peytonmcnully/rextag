"""Convert geodatabase features to GeoJSONL rows for BigQuery loading."""

import json
from collections.abc import Iterable, Iterator
from datetime import datetime, timezone

from pyproj import Transformer


def needs_reprojection(crs: str) -> bool:
    """Check if CRS needs reprojection to WGS84 (EPSG:4326)."""
    return crs.upper() != "EPSG:4326"


def reproject_geometry(geometry: dict, source_crs: str) -> dict:
    """Reproject a GeoJSON geometry dict from source_crs to EPSG:4326."""
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)

    def transform_coords(coords):
        if isinstance(coords[0], (int, float)):
            x, y = transformer.transform(coords[0], coords[1])
            return [x, y] if len(coords) == 2 else [x, y] + list(coords[2:])
        return [transform_coords(c) for c in coords]

    return {
        "type": geometry["type"],
        "coordinates": transform_coords(geometry["coordinates"]),
    }


def feature_to_row(
    feature: dict,
    source_file: str,
    layer_name: str,
    transformer: Transformer | None = None,
) -> dict:
    """Convert a single Fiona feature to a flat dict row for JSONL output.

    Geometry is serialized as a JSON string. Properties are flattened to
    top-level keys. Metadata columns are added.
    """
    row = {}

    # Geometry
    geom = feature.get("geometry")
    if geom is not None:
        if transformer is not None:
            geom = _reproject_with_transformer(geom, transformer)
        row["geometry"] = json.dumps(geom)
    else:
        row["geometry"] = None

    # Flatten properties
    props = feature.get("properties", {})
    for key, value in props.items():
        row[key] = value

    # Metadata
    row["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    row["_source_file"] = source_file
    row["_layer_name"] = layer_name

    return row


def _reproject_with_transformer(geometry: dict, transformer: Transformer) -> dict:
    """Reproject geometry using a pre-built Transformer."""

    def transform_coords(coords):
        if isinstance(coords[0], (int, float)):
            x, y = transformer.transform(coords[0], coords[1])
            return [x, y] if len(coords) == 2 else [x, y] + list(coords[2:])
        return [transform_coords(c) for c in coords]

    return {
        "type": geometry["type"],
        "coordinates": transform_coords(geometry["coordinates"]),
    }


def convert_features(
    features: Iterable[dict],
    crs: str,
    source_file: str,
    layer_name: str,
) -> Iterator[str]:
    """Convert an iterable of Fiona features to JSONL lines.

    Yields one JSON string per feature. Handles reprojection if needed.
    This is a streaming generator to handle large datasets without
    loading everything into memory.
    """
    transformer = None
    if needs_reprojection(crs):
        transformer = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)

    for feature in features:
        row = feature_to_row(feature, source_file, layer_name, transformer)
        yield json.dumps(row)

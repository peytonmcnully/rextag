"""Schema discovery — inspect geodatabases and build a catalog of datasets/layers/schemas."""

from dataclasses import dataclass, field
from pathlib import Path

import fiona

from rextag.extract import has_geometry
from rextag.schema import fiona_type_to_bq


@dataclass
class LayerInfo:
    """Discovered metadata for a single geodatabase layer."""

    name: str
    geometry_type: str | None
    fiona_schema: dict

    @property
    def file_extension(self) -> str:
        """File extension based on geometry presence: geojsonl or jsonl."""
        return "geojsonl" if has_geometry(self.fiona_schema) else "jsonl"

    @property
    def bq_columns(self) -> list[dict]:
        """Column definitions mapped to BigQuery types."""
        cols = []
        if has_geometry(self.fiona_schema):
            cols.append({
                "name": "geometry",
                "data_type": "GEOGRAPHY",
                "source_type": self.geometry_type,
            })
        for col_name, fiona_type in self.fiona_schema["properties"].items():
            cols.append({
                "name": col_name,
                "data_type": fiona_type_to_bq(fiona_type),
                "source_type": fiona_type,
            })
        cols.append({"name": "_loaded_at", "data_type": "TIMESTAMP", "source_type": None})
        cols.append({"name": "_source_file", "data_type": "STRING", "source_type": None})
        cols.append({"name": "_layer_name", "data_type": "STRING", "source_type": None})
        return cols


@dataclass
class DatasetInfo:
    """Discovered metadata for a geodatabase (one zip file)."""

    name: str
    layers: list[LayerInfo] = field(default_factory=list)


def inspect_geodatabase(gdb_path: Path, dataset_name: str) -> DatasetInfo:
    """Inspect a geodatabase and return metadata about all its layers."""
    layer_names = fiona.listlayers(gdb_path)
    layers = []

    for layer_name in layer_names:
        with fiona.open(gdb_path, layer=layer_name) as collection:
            schema = collection.schema
            geom_type = schema.get("geometry")
            if geom_type is not None and str(geom_type) == "None":
                geom_type = None

            layers.append(LayerInfo(
                name=layer_name,
                geometry_type=str(geom_type) if geom_type else None,
                fiona_schema=schema,
            ))

    return DatasetInfo(name=dataset_name, layers=layers)

"""Schema inference and mapping from Fiona/OGR types to BigQuery types."""

from google.cloud.bigquery import SchemaField

# Fiona type prefix -> BigQuery type
_TYPE_MAP = {
    "str": "STRING",
    "int": "INT64",
    "float": "FLOAT64",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "time": "TIME",
    "bool": "BOOL",
}


def fiona_type_to_bq(fiona_type: str) -> str:
    """Convert a Fiona field type string to a BigQuery type.

    Fiona types can include width specifiers like 'str:254' or 'int:10'.
    We strip those and map to BigQuery types. Unknown types default to STRING.
    """
    base_type = fiona_type.split(":")[0]
    return _TYPE_MAP.get(base_type, "STRING")


def build_bq_schema(fiona_schema: dict) -> list[SchemaField]:
    """Build a BigQuery schema from a Fiona collection schema.

    Adds a geometry column (STRING for GeoJSON text) and metadata columns.
    """
    fields = []

    # Geometry as STRING (will be cast to GEOGRAPHY in dbt)
    fields.append(SchemaField("geometry", "STRING", mode="NULLABLE"))

    # Property columns
    for name, ftype in fiona_schema["properties"].items():
        bq_type = fiona_type_to_bq(ftype)
        fields.append(SchemaField(name, bq_type, mode="NULLABLE"))

    # Metadata columns
    fields.append(SchemaField("_loaded_at", "TIMESTAMP", mode="NULLABLE"))
    fields.append(SchemaField("_source_file", "STRING", mode="NULLABLE"))
    fields.append(SchemaField("_layer_name", "STRING", mode="NULLABLE"))

    return fields

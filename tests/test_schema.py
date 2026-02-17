"""Tests for rextag.schema."""

from rextag.schema import fiona_type_to_bq, build_bq_schema


class TestFionaTypeToBq:
    def test_str(self):
        assert fiona_type_to_bq("str") == "STRING"

    def test_int(self):
        assert fiona_type_to_bq("int") == "INT64"

    def test_float(self):
        assert fiona_type_to_bq("float") == "FLOAT64"

    def test_date(self):
        assert fiona_type_to_bq("date") == "DATE"

    def test_datetime(self):
        assert fiona_type_to_bq("datetime") == "TIMESTAMP"

    def test_time(self):
        assert fiona_type_to_bq("time") == "TIME"

    def test_unknown_defaults_to_string(self):
        assert fiona_type_to_bq("bytes") == "STRING"

    def test_int_with_width(self):
        assert fiona_type_to_bq("int:10") == "INT64"

    def test_str_with_width(self):
        assert fiona_type_to_bq("str:254") == "STRING"


class TestBuildBqSchema:
    def test_basic_schema(self, sample_fiona_schema):
        schema = build_bq_schema(sample_fiona_schema)
        names = [f.name for f in schema]
        assert "geometry" in names
        assert "OBJECTID" in names
        assert "NAME" in names
        assert "AREA_SQFT" in names

        geom_field = next(f for f in schema if f.name == "geometry")
        assert geom_field.field_type == "STRING"

        int_field = next(f for f in schema if f.name == "OBJECTID")
        assert int_field.field_type == "INT64"

    def test_adds_metadata_columns(self, sample_fiona_schema):
        schema = build_bq_schema(sample_fiona_schema)
        names = [f.name for f in schema]
        assert "_loaded_at" in names
        assert "_source_file" in names
        assert "_layer_name" in names

        loaded_at = next(f for f in schema if f.name == "_loaded_at")
        assert loaded_at.field_type == "TIMESTAMP"

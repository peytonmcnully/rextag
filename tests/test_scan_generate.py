"""Tests for dbt file generation from scan results."""

import yaml
import pytest
from rextag.scan import LayerInfo, DatasetInfo, generate_sources_yml, generate_staging_sql


@pytest.fixture
def dataset_with_geometry():
    return DatasetInfo(
        name="county_parcels",
        layers=[
            LayerInfo(
                name="parcels",
                geometry_type="Polygon",
                fiona_schema={
                    "geometry": "Polygon",
                    "properties": {"OBJECTID": "int", "NAME": "str:254"},
                },
            ),
        ],
    )


@pytest.fixture
def dataset_mixed():
    return DatasetInfo(
        name="county_data",
        layers=[
            LayerInfo(
                name="boundaries",
                geometry_type="Polygon",
                fiona_schema={
                    "geometry": "Polygon",
                    "properties": {"GEO_ID": "int", "AREA": "float"},
                },
            ),
            LayerInfo(
                name="owners",
                geometry_type=None,
                fiona_schema={
                    "geometry": None,
                    "properties": {"OWNER_ID": "int", "OWNER_NAME": "str:100"},
                },
            ),
        ],
    )


class TestGenerateSourcesYml:
    def test_produces_valid_yaml(self, dataset_with_geometry):
        result = generate_sources_yml(
            dataset_with_geometry,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )
        parsed = yaml.safe_load(result)
        assert parsed["version"] == 2
        assert len(parsed["sources"]) == 1
        assert parsed["sources"][0]["name"] == "county_parcels"

    def test_table_has_external_config(self, dataset_with_geometry):
        result = generate_sources_yml(
            dataset_with_geometry,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )
        parsed = yaml.safe_load(result)
        table = parsed["sources"][0]["tables"][0]
        assert table["name"] == "parcels"
        ext = table["external"]
        assert "geojsonl" in ext["location"]
        assert ext["options"]["format"] == "JSON"
        assert ext["options"]["json_extension"] == "GEOJSON"
        assert "hive_partition_uri_prefix" in ext["options"]

    def test_columns_have_types(self, dataset_with_geometry):
        result = generate_sources_yml(
            dataset_with_geometry,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )
        parsed = yaml.safe_load(result)
        columns = parsed["sources"][0]["tables"][0]["columns"]
        names = [c["name"] for c in columns]
        assert "geometry" in names
        assert "OBJECTID" in names
        assert "_loaded_at" in names

        geom_col = next(c for c in columns if c["name"] == "geometry")
        assert geom_col["data_type"] == "GEOGRAPHY"

    def test_no_geometry_layer_uses_jsonl(self, dataset_mixed):
        result = generate_sources_yml(
            dataset_mixed,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )
        parsed = yaml.safe_load(result)
        tables = parsed["sources"][0]["tables"]
        owners_table = next(t for t in tables if t["name"] == "owners")
        assert "jsonl" in owners_table["external"]["location"]
        assert "json_extension" not in owners_table["external"]["options"]

    def test_meta_rename_null_by_default(self, dataset_with_geometry):
        result = generate_sources_yml(
            dataset_with_geometry,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )
        parsed = yaml.safe_load(result)
        columns = parsed["sources"][0]["tables"][0]["columns"]
        objectid_col = next(c for c in columns if c["name"] == "OBJECTID")
        assert objectid_col["meta"]["rename"] is None


class TestGenerateStagingSql:
    def test_geojsonl_layer(self, dataset_with_geometry):
        layer = dataset_with_geometry.layers[0]
        result = generate_staging_sql(dataset_with_geometry.name, layer)
        assert "county_parcels" in result
        assert "parcels" in result

    def test_contains_source_ref(self, dataset_with_geometry):
        layer = dataset_with_geometry.layers[0]
        result = generate_staging_sql(dataset_with_geometry.name, layer)
        assert "source(" in result


class TestGenerateToDisk:
    def test_writes_files(self, tmp_path, dataset_mixed):
        from rextag.scan import generate_dbt_files

        generate_dbt_files(
            dataset_mixed,
            output_dir=tmp_path,
            staging_bucket="siteselect-dbt",
            staging_prefix="staged",
        )

        dataset_dir = tmp_path / "county_data"
        assert dataset_dir.is_dir()
        assert (dataset_dir / "_sources.yml").exists()
        assert (dataset_dir / "stg_county_data_boundaries.sql").exists()
        assert (dataset_dir / "stg_county_data_owners.sql").exists()

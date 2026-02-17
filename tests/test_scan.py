"""Tests for rextag.scan."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from rextag.scan import inspect_geodatabase, LayerInfo, DatasetInfo


@pytest.fixture
def mock_fiona_polygon_schema():
    return {
        "geometry": "Polygon",
        "properties": {
            "OBJECTID": "int",
            "NAME": "str:254",
            "AREA": "float",
        },
    }


@pytest.fixture
def mock_fiona_none_schema():
    return {
        "geometry": None,
        "properties": {
            "OWNER_ID": "int",
            "OWNER_NAME": "str:100",
        },
    }


class TestLayerInfo:
    def test_file_extension_with_geometry(self, mock_fiona_polygon_schema):
        layer = LayerInfo(
            name="parcels",
            geometry_type="Polygon",
            fiona_schema=mock_fiona_polygon_schema,
        )
        assert layer.file_extension == "geojsonl"

    def test_file_extension_without_geometry(self, mock_fiona_none_schema):
        layer = LayerInfo(
            name="owners",
            geometry_type=None,
            fiona_schema=mock_fiona_none_schema,
        )
        assert layer.file_extension == "jsonl"

    def test_bq_columns_with_geometry(self, mock_fiona_polygon_schema):
        layer = LayerInfo(
            name="parcels",
            geometry_type="Polygon",
            fiona_schema=mock_fiona_polygon_schema,
        )
        cols = layer.bq_columns
        names = [c["name"] for c in cols]
        assert "geometry" in names
        assert "OBJECTID" in names
        assert "_loaded_at" in names
        geom_col = next(c for c in cols if c["name"] == "geometry")
        assert geom_col["data_type"] == "GEOGRAPHY"

    def test_bq_columns_without_geometry(self, mock_fiona_none_schema):
        layer = LayerInfo(
            name="owners",
            geometry_type=None,
            fiona_schema=mock_fiona_none_schema,
        )
        cols = layer.bq_columns
        names = [c["name"] for c in cols]
        assert "geometry" not in names
        assert "OWNER_ID" in names


class TestInspectGeodatabase:
    @patch("rextag.scan.fiona.listlayers")
    @patch("rextag.scan.fiona.open")
    def test_returns_dataset_info(self, mock_fiona_open, mock_listlayers, mock_fiona_polygon_schema):
        mock_listlayers.return_value = ["parcels", "zoning"]

        mock_collection = MagicMock()
        mock_collection.schema = mock_fiona_polygon_schema
        mock_fiona_open.return_value.__enter__ = lambda self: mock_collection
        mock_fiona_open.return_value.__exit__ = MagicMock(return_value=False)

        result = inspect_geodatabase(Path("/tmp/test.gdb"), "test_dataset")

        assert isinstance(result, DatasetInfo)
        assert result.name == "test_dataset"
        assert len(result.layers) == 2
        assert result.layers[0].name == "parcels"
        assert result.layers[0].geometry_type == "Polygon"

    @patch("rextag.scan.fiona.listlayers")
    @patch("rextag.scan.fiona.open")
    def test_handles_mixed_geometry(self, mock_fiona_open, mock_listlayers, mock_fiona_polygon_schema, mock_fiona_none_schema):
        mock_listlayers.return_value = ["parcels", "owners"]

        schemas = [mock_fiona_polygon_schema, mock_fiona_none_schema]
        call_count = 0

        def open_side_effect(path, layer=None):
            nonlocal call_count
            mock_col = MagicMock()
            mock_col.schema = schemas[call_count]
            mock_col.__enter__ = MagicMock(return_value=mock_col)
            mock_col.__exit__ = MagicMock(return_value=False)
            call_count += 1
            return mock_col

        mock_fiona_open.side_effect = open_side_effect

        result = inspect_geodatabase(Path("/tmp/test.gdb"), "mixed")

        assert result.layers[0].file_extension == "geojsonl"
        assert result.layers[1].file_extension == "jsonl"

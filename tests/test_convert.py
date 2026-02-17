"""Tests for rextag.convert."""

import json
import pytest
from rextag.convert import feature_to_row, convert_features, needs_reprojection, reproject_geometry


class TestFeatureToRow:
    def test_flattens_properties(self, sample_feature):
        row = feature_to_row(sample_feature, source_file="test.gdb.zip", layer_name="parcels")
        assert row["OBJECTID"] == 1
        assert row["NAME"] == "Parcel A"
        assert row["AREA_SQFT"] == 5000.5

    def test_geometry_as_json_string(self, sample_feature):
        row = feature_to_row(sample_feature, source_file="test.gdb.zip", layer_name="parcels")
        geom = json.loads(row["geometry"])
        assert geom["type"] == "Polygon"
        assert len(geom["coordinates"][0]) == 5

    def test_adds_metadata(self, sample_feature):
        row = feature_to_row(sample_feature, source_file="test.gdb.zip", layer_name="parcels")
        assert row["_source_file"] == "test.gdb.zip"
        assert row["_layer_name"] == "parcels"
        assert "_loaded_at" in row

    def test_handles_none_properties(self, sample_feature):
        row = feature_to_row(sample_feature, source_file="test.gdb.zip", layer_name="parcels")
        assert row["NOTES"] is None

    def test_null_geometry(self):
        feature = {
            "type": "Feature",
            "geometry": None,
            "properties": {"OBJECTID": 1},
        }
        row = feature_to_row(feature, source_file="test.gdb.zip", layer_name="layer")
        assert row["geometry"] is None


class TestNeedsReprojection:
    def test_wgs84_no_reprojection(self):
        assert needs_reprojection("EPSG:4326") is False

    def test_non_wgs84_needs_reprojection(self):
        assert needs_reprojection("EPSG:2227") is True

    def test_wgs84_alternate_format(self):
        assert needs_reprojection("epsg:4326") is False


class TestReprojectGeometry:
    def test_reproject_point(self):
        geom = {"type": "Point", "coordinates": [6000000.0, 2100000.0]}
        result = reproject_geometry(geom, "EPSG:2227")
        # Should be reprojected to WGS84 (lon/lat near California)
        assert result["type"] == "Point"
        lon, lat = result["coordinates"]
        assert -125.0 < lon < -115.0  # California longitude range
        assert 32.0 < lat < 42.0     # California latitude range


class TestConvertFeatures:
    def test_converts_to_jsonl_lines(self, sample_feature):
        features = [sample_feature, sample_feature]
        lines = list(convert_features(features, crs="EPSG:4326", source_file="test.gdb.zip", layer_name="parcels"))
        assert len(lines) == 2
        row = json.loads(lines[0])
        assert row["OBJECTID"] == 1
        assert "geometry" in row

    def test_streaming_generator(self, sample_feature):
        features = [sample_feature] * 100
        gen = convert_features(features, crs="EPSG:4326", source_file="test.gdb.zip", layer_name="parcels")
        # Should be a generator, not a list
        first = next(gen)
        assert json.loads(first)["OBJECTID"] == 1

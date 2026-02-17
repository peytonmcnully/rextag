"""Tests for extract helper functions."""

from unittest.mock import MagicMock, patch

from rextag.extract import list_blobs, parse_data_drop, has_geometry


class TestListBlobs:
    @patch("rextag.extract.storage.Client")
    def test_lists_zip_blobs(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        blob1 = MagicMock()
        blob1.name = "rextagsource/data_drop=2026-01/parcels.zip"
        blob2 = MagicMock()
        blob2.name = "rextagsource/data_drop=2026-01/zoning.zip"
        blob3 = MagicMock()
        blob3.name = "rextagsource/data_drop=2026-01/readme.txt"
        mock_bucket.list_blobs.return_value = [blob1, blob2, blob3]

        result = list_blobs("gs://siteselect-dbt/rextagsource/data_drop=2026-01/", suffix=".zip")

        mock_client.bucket.assert_called_once_with("siteselect-dbt")
        mock_bucket.list_blobs.assert_called_once_with(prefix="rextagsource/data_drop=2026-01/")
        assert result == [
            "gs://siteselect-dbt/rextagsource/data_drop=2026-01/parcels.zip",
            "gs://siteselect-dbt/rextagsource/data_drop=2026-01/zoning.zip",
        ]

    @patch("rextag.extract.storage.Client")
    def test_lists_all_blobs_no_suffix(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        blob1 = MagicMock()
        blob1.name = "path/file.zip"
        mock_bucket.list_blobs.return_value = [blob1]

        result = list_blobs("gs://bucket/path/")
        assert len(result) == 1


class TestParseDataDrop:
    def test_parses_from_uri(self):
        uri = "gs://siteselect-dbt/rextagsource/data_drop=2026-01/parcels.zip"
        assert parse_data_drop(uri) == "2026-01"

    def test_parses_from_prefix(self):
        prefix = "gs://siteselect-dbt/rextagsource/data_drop=2026-01/"
        assert parse_data_drop(prefix) == "2026-01"

    def test_returns_none_when_missing(self):
        uri = "gs://bucket/path/to/file.zip"
        assert parse_data_drop(uri) is None


class TestHasGeometry:
    def test_polygon_has_geometry(self):
        schema = {"geometry": "Polygon", "properties": {"ID": "int"}}
        assert has_geometry(schema) is True

    def test_point_has_geometry(self):
        schema = {"geometry": "Point", "properties": {"ID": "int"}}
        assert has_geometry(schema) is True

    def test_none_geometry(self):
        schema = {"geometry": None, "properties": {"ID": "int"}}
        assert has_geometry(schema) is False

    def test_string_none_geometry(self):
        schema = {"geometry": "None", "properties": {"ID": "int"}}
        assert has_geometry(schema) is False

    def test_missing_geometry_key(self):
        schema = {"properties": {"ID": "int"}}
        assert has_geometry(schema) is False

"""Tests for rextag.extract."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rextag.extract import (
    download_from_gcs,
    unzip_geodatabase,
    list_layers,
)


@pytest.fixture
def fake_gdb_zip(tmp_path):
    """Create a minimal fake .gdb.zip for testing unzip logic."""
    gdb_dir = tmp_path / "test.gdb"
    gdb_dir.mkdir()
    (gdb_dir / "a00000001.gdbtable").write_text("fake")
    zip_path = tmp_path / "test.gdb.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        for f in gdb_dir.rglob("*"):
            zf.write(f, f.relative_to(tmp_path))
    return zip_path


class TestUnzipGeodatabase:
    def test_extracts_gdb_directory(self, fake_gdb_zip, tmp_path):
        result = unzip_geodatabase(fake_gdb_zip, tmp_path / "output")
        assert result.name == "test.gdb"
        assert result.is_dir()
        assert (result / "a00000001.gdbtable").exists()

    def test_raises_on_missing_zip(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            unzip_geodatabase(tmp_path / "nonexistent.zip", tmp_path / "output")


class TestDownloadFromGcs:
    @patch("rextag.extract.storage.Client")
    def test_downloads_blob_to_local(self, mock_client_cls, tmp_path):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        dest = tmp_path / "downloaded.gdb.zip"
        download_from_gcs("gs://my-bucket/path/to/file.gdb.zip", dest)

        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("path/to/file.gdb.zip")
        mock_blob.download_to_filename.assert_called_once_with(str(dest))


class TestListLayers:
    @patch("rextag.extract.fiona.listlayers")
    def test_returns_layer_names(self, mock_listlayers):
        mock_listlayers.return_value = ["parcels", "zoning", "roads"]
        layers = list_layers(Path("/tmp/test.gdb"))
        assert layers == ["parcels", "zoning", "roads"]
        mock_listlayers.assert_called_once_with(Path("/tmp/test.gdb"))

"""Tests for rextag.load."""

from unittest.mock import MagicMock, patch

from rextag.load import upload_to_gcs


class TestUploadToGcs:
    @patch("rextag.load.storage.Client")
    def test_uploads_file(self, mock_client_cls, tmp_path):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        local_file = tmp_path / "test.jsonl"
        local_file.write_text('{"a": 1}\n')

        upload_to_gcs(local_file, "gs://staging-bucket/rextag/staging/parcels/layer.jsonl")

        mock_client.bucket.assert_called_once_with("staging-bucket")
        mock_bucket.blob.assert_called_once_with("rextag/staging/parcels/layer.jsonl")
        mock_blob.upload_from_filename.assert_called_once_with(str(local_file))

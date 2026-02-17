"""Tests for rextag.load."""

from unittest.mock import MagicMock, patch

from rextag.load import upload_to_gcs, load_jsonl_to_bigquery


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


class TestLoadJsonlToBigquery:
    @patch("rextag.load.bigquery.Client")
    def test_creates_load_job(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_job = MagicMock()
        mock_client.load_table_from_uri.return_value = mock_job

        load_jsonl_to_bigquery(
            gcs_uri="gs://staging-bucket/rextag/staging/parcels/layer.jsonl",
            table_id="project.dataset.parcels_layer",
            schema=None,
        )

        mock_client.load_table_from_uri.assert_called_once()
        args = mock_client.load_table_from_uri.call_args
        assert args[0][0] == "gs://staging-bucket/rextag/staging/parcels/layer.jsonl"
        assert args[0][1] == "project.dataset.parcels_layer"
        mock_job.result.assert_called_once()

    @patch("rextag.load.bigquery.Client")
    def test_uses_write_truncate(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_job = MagicMock()
        mock_client.load_table_from_uri.return_value = mock_job

        load_jsonl_to_bigquery(
            gcs_uri="gs://bucket/file.jsonl",
            table_id="project.dataset.table",
            schema=None,
        )

        job_config = mock_client.load_table_from_uri.call_args[1]["job_config"]
        assert job_config.write_disposition == "WRITE_TRUNCATE"
        assert job_config.source_format == "NEWLINE_DELIMITED_JSON"

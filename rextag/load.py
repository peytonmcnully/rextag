"""Upload files to GCS."""

from pathlib import Path

from google.cloud import storage


def upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
    """Upload a local file to GCS.

    Args:
        local_path: Path to the local file
        gcs_uri: Full GCS URI like gs://bucket/path/to/file.jsonl
    """
    parts = gcs_uri.replace("gs://", "").split("/", 1)
    bucket_name = parts[0]
    blob_path = parts[1]

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))

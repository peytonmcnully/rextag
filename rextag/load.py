"""Upload JSONL to GCS and load into BigQuery."""

from pathlib import Path

from google.cloud import bigquery, storage


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


def load_jsonl_to_bigquery(
    gcs_uri: str,
    table_id: str,
    schema: list | None,
) -> None:
    """Load a JSONL file from GCS into a BigQuery table.

    Args:
        gcs_uri: GCS URI of the JSONL file
        table_id: Fully-qualified BigQuery table ID (project.dataset.table)
        schema: BigQuery schema fields, or None for autodetect
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    if schema is not None:
        job_config.schema = schema
    else:
        job_config.autodetect = True

    job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    job.result()  # Wait for completion

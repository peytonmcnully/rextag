"""Pipeline configuration loading and validation."""

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SourceConfig:
    """A single geodatabase source definition."""

    name: str
    uri: str


@dataclass(frozen=True)
class PipelineConfig:
    """Full pipeline configuration."""

    gcs_source_bucket: str
    gcs_staging_bucket: str
    gcs_staging_prefix: str
    bq_project: str
    bq_staging_dataset: str
    bq_target_dataset: str
    sources: list[SourceConfig]

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineConfig":
        gcs = data["gcs"]
        bq = data["bigquery"]
        sources = [SourceConfig(**s) for s in data["sources"]]
        return cls(
            gcs_source_bucket=gcs["source_bucket"],
            gcs_staging_bucket=gcs["staging_bucket"],
            gcs_staging_prefix=gcs["staging_prefix"],
            bq_project=bq["project"],
            bq_staging_dataset=bq["staging_dataset"],
            bq_target_dataset=bq["target_dataset"],
            sources=sources,
        )

    def staging_gcs_path(self, dataset_name: str, layer_name: str) -> str:
        """GCS URI for a staging JSONL file."""
        prefix = self.gcs_staging_prefix.rstrip("/")
        return f"gs://{self.gcs_staging_bucket}/{prefix}/{dataset_name}/{layer_name}.jsonl"

    def staging_table_id(self, dataset_name: str, layer_name: str) -> str:
        """Fully-qualified BigQuery table ID for a staging table."""
        return f"{self.bq_project}.{self.bq_staging_dataset}.{dataset_name}_{layer_name}"


def load_config(path: Path) -> PipelineConfig:
    """Load pipeline config from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return PipelineConfig.from_dict(data)

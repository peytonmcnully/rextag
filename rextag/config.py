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

    gcs_staging_bucket: str
    gcs_staging_prefix: str
    sources: list[SourceConfig]
    scan_source_prefix: str | None = None
    scan_dbt_output_dir: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineConfig":
        gcs = data["gcs"]
        sources = [SourceConfig(**s) for s in data.get("sources", [])]

        scan = data.get("scan", {})

        return cls(
            gcs_staging_bucket=gcs["staging_bucket"],
            gcs_staging_prefix=gcs["staging_prefix"],
            sources=sources,
            scan_source_prefix=scan.get("source_prefix"),
            scan_dbt_output_dir=scan.get("dbt_output_dir"),
        )

    def hive_staging_path(
        self, dataset_name: str, layer_name: str, data_drop: str, extension: str
    ) -> str:
        """GCS URI for a hive-partitioned staging file.

        Returns: gs://bucket/prefix/dataset/layer/data_drop=VALUE/data.EXT
        """
        prefix = self.gcs_staging_prefix.rstrip("/")
        return (
            f"gs://{self.gcs_staging_bucket}/{prefix}/"
            f"{dataset_name}/{layer_name}/data_drop={data_drop}/data.{extension}"
        )

    def staging_gcs_path(self, dataset_name: str, layer_name: str) -> str:
        """GCS URI for a staging JSONL file (legacy flat layout)."""
        prefix = self.gcs_staging_prefix.rstrip("/")
        return f"gs://{self.gcs_staging_bucket}/{prefix}/{dataset_name}/{layer_name}.jsonl"


def load_config(path: Path) -> PipelineConfig:
    """Load pipeline config from a YAML file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return PipelineConfig.from_dict(data)

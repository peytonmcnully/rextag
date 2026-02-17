"""Tests for rextag.config."""

import pytest
import yaml
from rextag.config import PipelineConfig, load_config


@pytest.fixture
def config_dict():
    return {
        "gcs": {
            "source_bucket": "test-source",
            "staging_bucket": "test-staging",
            "staging_prefix": "rextag/staging/",
        },
        "bigquery": {
            "project": "test-project",
            "staging_dataset": "rextag_staging",
            "target_dataset": "rextag",
        },
        "sources": [
            {"name": "parcels", "uri": "gs://test-source/parcels.gdb.zip"},
            {"name": "zoning", "uri": "gs://test-source/zoning.gdb.zip"},
        ],
    }


@pytest.fixture
def config_file(tmp_path, config_dict):
    path = tmp_path / "config.yml"
    path.write_text(yaml.dump(config_dict))
    return path


class TestPipelineConfig:
    def test_from_dict(self, config_dict):
        config = PipelineConfig.from_dict(config_dict)
        assert config.gcs_source_bucket == "test-source"
        assert config.gcs_staging_bucket == "test-staging"
        assert config.gcs_staging_prefix == "rextag/staging/"
        assert config.bq_project == "test-project"
        assert config.bq_staging_dataset == "rextag_staging"
        assert config.bq_target_dataset == "rextag"
        assert len(config.sources) == 2
        assert config.sources[0].name == "parcels"
        assert config.sources[0].uri == "gs://test-source/parcels.gdb.zip"

    def test_staging_gcs_path(self, config_dict):
        config = PipelineConfig.from_dict(config_dict)
        path = config.staging_gcs_path("parcels", "boundaries")
        assert path == "gs://test-staging/rextag/staging/parcels/boundaries.jsonl"

    def test_staging_table_id(self, config_dict):
        config = PipelineConfig.from_dict(config_dict)
        table_id = config.staging_table_id("parcels", "boundaries")
        assert table_id == "test-project.rextag_staging.parcels_boundaries"


class TestLoadConfig:
    def test_load_from_file(self, config_file):
        config = load_config(config_file)
        assert config.gcs_source_bucket == "test-source"
        assert len(config.sources) == 2

    def test_load_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yml")

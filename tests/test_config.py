"""Tests for rextag.config."""

import pytest
import yaml
from rextag.config import PipelineConfig, load_config


@pytest.fixture
def config_dict():
    return {
        "gcs": {
            "staging_bucket": "test-staging",
            "staging_prefix": "staged/",
        },
        "scan": {
            "source_prefix": "gs://test-source/rextagsource/data_drop=2026-01/",
            "dbt_output_dir": "dbt_project/models/staging/",
        },
        "sources": [
            {"name": "parcels", "uri": "gs://test-source/rextagsource/data_drop=2026-01/parcels.zip"},
            {"name": "zoning", "uri": "gs://test-source/rextagsource/data_drop=2026-01/zoning.zip"},
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
        assert config.gcs_staging_bucket == "test-staging"
        assert config.gcs_staging_prefix == "staged/"
        assert config.scan_source_prefix == "gs://test-source/rextagsource/data_drop=2026-01/"
        assert config.scan_dbt_output_dir == "dbt_project/models/staging/"
        assert len(config.sources) == 2
        assert config.sources[0].name == "parcels"

    def test_hive_staging_path(self, config_dict):
        config = PipelineConfig.from_dict(config_dict)
        path = config.hive_staging_path("parcels", "boundaries", "2026-01", "geojsonl")
        assert path == "gs://test-staging/staged/parcels/boundaries/data_drop=2026-01/data.geojsonl"

    def test_hive_staging_path_jsonl(self, config_dict):
        config = PipelineConfig.from_dict(config_dict)
        path = config.hive_staging_path("parcels", "owners", "2026-01", "jsonl")
        assert path == "gs://test-staging/staged/parcels/owners/data_drop=2026-01/data.jsonl"


class TestPipelineConfigV2:
    def test_backward_compat_without_scan(self):
        data = {
            "gcs": {
                "staging_bucket": "test-staging",
                "staging_prefix": "rextag/staging/",
            },
            "sources": [
                {"name": "parcels", "uri": "gs://test-source/parcels.gdb.zip"},
            ],
        }
        config = PipelineConfig.from_dict(data)
        assert config.scan_source_prefix is None
        assert config.scan_dbt_output_dir is None
        assert config.gcs_staging_bucket == "test-staging"


class TestLoadConfig:
    def test_load_from_file(self, config_file):
        config = load_config(config_file)
        assert config.gcs_staging_bucket == "test-staging"
        assert len(config.sources) == 2

    def test_load_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yml")

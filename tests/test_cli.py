"""Tests for rextag.cli."""

import yaml
from unittest.mock import patch
from click.testing import CliRunner

import pytest
from rextag.cli import main


@pytest.fixture
def config_file(tmp_path):
    config = {
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
        ],
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.dump(config))
    return path


class TestCli:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "rextag" in result.output.lower() or "extract" in result.output.lower()

    def test_extract_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["extract", "--help"])
        assert result.exit_code == 0
        assert "--config" in result.output

    def test_list_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["list", "--help"])
        assert result.exit_code == 0
        assert "--source" in result.output

    @patch("rextag.cli.run_extract")
    def test_extract_calls_run_extract(self, mock_run, config_file):
        runner = CliRunner()
        result = runner.invoke(main, ["extract", "--config", str(config_file)])
        assert result.exit_code == 0
        mock_run.assert_called_once()

    @patch("rextag.cli.run_list")
    def test_list_calls_run_list(self, mock_run):
        runner = CliRunner()
        result = runner.invoke(main, ["list", "--source", "gs://bucket/test.gdb.zip"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with("gs://bucket/test.gdb.zip")

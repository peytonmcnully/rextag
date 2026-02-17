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
            "staging_bucket": "test-staging",
            "staging_prefix": "staged/",
        },
        "scan": {
            "source_prefix": "gs://test-source/rextagsource/data_drop=2026-01/",
            "dbt_output_dir": "dbt_project/models/staging/",
        },
        "sources": [
            {"name": "parcels", "uri": "gs://test-source/rextagsource/data_drop=2026-01/parcels.zip"},
        ],
    }
    path = tmp_path / "config.yml"
    path.write_text(yaml.dump(config))
    return path


class TestCliHelp:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0

    def test_scan_help(self):
        runner = CliRunner()
        result = runner.invoke(main, ["scan", "--help"])
        assert result.exit_code == 0
        assert "--prefix" in result.output

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


class TestScanCommand:
    @patch("rextag.cli.run_scan")
    def test_scan_calls_run_scan(self, mock_run):
        runner = CliRunner()
        result = runner.invoke(main, [
            "scan",
            "--prefix", "gs://bucket/path/data_drop=2026-01/",
            "--output-dir", "/tmp/output",
            "--staging-bucket", "my-bucket",
            "--staging-prefix", "staged/",
        ])
        assert result.exit_code == 0
        mock_run.assert_called_once()


class TestExtractCommand:
    @patch("rextag.cli.run_extract")
    def test_extract_calls_run_extract(self, mock_run, config_file):
        runner = CliRunner()
        result = runner.invoke(main, ["extract", "--config", str(config_file)])
        assert result.exit_code == 0
        mock_run.assert_called_once()


class TestListCommand:
    @patch("rextag.cli.run_list")
    def test_list_calls_run_list(self, mock_run):
        runner = CliRunner()
        result = runner.invoke(main, ["list", "--source", "gs://bucket/test.gdb.zip"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with("gs://bucket/test.gdb.zip")

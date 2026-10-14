"""Unit tests for config loader + pipeline orchestration (no live services)."""

from __future__ import annotations

from pathlib import Path

import pytest

import etl.sinks  # noqa: F401  (register plugins)
import etl.sources  # noqa: F401
import etl.transformers  # noqa: F401
from etl.config.loader import load_config
from etl.core.context import PipelineContext
from etl.core.exceptions import ConfigError
from etl.core.pipeline import run_pipeline


def test_load_config_ok(sample_yaml: Path) -> None:
    """Valid YAML loads into a PipelineConfig."""
    cfg = load_config(sample_yaml)
    assert cfg.name == "test_pipe"
    assert cfg.sources[0].type == "csv"


def test_load_config_missing_file() -> None:
    """Missing path -> ConfigError."""
    with pytest.raises(ConfigError):
        load_config("nope/missing.yaml")


def test_load_config_env_expansion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """${ENV_VAR} placeholders are expanded before validation."""
    monkeypatch.setenv("TEST_CSV_PATH", "data/sample_sales.csv")
    p = tmp_path / "env.yaml"
    p.write_text(
        """
name: env_pipe
sources:
  - {name: s, type: csv, params: {path: "${TEST_CSV_PATH}"}}
sinks:
  - {name: o, type: csv, params: {path: out.csv}}
""",
        encoding="utf-8",
    )
    cfg = load_config(p)
    assert cfg.sources[0].params["path"] == "data/sample_sales.csv"


def test_run_pipeline_csv_to_csv(tmp_path: Path) -> None:
    """End-to-end without Docker: CSV -> clean/enrich -> CSV."""
    import pandas as pd

    src = tmp_path / "in.csv"
    pd.DataFrame(
        {
            "order_id": [1, 2, 2, 3],
            "customer_id": [1, 2, 2, 3],
            "amount": [10.0, 20.0, 20.0, 30.0],
            "quantity": [1, 2, 2, 1],
        }
    ).to_csv(src, index=False)
    out = tmp_path / "out.csv"
    cfg_path = tmp_path / "pipe.yaml"
    cfg_path.write_text(
        f"""
name: e2e
sources:
  - {{name: s, type: csv, params: {{path: {src}, chunk_size: 10}}}}
transformers:
  - {{type: clean, params: {{dedup_on: [order_id]}}}}
  - {{type: enrich, params: {{add_columns: {{revenue: "amount * quantity"}}}}}}
sinks:
  - {{name: o, type: csv, params: {{path: {out}, mode: overwrite}}}}
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    result = run_pipeline(cfg, PipelineContext(max_concurrency=2))
    assert result.rows_in == 4
    assert result.rows_out == 3  # one duplicate removed
    assert out.exists()
    back = pd.read_csv(out)
    assert "revenue" in back.columns


def test_run_pipeline_source_failure_aggregated(tmp_path: Path) -> None:
    """One bad source + one good source -> partial success, failure recorded."""
    import pandas as pd

    good = tmp_path / "good.csv"
    pd.DataFrame({"a": [1, 2]}).to_csv(good, index=False)
    out = tmp_path / "out.csv"
    cfg_path = tmp_path / "pipe.yaml"
    cfg_path.write_text(
        f"""
name: partial
sources:
  - {{name: good, type: csv, params: {{path: {good}}}}}
  - {{name: bad, type: csv, params: {{path: {tmp_path}/missing.csv}}}}
sinks:
  - {{name: o, type: csv, params: {{path: {out}}}}}
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    result = run_pipeline(cfg, PipelineContext(max_concurrency=2))
    assert "good" in result.successes
    assert "bad" in result.failures
    assert result.rows_out == 2

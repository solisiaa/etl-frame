"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    """Small deterministic frame used by transformer tests."""
    return pd.DataFrame(
        {
            "order_id": [1, 2, 2, 3],
            "customer_id": [101, 102, 102, None],
            "amount": [10.0, 20.0, 20.0, None],
            "quantity": [2, 1, 1, 5],
        }
    )


@pytest.fixture()
def tmp_csv(tmp_path: Path) -> Path:
    """CSV file with 100 rows for streaming tests."""
    import numpy as np

    path = tmp_path / "data.csv"
    df = pd.DataFrame(
        {
            "order_id": range(100),
            "customer_id": np.random.default_rng(0).integers(1, 10, 100),
            "amount": np.random.default_rng(1).uniform(1, 100, 100),
            "quantity": np.random.default_rng(2).integers(1, 5, 100),
        }
    )
    df.to_csv(path, index=False)
    return path


@pytest.fixture()
def sample_yaml(tmp_path: Path) -> Path:
    """Minimal valid pipeline YAML (csv -> csv)."""
    text = """
name: test_pipe
description: unit test pipeline
sources:
  - name: s1
    type: csv
    params: {path: data/sample_sales.csv, chunk_size: 1000}
transformers:
  - type: clean
    params: {drop_nulls: [amount]}
sinks:
  - name: out
    type: csv
    params: {path: out.csv, mode: overwrite}
execution: {parallel_sources: true, max_concurrency: 2}
"""
    p = tmp_path / "pipe.yaml"
    p.write_text(text, encoding="utf-8")
    return p

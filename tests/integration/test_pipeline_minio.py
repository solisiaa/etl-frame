"""Integration: Parquet -> MinIO (skipped without reachable S3/MinIO)."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


def _s3_available() -> bool:
    endpoint = os.getenv("MINIO_ENDPOINT")
    if not endpoint:
        return False
    try:
        from etl.io.s3_client import S3Client

        with S3Client(endpoint_url=endpoint) as s3:
            s3.list_buckets()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _s3_available(), reason="MinIO/S3 not available")
def test_pipeline_minio_parquet(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """CSV -> S3 parquet; read back via boto3 and compare row counts."""
    import io

    import pandas as pd

    import etl.sinks  # noqa: F401
    import etl.sources  # noqa: F401
    import etl.transformers  # noqa: F401
    from etl.config.loader import load_config
    from etl.core.context import PipelineContext
    from etl.core.pipeline import run_pipeline
    from etl.io.s3_client import S3Client

    src = tmp_path / "sales.csv"
    df = pd.DataFrame(
        {"order_id": [1, 2], "customer_id": [1, 2], "amount": [9.0, 8.0], "quantity": [1, 2]}
    )
    df.to_csv(src, index=False)
    cfg_path = tmp_path / "s3.yaml"
    cfg_path.write_text(
        f"""
name: s3_e2e
sources:
  - {{name: s, type: csv, params: {{path: {src}}}}}
sinks:
  - {{name: m, type: s3, params: {{bucket: etl-output, key: "test/e2e.parquet", format: parquet}}}}
""",
        encoding="utf-8",
    )
    result = run_pipeline(load_config(cfg_path), PipelineContext())
    assert result.rows_out == 2

    endpoint = os.getenv("MINIO_ENDPOINT")
    with S3Client(endpoint_url=endpoint) as s3:
        buf = io.BytesIO()
        s3.download_fileobj("etl-output", "test/e2e.parquet", buf)
    buf.seek(0)
    back = pd.read_parquet(buf)
    assert len(back) == 2

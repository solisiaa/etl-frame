"""Integration: full YAML -> Postgres (skipped without $POSTGRES_URL / server)."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.integration


def _pg_available() -> bool:
    if not os.getenv("POSTGRES_URL"):
        return False
    try:
        import psycopg

        with psycopg.connect(os.environ["POSTGRES_URL"], connect_timeout=3):
            pass
        return True
    except Exception:
        return False


@pytest.mark.skipif(
    not _pg_available(), reason="Postgres not available (POSTGRES_URL unset/unreachable)"
)
def test_pipeline_postgres_upsert(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """CSV -> clean/aggregate -> Postgres upsert; second run is idempotent."""
    import pandas as pd

    import etl.sinks  # noqa: F401
    import etl.sources  # noqa: F401
    import etl.transformers  # noqa: F401
    from etl.config.loader import load_config
    from etl.core.context import PipelineContext
    from etl.core.pipeline import run_pipeline
    from etl.io.postgres_conn import PostgresConnection

    src = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 1, 2],
            "amount": [10.0, 20.0, 5.0],
            "quantity": [1, 2, 1],
        }
    ).to_csv(src, index=False)
    cfg_path = tmp_path / "pg.yaml"
    cfg_path.write_text(
        f"""
name: pg_e2e
sources:
  - {{name: s, type: csv, params: {{path: {src}}}}}
transformers:
  - {{type: enrich, params: {{add_columns: {{revenue: "amount * quantity"}}}}}}
  - {{type: aggregate, params: {{group_by: [customer_id], aggs: {{revenue: sum, quantity: sum}}}}}}
sinks:
  - {{name: pg, type: postgres, params: {{table: test_sales_agg, mode: upsert,
      conflict_keys: [customer_id], batch_size: 100}}}}
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    for _ in range(2):  # upsert twice -> same row count
        result = run_pipeline(cfg, PipelineContext())
        assert result.rows_out == 2

    with PostgresConnection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM test_sales_agg")
        assert cur.fetchone()[0] == 2
        cur.execute("DROP TABLE test_sales_agg")

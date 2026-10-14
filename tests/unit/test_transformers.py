"""Unit tests for clean / enrich / aggregate transformers."""

from __future__ import annotations

import pandas as pd

from etl.transformers.aggregate import AggregateTransformer
from etl.transformers.clean import CleanTransformer
from etl.transformers.enrich import EnrichTransformer


def test_clean_drop_nulls_and_dedup(sample_frame: pd.DataFrame) -> None:
    """drop_nulls removes NA rows; dedup_on removes repeated keys."""
    tr = CleanTransformer(params={"drop_nulls": ["amount"], "dedup_on": ["order_id"]})
    out = pd.concat(list(tr.transform(iter([sample_frame]))), ignore_index=True)
    # Row with amount=None dropped; one of the duplicated order_id=2 dropped.
    assert len(out) == 2
    assert set(out["order_id"]) == {1, 2}


def test_clean_fillna_and_cast() -> None:
    """fillna + astype are applied per chunk."""
    df = pd.DataFrame({"a": [1.0, None], "b": ["x", "y"]})
    tr = CleanTransformer(params={"fillna": {"a": 0.0}, "astype": {"a": "float"}})
    out = list(tr.transform(iter([df])))[0]
    assert out["a"].tolist() == [1.0, 0.0]


def test_enrich_add_columns() -> None:
    """Computed columns via pandas.eval expressions."""
    df = pd.DataFrame({"amount": [10.0, 20.0], "quantity": [2, 3]})
    tr = EnrichTransformer(params={"add_columns": {"revenue": "amount * quantity"}})
    out = list(tr.transform(iter([df])))[0]
    assert out["revenue"].tolist() == [20.0, 60.0]


def test_aggregate_groupby_sum() -> None:
    """Groupby aggregation with NumPy-backed reductions."""
    df = pd.DataFrame(
        {
            "customer_id": [1, 1, 2],
            "revenue": [10.0, 20.0, 5.0],
            "quantity": [1, 2, 1],
        }
    )
    tr = AggregateTransformer(
        params={"group_by": ["customer_id"], "aggs": {"revenue": "sum", "quantity": "sum"}}
    )
    out = list(tr.transform(iter([df])))[0]
    assert len(out) == 2
    row = out[out["customer_id"] == 1].iloc[0]
    assert row["revenue"] == 30.0
    assert row["quantity"] == 3

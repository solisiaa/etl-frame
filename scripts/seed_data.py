"""Generate deterministic sample sales data for demos and tests."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate(rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Build a reproducible sales table with a few nulls/duplicates."""
    rng = np.random.default_rng(seed)
    n_customers = max(10, rows // 20)
    df = pd.DataFrame(
        {
            "order_id": np.arange(1, rows + 1),
            "customer_id": rng.integers(1, n_customers + 1, size=rows),
            "amount": np.round(rng.uniform(5.0, 500.0, size=rows), 2),
            "quantity": rng.integers(1, 10, size=rows),
            "date": pd.date_range("2024-01-01", periods=rows, freq="min").strftime("%Y-%m-%d"),
        }
    )
    # Inject ~1% nulls and a handful of duplicate order_ids for clean-transformer demo.
    null_idx = rng.choice(rows, size=max(1, rows // 100), replace=False)
    df.loc[null_idx, "amount"] = np.nan
    dup = df.iloc[:5].copy()
    df = pd.concat([df, dup], ignore_index=True)
    return df


def main() -> None:
    """CLI entry: ``python scripts/seed_data.py --rows 5000 --out data/sample_sales.csv``."""
    parser = argparse.ArgumentParser(description="Generate sample sales CSV.")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--out", type=str, default="data/sample_sales.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    df = generate(rows=args.rows, seed=args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"Wrote {len(df)} rows -> {out}")


if __name__ == "__main__":
    main()

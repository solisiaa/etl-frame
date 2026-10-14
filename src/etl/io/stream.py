"""Chunked CSV streaming helpers (generators, low memory footprint)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd


def read_chunks(
    path: str | Path,
    chunk_size: int = 10_000,
    **read_csv_kwargs: Any,
) -> Iterator[pd.DataFrame]:
    """Yield ``pd.DataFrame`` chunks without loading the whole file.

    Args:
        path: CSV file path.
        chunk_size: Rows per chunk.
        read_csv_kwargs: Forwarded to :func:`pandas.read_csv`
            (e.g. ``dtype``, ``parse_dates``, ``usecols``).

    Yields:
        DataFrame chunks of up to ``chunk_size`` rows.

    Example:
        ```python
        for chunk in read_chunks("big.csv", chunk_size=50_000):
            process(chunk)
        ```
    """
    reader = pd.read_csv(str(path), chunksize=chunk_size, **read_csv_kwargs)
    # `reader` is a TextFileReader when chunksize is set.
    yield from reader


def concat_stream(chunks: Iterator[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a lazy stream into a single DataFrame (eager)."""
    frames = list(chunks)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)

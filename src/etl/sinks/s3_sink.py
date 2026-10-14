"""S3/MinIO sink: write stream as Parquet (or CSV) objects."""

from __future__ import annotations

import io
import os
from collections.abc import Iterator
from datetime import date
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from etl.core.base import BaseSink
from etl.core.context import PipelineContext
from etl.core.exceptions import SinkError
from etl.core.registry import register_sink
from etl.decorators import with_metrics
from etl.io.s3_client import S3Client


class S3SinkConfig(BaseModel):
    """Validated params for :class:`S3Sink`."""

    bucket: str
    key: str = Field(description="Object key; supports {date} placeholder.")
    format: Literal["parquet", "csv"] = "parquet"
    endpoint_url: str = Field(default="")
    partition_by: list[str] = Field(default_factory=list)


@register_sink("s3")
class S3Sink(BaseSink):
    """Buffer the stream in memory, serialize once, upload to S3/MinIO."""

    config_schema = S3SinkConfig

    @with_metrics(times=3, backoff=1.0, exceptions=(IOError, OSError))
    def write(self, data: Iterator[pd.DataFrame], ctx: PipelineContext | None = None) -> int:
        """Concatenate chunks, upload a single object, return row count."""
        cfg = self.config
        assert isinstance(cfg, S3SinkConfig)
        frames = [f for f in data if not f.empty]
        total = sum(len(f) for f in frames)
        if not frames:
            return 0
        merged = pd.concat(frames, ignore_index=True)
        key = cfg.key.format(date=date.today().isoformat())
        endpoint = cfg.endpoint_url or os.getenv("MINIO_ENDPOINT")
        try:
            buf = io.BytesIO()
            if cfg.format == "parquet":
                merged.to_parquet(buf, index=False)
                content_type = "application/octet-stream"
            else:
                buf.write(merged.to_csv(index=False).encode("utf-8"))
                content_type = "text/csv"
            buf.seek(0)
            with S3Client(endpoint_url=endpoint) as s3:
                # Create bucket if missing (MinIO-friendly).
                try:
                    s3.head_bucket(Bucket=cfg.bucket)
                except Exception:
                    s3.create_bucket(Bucket=cfg.bucket)
                s3.upload_fileobj(buf, cfg.bucket, key, ExtraArgs={"ContentType": content_type})
        except Exception as exc:
            raise SinkError(f"S3 sink '{self.name}' failed: {exc}") from exc
        return total

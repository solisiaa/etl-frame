"""Pipeline orchestrator: async source fan-out + lazy transform + sink fan-out."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import pandas as pd

from etl.config.models import PipelineConfig
from etl.core.base import BaseSink, BaseSource, BaseTransformer
from etl.core.context import PipelineContext
from etl.core.exceptions import PipelineError
from etl.core.registry import get_sink, get_source, get_transformer
from etl.decorators import with_metrics


@dataclass
class SourceResult:
    """Outcome of a single source load."""

    name: str
    frames: list[pd.DataFrame] = field(default_factory=list)
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        """True when no error was captured."""
        return self.error is None

    @property
    def rows(self) -> int:
        """Total rows across loaded frames."""
        return sum(len(f) for f in self.frames)


@dataclass
class PipelineResult:
    """Aggregated outcome of :func:`run_pipeline`."""

    pipeline: str
    successes: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    rows_in: int = 0
    rows_out: int = 0
    duration_s: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when at least one source succeeded and no fatal error occurred."""
        return len(self.successes) > 0 and not self.errors or len(self.failures) == 0


def build_sources(cfg: PipelineConfig) -> list[BaseSource]:
    """Instantiate source plugins from config (validates params via schema)."""
    sources: list[BaseSource] = []
    for s in cfg.sources:
        cls = get_source(s.type)
        sources.append(cls(name=s.name, params=s.params))
    return sources


def build_transformers(cfg: PipelineConfig) -> list[BaseTransformer]:
    """Instantiate transformer plugins from config."""
    transformers: list[BaseTransformer] = []
    for t in cfg.transformers:
        cls = get_transformer(t.type)
        transformers.append(cls(params=t.params))
    return transformers


def build_sinks(cfg: PipelineConfig) -> list[BaseSink]:
    """Instantiate sink plugins from config."""
    sinks: list[BaseSink] = []
    for s in cfg.sinks:
        cls = get_sink(s.type)
        sinks.append(cls(name=s.name, params=s.params))
    return sinks


def apply_transformers(
    data: Iterator[pd.DataFrame], transformers: list[BaseTransformer]
) -> Iterator[pd.DataFrame]:
    """Chain transformers lazily: ``source -> t1 -> t2 -> ...``."""
    stream = data
    for t in transformers:
        stream = t.transform(stream)
    return stream


async def load_all(sources: list[BaseSource], ctx: PipelineContext) -> list[SourceResult]:
    """Load all sources concurrently with a semaphore limit.

    A failing source does not cancel siblings (``return_exceptions=True``);
    errors are aggregated into :class:`SourceResult` entries.
    """

    sem = asyncio.Semaphore(ctx.max_concurrency)

    async def _load(src: BaseSource) -> SourceResult:
        async with sem:
            log = ctx.child(f"source:{src.name}")
            log.info("loading source '%s'", src.name)
            try:
                frames = await src.read_async()
                rows = sum(len(f) for f in frames)
                ctx.metrics.record(f"source.{src.name}.rows", float(rows))
                log.info("source '%s' loaded %d rows", src.name, rows)
                return SourceResult(name=src.name, frames=frames)
            except Exception as exc:  # noqa: BLE001 - aggregated, not swallowed
                log.error("source '%s' failed: %s", src.name, exc)
                return SourceResult(name=src.name, frames=[], error=exc)

    results = await asyncio.gather(*(_load(s) for s in sources))
    return list(results)


@with_metrics(times=1, backoff=0.0, exceptions=(ValueError,))
def run_pipeline(cfg: PipelineConfig, ctx: PipelineContext | None = None) -> PipelineResult:
    """Run a full ETL pipeline synchronously (async fan-out inside).

    Steps:
        1. Build plugins from config.
        2. ``asyncio.run(load_all(...))`` — parallel source loads.
        3. Concatenate successful frames, apply transformers lazily.
        4. Write the resulting stream into every sink.

    Args:
        cfg: Validated pipeline config.
        ctx: Optional context (created when omitted).

    Raises:
        PipelineError: When all sources fail.
    """
    ctx = ctx or PipelineContext(max_concurrency=cfg.execution.max_concurrency)
    ctx.max_concurrency = cfg.execution.max_concurrency
    started = time.perf_counter()

    sources = build_sources(cfg)
    transformers = build_transformers(cfg)
    sinks = build_sinks(cfg)

    results = asyncio.run(load_all(sources, ctx))
    ok_frames: list[pd.DataFrame] = []
    successes: list[str] = []
    failures: list[str] = []
    for r in results:
        if r.ok:
            successes.append(r.name)
            ok_frames.extend(r.frames)
        else:
            failures.append(r.name)

    if not ok_frames:
        details = "; ".join(f"{r.name}: {r.error}" for r in results if r.error)
        raise PipelineError(f"All {len(results)} sources failed: {details}")

    rows_in = sum(len(f) for f in ok_frames)
    ctx.metrics.record("rows_in", float(rows_in))

    def _source_stream() -> Iterator[pd.DataFrame]:
        yield from ok_frames

    transformed = apply_transformers(_source_stream(), transformers)
    # Materialize once so multiple sinks can consume the same data.
    materialized = list(transformed)
    rows_mid = sum(len(f) for f in materialized)
    ctx.metrics.record("rows_mid", float(rows_mid))

    rows_out_total = 0
    sink_errors: list[str] = []
    for sink in sinks:
        log = ctx.child(f"sink:{sink.name}")
        try:
            if ctx.dry_run:
                log.info("[dry-run] skip writing to sink '%s'", sink.name)
                continue
            with ctx.metrics.timer(f"sink.{sink.name}.duration"):
                rows = sink.write(iter(materialized), ctx)
            ctx.metrics.record(f"sink.{sink.name}.rows", float(rows))
            rows_out_total += rows
            log.info("sink '%s' wrote %d rows", sink.name, rows)
        except Exception as exc:  # noqa: BLE001 - one bad sink must not hide others
            msg = f"sink '{sink.name}' failed: {exc}"
            log.error(msg)
            sink_errors.append(msg)

    duration = time.perf_counter() - started
    ctx.metrics.record("duration", duration)
    return PipelineResult(
        pipeline=cfg.name,
        successes=successes,
        failures=failures,
        rows_in=rows_in,
        rows_out=rows_out_total,
        duration_s=duration,
        errors=sink_errors,
    )

"""Pipeline-scoped runtime context: logger, metrics, knobs."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field

from etl.utils.logging import get_logger
from etl.utils.metrics import MetricsCollector


@dataclass
class PipelineContext:
    """Carries per-run state through sources/transformers/sinks.

    Attributes:
        pipeline_id: Unique run id (for structured logs).
        max_concurrency: Semaphore limit for async source loading.
        dry_run: When True, sinks must not write.
        logger: Structured logger.
        metrics: Shared metrics collector.
    """

    pipeline_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    max_concurrency: int = 4
    dry_run: bool = False
    logger: logging.Logger = field(default_factory=lambda: get_logger("etl"))
    metrics: MetricsCollector = field(default_factory=MetricsCollector)

    def child(self, step: str) -> logging.LoggerAdapter[logging.Logger]:
        """Return a logger adapter tagging records with pipeline/step."""
        return logging.LoggerAdapter(self.logger, {"pipeline_id": self.pipeline_id, "step": step})

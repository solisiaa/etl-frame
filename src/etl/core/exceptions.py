"""Custom exception hierarchy for the ETL framework.

All framework errors derive from :class:`ETLError` so callers can catch
everything with a single ``except`` while still distinguishing layers.
"""

from __future__ import annotations


class ETLError(Exception):
    """Base class for all ETL errors."""


class ConfigError(ETLError):
    """Raised when a pipeline YAML config is invalid."""


class ValidationError(ConfigError):
    """Raised when Pydantic validation of config or data fails."""


class SourceError(ETLError):
    """Raised when a source fails to read data."""


class SinkError(ETLError):
    """Raised when a sink fails to write data."""


class TransformerError(ETLError):
    """Raised when a transformer fails to process data."""


class PipelineError(ETLError):
    """Raised when pipeline orchestration fails."""

    def __init__(self, message: str, failures: list[BaseException] | None = None) -> None:
        """Store aggregated sub-errors from ``asyncio.gather``.

        Args:
            message: Human-readable summary.
            failures: Individual exceptions collected with ``return_exceptions=True``.
        """
        super().__init__(message)
        self.failures: list[BaseException] = failures or []

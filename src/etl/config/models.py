"""Pydantic v2 schemas for pipeline YAML configs."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RetryConfig(BaseModel):
    """Retry policy for flaky steps."""

    times: int = Field(default=3, ge=1, description="Total attempts including first.")
    backoff: float = Field(default=2.0, ge=0.0, description="Base backoff seconds.")


class ExecutionConfig(BaseModel):
    """Orchestration knobs."""

    parallel_sources: bool = Field(default=True)
    max_concurrency: int = Field(default=4, ge=1)
    retry: RetryConfig = Field(default_factory=RetryConfig)


class SourceConfig(BaseModel):
    """One entry of ``sources:`` in YAML."""

    name: str
    type: str
    params: dict[str, Any] = Field(default_factory=dict)


class TransformerConfig(BaseModel):
    """One entry of ``transformers:`` in YAML."""

    type: str
    params: dict[str, Any] = Field(default_factory=dict)


class SinkConfig(BaseModel):
    """One entry of ``sinks:`` in YAML."""

    name: str
    type: str
    params: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """Top-level pipeline config, mirrors the YAML format."""

    name: str
    description: str = ""
    sources: list[SourceConfig] = Field(min_length=1)
    transformers: list[TransformerConfig] = Field(default_factory=list)
    sinks: list[SinkConfig] = Field(min_length=1)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)

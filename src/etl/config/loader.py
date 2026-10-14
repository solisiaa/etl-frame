"""Load and validate pipeline YAML configs (with env substitution)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError as PydanticValidationError

from etl.config.models import PipelineConfig
from etl.core.exceptions import ConfigError


def _expand_env(value: Any) -> Any:
    """Recursively apply ``os.path.expandvars`` to strings in parsed YAML."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def load_config(path: str | Path) -> PipelineConfig:
    """Load a YAML file, expand ``${ENV_VAR}`` and validate via Pydantic.

    Args:
        path: Path to pipeline YAML.

    Raises:
        ConfigError: On missing file, YAML errors or schema violations.
    """
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    try:
        raw: Any = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {p}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"Top-level YAML must be a mapping, got {type(raw).__name__}")
    raw = _expand_env(raw)
    try:
        return PipelineConfig.model_validate(raw)
    except PydanticValidationError as exc:
        raise ConfigError(f"Config validation failed for {p}:\n{exc}") from exc

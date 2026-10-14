"""Plugin registry for sources, sinks and transformers.

New components are plugged in without touching the core, e.g.::

    from etl import register_source
    from etl.core.base import BaseSource

    @register_source("kafka")
    class KafkaSource(BaseSource):
        ...

The registry maps a string ``type`` (as used in YAML configs) to a class.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from etl.core.base import BaseSink, BaseSource, BaseTransformer
from etl.core.exceptions import ConfigError

T_Source = TypeVar("T_Source", bound=type[BaseSource])
T_Sink = TypeVar("T_Sink", bound=type[BaseSink])
T_Transformer = TypeVar("T_Transformer", bound=type[BaseTransformer])

_SOURCES: dict[str, type[BaseSource]] = {}
_SINKS: dict[str, type[BaseSink]] = {}
_TRANSFORMERS: dict[str, type[BaseTransformer]] = {}


def register_source(name: str) -> Callable[[T_Source], T_Source]:
    """Register a :class:`BaseSource` subclass under ``name``.

    Args:
        name: Type key used in YAML (e.g. ``csv``, ``api``).

    Raises:
        ConfigError: If ``name`` is already registered.
    """

    def _decorator(cls: T_Source) -> T_Source:
        if name in _SOURCES:
            raise ConfigError(f"Source '{name}' already registered ({_SOURCES[name]!r})")
        _SOURCES[name] = cls
        return cls

    return _decorator


def register_sink(name: str) -> Callable[[T_Sink], T_Sink]:
    """Register a :class:`BaseSink` subclass under ``name``."""

    def _decorator(cls: T_Sink) -> T_Sink:
        if name in _SINKS:
            raise ConfigError(f"Sink '{name}' already registered ({_SINKS[name]!r})")
        _SINKS[name] = cls
        return cls

    return _decorator


def register_transformer(name: str) -> Callable[[T_Transformer], T_Transformer]:
    """Register a :class:`BaseTransformer` subclass under ``name``."""

    def _decorator(cls: T_Transformer) -> T_Transformer:
        if name in _TRANSFORMERS:
            raise ConfigError(f"Transformer '{name}' already registered")
        _TRANSFORMERS[name] = cls
        return cls

    return _decorator


def get_source(name: str) -> type[BaseSource]:
    """Return the source class for ``name`` or raise :class:`ConfigError`."""
    try:
        return _SOURCES[name]
    except KeyError as exc:
        raise ConfigError(f"Unknown source type '{name}'. Available: {sorted(_SOURCES)}") from exc


def get_sink(name: str) -> type[BaseSink]:
    """Return the sink class for ``name`` or raise :class:`ConfigError`."""
    try:
        return _SINKS[name]
    except KeyError as exc:
        raise ConfigError(f"Unknown sink type '{name}'. Available: {sorted(_SINKS)}") from exc


def get_transformer(name: str) -> type[BaseTransformer]:
    """Return the transformer class for ``name`` or raise :class:`ConfigError`."""
    try:
        return _TRANSFORMERS[name]
    except KeyError as exc:
        raise ConfigError(
            f"Unknown transformer type '{name}'. Available: {sorted(_TRANSFORMERS)}"
        ) from exc


def list_sources() -> list[str]:
    """Return sorted registered source names."""
    return sorted(_SOURCES)


def list_sinks() -> list[str]:
    """Return sorted registered sink names."""
    return sorted(_SINKS)


def list_transformers() -> list[str]:
    """Return sorted registered transformer names."""
    return sorted(_TRANSFORMERS)

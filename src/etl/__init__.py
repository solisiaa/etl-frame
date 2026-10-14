"""ETL framework — extensible extract/transform/load toolkit.

Public API re-exports core abstractions so that plugin authors can write::

    from etl import BaseSource, BaseSink, BaseTransformer
    from etl import register_source, register_sink, register_transformer
"""

from etl.core.base import BaseSink, BaseSource, BaseTransformer
from etl.core.registry import (
    get_sink,
    get_source,
    get_transformer,
    list_sinks,
    list_sources,
    list_transformers,
    register_sink,
    register_source,
    register_transformer,
)

__all__ = [
    "BaseSink",
    "BaseSource",
    "BaseTransformer",
    "get_sink",
    "get_source",
    "get_transformer",
    "list_sinks",
    "list_sources",
    "list_transformers",
    "register_sink",
    "register_source",
    "register_transformer",
]

__version__ = "0.1.0"

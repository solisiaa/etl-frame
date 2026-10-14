"""Unit tests for the plugin registry."""

from __future__ import annotations

import pandas as pd
import pytest

from etl.core.base import BaseSink, BaseSource, BaseTransformer
from etl.core.exceptions import ConfigError
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


def test_register_and_get_source() -> None:
    """A decorated class becomes retrievable by name."""

    @register_source("unit_dummy_xyz")
    class Dummy(BaseSource):
        def read(self):  # type: ignore[no-untyped-def]
            yield pd.DataFrame({"a": [1]})

    assert get_source("unit_dummy_xyz") is Dummy
    assert "unit_dummy_xyz" in list_sources()


def test_register_duplicate_raises() -> None:
    """Re-registering the same name raises ConfigError."""

    @register_source("unit_dup_xyz")
    class A(BaseSource):
        def read(self):  # type: ignore[no-untyped-def]
            yield pd.DataFrame()

    with pytest.raises(ConfigError):
        # Same name -> duplicate error, no core changes needed.
        register_source("unit_dup_xyz")(A)


def test_get_unknown_source_raises() -> None:
    """Unknown type -> ConfigError with available options listed."""
    with pytest.raises(ConfigError, match="Unknown source type"):
        get_source("definitely_not_a_source_xyz")


def test_sink_and_transformer_registry() -> None:
    """Sinks/transformers register, list, reject duplicates and unknown names."""

    @register_sink("unit_sink_xyz")
    class DummySink(BaseSink):
        def write(self, data, ctx=None):  # type: ignore[no-untyped-def]
            return 0

    @register_transformer("unit_tr_xyz")
    class DummyTr(BaseTransformer):
        def transform(self, data):  # type: ignore[no-untyped-def]
            yield from data

    assert get_sink("unit_sink_xyz") is DummySink
    assert get_transformer("unit_tr_xyz") is DummyTr
    assert "unit_sink_xyz" in list_sinks()
    assert "unit_tr_xyz" in list_transformers()
    with pytest.raises(ConfigError):
        register_sink("unit_sink_xyz")(DummySink)
    with pytest.raises(ConfigError):
        register_transformer("unit_tr_xyz")(DummyTr)
    with pytest.raises(ConfigError, match="Unknown sink type"):
        get_sink("definitely_not_a_sink_xyz")
    with pytest.raises(ConfigError, match="Unknown transformer type"):
        get_transformer("definitely_not_a_transformer_xyz")

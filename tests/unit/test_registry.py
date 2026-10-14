"""Unit tests for the plugin registry."""

from __future__ import annotations

import pandas as pd
import pytest

from etl.core.base import BaseSource
from etl.core.exceptions import ConfigError
from etl.core.registry import get_source, list_sources, register_source


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

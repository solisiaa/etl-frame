"""Import side-effects: registering all built-in transformers."""

from etl.transformers import aggregate as _agg  # noqa: F401
from etl.transformers import clean as _clean  # noqa: F401
from etl.transformers import enrich as _enrich  # noqa: F401

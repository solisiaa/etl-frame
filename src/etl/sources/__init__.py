"""Import side-effects: registering all built-in sources."""

from etl.sources import api_source as _api  # noqa: F401
from etl.sources import csv_source as _csv  # noqa: F401
from etl.sources import postgres_source as _pg  # noqa: F401

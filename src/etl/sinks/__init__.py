"""Import side-effects: registering all built-in sinks."""

from etl.sinks import csv_sink as _csv  # noqa: F401
from etl.sinks import postgres_sink as _pg  # noqa: F401
from etl.sinks import s3_sink as _s3  # noqa: F401

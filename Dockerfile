FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps for pandas/pyarrow/psycopg
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY pipelines/ ./pipelines/
COPY data/ ./data/
COPY scripts/ ./scripts/

RUN pip install --upgrade pip && pip install -e "."

ENTRYPOINT ["etl"]
CMD ["--help"]

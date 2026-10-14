"""S3/MinIO clients as context managers (sync boto3 + async aioboto3)."""

from __future__ import annotations

import os
from types import TracebackType
from typing import Any


class S3Client:
    """Sync boto3 S3 client wrapped in ``with``.

    Example:
        ```python
        with S3Client(endpoint_url) as s3:
            s3.list_buckets()
        ```
    """

    def __init__(
        self,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region_name: str = "us-east-1",
    ) -> None:
        """Store credentials; defaults come from env (MINIO_* / AWS_*)."""
        self.endpoint_url = endpoint_url or os.getenv("MINIO_ENDPOINT")
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = os.getenv("AWS_DEFAULT_REGION", region_name)
        self._client: Any = None

    def __enter__(self) -> Any:
        """Create the boto3 client (imported lazily)."""
        import boto3

        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return self._client

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the underlying HTTP pool."""
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None


class S3ClientAsync:
    """Async aioboto3 S3 client wrapped in ``async with``.

    Example:
        ```python
        async with S3ClientAsync(endpoint_url) as s3:
            await s3.list_buckets()
        ```
    """

    def __init__(
        self,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region_name: str = "us-east-1",
    ) -> None:
        """Store credentials; defaults come from env."""
        self.endpoint_url = endpoint_url or os.getenv("MINIO_ENDPOINT")
        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region_name = os.getenv("AWS_DEFAULT_REGION", region_name)
        self._cm: Any = None
        self._client: Any = None

    async def __aenter__(self) -> Any:
        """Enter aioboto3 client context."""
        import aioboto3

        session = aioboto3.Session()
        self._cm = session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        self._client = await self._cm.__aenter__()
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Exit aioboto3 client context."""
        if self._cm is not None:
            await self._cm.__aexit__(exc_type, exc, tb)
            self._cm = None
            self._client = None

    def __await__(self) -> Any:  # pragma: no cover - convenience only
        """Allow ``client = await S3ClientAsync(...)`` misuse to fail loudly."""
        raise TypeError("Use 'async with S3ClientAsync(...) as s3:' instead of await.")

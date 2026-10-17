"""Object storage behind one small interface: local disk, Amazon S3, Azure Data Lake Storage Gen2.

    storage_for("data/lake/aws")                                       -> LocalStorage
    storage_for("s3://acme-landing/raw")                                -> S3Storage
    storage_for("abfss://landing@acmelake.dfs.core.windows.net/raw")    -> AdlsStorage

Writes of small control files (manifests, state) go through `put_atomic`, so a reader never
sees a half-written file: S3 and ADLS PUTs are atomic per object, and locally we write to a
temporary name and rename.
"""

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from pathlib import Path


class Storage(ABC):
    uri: str

    @abstractmethod
    def put(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def get(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str = "") -> list[str]: ...

    def put_atomic(self, key: str, data: bytes) -> None:
        self.put(key, data)

    def url(self, key: str) -> str:
        return f"{self.uri.rstrip('/')}/{key}"


class LocalStorage(Storage):
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.uri = str(self.root)

    def _p(self, key: str) -> Path:
        return self.root / key

    def put(self, key: str, data: bytes) -> None:
        p = self._p(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def put_atomic(self, key: str, data: bytes) -> None:
        p = self._p(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f".{p.name}.tmp")
        tmp.write_bytes(data)
        os.replace(tmp, p)

    def get(self, key: str) -> bytes:
        return self._p(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._p(key).exists()

    def list(self, prefix: str = "") -> list[str]:
        base = self._p(prefix)
        if base.is_file():
            return [prefix]
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file() and not p.name.startswith("."))


class S3Storage(Storage):
    def __init__(self, bucket: str, prefix: str = "", client=None):
        import boto3

        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.client = client or boto3.client("s3")
        self.uri = f"s3://{bucket}/{self.prefix}".rstrip("/")

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._k(key), Body=data, ServerSideEncryption="AES256")

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=self._k(key))["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self._k(key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def list(self, prefix: str = "") -> list[str]:
        out = []
        paginator = self.client.get_paginator("list_objects_v2")
        strip = len(self.prefix) + 1 if self.prefix else 0
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._k(prefix)):
            out += [o["Key"][strip:] for o in page.get("Contents", [])]
        return sorted(out)


class AdlsStorage(Storage):
    """ADLS Gen2 through the DataLake SDK. Auth: DefaultAzureCredential (managed identity in Azure)."""

    def __init__(self, account: str, container: str, prefix: str = "", service_client=None):
        if service_client is None:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            service_client = DataLakeServiceClient(f"https://{account}.dfs.core.windows.net", credential=DefaultAzureCredential())
        self.fs = service_client.get_file_system_client(container)
        self.prefix = prefix.strip("/")
        self.uri = f"abfss://{container}@{account}.dfs.core.windows.net/{self.prefix}".rstrip("/")

    def _k(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def put(self, key: str, data: bytes) -> None:
        self.fs.get_file_client(self._k(key)).upload_data(data, overwrite=True)

    def get(self, key: str) -> bytes:
        return self.fs.get_file_client(self._k(key)).download_file().readall()

    def exists(self, key: str) -> bool:
        return self.fs.get_file_client(self._k(key)).exists()

    def list(self, prefix: str = "") -> list[str]:
        strip = len(self.prefix) + 1 if self.prefix else 0
        try:
            paths = self.fs.get_paths(path=self._k(prefix), recursive=True)
            return sorted(p.name[strip:] for p in paths if not p.is_directory)
        except Exception as e:  # noqa: BLE001 - the SDK raises ResourceNotFoundError for a missing folder
            if type(e).__name__ == "ResourceNotFoundError":
                return []
            raise


_ABFSS = re.compile(r"^abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net/?(?P<prefix>.*)$")


def storage_for(uri: str) -> Storage:
    if uri.startswith("s3://"):
        bucket, _, prefix = uri[5:].partition("/")
        return S3Storage(bucket, prefix)
    m = _ABFSS.match(uri)
    if m:
        return AdlsStorage(m["account"], m["container"], m["prefix"])
    if "://" in uri and not uri.startswith("file://"):
        raise ValueError(f"unsupported storage URI {uri!r}")
    return LocalStorage(uri.removeprefix("file://"))

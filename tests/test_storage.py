import boto3
import pytest
from moto import mock_aws

from ingest.storage import AdlsStorage, LocalStorage, S3Storage, storage_for


def exercise(st):
    st.put("a/b/one.parquet", b"1")
    st.put_atomic("a/_manifest.json", b"{}")
    st.put("c/two.txt", b"22")
    assert st.get("a/b/one.parquet") == b"1"
    assert st.exists("a/_manifest.json") and not st.exists("a/missing")
    assert st.list("a") == ["a/_manifest.json", "a/b/one.parquet"]
    assert st.list() == ["a/_manifest.json", "a/b/one.parquet", "c/two.txt"]
    assert st.list("nope") == []


def test_local(tmp_path):
    st = LocalStorage(tmp_path)
    exercise(st)
    assert not list(tmp_path.rglob(".*.tmp"))


@mock_aws
def test_s3_with_prefix():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="landing")
    st = S3Storage("landing", "raw/", client=s3)
    exercise(st)
    assert s3.list_objects_v2(Bucket="landing")["KeyCount"] == 3
    head = s3.head_object(Bucket="landing", Key="raw/c/two.txt")
    assert head["ServerSideEncryption"] == "AES256"
    assert st.url("x") == "s3://landing/raw/x"


class _FakeAdls:
    """Just enough of azure.storage.filedatalake to run the storage contract in memory."""

    class _Path:
        def __init__(self, name):
            self.name, self.is_directory = name, False

    class ResourceNotFoundError(Exception):
        pass

    def __init__(self):
        self.files = {}

    def get_file_system_client(self, container):
        return self

    def get_file_client(self, path):
        fs = self

        class Client:
            def upload_data(self, data, overwrite):
                fs.files[path] = data

            def download_file(self):
                return type("D", (), {"readall": lambda _: fs.files[path]})()

            def exists(self):
                return path in fs.files

        return Client()

    def get_paths(self, path, recursive):
        hits = [k for k in self.files if k == path or k.startswith(path.rstrip("/") + "/")]
        if not hits:
            raise _FakeAdls.ResourceNotFoundError(path)
        return [self._Path(k) for k in sorted(hits)]


def test_adls_contract():
    st = AdlsStorage("acmelake", "landing", "raw", service_client=_FakeAdls())
    exercise(st)
    assert st.url("x") == "abfss://landing@acmelake.dfs.core.windows.net/raw/x"


def test_uri_dispatch(tmp_path):
    assert isinstance(storage_for(str(tmp_path)), LocalStorage)
    assert isinstance(storage_for(f"file://{tmp_path}"), LocalStorage)
    with mock_aws():
        assert isinstance(storage_for("s3://b/p"), S3Storage)
    with pytest.raises(ValueError):
        storage_for("gs://bucket/x")

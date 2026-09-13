import pytest


@pytest.fixture
def local_bucket(tmp_path, monkeypatch):
    """Point the dlt filesystem destination at a throwaway local bucket."""
    bucket = tmp_path / "bucket"
    bucket.mkdir()
    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", f"file://{bucket}")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / ".dlt"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    return bucket

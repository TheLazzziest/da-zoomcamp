import boto3
import pendulum
import pytest
from botocore.config import Config

from dlt_sources import pipelines
from dlt_sources.core.containers import Container
from dlt_sources.core.enums import Destination

pytestmark = pytest.mark.integration

PERIOD = pendulum.interval(pendulum.parse("2024-01-01"), pendulum.parse("2024-01-04"))


def test_bronze_calendar_to_rustfs(rustfs, monkeypatch, tmp_path):
    s3 = boto3.client(
        "s3",
        endpoint_url=rustfs.endpoint,
        aws_access_key_id=rustfs.access_key,
        aws_secret_access_key=rustfs.secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    s3.create_bucket(Bucket=rustfs.bucket)

    monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", f"s3://{rustfs.bucket}")
    monkeypatch.setenv(
        "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID", rustfs.access_key
    )
    monkeypatch.setenv(
        "DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY", rustfs.secret_key
    )
    monkeypatch.setenv(
        "DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL", rustfs.endpoint
    )
    monkeypatch.setenv("DESTINATION__FILESYSTEM__CREDENTIALS__REGION_NAME", "us-east-1")
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / ".dlt"))

    info = pipelines.run_calendar(
        PERIOD,
        container=Container(),
        granularities=["daily"],
        destination=Destination.S3,
        pipeline_name="test_bronze_s3",
        dataset_name="calendar",
        dev_mode=True,
    )

    assert info.loads_ids
    keys = [
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=rustfs.bucket).get("Contents", [])
    ]
    assert any(key.endswith(".metadata.json") for key in keys), keys

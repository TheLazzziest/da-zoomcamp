import httpx
import pendulum

from dlt_sources import pipelines
from dlt_sources.core.containers import Container
from dlt_sources.core.enums import Destination

PERIOD = pendulum.interval(pendulum.parse("2024-01-01"), pendulum.parse("2024-01-04"))

ZONE_CSV = (
    '"LocationID","Borough","Zone","service_zone"\n'
    '1,"EWR","Newark Airport","EWR"\n'
    '2,"Queens","Jamaica Bay","Boro Zone"\n'
)

WEATHER_PAYLOAD = {
    "latitude": 40.7128,
    "longitude": -74.006,
    "elevation": 10.0,
    "hourly": {
        "time": [f"2024-01-01T0{i}:00" for i in range(5)],
        "temperature_2m": [1.0, 1.5, 2.0, 2.5, 3.0],
        "relative_humidity_2m": [80, 81, 82, 83, 84],
        "precipitation": [0.0, 0.1, 0.0, 0.0, 0.2],
        "rain": [0.0, 0.1, 0.0, 0.0, 0.2],
        "snowfall": [0.0, 0.0, 0.0, 0.0, 0.0],
        "surface_pressure": [1010.0] * 5,
        "wind_speed_10m": [5.0, 6.0, 7.0, 8.0, 9.0],
        "wind_direction_10m": [180, 190, 200, 210, 220],
        "is_day": [0, 0, 0, 1, 1],
    },
}

SERVICE_REQUESTS = [
    {
        "unique_key": "1",
        "created_date": "2024-01-01T00:04:14.000",
        "complaint_type": "Illegal Parking",
        "status": "Closed",
        "borough": "BRONX",
    },
    {
        "unique_key": "2",
        "created_date": "2024-01-01T00:10:00.000",
        "complaint_type": "Noise",
        "status": "Open",
        "borough": "QUEENS",
    },
]


def patch_client(monkeypatch, module_path: str, handler) -> None:
    """Replace ``build_client`` in a source module with an httpx MockTransport client."""
    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        f"{module_path}.build_client",
        lambda **kwargs: httpx.Client(transport=transport, **kwargs),
    )


def _loaded(bucket) -> list:
    return list(bucket.rglob("*.metadata.json"))


def test_run_calendar_e2e(local_bucket):
    info = pipelines.run_calendar(
        PERIOD,
        container=Container(),
        granularities=["daily", "weekly"],
        destination=Destination.S3,
        pipeline_name="test_calendar",
        dataset_name="calendar",
        dev_mode=True,
    )
    assert info.loads_ids
    assert _loaded(local_bucket)


def test_run_zones_e2e(local_bucket, monkeypatch):
    patch_client(
        monkeypatch,
        "dlt_sources.sources.tlc_lookup.tlc_lookup",
        lambda request: httpx.Response(200, content=ZONE_CSV.encode()),
    )
    info = pipelines.run_zones(
        container=Container(),
        destination=Destination.S3,
        pipeline_name="test_zones",
        dataset_name="tlc_lookup",
        dev_mode=True,
    )
    assert info.loads_ids
    assert _loaded(local_bucket)


def test_run_weather_e2e(local_bucket, monkeypatch):
    patch_client(
        monkeypatch,
        "dlt_sources.sources.weather.weather",
        lambda request: httpx.Response(200, json=WEATHER_PAYLOAD),
    )
    info = pipelines.run_weather(
        PERIOD,
        container=Container(),
        granularities=["hourly", "daily"],
        destination=Destination.S3,
        pipeline_name="test_weather",
        dataset_name="weather",
        dev_mode=True,
    )
    assert info.loads_ids
    assert _loaded(local_bucket)


def test_run_nyc311_e2e(local_bucket, monkeypatch):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        # First (unpaged) request returns rows; the keyset follow-up returns empty.
        return httpx.Response(200, json=SERVICE_REQUESTS if calls["n"] == 1 else [])

    patch_client(monkeypatch, "dlt_sources.sources.nyc311.nyc311", handler)
    info = pipelines.run_nyc311(
        PERIOD,
        container=Container(),
        destination=Destination.S3,
        pipeline_name="test_nyc311",
        dataset_name="nyc311",
        dev_mode=True,
    )
    assert info.loads_ids
    assert _loaded(local_bucket)

from collections.abc import Generator, Iterable, Iterator, Sequence
from datetime import date as date_type
from typing import Literal

import dlt
import httpx
import pendulum
import pyarrow as pa
from dlt.sources import DltResource

from .schemas import WeatherRecord

Granularity = Literal["hourly", "daily", "weekly", "monthly"]

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "is_day",
)
FLOAT_VARIABLES = tuple(v for v in HOURLY_VARIABLES if v != "is_day")

NYC_POINT = (40.7128, -74.0060)
DEFAULT_GRANULARITIES: tuple[Granularity, ...] = ("hourly",)
DEFAULT_CHUNK_SIZE = 24 * 14  # two weeks of hourly observations per batch


def discover_granularities() -> dict[str, type[WeatherRecord]]:
    def descendants(cls: type[WeatherRecord]) -> Iterator[type[WeatherRecord]]:
        for subclass in cls.__subclasses__():
            yield subclass
            yield from descendants(subclass)

    return {
        cls.granularity: cls for cls in descendants(WeatherRecord) if cls.granularity
    }


def _as_float(value: float | int | None) -> float | None:
    """Coerce numeric API values to float so inferred Arrow types match the schema."""
    return None if value is None else float(value)


def _mean(values: Iterable[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) / len(present) if present else None


def _sum(values: Iterable[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _stats(hours: Sequence[dict]) -> dict:
    def column(name: str) -> list[float | None]:
        return [hour[name] for hour in hours]

    return {
        "hours": len(hours),
        "daylight_hours": sum(1 for hour in hours if hour["is_day"]),
        "temperature_2m_mean": _mean(column("temperature_2m")),
        "temperature_2m_min": min(
            (value for value in column("temperature_2m") if value is not None),
            default=None,
        ),
        "temperature_2m_max": max(
            (value for value in column("temperature_2m") if value is not None),
            default=None,
        ),
        "relative_humidity_2m_mean": _mean(column("relative_humidity_2m")),
        "precipitation_sum": _sum(column("precipitation")),
        "rain_sum": _sum(column("rain")),
        "snowfall_sum": _sum(column("snowfall")),
        "surface_pressure_mean": _mean(column("surface_pressure")),
        "wind_speed_10m_mean": _mean(column("wind_speed_10m")),
        "wind_speed_10m_max": max(
            (value for value in column("wind_speed_10m") if value is not None),
            default=None,
        ),
        "wind_direction_10m_mean": _mean(column("wind_direction_10m")),
    }


def _hourly_rows(hours: Sequence[dict]) -> Iterator[dict]:
    yield from hours


def _aggregate_rows(
    hours: Sequence[dict],
    key_fn,
    identity_fn,
) -> Iterator[dict]:
    groups: dict[tuple, list[dict]] = {}
    for hour in hours:
        groups.setdefault(key_fn(hour["time"]), []).append(hour)

    for key, group in groups.items():
        yield {
            "latitude": group[0]["latitude"],
            "longitude": group[0]["longitude"],
            **identity_fn(key, group),
            **_stats(group),
        }


def _daily_rows(hours: Sequence[dict]) -> Iterator[dict]:
    return _aggregate_rows(
        hours,
        key_fn=lambda time: (time.year, time.month, time.day),
        identity_fn=lambda key, group: {
            "date": date_type(key[0], key[1], key[2]),
        },
    )


def _weekly_rows(hours: Sequence[dict]) -> Iterator[dict]:
    def identity(key: tuple, group: Sequence[dict]) -> dict:
        dates = [hour["time"] for hour in group]
        start, end = min(dates), max(dates)
        return {
            "iso_year": key[0],
            "iso_week": key[1],
            "week_start": date_type(start.year, start.month, start.day),
            "week_end": date_type(end.year, end.month, end.day),
        }

    return _aggregate_rows(
        hours,
        key_fn=lambda time: time.isocalendar()[:2],
        identity_fn=identity,
    )


def _monthly_rows(hours: Sequence[dict]) -> Iterator[dict]:
    def identity(key: tuple, group: Sequence[dict]) -> dict:
        dates = [hour["time"] for hour in group]
        start, end = min(dates), max(dates)
        return {
            "year": key[0],
            "month": key[1],
            "month_start": date_type(start.year, start.month, start.day),
            "month_end": date_type(end.year, end.month, end.day),
        }

    return _aggregate_rows(
        hours,
        key_fn=lambda time: (time.year, time.month),
        identity_fn=identity,
    )


EXTRACTORS = {
    "hourly": _hourly_rows,
    "daily": _daily_rows,
    "weekly": _weekly_rows,
    "monthly": _monthly_rows,
}


@dlt.source(name="weather", max_table_nesting=1)
def factory(
    points: Sequence[tuple[float, float]] | None = None,
    *,
    period: pendulum.Interval[pendulum.Date],
    granularities: Sequence[Granularity] | None = None,
    base_url: str = dlt.config.value,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    write_disposition: Literal["append", "replace", "merge"] = "merge",
) -> Generator[DltResource]:
    """
    A source for historical weather observations from the Open-Meteo archive API.
    Args:
        points (Sequence[tuple[float, float]]): Geographic points (latitude, longitude) to collect weather for.
            Defaults to a single point in New York City.
        period (pendulum.Interval[pendulum.Date]): The time period for which to collect weather data.
        granularities (Sequence[Granularity]): Temporal grains to emit, each as its own table
            (``weather_hourly``, ``weather_daily``, ``weather_weekly``, ``weather_monthly``). Defaults to ``hourly``.
        base_url (str): The Open-Meteo archive API URL.
        chunk_size (int): Arrow record-batch size (rows) for every resource.
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
    Returns:
        Generator[SourceFactory, None, None]: A generator of source factories for the given period.
    """

    points = points or [NYC_POINT]
    mapping = discover_granularities()
    selected = granularities or DEFAULT_GRANULARITIES
    unknown = set(selected) - set(mapping)
    if unknown:
        raise ValueError(f"Unknown granularities: {sorted(unknown)}")

    cache: dict[tuple[float, float], list[dict]] = {}

    def observations(point: tuple[float, float]) -> list[dict]:
        if point not in cache:
            latitude, longitude = point
            with httpx.Client(base_url=base_url) as client:
                response = client.get(
                    "/v1/archive",
                    params={
                        "latitude": latitude,
                        "longitude": longitude,
                        "start_date": period.start.format("YYYY-MM-DD"),
                        "end_date": period.end.format("YYYY-MM-DD"),
                        "hourly": ",".join(HOURLY_VARIABLES),
                        "timezone": "America/New_York",
                    },
                )
            response.raise_for_status()
            payload = response.json()
            hourly = payload.get("hourly") or {}
            timestamps = hourly.get("time", [])
            elevation = payload.get("elevation")
            cache[point] = [
                {
                    "time": pendulum.parse(timestamp, tz="America/New_York"),
                    "latitude": latitude,
                    "longitude": longitude,
                    "elevation": _as_float(elevation),
                    **{
                        variable: _as_float(hourly[variable][idx])
                        for variable in FLOAT_VARIABLES
                    },
                    "is_day": hourly["is_day"][idx],
                }
                for idx, timestamp in enumerate(timestamps)
            ]
        return cache[point]

    for granularity in selected:
        schema = mapping[granularity]
        extractor = EXTRACTORS[granularity]

        def extract(
            extractor=extractor, size: int = chunk_size
        ) -> Iterator[pa.RecordBatch]:
            rows: list[dict] = []
            for point in points:
                for row in extractor(observations(point)):
                    rows.append(row)
                    if len(rows) >= size:
                        yield pa.RecordBatch.from_pylist(rows)
                        rows.clear()
            if rows:
                yield pa.RecordBatch.from_pylist(rows)

        yield dlt.resource(
            extract(),
            name=granularity,
            table_name=f"weather_{granularity}",
            primary_key=schema.primary_key(),
            columns=schema,
            write_disposition=write_disposition,
        )

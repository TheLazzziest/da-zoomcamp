import abc
from typing import ClassVar

import pydantic
from pydantic_extra_types import pendulum_dt as pendulum


class WeatherRecord(pydantic.BaseModel, abc.ABC):
    """Base for weather observation rows, one subclass per temporal granularity."""

    granularity: ClassVar[str] = ""

    latitude: float
    longitude: float

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        raise NotImplementedError

    model_config = pydantic.ConfigDict()


class HourlyRecord(WeatherRecord):
    """An hourly weather observation from the Open-Meteo historical API for a single point."""

    granularity: ClassVar[str] = "hourly"

    time: pendulum.DateTime
    elevation: float | None = None
    temperature_2m: float | None = None
    relative_humidity_2m: float | None = None
    precipitation: float | None = None
    rain: float | None = None
    snowfall: float | None = None
    surface_pressure: float | None = None
    wind_speed_10m: float | None = None
    wind_direction_10m: float | None = None
    is_day: int | None = None

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("latitude", "longitude", "time")


class _AggregateRecord(WeatherRecord):
    """Shared aggregates rolled up from hourly observations."""

    hours: int
    daylight_hours: int
    temperature_2m_mean: float | None = None
    temperature_2m_min: float | None = None
    temperature_2m_max: float | None = None
    relative_humidity_2m_mean: float | None = None
    precipitation_sum: float | None = None
    rain_sum: float | None = None
    snowfall_sum: float | None = None
    surface_pressure_mean: float | None = None
    wind_speed_10m_mean: float | None = None
    wind_speed_10m_max: float | None = None
    wind_direction_10m_mean: float | None = None


class DailyRecord(_AggregateRecord):
    """A daily weather aggregate for a single point."""

    granularity: ClassVar[str] = "daily"

    date: pendulum.Date

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("latitude", "longitude", "date")


class WeeklyRecord(_AggregateRecord):
    """A weekly weather aggregate over ISO weeks for a single point."""

    granularity: ClassVar[str] = "weekly"

    iso_year: int
    iso_week: int
    week_start: pendulum.Date
    week_end: pendulum.Date

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("latitude", "longitude", "iso_year", "iso_week")


class MonthlyRecord(_AggregateRecord):
    """A monthly weather aggregate for a single point."""

    granularity: ClassVar[str] = "monthly"

    year: int
    month: int
    month_start: pendulum.Date
    month_end: pendulum.Date

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("latitude", "longitude", "year", "month")

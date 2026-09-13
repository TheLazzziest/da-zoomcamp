import abc
from typing import ClassVar

import pydantic
from pydantic_extra_types import pendulum_dt as pendulum


class CalendarRecord(pydantic.BaseModel, abc.ABC):
    """Base for calendar reference rows, one subclass per temporal granularity."""

    granularity: ClassVar[str] = ""

    calendar: str

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        raise NotImplementedError

    model_config = pydantic.ConfigDict()


class DailyRecord(CalendarRecord):
    """A daily calendar reference row: weekday/weekend and holiday flags for a date."""

    granularity: ClassVar[str] = "daily"

    date: pendulum.Date
    year: int
    month: int
    day: int
    day_of_week: int
    week_of_year: int
    is_weekend: bool
    is_holiday: bool
    is_workday: bool
    holiday_name: str | None = None

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("calendar", "date")


class WeeklyRecord(CalendarRecord):
    """A weekly calendar reference row aggregated over ISO weeks."""

    granularity: ClassVar[str] = "weekly"

    iso_year: int
    iso_week: int
    week_start: pendulum.Date
    week_end: pendulum.Date
    days: int
    workdays: int
    holidays: int
    is_holiday: bool
    holiday_names: list[str]

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("calendar", "iso_year", "iso_week")


class MonthlyRecord(CalendarRecord):
    """A monthly calendar reference row aggregated over calendar months."""

    granularity: ClassVar[str] = "monthly"

    year: int
    month: int
    month_start: pendulum.Date
    month_end: pendulum.Date
    days: int
    workdays: int
    holidays: int
    is_holiday: bool
    holiday_names: list[str]

    @classmethod
    def primary_key(cls) -> tuple[str, ...]:
        return ("calendar", "year", "month")

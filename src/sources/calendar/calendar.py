from collections.abc import Generator, Iterator, Sequence
from datetime import date as date_type
from typing import Literal

import dlt
import pendulum
import pyarrow as pa
from dlt.sources import DltResource
from workalendar import registry

from .schemas import CalendarRecord

Granularity = Literal["daily", "weekly", "monthly"]

WEEKEND_DAYS = {5, 6}  # workalendar weekdays: Monday(0) .. Sunday(6)
DEFAULT_CALENDARS = ("US-NY",)
DEFAULT_GRANULARITIES: tuple[Granularity, ...] = ("daily",)
BATCH_SIZE = 1024


def discover_granularities() -> dict[str, type[CalendarRecord]]:
    return {
        cls.granularity: cls
        for cls in CalendarRecord.__subclasses__()
        if cls.granularity
    }


def _day_facts(
    calendar: registry.Calendar, period: pendulum.Interval[pendulum.Date]
) -> Iterator[tuple[pendulum.Date, bool, str | None]]:
    for date in period.range("days"):
        is_holiday = calendar.is_holiday(date)
        yield (
            date,
            is_holiday,
            (calendar.get_holiday_label(date) if is_holiday else None),
        )


def _daily_rows(
    code: str, calendar: registry.Calendar, period: pendulum.Interval[pendulum.Date]
) -> Iterator[dict]:
    for date, is_holiday, label in _day_facts(calendar, period):
        day_of_week = date.weekday()
        yield {
            "calendar": code,
            "date": date_type(date.year, date.month, date.day),
            "year": date.year,
            "month": date.month,
            "day": date.day,
            "day_of_week": day_of_week,
            "week_of_year": date.week_of_year,
            "is_weekend": day_of_week in WEEKEND_DAYS,
            "is_holiday": is_holiday,
            "is_workday": not is_holiday and day_of_week not in WEEKEND_DAYS,
            "holiday_name": label,
        }


def _weekly_rows(
    code: str, calendar: registry.Calendar, period: pendulum.Interval[pendulum.Date]
) -> Iterator[dict]:
    groups: dict[tuple[int, int], list[tuple[pendulum.Date, bool, str | None]]] = {}
    for fact in _day_facts(calendar, period):
        iso = fact[0].isocalendar()
        groups.setdefault((iso.year, iso.week), []).append(fact)

    for (iso_year, iso_week), days in groups.items():
        dates = [day for day, _, _ in days]
        yield {
            "calendar": code,
            "iso_year": iso_year,
            "iso_week": iso_week,
            "week_start": date_type(min(dates).year, min(dates).month, min(dates).day),
            "week_end": date_type(max(dates).year, max(dates).month, max(dates).day),
            "days": len(days),
            "workdays": sum(
                1
                for day, is_holiday, _ in days
                if not is_holiday and day.weekday() not in WEEKEND_DAYS
            ),
            "holidays": sum(1 for _, is_holiday, _ in days if is_holiday),
            "is_holiday": any(is_holiday for _, is_holiday, _ in days),
            "holiday_names": [label for _, _, label in days if label],
        }


def _monthly_rows(
    code: str, calendar: registry.Calendar, period: pendulum.Interval[pendulum.Date]
) -> Iterator[dict]:
    groups: dict[tuple[int, int], list[tuple[pendulum.Date, bool, str | None]]] = {}
    for fact in _day_facts(calendar, period):
        date = fact[0]
        groups.setdefault((date.year, date.month), []).append(fact)

    for (year, month), days in groups.items():
        dates = [day for day, _, _ in days]
        yield {
            "calendar": code,
            "year": year,
            "month": month,
            "month_start": date_type(min(dates).year, min(dates).month, min(dates).day),
            "month_end": date_type(max(dates).year, max(dates).month, max(dates).day),
            "days": len(days),
            "workdays": sum(
                1
                for day, is_holiday, _ in days
                if not is_holiday and day.weekday() not in WEEKEND_DAYS
            ),
            "holidays": sum(1 for _, is_holiday, _ in days if is_holiday),
            "is_holiday": any(is_holiday for _, is_holiday, _ in days),
            "holiday_names": [label for _, _, label in days if label],
        }


EXTRACTORS = {
    "daily": _daily_rows,
    "weekly": _weekly_rows,
    "monthly": _monthly_rows,
}


@dlt.source(name="calendar", max_table_nesting=1)
def factory(
    *,
    period: pendulum.Interval[pendulum.Date],
    calendars: Sequence[str] | None = None,
    granularities: Sequence[Granularity] | None = None,
    write_disposition: Literal["append", "replace", "merge"] = "merge",
) -> Generator[DltResource]:
    """
    A source for calendar references (holidays, weekends, workdays), one or more calendars at a time.
    Args:
        period (pendulum.Interval[pendulum.Date]): The time period for which to collect calendar data.
        calendars (Sequence[str]): ISO 3166-1/3166-2 region codes resolved through the workalendar registry
            (e.g. ``US``, ``US-NY``). Defaults to ``US-NY``.
        granularities (Sequence[Granularity]): Temporal grains to emit, each as its own table
            (``calendar_daily``, ``calendar_weekly``, ``calendar_monthly``). Defaults to ``daily``.
        write_disposition (Literal["append", "replace", "merge"]): The write disposition for the source.
    Returns:
        Generator[SourceFactory, None, None]: A generator of source factories for the given period.
    """

    codes = [code.upper() for code in (calendars or DEFAULT_CALENDARS)]
    resolved = {}
    for code in codes:
        calendar_class = registry.registry.get(code)
        if calendar_class is None:
            raise ValueError(f"Unknown calendar region code: {code!r}")
        resolved[code] = calendar_class()

    mapping = discover_granularities()
    selected = granularities or DEFAULT_GRANULARITIES
    unknown = set(selected) - set(mapping)
    if unknown:
        raise ValueError(f"Unknown granularities: {sorted(unknown)}")

    for granularity in selected:
        schema = mapping[granularity]
        extractor = EXTRACTORS[granularity]

        def extract(
            schema: type[CalendarRecord] = schema, extractor=extractor
        ) -> Iterator[pa.RecordBatch]:
            rows: list[dict] = []
            for code, calendar in resolved.items():
                for row in extractor(code, calendar, period):
                    rows.append(row)
                    if len(rows) >= BATCH_SIZE:
                        yield pa.RecordBatch.from_pylist(rows)
                        rows.clear()
            if rows:
                yield pa.RecordBatch.from_pylist(rows)

        yield dlt.resource(
            extract(),
            name=granularity,
            table_name=f"calendar_{granularity}",
            primary_key=schema.primary_key(),
            columns=schema,
            write_disposition=write_disposition,
            file_format="parquet",
        )

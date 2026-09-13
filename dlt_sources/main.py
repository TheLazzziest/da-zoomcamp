from typing import Annotated

import pendulum
import typer
from loguru import logger

from dlt_sources import pipelines
from dlt_sources.core.enums import Destination
from dlt_sources.core.loguru import configure
from dlt_sources.core.settings import get_settings
from dlt_sources.sources.nyc.enums import NYCTripCategory
from dlt_sources.transformers.clickhouse import TableEngine

run_pipeline_app = typer.Typer(
    name="run", help="Run a pipeline for a specific data source."
)


@run_pipeline_app.command(
    "nyc",
    help="Ingest NYC taxi trip data for a given period and categories.",
)
def run_nyc(
    ctx: typer.Context,
    categories: Annotated[
        list[NYCTripCategory],
        typer.Argument(
            help="A list of trip categories to ingest. Defaults to all categories."
        ),
    ],
    start_datetime: Annotated[
        pendulum.DateTime,
        typer.Argument(
            parser=pendulum.parse,
            help="The start datetime for the data period (inclusive), in ISO 8601 format.",
        ),
    ],
    end_datetime: Annotated[
        pendulum.DateTime | None,
        typer.Option(
            parser=pendulum.parse,
            help="The end datetime for the data period (exclusive). If not provided, it defaults to the current time.",
        ),
    ] = None,
    table_engine: Annotated[
        TableEngine | None,
        typer.Option(
            help="ClickHouse table engine. Applied only when destination is clickhouse."
        ),
    ] = None,
):
    """Runs the NYC taxi trip data ingestion pipeline."""
    end_datetime = end_datetime or pendulum.now(tz=pendulum.UTC).start_of("month")
    period = pendulum.interval(start_datetime, end_datetime)

    logger.info(
        f"Running NYC pipeline for {period} and categories: {[c.value for c in categories]}"
    )

    pipelines.run_nyc(
        categories,
        period,
        destination=ctx.obj["destination"],
        pipeline_name=ctx.obj.get("pipeline_name") or "nyc_trip_data_ingestion",
        dataset_name=ctx.obj.get("dataset_name") or "nyc",
        table_engine=table_engine,
        dev_mode=ctx.obj.get("debug", False),
        max_items=ctx.obj.get("max_items"),
    )


@run_pipeline_app.command(
    "weather",
    help="Ingest historical hourly weather observations for a given period.",
)
def run_weather(
    ctx: typer.Context,
    start_datetime: Annotated[
        pendulum.DateTime,
        typer.Argument(
            parser=pendulum.parse,
            help="The start datetime for the data period (inclusive), in ISO 8601 format.",
        ),
    ],
    end_datetime: Annotated[
        pendulum.DateTime | None,
        typer.Option(
            parser=pendulum.parse,
            help="The end datetime for the data period (exclusive). If not provided, it defaults to the current time.",
        ),
    ] = None,
    granularity: Annotated[
        list[str] | None,
        typer.Option(
            "--granularity",
            "-g",
            help="Temporal grain(s) to emit: hourly, daily, weekly, monthly. Repeatable.",
        ),
    ] = None,
):
    """Runs the weather observations ingestion pipeline."""
    end_datetime = end_datetime or pendulum.now(tz=pendulum.UTC).start_of("month")
    period = pendulum.interval(start_datetime, end_datetime)

    logger.info(
        f"Running weather pipeline for {period} and granularities: {granularity}"
    )

    pipelines.run_weather(
        period,
        granularities=granularity,
        destination=ctx.obj["destination"],
        pipeline_name=ctx.obj.get("pipeline_name") or "weather_ingestion",
        dataset_name=ctx.obj.get("dataset_name") or "weather",
        dev_mode=ctx.obj.get("debug", False),
        max_items=ctx.obj.get("max_items"),
    )


@run_pipeline_app.command(
    "311",
    help="Ingest NYC 311 service requests for a given period.",
)
def run_nyc311(
    ctx: typer.Context,
    start_datetime: Annotated[
        pendulum.DateTime,
        typer.Argument(
            parser=pendulum.parse,
            help="The start datetime for the created_date period (inclusive), in ISO 8601 format.",
        ),
    ],
    end_datetime: Annotated[
        pendulum.DateTime | None,
        typer.Option(
            parser=pendulum.parse,
            help="The end datetime for the created_date period (exclusive). If not provided, it defaults to the current time.",
        ),
    ] = None,
):
    """Runs the NYC 311 service requests ingestion pipeline."""
    end_datetime = end_datetime or pendulum.now(tz=pendulum.UTC).start_of("month")
    period = pendulum.interval(start_datetime, end_datetime)

    logger.info(f"Running NYC 311 pipeline for {period}")

    pipelines.run_nyc311(
        period,
        destination=ctx.obj["destination"],
        pipeline_name=ctx.obj.get("pipeline_name") or "nyc311_ingestion",
        dataset_name=ctx.obj.get("dataset_name") or "nyc311",
        dev_mode=ctx.obj.get("debug", False),
        max_items=ctx.obj.get("max_items"),
    )


@run_pipeline_app.command(
    "zones",
    help="Ingest the TLC taxi zone lookup dimension.",
)
def run_zones(ctx: typer.Context):
    """Runs the TLC taxi zone lookup ingestion pipeline."""
    logger.info("Running TLC taxi zone lookup pipeline")

    pipelines.run_zones(
        destination=ctx.obj["destination"],
        pipeline_name=ctx.obj.get("pipeline_name") or "tlc_lookup_ingestion",
        dataset_name=ctx.obj.get("dataset_name") or "tlc_lookup",
        dev_mode=ctx.obj.get("debug", False),
        max_items=ctx.obj.get("max_items"),
    )


@run_pipeline_app.command(
    "calendar",
    help="Ingest calendar references (holidays, weekends, workdays) for a given period.",
)
def run_calendar(
    ctx: typer.Context,
    start_datetime: Annotated[
        pendulum.DateTime,
        typer.Argument(
            parser=pendulum.parse,
            help="The start datetime for the data period (inclusive), in ISO 8601 format.",
        ),
    ],
    end_datetime: Annotated[
        pendulum.DateTime | None,
        typer.Option(
            parser=pendulum.parse,
            help="The end datetime for the data period (exclusive). If not provided, it defaults to the current time.",
        ),
    ] = None,
    calendar: Annotated[
        list[str] | None,
        typer.Option(
            "--calendar",
            "-c",
            help="ISO region code(s) resolved through the workalendar registry (e.g. US, US-NY). Repeatable.",
        ),
    ] = None,
    granularity: Annotated[
        list[str] | None,
        typer.Option(
            "--granularity",
            "-g",
            help="Temporal grain(s) to emit: daily, weekly, monthly. Repeatable.",
        ),
    ] = None,
):
    """Runs the calendar reference ingestion pipeline."""
    end_datetime = end_datetime or pendulum.now(tz=pendulum.UTC).start_of("month")
    period = pendulum.interval(start_datetime, end_datetime)

    logger.info(
        f"Running calendar pipeline for {period}, calendars: {calendar}, granularities: {granularity}"
    )

    pipelines.run_calendar(
        period,
        calendars=calendar,
        granularities=granularity,
        destination=ctx.obj["destination"],
        pipeline_name=ctx.obj.get("pipeline_name") or "calendar_ingestion",
        dataset_name=ctx.obj.get("dataset_name") or "calendar",
        dev_mode=ctx.obj.get("debug", False),
        max_items=ctx.obj.get("max_items"),
    )


app = typer.Typer(name="da-zoomcamp")
app.add_typer(run_pipeline_app)


@app.callback()
def main(
    ctx: typer.Context,
    debug: Annotated[bool, typer.Option(help="Enable debug mode.")] = False,
    destination: Annotated[
        Destination, typer.Option(help="A target storage for the ingested data")
    ] = Destination.DUCKDB,
    pipeline_name: Annotated[
        str | None, typer.Option(help="Name of the pipeline.")
    ] = None,
    dataset_name: Annotated[
        str | None,
        typer.Option(
            help="A target namespace where the ingested data will be put into. Defaults to the name of the source"
        ),
    ] = None,
    max_items: Annotated[
        int | None, typer.Option(help="Limit the number of items to process.")
    ] = None,
):
    settings = get_settings()
    if settings.is_production:
        configure(settings)

    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    ctx.obj["destination"] = destination
    ctx.obj["pipeline_name"] = pipeline_name
    ctx.obj["dataset_name"] = dataset_name
    ctx.obj["max_items"] = max_items

    logger.info("Starting application...")
    logger.info(f"Debug mode: {debug}")


if __name__ == "__main__":
    app()

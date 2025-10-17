from typing import Annotated

import dlt
import pendulum
import typer
from loguru import logger

from src.core.containers import Container
from src.core.enums import Destination
from src.core.loguru import configure
from src.core.settings import ProjectSettings
from src.sources.nyc.enums import NYCTripCategory


run_pipeline_app = typer.Typer(name="run", help="Run a pipeline for a specific data source.")

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
            help="The start datetime for the data period (inclusive), in ISO 8601 format."
        ),
    ], 
    end_datetime: Annotated[
        pendulum.DateTime | None,
        typer.Option(
            parser=pendulum.parse,
            help="The end datetime for the data period (exclusive). If not provided, it defaults to the current time."
        ),
    ] = None, 
):
    """Runs the NYC taxi trip data ingestion pipeline."""
    container: Container = ctx.obj["container"]

    end_datetime = end_datetime or pendulum.now(tz=pendulum.UTC).start_of("month")
    period = pendulum.interval(start_datetime, end_datetime)

    logger.info(
        f"Running NYC pipeline for {period} and categories: {[c.value for c in categories]}"
    )

    source = container.nyc_source(
        categories=categories, period=period
    )

    resources = source

    if max_items := ctx.obj.get("max_items", None):
        resources = resources.add_limit(max_items=max_items)

    pipeline = dlt.pipeline(
        pipeline_name=ctx.obj.get("pipeline_name", "nyc_trip_data_ingestion"),
        destination=ctx.obj["destination"].value,
        staging="filesystem",
        dataset_name=ctx.obj.get("dataset_name", "nyc"),
        dev_mode=ctx.obj.get("debug", False),
    )
    load_info = pipeline.run(resources)
    logger.info(load_info)


app = typer.Typer(name="da-zoomcamp")
app.add_typer(run_pipeline_app)


@app.callback()
def main(
    ctx: typer.Context,
    debug: Annotated[bool, typer.Option(help="Enable debug mode.")] = False,
    destination: Annotated[Destination, typer.Option(help="A target storage for the ingested data")] = Destination.DUCKDB,
    pipeline_name: Annotated[str | None, typer.Option(help="Name of the pipeline.")] = None,
    dataset_name: Annotated[str | None, typer.Option(help="A target namespace where the ingested data will be put into. Defaults to the name of the source")] = None,
    max_items: Annotated[
        int | None, typer.Option(help="Limit the number of items to process.")
    ] = None
):
    settings = ProjectSettings()
    if settings.is_production:
        configure(settings)

    container = Container()
    container.config.from_pydantic(settings)
    
    ctx.ensure_object(dict)
    ctx.obj["container"] = container
    ctx.obj["debug"] = debug
    ctx.obj["destination"] = destination
    ctx.obj["pipeline_name"] = pipeline_name
    ctx.obj["dataset_name"] = dataset_name
    ctx.obj["max_items"] = max_items

    logger.info("Starting application...")
    logger.info(f"Debug mode: {debug}")


if __name__ == "__main__":
    app()

# DA-Zoomcamp

## Quickstart

1. Build and run the Docker containers:
    ```bash
    docker compose up -d
    ```
2. Install the Python dependencies:
    ```bash
    uv sync --all-extras
    ```
3. Explore the current pipelines:
    ```bash
    dlt pipeline --list-pipelines dlt_sources/pipelines
    ```
4. Run the NYC taxi trip data ingestion pipeline:
   ```bash
    python -m dlt_sources.main --destination duckdb run nyc yellow green --start-datetime 2023-01-01 --end-datetime 2023-03-01
   ```

## Resources

* [DLT](https://dlthub.com/)
* [TCL Record Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

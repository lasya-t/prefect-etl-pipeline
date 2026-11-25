"""Serve the flow on a daily schedule (06:15 every morning) from a long-running process.

    PYTHONPATH=src python -m weather_etl.deploy http://your-weather-api

Prefect's UI (prefect server start) then shows each run, its retries and any failures.
"""
import sys

from .flow import weather_etl
from .mock_api import CITIES

if __name__ == "__main__":
    weather_etl.serve(
        name="daily-weather",
        cron="15 6 * * *",
        parameters={"base_url": sys.argv[1], "cities": list(CITIES), "db_path": "weather.duckdb"},
    )

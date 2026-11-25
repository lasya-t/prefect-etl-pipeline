"""Run a week of daily loads against a flaky API, then re-run the week to show caching."""
import logging
import time
from datetime import date, timedelta

import duckdb

from weather_etl.flow import ALERT_FILE, weather_etl
from weather_etl.mock_api import CITIES, WeatherAPI, serve

logging.getLogger("prefect").setLevel(logging.WARNING)
ALERT_FILE.unlink(missing_ok=True)
api = WeatherAPI(error_rate=0.25, seed=7, bad_city="Pittsburgh")
server, url = serve(api)
days = [(date(2025, 6, 1) + timedelta(days=i)).isoformat() for i in range(7)]

for label in ("first run", "re-run (cached)"):
    t0, calls0 = time.perf_counter(), api.calls
    for d in days:
        weather_etl(url, list(CITIES), d, "demo.duckdb")
    print(f"{label:16s} 7 days x 5 cities: {api.calls - calls0:3d} API calls, {time.perf_counter() - t0:.1f}s")

con = duckdb.connect("demo.duckdb", read_only=True)
print(f"\ndaily_weather: {con.execute('select count(*) from daily_weather').fetchall()[0][0]} rows "
      f"(7 days x 4 good cities; re-run added none)")
print(con.sql("select city, round(avg(temp_max),1) as avg_high, max(wind_max) as max_wind from daily_weather group by 1 order by 1").df().to_string(index=False))
print(f"\nalerts.log: {len(ALERT_FILE.read_text().splitlines())} lines, first:\n  {ALERT_FILE.read_text().splitlines()[0][:130]}")
server.shutdown()

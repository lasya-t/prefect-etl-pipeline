# Daily Weather ETL with Prefect

A Prefect 3 flow that pulls hourly weather for five cities from an unreliable API, validates it, summarizes it per day and loads it into DuckDB. It's built to handle the things that go wrong in real daily jobs.

```
extract (x5 cities, concurrent) ──▶ validate (per city) ──▶ transform ──▶ load (DuckDB)
   retries on 5xx / timeouts         range checks             daily stats   overwrite the day
   cached per (city, date)           bad city skipped                       idempotent
                                     + partial-load alert      flow failure ──▶ alert hook
```

## What each Prefect feature is doing

| Feature | Used for |
|---|---|
| `extract.map(...)` | one task per city, running concurrently |
| `retries` + `retry_condition_fn` | retry 5xx, 429 and network errors with increasing delays, but **not** a 404 (an unknown city never succeeds) |
| `cache_policy=INPUTS` + `persist_result` | a re-run for the same city and date reuses the stored result instead of calling the API again |
| per-city task states | a bad reading fails that one city's validation; the others still load |
| `on_failure` hook | writes an alert when the whole flow fails |
| `flow.serve(cron=...)` | schedules the flow to run daily at 06:15 (`deploy.py`) |

## Demo: one week, 25% of API calls failing, one broken sensor

```
first run        7 days x 5 cities:  51 API calls
re-run (cached)  7 days x 5 cities:   0 API calls

daily_weather: 28 rows (7 days x 4 good cities; re-run added none)
alerts.log: [2025-06-01] partial load, 1 cities skipped: Pittsburgh: ...
            DataQualityError: Pittsburgh: temp_c out of range at hours [13] (values [999.0])
```

- 35 city-days needed **51 calls**: the retries absorbed every transient failure.
- The full re-run made **zero** API calls and added **zero** rows: it hit the cache, and the load overwrites each day instead of appending.
- Pittsburgh's sensor reports 999 °C at 1pm, so Pittsburgh is skipped each day and the alert names the exact hour and value, instead of a bad number reaching the table.

## Tests

The tests run against a temporary Prefect database (`prefect_test_harness`):
- a clean run loads every city
- transient errors are retried until the run succeeds
- a bad reading skips only that city and writes an alert
- a re-run makes no API calls and leaves the table unchanged
- the flow fails, and the failure hook fires, when every city fails

## How to run

```bash
pip install -r requirements.txt
PYTHONPATH=src python run_demo.py
pytest

# optional: Prefect UI + daily schedule
prefect server start &
PYTHONPATH=src python -m weather_etl.deploy http://localhost:8000
```

When the demo exits you may see a `CancelledError` traceback. That's Prefect's temporary local server shutting down, not a pipeline error.

## Next steps
- Send alerts to Slack with a Prefect notification block instead of a file
- Backfill a date range by running the flow once per date

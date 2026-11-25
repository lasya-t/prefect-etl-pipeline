"""Daily weather ETL as a Prefect flow.

extract    one task per city (mapped, runs concurrently), retried on 5xx/timeouts,
           cached per (city, date) so a re-run doesn't call the API again
validate   physical range checks; a bad reading fails the city, not the whole flow
transform  hourly -> daily summary per city
load       overwrite that date's rows in DuckDB (idempotent)
alert      flow-level hook writes a message when the run fails or some cities fail
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import httpx
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.cache_policies import INPUTS
from prefect.futures import wait

ALERT_FILE = Path("alerts.log")
RANGES = {"temp_c": (-50, 55), "humidity": (0, 100), "wind_kph": (0, 250)}


class DataQualityError(Exception):
    pass


def _retry_on_transient(task, task_run, state) -> bool:
    """Retry server errors and network problems; a 404 (unknown city) will never succeed."""
    try:
        state.result()
    except httpx.HTTPStatusError as e:
        return e.response.status_code >= 500 or e.response.status_code == 429
    except httpx.TransportError:
        return True
    except Exception:
        return False
    return False


@task(retries=4, retry_delay_seconds=[0.2, 0.5, 1, 2], retry_condition_fn=_retry_on_transient,
      cache_policy=INPUTS, cache_expiration=timedelta(days=1), persist_result=True, log_prints=True)
def extract(base_url: str, city: str, day: str) -> list[dict]:
    r = httpx.get(f"{base_url}/hourly", params={"city": city, "date": day}, timeout=5)
    r.raise_for_status()
    return r.json()["hours"]


@task
def validate(city: str, hours: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(hours)
    if len(df) != 24 or sorted(df["hour"]) != list(range(24)):
        raise DataQualityError(f"{city}: expected 24 hourly readings, got {len(df)}")
    for col, (lo, hi) in RANGES.items():
        bad = df[(df[col] < lo) | (df[col] > hi)]
        if len(bad):
            raise DataQualityError(f"{city}: {col} out of range at hours {bad['hour'].tolist()} "
                                   f"(values {bad[col].tolist()})")
    return df.assign(city=city)


@task
def transform(day: str, frames: list[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(frames)
    out = df.groupby("city").agg(
        temp_min=("temp_c", "min"), temp_max=("temp_c", "max"), temp_mean=("temp_c", "mean"),
        humidity_mean=("humidity", "mean"), wind_max=("wind_kph", "max"),
    ).round(1).reset_index()
    out.insert(0, "dt", pd.to_datetime(day).date())
    return out


@task
def load(db_path: str, df: pd.DataFrame) -> int:
    con = duckdb.connect(db_path)
    con.execute("""CREATE TABLE IF NOT EXISTS daily_weather (dt DATE, city VARCHAR, temp_min DOUBLE, temp_max DOUBLE,
                   temp_mean DOUBLE, humidity_mean DOUBLE, wind_max DOUBLE)""")
    con.register("incoming", df)
    con.execute("BEGIN")
    con.execute("DELETE FROM daily_weather WHERE dt = ? AND city IN (SELECT city FROM incoming)", [df["dt"].iloc[0]])
    con.execute("INSERT INTO daily_weather SELECT * FROM incoming")
    con.execute("COMMIT")
    n = con.execute("SELECT count(*) FROM daily_weather").fetchall()[0][0]
    con.close()
    return n


def _alert(flow, flow_run, state):
    ALERT_FILE.write_text(ALERT_FILE.read_text() + f"[{flow_run.name}] {state.name}: {state.message}\n"
                          if ALERT_FILE.exists() else f"[{flow_run.name}] {state.name}: {state.message}\n")


@flow(name="daily-weather-etl", on_failure=[_alert], log_prints=True)
def weather_etl(base_url: str, cities: list[str], day: str | None = None, db_path: str = "weather.duckdb") -> dict:
    logger = get_run_logger()
    day = day or (date.today() - timedelta(days=1)).isoformat()

    raw = extract.map(base_url, cities, day)
    checked = [validate.submit(c, h) for c, h in zip(cities, raw)]
    wait(checked)

    good, failed = [], {}
    for city, fut in zip(cities, checked):
        if fut.state.is_completed():
            good.append(fut.result())
        else:
            failed[city] = fut.state.message
            logger.warning(f"skipping {city}: {fut.state.message}")
    if not good:
        raise RuntimeError(f"all cities failed for {day}: {failed}")

    rows = load(db_path, transform(day, good))
    if failed:
        _alert_partial(day, failed)
    return {"day": day, "loaded_cities": len(good), "failed": failed, "table_rows": rows}


def _alert_partial(day: str, failed: dict) -> None:
    msg = f"[{day}] partial load, {len(failed)} cities skipped: " + "; ".join(f"{c}: {m}" for c, m in failed.items())
    with ALERT_FILE.open("a") as f:
        f.write(msg + "\n")

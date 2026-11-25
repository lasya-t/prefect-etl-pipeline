import duckdb
import pytest

from weather_etl import flow as etl
from weather_etl.mock_api import CITIES, WeatherAPI, serve


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(etl, "ALERT_FILE", tmp_path / "alerts.log")
    servers = []

    def start(**kw):
        api = WeatherAPI(**kw)
        s, url = serve(api)
        servers.append(s)
        return api, url

    yield start, str(tmp_path / "w.duckdb"), tmp_path / "alerts.log"
    for s in servers:
        s.shutdown()


def rows(db):
    c = duckdb.connect(db, read_only=True)
    out = c.execute("SELECT city, temp_max FROM daily_weather ORDER BY city").fetchall()
    c.close()
    return out


def test_clean_run_loads_every_city(env):
    start, db, _ = env
    api, url = start()
    r = etl.weather_etl(url, list(CITIES), "2025-07-01", db)
    assert r["loaded_cities"] == 5 and not r["failed"] and len(rows(db)) == 5


def test_transient_errors_are_retried(env):
    start, db, _ = env
    api, url = start(error_rate=0.4, seed=3)
    r = etl.weather_etl(url, list(CITIES), "2025-07-02", db)
    assert r["loaded_cities"] == 5 and api.calls > 5


def test_bad_reading_skips_only_that_city_and_alerts(env):
    start, db, alerts = env
    api, url = start(bad_city="Newark")
    r = etl.weather_etl(url, list(CITIES), "2025-07-03", db)
    assert list(r["failed"]) == ["Newark"] and r["loaded_cities"] == 4
    assert "temp_c out of range" in alerts.read_text()


def test_rerun_uses_cache_and_is_idempotent(env):
    start, db, _ = env
    api, url = start()
    etl.weather_etl(url, list(CITIES), "2025-07-04", db)
    first, calls = rows(db), api.calls
    etl.weather_etl(url, list(CITIES), "2025-07-04", db)
    assert api.calls == calls and rows(db) == first


def test_all_cities_failing_fails_the_flow(env):
    start, db, alerts = env
    api, url = start()
    state = etl.weather_etl(url, ["Atlantis"], "2025-07-05", db, return_state=True)
    assert state.is_failed()
    assert alerts.exists()  # on_failure hook fired

"""Local hourly-weather API that fails sometimes, like real third-party APIs do.

GET /hourly?city=<name>&date=YYYY-MM-DD -> {"city":..., "date":..., "hours": [{hour, temp_c, humidity, wind_kph}, ...]}
"""
from __future__ import annotations

import json
import math
import random
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

CITIES = {"Philadelphia": 12.0, "Pittsburgh": 10.5, "Newark": 12.5, "Wilmington": 13.0, "Baltimore": 13.5}


class WeatherAPI:
    def __init__(self, error_rate: float = 0.0, seed: int = 0, bad_city: str | None = None):
        self.error_rate, self.bad_city = error_rate, bad_city
        self.rnd = random.Random(seed)
        self.lock = threading.Lock()
        self.calls = 0

    def hourly(self, city: str, day: str):
        with self.lock:
            self.calls += 1
            fail = self.rnd.random() < self.error_rate
        if fail:
            return 503, {"error": "temporarily unavailable"}
        if city not in CITIES:
            return 404, {"error": f"unknown city {city}"}
        r = random.Random(f"{city}{day}")
        base = CITIES[city]
        hours = []
        for h in range(24):
            temp = base + 6 * math.sin((h - 9) / 24 * 2 * math.pi) + r.gauss(0, 1)
            if city == self.bad_city and h == 13:
                temp = 999.0  # broken sensor
            hours.append({"hour": h, "temp_c": round(temp, 1), "humidity": r.randint(35, 95),
                          "wind_kph": round(abs(r.gauss(12, 6)), 1)})
        return 200, {"city": city, "date": day, "hours": hours}


def serve(api: WeatherAPI):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            q = parse_qs(urlparse(self.path).query)
            status, body = api.hourly(q.get("city", [""])[0], q.get("date", [""])[0])
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"

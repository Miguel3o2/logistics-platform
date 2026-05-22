
from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

log = logging.getLogger("logistics.weather_producer")

KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                     "kafka-1:9092,kafka-2:9093,kafka-3:9094")
TOPIC_WEATHER      = "logistics.weather.raw"
WEATHER_API_KEY    = os.environ.get("OPENWEATHER_API_KEY", "")
POLL_INTERVAL_S    = int(os.environ.get("WEATHER_POLL_INTERVAL", "900"))

CORRIDORS = {
    "PK-ISB-LHR": {"lat": 32.5, "lon": 73.8},
    "PK-LHR-KHI": {"lat": 28.0, "lon": 70.5},
    "PK-ISB-PES": {"lat": 33.9, "lon": 72.0},
    "PK-KHI-QTA": {"lat": 27.0, "lon": 67.5},
}

CONDITIONS    = ["CLEAR", "RAIN", "SNOW", "FOG", "STORM", "EXTREME"]
COND_WEIGHTS  = [0.45,    0.25,   0.05,   0.10,  0.10,    0.05]
RISK_MAP = {
    "CLEAR": (0, 2), "RAIN": (3, 6), "SNOW": (6, 9),
    "FOG": (5, 8), "STORM": (7, 10), "EXTREME": (8, 10),
}


def simulate_weather(lat: float, lon: float) -> dict:
    condition = random.choices(CONDITIONS, weights=COND_WEIGHTS)[0]
    temp_base = 28.0 if lat < 28 else 22.0
    risk_lo, risk_hi = RISK_MAP[condition]
    return {
        "condition":        condition,
        "temp_c":           round(temp_base + random.gauss(0, 6), 1),
        "wind_kph":         round(abs(random.gauss(20, 15)), 1),
        "visibility_km":    round(random.uniform(1 if condition != "CLEAR" else 15, 50), 1),
        "precipitation_mm": round(random.expovariate(0.3) if condition != "CLEAR" else 0, 1),
        "humidity_pct":     round(random.uniform(30, 95), 1),
        "road_risk_score":  round(random.uniform(risk_lo, risk_hi), 2),
    }


def fetch_real_weather(lat: float, lon: float) -> dict:
    import requests
    resp = requests.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params={"lat": lat, "lon": lon, "appid": WEATHER_API_KEY, "units": "metric"},
        timeout=10,
    )
    resp.raise_for_status()
    raw = resp.json()
    owm_main = raw["weather"][0]["main"]
    condition_map = {
        "Clear": "CLEAR", "Clouds": "CLEAR", "Rain": "RAIN",
        "Drizzle": "RAIN", "Snow": "SNOW", "Fog": "FOG",
        "Mist": "FOG", "Haze": "FOG", "Thunderstorm": "STORM",
        "Tornado": "EXTREME", "Squall": "STORM",
    }
    condition = condition_map.get(owm_main, "CLEAR")
    risk_lo, risk_hi = RISK_MAP[condition]
    return {
        "condition":        condition,
        "temp_c":           raw["main"]["temp"],
        "wind_kph":         round(raw["wind"]["speed"] * 3.6, 1),
        "visibility_km":    round(raw.get("visibility", 10000) / 1000, 1),
        "precipitation_mm": raw.get("rain", {}).get("1h", 0),
        "humidity_pct":     raw["main"]["humidity"],
        "road_risk_score":  round(random.uniform(risk_lo, risk_hi), 2),
    }


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "gzip",
    })


def poll_and_publish(producer: Producer) -> None:
    for corridor_id, meta in CORRIDORS.items():
        try:
            data = (fetch_real_weather(meta["lat"], meta["lon"])
                    if WEATHER_API_KEY
                    else simulate_weather(meta["lat"], meta["lon"]))

            event = {
                "schema_version":   "1.0",
                "event_type":       "weather",
                "received_at":      datetime.now(timezone.utc).isoformat(),
                "corridor_id":      corridor_id,
                "lat":              meta["lat"],
                "lon":              meta["lon"],
                "observed_at":      datetime.now(timezone.utc).isoformat(),
                **data,
            }

            producer.produce(
                TOPIC_WEATHER,
                key   = corridor_id.encode(),
                value = json.dumps(event).encode(),
            )
            producer.poll(0)
            log.info("Weather published for %s: %s risk=%.1f",
                     corridor_id, data["condition"], data["road_risk_score"])

        except Exception as exc:
            log.error("Weather fetch failed for %s: %s", corridor_id, exc)


def main():
    producer = build_producer()
    log.info("Weather producer started (interval=%ds)", POLL_INTERVAL_S)
    while True:
        poll_and_publish(producer)
        producer.flush(timeout=10)
        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    main()

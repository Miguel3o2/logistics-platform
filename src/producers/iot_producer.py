
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

log = logging.getLogger("logistics.iot_producer")

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS",
                                   "kafka-1:9092,kafka-2:9093,kafka-3:9094")
TOPIC_TELEMETRY  = "logistics.telemetry.raw"
TOPIC_DLQ        = "logistics.dlq"
NOISE_RATE       = 0.02
DTC_ANOMALY_RATE = 0.02
HARSH_EVENT_RATE = 0.01

CORRIDORS = [
    {"id": "PK-ISB-LHR", "lat": (33.7, 31.5), "lon": (73.0, 74.3)},
    {"id": "PK-LHR-KHI", "lat": (31.5, 24.9), "lon": (74.3, 67.0)},
    {"id": "PK-ISB-PES", "lat": (33.7, 34.0), "lon": (73.0, 71.5)},
    {"id": "PK-KHI-QTA", "lat": (24.9, 30.2), "lon": (67.0, 66.9)},
]

DTC_CODES = [
    "P0128", "P0171", "P0300", "P0401",
    "P0420", "P0442", "P0455", "P0505",
    "B0001", "C0035", "U0073",
]


@dataclass
class VehicleState:
    vehicle_id:   str
    fleet_id:     str
    corridor:     dict
    driver_id:    str
    lat:          float = 0.0
    lon:          float = 0.0
    speed_kmh:    float = 0.0
    heading_deg:  float = 0.0
    fuel_pct:     float = 100.0
    coolant_temp: float = 85.0
    oil_pressure: float = 350.0
    battery_v:    float = 13.8
    rpm:          int   = 1800
    phase:        float = field(default_factory=lambda: random.uniform(0, math.pi * 2))

    def __post_init__(self):
        self.lat = random.uniform(*self.corridor["lat"])
        self.lon = random.uniform(*self.corridor["lon"])

    def tick(self):
        self.phase    += 0.01
        self.speed_kmh = max(0, 85 + 25 * math.sin(self.phase) + random.gauss(0, 3))
        lat_r, lon_r   = self.corridor["lat"], self.corridor["lon"]
        self.lat       = lat_r[0] + (lat_r[1] - lat_r[0]) * (0.5 + 0.5 * math.sin(self.phase * 0.3))
        self.lon       = lon_r[0] + (lon_r[1] - lon_r[0]) * (0.5 + 0.5 * math.cos(self.phase * 0.3))
        self.fuel_pct  = max(0, self.fuel_pct - 0.001)
        self.coolant_temp = 87.0 + random.gauss(0, 1.5)
        self.rpm       = max(700, min(5000, int(self.speed_kmh * 22 + random.gauss(0, 50))))


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":            KAFKA_BOOTSTRAP,
        "enable.idempotence":           True,
        "acks":                         "all",
        "compression.type":             "gzip",
        "linger.ms":                    5,
        "batch.num.messages":           500,
        "retries":                      10,
        "retry.backoff.ms":             200,
        "delivery.timeout.ms":          120_000,
        "queue.buffering.max.messages": 200_000,
    })


def build_event(v: VehicleState) -> dict:
    dtc = [random.choice(DTC_CODES)] if random.random() < DTC_ANOMALY_RATE else []
    return {
        "schema_version": "1.0",
        "event_type":     "iot_telemetry",
        "received_at":    datetime.now(timezone.utc).isoformat(),
        "producer_id":    v.vehicle_id,
        "vehicle_id":     v.vehicle_id,
        "driver_id":      v.driver_id,
        "fleet_id":       v.fleet_id,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "status":         "MOVING" if v.speed_kmh > 2 else "IDLE",
        "speed_kmh":      round(v.speed_kmh, 2),
        "heading_deg":    round(v.heading_deg, 1),
        "gps": {
            "latitude":  round(v.lat, 6),
            "longitude": round(v.lon, 6),
            "altitude_m": round(random.uniform(200, 600), 1),
            "accuracy_m": round(random.uniform(1.5, 8.0), 1),
        },
        "engine": {
            "rpm":               v.rpm,
            "coolant_temp_c":    round(v.coolant_temp, 1),
            "oil_pressure_kpa":  round(v.oil_pressure + random.gauss(0, 5), 1),
            "fuel_level_pct":    round(v.fuel_pct, 2),
            "battery_voltage_v": round(v.battery_v + random.gauss(0, 0.05), 2),
            "dtc_codes":         dtc,
        },
        "harsh_brake":   random.random() < HARSH_EVENT_RATE,
        "harsh_accel":   random.random() < HARSH_EVENT_RATE,
        "geofence_exit": False,
    }


class VehicleThread(threading.Thread):
    def __init__(self, vehicle, producer, rate_hz, shutdown):
        super().__init__(daemon=True, name=f"vehicle-{vehicle.vehicle_id}")
        self.vehicle  = vehicle
        self.producer = producer
        self.interval = 1.0 / rate_hz
        self.shutdown = shutdown
        self.sent = self.errors = 0

    def run(self):
        while not self.shutdown.is_set():
            start = time.monotonic()
            self.vehicle.tick()

            if random.random() < NOISE_RATE:
                dlq_val = json.dumps({
                    "raw_payload":  '{"malformed":true}',
                    "source_topic": TOPIC_TELEMETRY,
                    "error_type":   "SimulatedNoise",
                    "error_detail": "intentional malformed payload",
                    "failed_at":    datetime.now(timezone.utc).isoformat(),
                }).encode()
                self.producer.produce(TOPIC_DLQ, key=b"noise", value=dlq_val)
            else:
                try:
                    event = build_event(self.vehicle)
                    self.producer.produce(
                        TOPIC_TELEMETRY,
                        key   = self.vehicle.vehicle_id.encode(),
                        value = json.dumps(event).encode(),
                        callback = lambda err, _: setattr(
                            self, "errors" if err else "sent",
                            getattr(self, "errors" if err else "sent") + 1
                        ),
                    )
                    self.producer.poll(0)
                except KafkaException as exc:
                    log.error("Kafka error %s: %s", self.vehicle.vehicle_id, exc)
                    self.errors += 1

            time.sleep(max(0, self.interval - (time.monotonic() - start)))


def main():
    parser = argparse.ArgumentParser(description="IoT Telematics Producer")
    parser.add_argument("--fleet-size", type=int,   default=50)
    parser.add_argument("--rate",       type=float, default=4.0)
    parser.add_argument("--duration",   type=float, default=0)
    args = parser.parse_args()

    log.info("Fleet: %d vehicles × %.1f msg/s = %.0f msg/s",
             args.fleet_size, args.rate, args.fleet_size * args.rate)

    producer = build_producer()
    fleets   = ["FK-NORTH", "FK-SOUTH", "FK-EXPRESS", "FK-CARGO"]
    shutdown = threading.Event()

    vehicles = []
    for i in range(args.fleet_size):
        corridor = CORRIDORS[i % len(CORRIDORS)]
        vehicles.append(VehicleState(
            vehicle_id = f"TRK-{i+1:04d}",
            fleet_id   = fleets[i % len(fleets)],
            corridor   = corridor,
            driver_id  = f"DRV-{random.randint(1000, 9999)}",
        ))

    threads = [VehicleThread(v, producer, args.rate, shutdown) for v in vehicles]
    for t in threads:
        t.start()

    def metrics():
        while not shutdown.is_set():
            time.sleep(10)
            total = sum(t.sent for t in threads)
            errs  = sum(t.errors for t in threads)
            log.info("Sent=%d Errors=%d Rate=%.1f msg/s", total, errs, total / 10)
            for t in threads:
                t.sent = t.errors = 0

    threading.Thread(target=metrics, daemon=True).start()

    signal.signal(signal.SIGINT,  lambda s, f: shutdown.set())
    signal.signal(signal.SIGTERM, lambda s, f: shutdown.set())

    if args.duration > 0:
        time.sleep(args.duration)
        shutdown.set()
    else:
        shutdown.wait()

    for t in threads:
        t.join(timeout=5)
    producer.flush(timeout=30)
    log.info("IoT producer stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
    main()

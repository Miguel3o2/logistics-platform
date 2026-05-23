# Global Logistics & Predictive Analytics Platform

End-to-end real-time data engineering platform that ingests vehicle telematics, relational CDC, and external weather data — processes it through a multi-source Medallion architecture — and serves ETA predictions and engine failure alerts via a production FastAPI inference server.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [What Makes This Different](#what-makes-this-different-from-a-standard-medallion-pipeline)
- [Project Structure](#project-structure)
- [Service URLs](#service-urls)
- [Technical Decisions](#key-technical-decisions-explained)
- [Highlights](#highlights)

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Git

### Setup (Windows PowerShell / Linux / macOS)

```bash
# 1. Clone and configure
git clone https://github.com/yourusername/logistics-platform
cd logistics-platform
cp .env.example .env          # fill in API keys if you have them

# 2. Start the infrastructure
docker compose -f deploy/docker-compose.yml up -d

# 3. Wait for Kafka to be healthy (~30 seconds)
docker compose -f deploy/docker-compose.yml ps
```

### Create Kafka Topics

```bash
docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --if-not-exists \
  --topic logistics.telemetry.raw --partitions 6 --replication-factor 3

docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --if-not-exists \
  --topic logistics.weather.raw --partitions 6 --replication-factor 3

docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --if-not-exists \
  --topic logistics.dlq --partitions 6 --replication-factor 3
```

### Register Debezium CDC Connector

```bash
# Windows PowerShell
.\deploy\debezium\register_connectors.ps1

# Linux / macOS
bash deploy/debezium/register_connectors.sh
```

### Start Data Producers

**Terminal 1 - IoT Producer** (200 msg/s from 50 vehicles):
```bash
# Windows PowerShell
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
.\venv311\Scripts\python.exe -m src.producers.iot_producer --fleet-size 50 --rate 4

# Linux / macOS
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
python -m src.producers.iot_producer --fleet-size 50 --rate 4
```

**Terminal 2 - Weather Producer** (15-min API poll):
```bash
# Windows PowerShell
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
.\venv311\Scripts\python.exe -m src.producers.weather_producer

# Linux / macOS
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
python -m src.producers.weather_producer
```

### Submit Spark Streaming Jobs

**Terminal 3 - Bronze Ingestion**:
```bash
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=2 \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/streaming/bronze/ingest_bronze.py
```

**Terminal 4 - Silver Transformation**:
```bash
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=2 \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  src/streaming/silver/transform_silver.py
```

### Run Batch & ML Pipelines

**Gold Layer Batch** (Airflow runs daily, or trigger manually):
```bash
# Windows PowerShell
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=2 `
  --packages io.delta:delta-spark_2.12:3.1.0 `
  src/streaming/gold/aggregate_gold.py --run-date (Get-Date -Format yyyy-MM-dd)

# Linux / macOS
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.cores.max=2 \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  src/streaming/gold/aggregate_gold.py --run-date $(date +%Y-%m-%d)
```

**dbt Models**:
```bash
cd src/dbt
../../venv311/Scripts/dbt.exe run --profiles-dir . --target dev
../../venv311/Scripts/dbt.exe test --profiles-dir . --target dev
cd ../..
```

**ML Training & Registry**:
```bash
# Windows PowerShell
docker compose -f deploy/docker-compose.yml exec ml-server python -m training.train_models --run-date (Get-Date -Format yyyy-MM-dd)
Invoke-RestMethod -Method Post -Uri http://localhost:8000/models/reload

# Linux / macOS
docker compose -f deploy/docker-compose.yml exec ml-server python -m training.train_models --run-date $(date +%Y-%m-%d)
curl -X POST http://localhost:8000/models/reload
```

### Test the FastAPI Server

```bash
curl -X POST http://localhost:8000/predict/eta \
  -H "Content-Type: application/json" \
  -d '{
    "shipment_id": "S001",
    "vehicle_id": "TRK-0001",
    "current_speed_kmh": 85,
    "fuel_level_pct": 70,
    "distance_remaining_km": 150
  }'
```

---

## Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                        │
│                                                                       │
│  IoT Sensors (200+ msg/s)   Postgres CDC (Debezium)   Weather API   │
│  src/producers/iot_producer  logistics.vehicles         15-min poll  │
│         │                    logistics.shipments              │      │
│         └──────────────────────────┬───────────────────────── ┘      │
│                                    │                                 │
│                          KAFKA (3-broker KRaft)                     │
│              logistics.telemetry.raw  logistics.cdc.*  logistics.   │
│                                        weather                       │
└────────────────────────────────────┬─────────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────────┐
│  BRONZE LAYER  (src/streaming/bronze/ingest_bronze.py)              │
│                                                                       │
│  • Spark Structured Streaming — 4 parallel micro-batch readers      │
│  • mergeSchema=true — IoT firmware updates never break ingestion    │
│  • rescuedDataColumn="_rescued" — unexpected fields captured        │
│  • foreachBatch DLQ routing — bad envelopes → logistics.dlq + S3   │
│  • Partitioned by (event_type, year, month, day) on Minio/S3       │
└────────────────────────────────────┬─────────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────────┐
│  SILVER LAYER  (src/streaming/silver/transform_silver.py)           │
│                                                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────┐ │
│  │ Telemetry Clean  │  │ SCD Type 2       │  │ Shipments MERGE   │ │
│  │ • Explode JSON   │  │ Vehicle Dim.     │  │ • Upsert from CDC │ │
│  │ • Dedup on       │  │ • Close old rows │  │ • Current-state   │ │
│  │   (id, ts)       │  │ • Insert new ver.│  │ • INSERT/UPDATE   │ │
│  │ • Derived cols   │  │ • Delta MERGE    │  └───────────────────┘ │
│  │ • engine_health  │  └──────────────────┘                         │
│  └────────┬─────────┘                                                │
│           │                                                          │
│  ┌────────▼──────────────────────────────────────────────────────┐  │
│  │  STREAM-TO-STREAM JOIN                                       │  │
│  │  Telemetry stream ⋈ Weather stream                           │  │
│  │  • Both watermarked (10 min + 30 min)                        │  │
│  │  • Join key: GPS bucket (1° grid) + time window (±15 min)   │  │
│  │  • Output: enriched_telemetry with road_risk_score          │  │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬─────────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────────┐
│  GOLD LAYER  (src/streaming/gold/aggregate_gold.py)                 │
│                                                                       │
│  ML Feature Stores:                 Business KPIs:                  │
│  • eta_features                     • fleet_kpis                    │
│    (18 features per shipment)         (daily fleet health score)     │
│  • engine_failure_features          dbt Marts:                      │
│    (12 hourly engine metrics)        • mart_fleet_performance        │
│                                      • mart_route_analytics          │
│  Delta OPTIMIZE + Z-ORDER + VACUUM run weekly by Airflow            │
└────────────────────────────────────┬─────────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────────┐
│  ML SERVING  (src/ml/serving/app.py)                                │
│                                                                       │
│  FastAPI server — 2 endpoints:                                       │
│  POST /predict/eta      → hours until delivery (GBR + conf. band)   │
│  POST /predict/failure  → engine failure prob. (calibrated GBDT)    │
│                                                                       │
│  Models loaded from MLflow Registry (Production stage)               │
│  Feature freshness checked at /health — 503 if Gold > 2h stale      │
└────────────────────────────────────┬─────────────────────────────────┘
                                     │
┌────────────────────────────────────▼─────────────────────────────────┐
│  ORCHESTRATION  (dag/logistics_orchestration.py)                     │
│                                                                       │
│  DAG 1: streaming_health (*/5 min)  — Kafka lag + PagerDuty alerts  │
│  DAG 2: daily_batch (01:00 UTC)     — Diamond dependency pipeline   │
│  DAG 3: maintenance (Sun 03:00 UTC) — Delta OPTIMIZE + retrain      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## What Makes This Different from a Standard Medallion Pipeline

| Feature | Standard Pipeline | This Project |
|---------|-------------------|--------------|
| **Sources** | 1 | 3 (IoT + CDC + API) |
| **Schema Evolution** | Fixed | Delta `mergeSchema` + `rescuedDataColumn` |
| **Stream Joins** | None | Stream-to-stream (telemetry ⋈ weather) |
| **Dimensions** | Snapshot | SCD Type 2 with Delta MERGE |
| **CDC** | Simulated | Real Debezium on `pg_logical` |
| **ML** | Batch scoring | FastAPI serving from MLflow Registry |
| **Storage** | HDFS/local | Minio (S3-compatible) |
| **Observability** | Logs | Prometheus + Grafana + MLflow |

---

## Project Structure

```
logistics-platform/
├── deploy/
│   ├── docker-compose.yml           ← 12 services, 3-broker Kafka, Minio
│   ├── postgres/init/               ← Operational DB schema + seed data
│   ├── debezium/                    ← Connector registration scripts
│   └── observability/               ← Prometheus + Grafana config
│
├── src/
│   ├── contracts/schemas.py         ← Pydantic V2 discriminated union (4 types)
│   ├── producers/
│   │   ├── iot_producer.py          ← 200+ msg/s fleet simulator (threaded)
│   │   └── weather_producer.py      ← External API poller → Kafka
│   ├── streaming/
│   │   ├── bronze/ingest_bronze.py  ← Multi-source Spark Streaming + DLQ
│   │   ├── silver/transform_silver.py ← SCD Type 2 + stream-to-stream join
│   │   └── gold/aggregate_gold.py   ← ML feature stores + Z-ORDER + VACUUM
│   ├── dbt/
│   │   ├── models/staging/          ← Incremental staging w/ validation
│   │   ├── models/gold/             ← Fleet performance + route analytics
│   │   └── tests/                   ← 4 custom SQL data tests
│   └── ml/
│       ├── training/train_models.py ← GBR + calibrated GBDT + MLflow
│       └── serving/app.py           ← FastAPI ETA + failure inference
│
├── dag/
│   └── logistics_orchestration.py   ← 3 Airflow DAGs (health/batch/maint)
│
└── README.md
```

---

## Service URLs

| Service | URL | Login |
|---------|-----|-------|
| **Kafka UI** | http://localhost:8080 | — |
| **Minio Console** | http://localhost:9001 | `logistics` / `logistics123` |
| **Spark Master** | http://localhost:8081 | — |
| **Airflow** | http://localhost:8888 | `admin` / `admin` |
| **MLflow** | http://localhost:5000 | — |
| **FastAPI Docs** | http://localhost:8000/docs | — |
| **Prometheus** | http://localhost:9090 | — |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| **Debezium REST** | http://localhost:8083 | — |
| **Postgres** | `localhost:55432` | `logistics` / `logistics123` |

---

## Key Technical Decisions Explained

### Why stream-to-stream joins instead of a static weather lookup?

Weather data changes every 15 minutes. A static lookup would mean a vehicle at 2:30 AM uses the 2:15 AM weather — usually fine. But in a storm that intensifies rapidly, the 15-minute-old reading could classify a route as `road_risk=3` when the real value is `road_risk=9`.

**Solution:** Stream joining both streams with watermarks ensures the join state is bounded and the risk score is always the most temporally relevant reading.

### Why SCD Type 2 on vehicles but MERGE (no SCD2) on shipments?

Shipment status changes are terminal — once a shipment is `DELIVERED`, no analyst ever needs to know what status it had 3 hours ago. Vehicle attributes change rarely but the **history matters**: if a truck was on the wrong fleet assignment when it broke down, you need the historical `fleet_id` to audit the maintenance response.

### Why Minio instead of a real S3 bucket?

Minio is S3-API-compatible. Every `s3a://` path in the codebase works unchanged against real AWS S3 — just change the endpoint and credentials in `.env`. 

**Benefit:** Run the full platform locally without an AWS account or egress costs.

### Why MLflow for model registry?

The FastAPI server loads models by **stage name** (`Production`), not by version number. This means the Airflow maintenance DAG can:
1. Retrain models
2. Run evaluation in MLflow
3. Promote the best model to `Production`
4. Call `/models/reload`

**Result:** Zero code changes, zero server restarts. The old model stays live until the reload completes.

### Why CalibratedClassifierCV on the failure model?

A raw GBT `predict_proba` gives sharp discrimination but miscalibrated probabilities. If the model says "80% chance of failure" but only 40% of those vehicles actually fail, the CSM team will stop trusting it within a week.

**Solution:** Isotonic calibration ensures probability scores are actionable, not just rankings.

---

## Highlights

### 🏗️ Architecture

- **Multi-source ingestion:** IoT (200+ msg/s), Debezium CDC (PostgreSQL), external APIs
- **3-broker Kafka** with KRaft mode (no ZooKeeper)
- **Medallion architecture** (Bronze → Silver → Gold) with schema evolution
- **Stream-to-stream joins** with watermarking for rich feature engineering
- **Spark Structured Streaming** with dynamic scaling

### 🔄 Data Quality

- **DLQ (Dead Letter Queue)** with automatic routing to S3
- **SCD Type 2** dimensions for temporal analysis
- **dbt testing** (4 custom SQL data tests)
- **Prometheus + Grafana** for real-time monitoring

### 🤖 ML & Inference

- **18 ETA features** (route, vehicle, weather, temporal)
- **12 hourly engine health metrics** (temperature trends, DTC rate, oil pressure)
- **Calibrated GBT** for interpretable failure probabilities
- **FastAPI inference server** with model freshness checks
- **MLflow model registry** for zero-downtime model updates

### 🎯 Orchestration

- **Airflow DAGs** with diamond dependencies
- **Kafka lag monitoring** (every 5 min)
- **PagerDuty SLA callbacks**
- **Weekly Delta maintenance** (OPTIMIZE + Z-ORDER + VACUUM)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.

## License

MIT License — see [LICENSE](LICENSE) for details.

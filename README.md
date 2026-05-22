# Global Logistics & Predictive Analytics Platform

End-to-end real-time data engineering platform that ingests vehicle telematics,
relational CDC, and external weather data — processes it through a multi-source
Medallion architecture — and serves ETA predictions and engine failure alerts
via a production FastAPI inference server.

---

## Architecture at a glance

```
┌─────────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                           │
│                                                                         │
│  IoT Sensors (200+ msg/s)   Postgres CDC (Debezium)   Weather API      │
│  src/producers/iot_producer  logistics.vehicles         15-min poll     │
│         │                    logistics.shipments              │         │
│         └──────────────────────────┬───────────────────────── ┘         │
│                                    │                                    │
│                          KAFKA (3-broker KRaft)                        │
│              logistics.telemetry.raw  logistics.cdc.*  logistics.weather│
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│  BRONZE LAYER  (src/streaming/bronze/ingest_bronze.py)                 │
│                                                                         │
│  • Spark Structured Streaming — 4 parallel micro-batch readers         │
│  • mergeSchema=true — IoT firmware updates never break ingestion       │
│  • rescuedDataColumn="_rescued" — unexpected fields captured, not lost │
│  • foreachBatch DLQ routing — bad envelopes → logistics.dlq + S3      │
│  • Partitioned by (event_type, year, month, day) on Minio/S3          │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│  SILVER LAYER  (src/streaming/silver/transform_silver.py)              │
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ Telemetry Clean │  │ SCD Type 2       │  │ Shipments MERGE      │  │
│  │ • Explode JSON  │  │ Vehicle Dimension│  │ • Upsert from CDC    │  │
│  │ • Dedup on      │  │ • Close old rows │  │ • Current-state only │  │
│  │   (id, ts)      │  │ • Insert new ver.│  │ • INSERT/UPDATE/DEL  │  │
│  │ • Derived cols  │  │ • Delta MERGE    │  └──────────────────────┘  │
│  │ • engine_health │  └──────────────────┘                            │
│  └────────┬────────┘                                                   │
│           │                                                             │
│  ┌────────▼─────────────────────────────────────────────────────────┐  │
│  │  STREAM-TO-STREAM JOIN                                           │  │
│  │  Telemetry stream ⋈ Weather stream                               │  │
│  │  • Both watermarked (10 min + 30 min)                            │  │
│  │  • Join key: GPS bucket (1° grid) + time window (±15 min)       │  │
│  │  • Output: enriched_telemetry with road_risk_score               │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│  GOLD LAYER  (src/streaming/gold/aggregate_gold.py)                    │
│                                                                         │
│  ML Feature Stores:                 Business KPIs:                     │
│  • eta_features                     • fleet_kpis                       │
│    (18 features per shipment)         (daily fleet health score)        │
│  • engine_failure_features          dbt Marts:                         │
│    (12 hourly engine metrics)        • mart_fleet_performance           │
│                                      • mart_route_analytics             │
│  Delta OPTIMIZE + Z-ORDER + VACUUM run weekly by Airflow               │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│  ML SERVING  (src/ml/serving/app.py)                                   │
│                                                                         │
│  FastAPI server — 2 endpoints:                                          │
│  POST /predict/eta      → hours until delivery (GBR + confidence band)  │
│  POST /predict/failure  → engine failure probability (calibrated GBDT)  │
│                                                                         │
│  Models loaded from MLflow Registry (Production stage)                  │
│  Feature freshness checked at /health — 503 if Gold > 2h stale         │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│  ORCHESTRATION  (dag/logistics_orchestration.py)                        │
│                                                                         │
│  DAG 1: streaming_health (*/5 min)  — Kafka lag monitor + PagerDuty   │
│  DAG 2: daily_batch (01:00 UTC)     — Diamond dependency pipeline      │
│  DAG 3: maintenance (Sun 03:00 UTC) — Delta OPTIMIZE + model retrain   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## What makes this different from a standard Medallion pipeline

| Feature | Standard pipeline | This project |
|---|---|---|
| Sources | 1 | 3 (IoT + CDC + API) |
| Schema evolution | Fixed | Delta mergeSchema + rescuedDataColumn |
| Stream joins | None | Stream-to-stream (telemetry ⋈ weather) |
| Dimensions | Snapshot | SCD Type 2 with Delta MERGE |
| CDC | Simulated | Real Debezium on pg_logical |
| ML | Batch scoring | FastAPI serving from MLflow Registry |
| Storage | HDFS/local | Minio (S3-compatible) |
| Observability | Logs | Prometheus + Grafana + MLflow |

---

## Quick start

PowerShell on Windows was verified with the commands below. The Kafka CLI path
(`/opt/kafka/bin/kafka-topics.sh`) is inside the Kafka container, so run it
through `docker compose exec`; do not run it directly at the PowerShell prompt.

```bash
# 1. Clone and configure
git clone https://github.com/yourusername/logistics-platform
cd logistics-platform
cp .env.example .env          # fill in API keys if you have them

# 2. Start the infrastructure
docker compose -f deploy/docker-compose.yml up -d

# 3. Wait for Kafka to be healthy (~30 seconds)
docker compose -f deploy/docker-compose.yml ps

# 4. Create app topics used by the producers and Spark streams
docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic logistics.telemetry.raw --partitions 6 --replication-factor 3

docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic logistics.weather.raw --partitions 6 --replication-factor 3

docker compose -f deploy/docker-compose.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --create --if-not-exists --topic logistics.dlq --partitions 6 --replication-factor 3

# 5. Register the Debezium CDC connector
.\deploy\debezium\register_connectors.ps1

# 6. Start the IoT producer (200 msg/s from 50 vehicles)
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
.\venv311\Scripts\python.exe -m src.producers.iot_producer --fleet-size 50 --rate 4

# 7. Start the weather producer (separate terminal)
$env:KAFKA_BOOTSTRAP_SERVERS='localhost:9092,localhost:9093,localhost:9094'
.\venv311\Scripts\python.exe -m src.producers.weather_producer

# 8. Submit the Bronze streaming job
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=2 `
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  src/streaming/bronze/ingest_bronze.py

# 9. Submit the Silver transformation job
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=2 `
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  src/streaming/silver/transform_silver.py

# 10. Run Gold batch (Airflow runs this daily, but you can trigger manually)
docker compose -f deploy/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.cores.max=2 `
  --packages io.delta:delta-spark_2.12:3.1.0 `
  src/streaming/gold/aggregate_gold.py --run-date (Get-Date -Format yyyy-MM-dd)

# 11. Run dbt models
cd src/dbt
..\..\venv311\Scripts\dbt.exe run --profiles-dir . --target dev
..\..\venv311\Scripts\dbt.exe test --profiles-dir . --target dev
cd ..\..

# 12. Train and register ML models
docker compose -f deploy/docker-compose.yml exec ml-server python -m training.train_models --run-date (Get-Date -Format yyyy-MM-dd)
Invoke-RestMethod -Method Post -Uri http://localhost:8000/models/reload

# 13. The FastAPI server starts automatically via docker-compose
# Test it:
curl -X POST http://localhost:8000/predict/eta \
  -H "Content-Type: application/json" \
  -d '{"shipment_id":"S001","vehicle_id":"TRK-0001","current_speed_kmh":85,"fuel_level_pct":70,...}'
```

---

## Service URLs

| Service | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Minio Console | http://localhost:9001 | logistics / logistics123 |
| Spark Master | http://localhost:8081 | — |
| Airflow | http://localhost:8888 | admin / admin |
| MLflow | http://localhost:5000 | — |
| FastAPI docs | http://localhost:8000/docs | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Debezium REST | http://localhost:8083 | — |
| Postgres | localhost:55432 | logistics / logistics123 |

---

## Project structure

```
logistics-platform/
├── deploy/
│   ├── docker-compose.yml           ← 12 services, 3-broker Kafka, Minio
│   ├── postgres/init/               ← Operational DB schema + seed data
│   ├── debezium/                    ← Connector registration script
│   └── observability/               ← Prometheus + Grafana config
│
├── src/
│   ├── contracts/schemas.py         ← Pydantic V2 discriminated union (4 event types)
│   ├── producers/
│   │   ├── iot_producer.py          ← 200+ msg/s fleet simulator (threaded)
│   │   └── weather_producer.py      ← External API poller → Kafka
│   ├── streaming/
│   │   ├── bronze/ingest_bronze.py  ← Multi-source Spark Streaming + DLQ
│   │   ├── silver/transform_silver.py ← SCD Type 2 + stream-to-stream join
│   │   └── gold/aggregate_gold.py   ← ML feature stores + Z-ORDER + VACUUM
│   ├── dbt/
│   │   ├── models/staging/          ← Incremental staging with validation
│   │   ├── models/gold/             ← Fleet performance + route analytics
│   │   └── tests/                   ← 4 custom SQL data tests
│   └── ml/
│       ├── training/train_models.py ← GBR + calibrated GBDT + MLflow tracking
│       └── serving/app.py           ← FastAPI ETA + failure inference server
│
├── dag/
│   └── logistics_orchestration.py   ← 3 Airflow DAGs (health/batch/maintenance)
│
└── README.md
```

---

## Key technical decisions explained

### Why stream-to-stream joins instead of a static weather lookup?
Weather data changes every 15 minutes. A static lookup would mean a vehicle at
2:30 AM uses the 2:15 AM weather — which is usually fine. But in a storm that
intensifies rapidly, the 15-minute-old reading could classify a route as
`road_risk=3` when the real value is `road_risk=9`. Stream joining both streams
with watermarks ensures the join state is bounded and the risk score is always
the most temporally relevant reading.

### Why SCD Type 2 on vehicles but MERGE (no SCD2) on shipments?
Shipment status changes are terminal — once a shipment is DELIVERED, no analyst
ever needs to know what status it had 3 hours ago. Vehicle attributes change
rarely but the history matters: if a truck was on the wrong fleet assignment when
it broke down, you need the historical fleet_id to audit the maintenance response.

### Why Minio instead of a real S3 bucket?
Minio is S3-API-compatible. Every `s3a://` path in the codebase works unchanged
against real AWS S3 — just change the endpoint and credentials in `.env`. Minio
lets you run the full platform locally without an AWS account or egress costs.

### Why MLflow for model registry?
The FastAPI server loads models by stage name (`Production`), not by version
number. This means the Airflow maintenance DAG can retrain models, run evaluation
in MLflow, promote the best model to `Production`, and then call `/models/reload`
— with zero code changes and zero server restarts. The old model stays live until
the reload completes.

### Why CalibratedClassifierCV on the failure model?
A raw GBT `predict_proba` gives sharp discrimination but miscalibrated
probabilities. If the model says "80% chance of failure" but only 40% of those
vehicles actually fail, the CSM team will stop trusting it within a week.
Isotonic calibration ensures the probability scores are actionable, not just
rankings.

---

## CV bullet points for this project

**Senior Data Engineer:**
- Designed a multi-source real-time data platform ingesting 200+ IoT msg/s, Debezium CDC from PostgreSQL, and external weather API enrichment into a 3-broker Kafka cluster — processing through a Medallion architecture on Delta Lake with schema evolution (mergeSchema + rescuedDataColumn) for zero-downtime firmware updates.
- Implemented a Spark Structured Streaming stream-to-stream join between vehicle telemetry and weather enrichment streams, watermarked at 10 and 30 minutes respectively, producing road-risk-enriched telemetry with bounded state for downstream ML features.
- Applied SCD Type 2 via Delta MERGE on the vehicle dimension, preserving full point-in-time history for fleet assignment auditing; applied weekly OPTIMIZE + Z-ORDER + VACUUM with 7-day time travel retention across all Gold tables.
- Orchestrated a diamond-dependency Airflow DAG (3 parallel Silver jobs fan out to 3 Gold jobs, converging on dbt + ML scoring) with PagerDuty SLA callbacks and Kafka lag monitoring every 5 minutes.

**ML Engineer:**
- Built and deployed a FastAPI ML inference server serving calibrated GBT predictions for ETA (MAE < 1h) and engine failure probability (AUROC + Brier score reported), loading models from MLflow Model Registry with zero-downtime hot-reload via background task.
- Engineered ML feature stores in Delta Gold covering 18 ETA features (route, vehicle, weather, time) and 12 hourly engine health features (temperature trend, DTC rate, oil pressure) — staleness-tested by a custom dbt SQL assertion.
#   l o g i s t i c s - p l a t f o r m  
 
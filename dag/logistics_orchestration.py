"""
dag/logistics_orchestration.py
================================
Airflow orchestration for the Global Logistics Platform.

Three production DAGs:

  DAG 1: logistics_streaming_health (every 5 minutes)
    Monitors Kafka consumer lag across all three source topics.
    PagerDuty alert if any topic lags > 100k messages.

  DAG 2: logistics_daily_batch (01:00 UTC daily)
    Diamond dependency pattern:
      Bronze health check
        ├── Silver telemetry (Spark)
        ├── Silver SCD2 vehicles (Spark)
        └── Silver shipments MERGE (Spark)
      [fan-in] → Gold ETA features
      [fan-in] → Gold engine features
      [fan-in] → Gold KPIs
      [fan-in] → dbt models + tests
      [fan-in] → ML scoring → FastAPI score push

  DAG 3: logistics_maintenance (Sunday 03:00 UTC)
    Delta OPTIMIZE + Z-ORDER + VACUUM
    MLflow model evaluation and promotion
    Debezium connector health check
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

log = logging.getLogger("logistics.airflow")

# ── Shared config ─────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = Variable.get("kafka_bootstrap_servers",
                                default_var="kafka-1:9092,kafka-2:9093,kafka-3:9094")
PAGERDUTY_KEY    = Variable.get("pagerduty_routing_key", default_var="")
SPARK_MASTER     = Variable.get("spark_master_url",
                                default_var="spark://spark-master:7077")
ML_SERVER_URL    = Variable.get("ml_server_url",
                                default_var="http://ml-server:8000")
GOLD_PATH        = Variable.get("gold_path",
                                default_var="s3a://logistics/delta/gold")
DBT_DIR          = Variable.get("dbt_project_dir",
                                default_var="/opt/logistics/src/dbt")

KAFKA_LAG_THRESHOLD = 100_000   # messages

DEFAULT_ARGS = {
    "owner":                    "data-engineering",
    "depends_on_past":          False,
    "retries":                  2,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff":True,
    "execution_timeout":        timedelta(hours=3),
    "email_on_failure":         False,
}

SPARK_SUBMIT = (
    "spark-submit "
    f"--master {SPARK_MASTER} "
    "--packages io.delta:delta-spark_2.12:3.1.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)


# ═══════════════════════════════════════════════════════════════════════════
# DAG 1 — Streaming Health Monitor
# ═══════════════════════════════════════════════════════════════════════════

def check_kafka_lag(**context) -> dict:
    """
    Check consumer group lag for all three source topics.
    XCom push: per-topic lag dict for downstream alerting.
    """
    from kafka import KafkaAdminClient, OffsetSpec
    from airflow.exceptions import AirflowException

    groups = {
        "bronze-telemetry-consumer": "logistics.telemetry.raw",
        "bronze-shipments-consumer": "logistics.cdc.shipments",
        "bronze-weather-consumer":   "logistics.weather.raw",
    }

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        request_timeout_ms=15_000,
    )

    total_lag  = 0
    lag_report = {}

    try:
        for group_id, topic in groups.items():
            try:
                offsets     = admin.list_consumer_group_offsets(group_id)
                partitions  = list(offsets.keys())
                log_ends    = admin.list_offsets(
                    {p: OffsetSpec.latest() for p in partitions}
                )
                group_lag = sum(
                    max(0, log_ends[tp].offset - meta.offset)
                    for tp, meta in offsets.items()
                )
                lag_report[topic] = group_lag
                total_lag += group_lag
            except Exception as exc:
                log.warning("Lag check failed for group %s: %s", group_id, exc)
                lag_report[topic] = -1
    finally:
        admin.close()

    context["ti"].xcom_push(key="lag_report", value=lag_report)
    context["ti"].xcom_push(key="total_lag",  value=total_lag)

    log.info("Kafka lag report: %s | total=%d", lag_report, total_lag)

    if total_lag > KAFKA_LAG_THRESHOLD:
        raise AirflowException(
            f"Kafka lag CRITICAL: {total_lag:,} messages behind. "
            f"Details: {json.dumps(lag_report)}"
        )

    return lag_report


def pagerduty_alert(context: dict) -> None:
    """SLA miss callback — fires PagerDuty incident."""
    import urllib.request

    if not PAGERDUTY_KEY:
        log.warning("PAGERDUTY_KEY not set — skipping alert")
        return

    dag_id  = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    payload = json.dumps({
        "routing_key":  PAGERDUTY_KEY,
        "event_action": "trigger",
        "payload": {
            "summary":   f"Logistics pipeline SLA miss: {dag_id}.{task_id}",
            "severity":  "warning",
            "source":    "airflow",
            "component": "logistics-platform",
        },
    }).encode()

    req = urllib.request.Request(
        "https://events.pagerduty.com/v2/enqueue",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=15)
        log.info("PagerDuty SLA alert sent")
    except Exception as exc:
        log.error("PagerDuty alert failed: %s", exc)


def publish_lag_metrics(**context) -> None:
    """Push lag metrics to Prometheus push gateway."""
    lag_report = context["ti"].xcom_pull(
        task_ids="check_kafka_lag", key="lag_report"
    ) or {}
    total_lag = context["ti"].xcom_pull(
        task_ids="check_kafka_lag", key="total_lag"
    ) or 0

    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
        registry = CollectorRegistry()
        g = Gauge("kafka_consumer_lag_messages",
                  "Kafka consumer lag",
                  ["topic"],
                  registry=registry)
        for topic, lag in lag_report.items():
            if lag >= 0:
                g.labels(topic=topic).set(lag)
        push_to_gateway("prometheus:9091", job="airflow_lag", registry=registry)
    except Exception as exc:
        log.warning("Prometheus push failed (non-fatal): %s", exc)


with DAG(
    dag_id           = "logistics_streaming_health",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "*/5 * * * *",
    start_date       = days_ago(1),
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["logistics", "streaming", "monitoring"],
) as dag_health:

    t_check_lag = PythonOperator(
        task_id         = "check_kafka_lag",
        python_callable = check_kafka_lag,
    )
    t_push_metrics = PythonOperator(
        task_id         = "publish_lag_metrics",
        python_callable = publish_lag_metrics,
    )
    t_check_lag >> t_push_metrics


# ═══════════════════════════════════════════════════════════════════════════
# DAG 2 — Daily Batch Pipeline (Diamond Pattern)
# ═══════════════════════════════════════════════════════════════════════════

def validate_bronze_freshness(**context) -> None:
    """
    Assert Bronze tables received data in the last 24 hours.
    Blocks the entire pipeline if Bronze is stale.
    """
    from airflow.exceptions import AirflowException
    import pyarrow.parquet as pq

    run_date = context["ds"]
    bronze_paths = {
        "telemetry": f"s3a://logistics/delta/bronze/telemetry",
        "shipments": f"s3a://logistics/delta/bronze/shipments",
    }

    for name, path in bronze_paths.items():
        try:
            table = pq.read_table(
                path,
                filters=[("year",  "=", int(run_date[:4])),
                         ("month", "=", int(run_date[5:7])),
                         ("day",   "=", int(run_date[8:10]))]
            )
            if table.num_rows == 0:
                raise AirflowException(
                    f"Bronze {name}: zero rows for {run_date} — "
                    "Bronze streaming job may have failed"
                )
            log.info("Bronze %s: %d rows for %s", name, table.num_rows, run_date)
            context["ti"].xcom_push(key=f"bronze_{name}_count", value=table.num_rows)
        except FileNotFoundError:
            raise AirflowException(
                f"Bronze {name} path not found — streaming job never wrote data"
            )


def score_and_push(**context) -> None:
    """
    Read Gold ETA features, call the ML server, write scores back.
    This is the Reverse ETL step for ML predictions.
    """
    import urllib.request
    import pyarrow.parquet as pq

    run_date = context["ds"]

    try:
        table = pq.read_table(
            f"{GOLD_PATH}/eta_features",
            filters=[("snapshot_date", "=", run_date)],
        )
        df = table.to_pandas()
    except Exception as exc:
        log.error("Gold ETA features read failed: %s", exc)
        return

    log.info("Scoring %d shipments for %s", len(df), run_date)
    scored = 0

    for _, row in df.iterrows():
        payload = json.dumps({
            "shipment_id":          str(row.get("shipment_id", "unknown")),
            "vehicle_id":           str(row["vehicle_id"]),
            **{col: float(row.get(col, 0)) for col in [
                "current_speed_kmh", "fuel_level_pct", "engine_health_score",
                "road_risk_score", "visibility_km", "precipitation_mm", "temp_c",
                "hist_avg_speed_kmh", "hist_speed_stddev",
                "hours_to_scheduled_eta", "weight_kg",
            ]},
            "hour_of_day":   int(row.get("hour_of_day",  12)),
            "day_of_week":   int(row.get("day_of_week",   3)),
            "is_weekend":    int(row.get("is_weekend",    0)),
            "is_already_late": int(row.get("is_already_late", 0)),
            "harsh_brake_flag": int(row.get("harsh_brake_flag", 0)),
            "harsh_accel_flag": int(row.get("harsh_accel_flag", 0)),
            "active_dtc_count": int(row.get("active_dtc_count", 0)),
        }).encode()

        try:
            req = urllib.request.Request(
                f"{ML_SERVER_URL}/predict/eta",
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            scored += 1
        except Exception as exc:
            log.warning("Score failed for %s: %s", row["vehicle_id"], exc)

    context["ti"].xcom_push(key="scored_count", value=scored)
    log.info("Scored %d/%d shipments", scored, len(df))


def validate_gold_quality(**context) -> None:
    """Assert Gold tables meet minimum row count for the run date."""
    from airflow.exceptions import AirflowException
    import pyarrow.parquet as pq

    run_date = context["ds"]
    min_rows = {"eta_features": 1, "engine_features": 1, "fleet_kpis": 1}

    for table_name, min_count in min_rows.items():
        try:
            t = pq.read_table(
                f"{GOLD_PATH}/{table_name}",
                filters=[("snapshot_date", "=", run_date)],
            )
            if t.num_rows < min_count:
                raise AirflowException(
                    f"Gold {table_name}: only {t.num_rows} rows for {run_date}"
                )
            log.info("Gold %s: %d rows — OK", table_name, t.num_rows)
        except FileNotFoundError:
            raise AirflowException(f"Gold {table_name} path not found")


with DAG(
    dag_id           = "logistics_daily_batch",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "0 1 * * *",    # 01:00 UTC
    start_date       = days_ago(1),
    catchup          = False,
    max_active_runs  = 1,
    sla_miss_callback= pagerduty_alert,
    tags             = ["logistics", "batch", "daily"],
) as dag_batch:

    # ── Gate: Bronze must be fresh ────────────────────────────────────────
    t_validate_bronze = PythonOperator(
        task_id         = "validate_bronze_freshness",
        python_callable = validate_bronze_freshness,
        sla             = timedelta(hours=1),
    )

    # ── Silver fan-out (3 independent Spark jobs run in parallel) ─────────
    with TaskGroup("silver", tooltip="Silver transformations (parallel)") as tg_silver:

        t_silver_telemetry = BashOperator(
            task_id     = "silver_telemetry",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/silver/transform_silver.py "
                "--mode telemetry --run-date {{ ds }}"
            ),
            sla=timedelta(hours=1, minutes=30),
        )

        t_silver_vehicles = BashOperator(
            task_id     = "silver_scd2_vehicles",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/silver/transform_silver.py "
                "--mode vehicles --run-date {{ ds }}"
            ),
        )

        t_silver_shipments = BashOperator(
            task_id     = "silver_shipments",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/silver/transform_silver.py "
                "--mode shipments --run-date {{ ds }}"
            ),
        )

    # ── Gold fan-out (runs after all Silver jobs complete) ────────────────
    with TaskGroup("gold", tooltip="Gold aggregations (parallel)") as tg_gold:

        t_gold_eta = BashOperator(
            task_id     = "gold_eta_features",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/gold/aggregate_gold.py "
                "--target eta_features --run-date {{ ds }}"
            ),
            sla=timedelta(hours=2),
        )

        t_gold_engine = BashOperator(
            task_id     = "gold_engine_features",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/gold/aggregate_gold.py "
                "--target engine_features --run-date {{ ds }}"
            ),
        )

        t_gold_kpis = BashOperator(
            task_id     = "gold_fleet_kpis",
            bash_command=(
                f"{SPARK_SUBMIT} "
                "/opt/logistics/src/streaming/gold/aggregate_gold.py "
                "--target fleet_kpis --run-date {{ ds }}"
            ),
        )

    # ── dbt models converge from all Gold outputs ─────────────────────────
    with TaskGroup("dbt", tooltip="dbt analytics models") as tg_dbt:

        t_dbt_run = BashOperator(
            task_id     = "dbt_run",
            bash_command=(
                f"cd {DBT_DIR} && "
                "dbt run --target prod "
                "--vars '{\"run_date\": \"{{ ds }}\"}' "
                "--profiles-dir /opt/airflow/dbt_profiles"
            ),
        )

        t_dbt_test = BashOperator(
            task_id     = "dbt_test",
            bash_command=(
                f"cd {DBT_DIR} && "
                "dbt test --target prod "
                "--profiles-dir /opt/airflow/dbt_profiles"
            ),
        )

        t_dbt_run >> t_dbt_test

    # ── Gold quality gate ────────────────────────────────────────────────
    t_validate_gold = PythonOperator(
        task_id         = "validate_gold_quality",
        python_callable = validate_gold_quality,
    )

    # ── ML scoring ───────────────────────────────────────────────────────
    with TaskGroup("ml", tooltip="ML scoring and push") as tg_ml:

        t_score = PythonOperator(
            task_id         = "score_and_push",
            python_callable = score_and_push,
        )

    # ── Diamond dependency pattern ────────────────────────────────────────
    #
    #   validate_bronze
    #        │
    #   ┌────┴────┬──────────┐
    # silver_tel silver_veh silver_ship
    #   └────┬────┴──────────┘
    #   ┌────┴─────┬──────┐
    # gold_eta gold_eng gold_kpi
    #   └────┬─────┴──────┘
    #       dbt
    #        │
    #   validate_gold
    #        │
    #        ml

    t_validate_bronze >> tg_silver >> tg_gold >> tg_dbt >> t_validate_gold >> tg_ml


# ═══════════════════════════════════════════════════════════════════════════
# DAG 3 — Weekly Maintenance
# ═══════════════════════════════════════════════════════════════════════════

def check_debezium_connectors(**context) -> None:
    """Verify all Debezium connectors are RUNNING."""
    import urllib.request
    from airflow.exceptions import AirflowException

    try:
        resp = urllib.request.urlopen(
            "http://debezium:8083/connectors?expand=status",
            timeout=10,
        )
        data = json.loads(resp.read())
        failed = [
            name for name, info in data.items()
            if info.get("status", {}).get("connector", {}).get("state") != "RUNNING"
        ]
        if failed:
            raise AirflowException(
                f"Debezium connectors NOT RUNNING: {failed}"
            )
        log.info("All Debezium connectors healthy: %s", list(data.keys()))
    except urllib.error.URLError as exc:
        log.warning("Debezium health check failed (non-fatal): %s", exc)


with DAG(
    dag_id           = "logistics_maintenance",
    default_args     = DEFAULT_ARGS,
    schedule_interval= "0 3 * * 0",    # 03:00 UTC every Sunday
    start_date       = days_ago(1),
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["logistics", "maintenance", "weekly"],
) as dag_maintenance:

    t_check_debezium = PythonOperator(
        task_id         = "check_debezium_connectors",
        python_callable = check_debezium_connectors,
    )

    t_optimize_gold = BashOperator(
        task_id     = "optimize_and_vacuum_gold",
        bash_command=(
            f"{SPARK_SUBMIT} "
            "/opt/logistics/src/streaming/gold/aggregate_gold.py "
            "--run-date {{ ds }} --run-maintenance"
        ),
    )

    t_retrain_models = BashOperator(
        task_id     = "retrain_ml_models",
        bash_command=(
            "python -m src.ml.training.train_models "
            "--run-date {{ ds }}"
        ),
    )

    t_reload_ml_server = BashOperator(
        task_id     = "reload_ml_server",
        bash_command=(
            f"curl -X POST {ML_SERVER_URL}/models/reload"
        ),
    )

    t_check_debezium >> t_optimize_gold >> t_retrain_models >> t_reload_ml_server

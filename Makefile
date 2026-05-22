# Makefile — Global Logistics Platform
# Shortcuts for all common operations.
# Usage: make <target>

.PHONY: up down build logs ps \
        register-debezium \
        produce-iot produce-weather \
        bronze silver gold train \
        dbt-run dbt-test \
        api-health api-test-eta api-test-failure \
        clean

# ── Infrastructure ─────────────────────────────────────────────────────────

up:
	docker compose -f deploy/docker-compose.yml up -d

down:
	docker compose -f deploy/docker-compose.yml down

down-volumes:
	docker compose -f deploy/docker-compose.yml down -v

build:
	docker compose -f deploy/docker-compose.yml up -d --build

logs:
	docker compose -f deploy/docker-compose.yml logs -f

ps:
	docker compose -f deploy/docker-compose.yml ps

register-debezium:
	bash deploy/debezium/register_connectors.sh

# ── Producers ──────────────────────────────────────────────────────────────

produce-iot:
	python -m src.producers.iot_producer --fleet-size 50 --rate 4

produce-weather:
	python -m src.producers.weather_producer

# ── Spark jobs ─────────────────────────────────────────────────────────────

SPARK_PKGS = io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4
SPARK_CONFS = --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
              --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

bronze:
	spark-submit --packages $(SPARK_PKGS) $(SPARK_CONFS) \
	  src/streaming/bronze/ingest_bronze.py

silver:
	spark-submit --packages $(SPARK_PKGS) $(SPARK_CONFS) \
	  src/streaming/silver/transform_silver.py

gold:
	python src/streaming/gold/aggregate_gold.py --run-date $(shell date +%F)

gold-maintenance:
	python src/streaming/gold/aggregate_gold.py --run-date $(shell date +%F) --run-maintenance

# ── dbt ────────────────────────────────────────────────────────────────────

dbt-run:
	cd src/dbt && dbt run --target dev --profiles-dir .

dbt-test:
	cd src/dbt && dbt test --target dev --profiles-dir .

dbt-freshness:
	cd src/dbt && dbt source freshness --target dev --profiles-dir .

# ── ML ─────────────────────────────────────────────────────────────────────

train:
	python -m src.ml.training.train_models --run-date $(shell date +%F)

api-health:
	curl -s http://localhost:8000/health | python3 -m json.tool

api-test-eta:
	curl -s -X POST http://localhost:8000/predict/eta \
	  -H "Content-Type: application/json" \
	  -d '{"shipment_id":"S001","vehicle_id":"TRK-0001","current_speed_kmh":85,"fuel_level_pct":70,"engine_health_score":90,"road_risk_score":2.5,"visibility_km":20,"precipitation_mm":0,"temp_c":25,"hist_avg_speed_kmh":80,"hist_speed_stddev":12,"hour_of_day":14,"day_of_week":3,"is_weekend":0,"is_already_late":0,"hours_to_scheduled_eta":4,"weight_kg":8500,"harsh_brake_flag":0,"harsh_accel_flag":0,"active_dtc_count":0}' \
	  | python3 -m json.tool

api-test-failure:
	curl -s -X POST http://localhost:8000/predict/failure \
	  -H "Content-Type: application/json" \
	  -d '{"vehicle_id":"TRK-0001","avg_coolant_temp":88,"max_coolant_temp":95,"std_coolant_temp":3,"avg_oil_pressure":340,"min_oil_pressure":290,"avg_rpm":2100,"max_rpm":3800,"avg_battery_v":13.4,"min_battery_v":12.8,"dtc_rate":0.0,"avg_engine_health":85,"total_readings":120}' \
	  | python3 -m json.tool

# ── Utilities ─────────────────────────────────────────────────────────────

install:
	pip install -r requirements.txt

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

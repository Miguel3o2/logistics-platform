#!/bin/bash
# deploy/debezium/register_connectors.sh
# =========================================================================
# Registers Debezium CDC connectors after Debezium Connect is healthy.
# Run once after docker compose up:
#   bash deploy/debezium/register_connectors.sh
# =========================================================================

DEBEZIUM_URL="http://localhost:8083"
KAFKA_BOOTSTRAP="kafka-1:9092,kafka-2:9093,kafka-3:9094"

echo "Waiting for Debezium Connect to be ready..."
until curl -sf "$DEBEZIUM_URL/connectors" > /dev/null; do
    echo "  Not ready yet — sleeping 5s"
    sleep 5
done
echo "Debezium Connect is ready."

# ── Vehicles + Shipments connector ────────────────────────────────────────
echo "Registering logistics-postgres connector..."
curl -s -X POST "$DEBEZIUM_URL/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "logistics-postgres-connector",
    "config": {
      "connector.class":           "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name":               "pgoutput",
      "database.hostname":         "postgres-operational",
      "database.port":             "5432",
      "database.user":             "logistics",
      "database.password":         "logistics123",
      "database.dbname":           "logistics_ops",
      "database.server.name":      "logistics",
      "slot.name":                 "debezium_slot",
      "publication.name":          "logistics_cdc",
      "table.include.list":        "logistics.vehicles,logistics.drivers,logistics.shipments,logistics.maintenance_records",
      "topic.prefix":              "logistics",
      "topic.naming.strategy":     "io.debezium.schema.SchemaTopicNamingStrategy",

      "transforms":                "route",
      "transforms.route.type":     "org.apache.kafka.connect.transforms.ReplaceField$Value",

      "key.converter":             "org.apache.kafka.connect.json.JsonConverter",
      "value.converter":           "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable":   "false",
      "value.converter.schemas.enable": "false",

      "decimal.handling.mode":     "double",
      "time.precision.mode":       "connect",
      "snapshot.mode":             "initial",

      "heartbeat.interval.ms":     "30000",
      "errors.tolerance":          "all",
      "errors.dead.letter.queue.topic.name": "logistics.dlq",
      "errors.dead.letter.queue.context.headers.enable": "true",

      "database.history.kafka.bootstrap.servers": "'"$KAFKA_BOOTSTRAP"'",
      "database.history.kafka.topic": "logistics.schema.history"
    }
  }' | python3 -m json.tool

echo ""
echo "Registered connectors:"
curl -s "$DEBEZIUM_URL/connectors" | python3 -m json.tool

echo ""
echo "Connector status:"
curl -s "$DEBEZIUM_URL/connectors/logistics-postgres-connector/status" | python3 -m json.tool

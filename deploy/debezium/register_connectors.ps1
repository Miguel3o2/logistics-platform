$ErrorActionPreference = "Stop"

$debeziumUrl = "http://localhost:8083"

Write-Host "Waiting for Debezium Connect to be ready..."
while ($true) {
    try {
        Invoke-RestMethod -Uri "$debeziumUrl/connectors" | Out-Null
        break
    } catch {
        Write-Host "  Not ready yet - sleeping 5s"
        Start-Sleep -Seconds 5
    }
}

$body = @{
    name = "logistics-postgres-connector"
    config = @{
        "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
        "plugin.name" = "pgoutput"
        "database.hostname" = "postgres-operational"
        "database.port" = "5432"
        "database.user" = "logistics"
        "database.password" = "logistics123"
        "database.dbname" = "logistics_ops"
        "database.server.name" = "logistics"
        "slot.name" = "debezium_slot"
        "publication.name" = "logistics_cdc"
        "table.include.list" = "logistics.vehicles,logistics.drivers,logistics.shipments,logistics.maintenance_records"
        "topic.prefix" = "logistics"
        "topic.naming.strategy" = "io.debezium.schema.SchemaTopicNamingStrategy"
        "transforms" = "route"
        "transforms.route.type" = 'org.apache.kafka.connect.transforms.ReplaceField$Value'
        "key.converter" = "org.apache.kafka.connect.json.JsonConverter"
        "value.converter" = "org.apache.kafka.connect.json.JsonConverter"
        "key.converter.schemas.enable" = "false"
        "value.converter.schemas.enable" = "false"
        "decimal.handling.mode" = "double"
        "time.precision.mode" = "connect"
        "snapshot.mode" = "initial"
        "heartbeat.interval.ms" = "30000"
        "errors.tolerance" = "all"
        "errors.dead.letter.queue.topic.name" = "logistics.dlq"
        "errors.dead.letter.queue.context.headers.enable" = "true"
    }
} | ConvertTo-Json -Depth 5

$existing = Invoke-RestMethod -Uri "$debeziumUrl/connectors"
if ($existing -contains "logistics-postgres-connector") {
    Write-Host "Connector already exists. Current status:"
} else {
    Write-Host "Registering logistics-postgres connector..."
    Invoke-RestMethod -Method Post -Uri "$debeziumUrl/connectors" -ContentType "application/json" -Body $body | ConvertTo-Json -Depth 5
}
Write-Host "Fetching connector status..."
$status = $null
$maxRetries = 10
$retryCount = 0

while ($null -eq $status -and $retryCount -lt $maxRetries) {
    try {
        $status = Invoke-RestMethod -Uri "$debeziumUrl/connectors/logistics-postgres-connector/status"
    } catch {
        $retryCount++
        if ($retryCount -lt $maxRetries) {
            Write-Host "  Connector status not available yet - retrying in 2s ($retryCount/$maxRetries)..."
            Start-Sleep -Seconds 2
        } else {
            throw $_
        }
    }
}

if ($null -ne $status) {
    $status | ConvertTo-Json -Depth 8
}


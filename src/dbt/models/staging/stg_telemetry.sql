-- =============================================================================
-- src/dbt/models/staging/sources.yml
-- =============================================================================
-- version: 2
--
-- sources:
--   - name: silver
--     schema: silver
--     freshness:
--       warn_after:  {count: 2, period: hour}
--       error_after: {count: 6, period: hour}
--     loaded_at_field: ingest_ts
--     tables:
--       - name: telemetry
--         description: Cleaned vehicle telematics — one row per sensor reading
--         freshness:
--           warn_after:  {count: 30, period: minute}
--           error_after: {count: 2,  period: hour}
--         columns:
--           - name: vehicle_id
--             tests: [not_null]
--           - name: sensor_ts
--             tests: [not_null]
--           - name: engine_health_score
--             tests:
--               - not_null
--               - dbt_utils.expression_is_true:
--                   expression: "between 0 and 100"
--       - name: dim_vehicles
--         description: SCD Type 2 vehicle dimension
--         columns:
--           - name: vehicle_id
--             tests: [not_null]
--           - name: is_current
--             tests: [not_null]
--       - name: shipments
--         description: Current-state shipments (upserted from CDC)
--         columns:
--           - name: shipment_id
--             tests: [not_null, unique]
--           - name: status
--             tests:
--               - accepted_values:
--                   values: [PENDING, IN_TRANSIT, DELIVERED, DELAYED, RETURNED]
--       - name: enriched_telemetry
--         description: Stream-to-stream join output — telemetry + weather
--         freshness:
--           warn_after:  {count: 30, period: minute}
--           error_after: {count: 2,  period: hour}
-- =============================================================================


-- =============================================================================
-- src/dbt/models/silver/stg_telemetry.sql
-- Staging model: apply final business rules before marts
-- =============================================================================

{{
  config(
    materialized     = 'incremental',
    unique_key       = ['vehicle_id', 'sensor_ts'],
    on_schema_change = 'sync_all_columns',
    tags             = ['silver', 'streaming'],
  )
}}

select
    vehicle_id,
    driver_id,
    fleet_id,
    sensor_ts,
    ingest_ts,
    status,
    speed_kmh,
    heading_deg,
    latitude,
    longitude,
    altitude_m,
    rpm,
    coolant_temp_c,
    oil_pressure_kpa,
    fuel_level_pct,
    battery_voltage_v,
    dtc_codes,
    harsh_brake,
    harsh_accel,
    geofence_exit,
    speed_category,
    engine_health_score,
    _validation_status,
    event_date,
    year,
    month,
    current_timestamp as dbt_updated_at

from {{ ref('telemetry') }}

{% if is_incremental() %}
-- Only process records newer than the last successful run
where ingest_ts > (select max(ingest_ts) from {{ this }})
{% endif %}

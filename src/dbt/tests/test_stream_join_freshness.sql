select
    max(sensor_ts) as latest_enriched_ts,
    current_timestamp as checked_at,
    extract(epoch from (current_timestamp - max(sensor_ts))) / 3600.0 as hours_stale,
    'enriched_telemetry_stale' as failure_reason
from {{ ref('enriched_telemetry') }}
having extract(epoch from (current_timestamp - max(sensor_ts))) / 3600.0 > 2

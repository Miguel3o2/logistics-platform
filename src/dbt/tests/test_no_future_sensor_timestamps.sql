select
    vehicle_id,
    sensor_ts,
    extract(epoch from (sensor_ts - current_timestamp)) / 60.0 as minutes_in_future
from {{ ref('stg_telemetry') }}
where sensor_ts > current_timestamp + interval '5 minutes'

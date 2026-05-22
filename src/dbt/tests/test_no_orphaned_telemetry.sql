select
    t.vehicle_id,
    count(*) as orphaned_readings
from {{ ref('stg_telemetry') }} t
left join {{ ref('dim_vehicles') }} v
    on v.vehicle_id = t.vehicle_id
    and v.is_current = true
where v.vehicle_id is null
group by 1
having count(*) > 0

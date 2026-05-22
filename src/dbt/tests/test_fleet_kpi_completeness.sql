select distinct v.fleet_id
from {{ ref('dim_vehicles') }} v
where v.is_current = true
  and v.fleet_id not in (
    select fleet_id
    from {{ ref('mart_fleet_performance') }}
    where snapshot_date = current_date
  )

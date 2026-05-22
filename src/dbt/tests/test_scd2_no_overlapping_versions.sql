select
    a.vehicle_id,
    a.effective_from as a_from,
    a.effective_to as a_to,
    b.effective_from as b_from,
    b.effective_to as b_to
from {{ ref('dim_vehicles') }} a
join {{ ref('dim_vehicles') }} b
    on a.vehicle_id = b.vehicle_id
    and a.effective_from < coalesce(b.effective_to, '9999-12-31'::timestamp)
    and b.effective_from < coalesce(a.effective_to, '9999-12-31'::timestamp)
    and a.effective_from != b.effective_from
where a.is_current = true or b.is_current = true

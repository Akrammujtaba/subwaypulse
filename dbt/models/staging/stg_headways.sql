select
    route_id,
    direction,
    stop_id,
    trip_key,
    prev_trip_key,
    arrival_ts,
    prev_arrival_ts,
    headway_s,
    scheduled_headway_s,
    status,
    detected_at,
    {{ to_nyc('arrival_ts') }} as arrival_local
from {{ source('silver', 'headways') }}
where headway_s > 0

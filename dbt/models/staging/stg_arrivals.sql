select
    trip_key,
    route_id,
    direction,
    stop_id,
    arrival_ts,
    detected_at,
    {{ to_nyc('arrival_ts') }} as arrival_local
from {{ source('silver', 'arrivals') }}

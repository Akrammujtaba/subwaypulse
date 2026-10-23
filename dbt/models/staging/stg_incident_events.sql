select
    incident_id,
    event_type,
    case when event_type = 'BUNCHING' then 'BUNCHING' else 'GAP' end as incident_type,
    route_id,
    direction,
    stop_id,
    prev_arrival_ts,
    prev_trip_key,
    arrival_ts,
    trip_key,
    event_ts,
    headway_s,
    scheduled_headway_s,
    threshold_s,
    detected_at
from {{ source('gold', 'incident_events') }}

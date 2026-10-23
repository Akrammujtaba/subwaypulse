select
    s.stop_id,
    s.station_name,
    s.parent_station,
    s.direction,
    s.lat,
    s.lon
from {{ ref('stg_stops') }} s

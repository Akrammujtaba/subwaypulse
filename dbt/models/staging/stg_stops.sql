-- Directional platform stops only (ids ending in N or S); parents are stations.
select
    s.stop_id,
    coalesce(p.stop_name, s.stop_name) as station_name,
    s.parent_station,
    substr(s.stop_id, length(s.stop_id), 1) as direction,
    s.lat,
    s.lon
from {{ source('ref', 'stops') }} s
left join {{ source('ref', 'stops') }} p on p.stop_id = s.parent_station
where s.parent_station is not null

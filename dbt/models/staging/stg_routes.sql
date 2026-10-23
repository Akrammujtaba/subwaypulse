select route_id, short_name, long_name, color
from {{ source('ref', 'routes') }}

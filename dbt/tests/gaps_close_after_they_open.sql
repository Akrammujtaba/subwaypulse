-- A gap can't end before the train it started from.
select incident_id
from {{ ref('fct_incidents') }}
where incident_type = 'GAP' and ended_at is not null and ended_at <= started_at

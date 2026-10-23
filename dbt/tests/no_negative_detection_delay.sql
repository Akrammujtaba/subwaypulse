-- Flagging an incident before it was knowable would mean the pipeline used future data.
-- A few seconds of slack covers clock differences between containers.
select incident_id, detection_delay_s
from {{ ref('fct_incidents') }}
where detection_delay_s < -5

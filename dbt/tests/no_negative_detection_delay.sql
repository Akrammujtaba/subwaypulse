-- Flagging an incident before it was knowable would mean the pipeline used future data.
-- A few seconds of slack covers clock differences between containers.
-- Simulator runs replay a different day, so their event times are days away from the
-- wall-clock detection time; those rows are left out by the lower bound.
select incident_id, detection_delay_s
from {{ ref('fct_incidents') }}
where detection_delay_s < -5
  and detection_delay_s > -86400

-- One row per incident. Gaps have an open and (usually) a close event; bunching has one.
with events as (
    select * from {{ ref('stg_incident_events') }}
),

opened as (
    select
        incident_id,
        min(incident_type) as incident_type,
        min(route_id) as route_id,
        min(direction) as direction,
        min(stop_id) as stop_id,
        min(prev_arrival_ts) as prev_arrival_ts,
        min(prev_trip_key) as prev_trip_key,
        min(scheduled_headway_s) as scheduled_headway_s,
        min(threshold_s) as threshold_s,
        -- when the problem became knowable, and when the pipeline flagged it
        min(event_ts) as knowable_at,
        min(detected_at) as detected_at
    from events
    where event_type in ('BUNCHING', 'GAP_OPENED')
    group by incident_id
),

ended as (
    select
        incident_id,
        max(arrival_ts) as ended_at,
        max(trip_key) as ending_trip_key,
        max(headway_s) as headway_s
    from events
    where event_type in ('BUNCHING', 'GAP_CLOSED')
    group by incident_id
)

select
    o.incident_id,
    o.incident_type,
    o.route_id,
    o.direction,
    o.stop_id,
    st.station_name,
    o.prev_arrival_ts as started_at,
    o.prev_trip_key,
    e.ended_at,
    e.ending_trip_key,
    e.headway_s,
    o.scheduled_headway_s,
    o.threshold_s,
    case
        when o.incident_type = 'BUNCHING' then 'CLOSED'
        when e.ended_at is null then 'OPEN'
        else 'CLOSED'
    end as status,
    o.knowable_at,
    o.detected_at,
    date_diff('second', o.knowable_at, o.detected_at) as detection_delay_s,
    {{ to_nyc('o.knowable_at') }} as knowable_at_local
from opened o
left join ended e on e.incident_id = o.incident_id
left join {{ ref('stg_stops') }} st on st.stop_id = o.stop_id

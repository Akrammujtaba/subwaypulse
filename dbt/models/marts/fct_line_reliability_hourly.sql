{{
    config(
        materialized='incremental',
        unique_key=['route_id', 'direction', 'hour_start'],
        incremental_strategy='merge' if target.type == 'trino' else 'delete+insert',
        on_schema_change='fail'
    )
}}

-- Service reliability per route, direction, and local hour.
--
-- Excess wait time: for a rider who shows up at a random moment, the expected wait
-- is sum(h^2) / (2 * sum(h)) over the headways h. Doing that for actual and for
-- scheduled headways and subtracting gives the extra wait irregular service causes.
-- Bunching hurts riders even when the average headway is fine, and this captures it.
--
-- Incremental: each run rebuilds only the last few hours, since late headways can
-- still land in a recent hour.

with headways as (
    select * from {{ ref('stg_headways') }}
    where scheduled_headway_s is not null
    {% if is_incremental() %}
      and arrival_local >= (select max(hour_start) from {{ this }}) - interval '3' hour
    {% endif %}
),

incidents as (
    select
        route_id,
        direction,
        date_trunc('hour', knowable_at_local) as hour_start,
        count_if(incident_type = 'BUNCHING') as bunching_incidents,
        count_if(incident_type = 'GAP') as gap_incidents
    from {{ ref('fct_incidents') }}
    group by 1, 2, 3
),

hourly as (
    select
        route_id,
        direction,
        date_trunc('hour', arrival_local) as hour_start,
        count(*) as headways,
        count(distinct stop_id) as stops_observed,
        {{ median('headway_s') }} as median_headway_s,
        {{ median('scheduled_headway_s') }} as median_scheduled_headway_s,
        sum(cast(headway_s as double) * headway_s) / (2 * sum(headway_s)) as expected_wait_s,
        sum(cast(scheduled_headway_s as double) * scheduled_headway_s)
            / (2 * sum(scheduled_headway_s)) as scheduled_expected_wait_s,
        avg(case when headway_s <= 1.25 * scheduled_headway_s then 1.0 else 0.0 end) as share_within_schedule
    from headways
    group by 1, 2, 3
)

select
    h.route_id,
    h.direction,
    h.hour_start,
    h.headways,
    h.stops_observed,
    h.median_headway_s,
    h.median_scheduled_headway_s,
    round(h.expected_wait_s, 1) as expected_wait_s,
    round(h.scheduled_expected_wait_s, 1) as scheduled_expected_wait_s,
    round(h.expected_wait_s - h.scheduled_expected_wait_s, 1) as excess_wait_s,
    round(h.share_within_schedule, 4) as share_within_schedule,
    coalesce(i.bunching_incidents, 0) as bunching_incidents,
    coalesce(i.gap_incidents, 0) as gap_incidents
from hourly h
left join incidents i
    on i.route_id = h.route_id and i.direction = h.direction and i.hour_start = h.hour_start

with flights as (

    select *
    from {{ ref('prep_flights') }}

),

airports as (

    select *
    from {{ ref('prep_airports') }}

),

-- -------------------------
-- Departures aggregation
-- -------------------------
departures as (

    select
        origin as airport_id,

        count(distinct dest) as unique_departure_connections,

        count(*) as total_departures_planned,

        sum(case when cancelled = 1 then 1 else 0 end) as total_departures_cancelled,

        sum(case when diverted = 1 then 1 else 0 end) as total_departures_diverted,

        sum(case 
                when cancelled = 0 and diverted = 0 then 1 
                else 0 
            end) as total_departures_occurred,

        count(distinct tail_number) as unique_airplanes_departures,

        count(distinct airline) as unique_airlines_departures

    from flights
    group by origin
),

-- -------------------------
-- Arrivals aggregation
-- -------------------------
arrivals as (

    select
        dest as airport_id,

        count(distinct origin) as unique_arrival_connections,

        count(*) as total_arrivals_planned,

        sum(case when cancelled = 1 then 1 else 0 end) as total_arrivals_cancelled,

        sum(case when diverted = 1 then 1 else 0 end) as total_arrivals_diverted,

        sum(case 
                when cancelled = 0 and diverted = 0 then 1 
                else 0 
            end) as total_arrivals_occurred,

        count(distinct tail_number) as unique_airplanes_arrivals,

        count(distinct airline) as unique_airlines_arrivals

    from flights
    group by dest
)

-- -------------------------
-- Final mart
-- -------------------------
select

    a.airport_id,
    a.airport_name,
    a.city,
    a.country,

    -- Connections
    coalesce(d.unique_departure_connections, 0) as unique_departure_connections,
    coalesce(ar.unique_arrival_connections, 0) as unique_arrival_connections,

    -- Planned
    coalesce(d.total_departures_planned, 0)
        + coalesce(ar.total_arrivals_planned, 0)
        as total_flights_planned,

    -- Cancelled
    coalesce(d.total_departures_cancelled, 0)
        + coalesce(ar.total_arrivals_cancelled, 0)
        as total_flights_cancelled,

    -- Diverted
    coalesce(d.total_departures_diverted, 0)
        + coalesce(ar.total_arrivals_diverted, 0)
        as total_flights_diverted,

    -- Occurred
    coalesce(d.total_departures_occurred, 0)
        + coalesce(ar.total_arrivals_occurred, 0)
        as total_flights_occurred
from airports a
left join departures d
    on a.faa = d.airport_id
left join arrivals ar
    on a.faa = ar.airport_id
with flights as (

    select *
    from {{ ref('prep_flights') }}

),

airports as (

    select *
    from {{ ref('prep_airports') }}

),

-- --------------------------------
-- Route Aggregation
-- --------------------------------
route_stats as (

    select
        origin,
        dest,

        count(*) as total_flights,

        count(distinct tail_number) as unique_airplanes,

        count(distinct airline) as unique_airlines,

        avg(actual_elapsed_time) as avg_actual_elapsed_time,

        avg(arr_delay) as avg_arrival_delay,

        max(arr_delay) as max_arrival_delay,

        min(arr_delay) as min_arrival_delay,

        sum(case when cancelled = 1 then 1 else 0 end) as total_cancelled,

        sum(case when diverted = 1 then 1 else 0 end) as total_diverted

    from flights
    group by origin, dest
)

-- --------------------------------
-- Final Mart
-- --------------------------------
select

    r.origin as origin_airport_code,
    r.dest as destination_airport_code,

    r.total_flights,
    r.unique_airplanes,
    r.unique_airlines,
    r.avg_actual_elapsed_time,
    r.avg_arrival_delay,
    r.max_arrival_delay,
    r.min_arrival_delay,
    r.total_cancelled,
    r.total_diverted,

    -- Origin airport details
    ao.airport_name as origin_airport_name,
    ao.city as origin_city,
    ao.country as origin_country,

    -- Destination airport details
    ad.airport_name as destination_airport_name,
    ad.city as destination_city,
    ad.country as destination_country

from route_stats r

left join airports ao
    on r.origin = ao.faa

left join airports ad
    on r.dest = ad.faa
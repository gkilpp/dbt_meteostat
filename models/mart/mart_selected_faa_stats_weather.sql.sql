with flights as (

    select *
    from {{ ref('prep_flights') }}

),

weather as (

    select *
    from {{ ref('prep_weather_daily') }}

),

airports as (

    select *
    from {{ ref('prep_airports') }}

),

-- ------------------------------------------------
-- Departures per airport per day
-- ------------------------------------------------
departures as (

    select
        origin as airport_code,
        flight_date,

        count(distinct dest) as unique_departure_connections,

        count(*) as total_departures_planned,

        sum(case when cancelled = 1 then 1 else 0 end) as total_departures_cancelled,

        sum(case when diverted = 1 then 1 else 0 end) as total_departures_diverted,

        sum(case when cancelled = 0 and diverted = 0 then 1 else 0 end)
            as total_departures_occurred,

        count(distinct tail_number) as unique_airplanes_departures,

        count(distinct airline) as unique_airlines_departures

    from flights
    group by origin, flight_date
),

-- ------------------------------------------------
-- Arrivals per airport per day
-- ------------------------------------------------
arrivals as (

    select
        dest as airport_code,
        flight_date,

        count(distinct origin) as unique_arrival_connections,

        count(*) as total_arrivals_planned,

        sum(case when cancelled = 1 then 1 else 0 end) as total_arrivals_cancelled,

        sum(case when diverted = 1 then 1 else 0 end) as total_arrivals_diverted,

        sum(case when cancelled = 0 and diverted = 0 then 1 else 0 end)
            as total_arrivals_occurred,

        count(distinct tail_number) as unique_airplanes_arrivals,

        count(distinct airline) as unique_airlines_arrivals

    from flights
    group by dest, flight_date
),

-- ------------------------------------------------
-- Combine departures + arrivals
-- ------------------------------------------------
daily_airport_stats as (

    select
        coalesce(d.airport_code, a.airport_code) as airport_code,
        coalesce(d.flight_date, a.flight_date) as flight_date,

        coalesce(d.unique_departure_connections, 0) as unique_departure_connections,
        coalesce(a.unique_arrival_connections, 0) as unique_arrival_connections,

        coalesce(d.total_departures_planned, 0)
            + coalesce(a.total_arrivals_planned, 0)
            as total_flights_planned,

        coalesce(d.total_departures_cancelled, 0)
            + coalesce(a.total_arrivals_cancelled, 0)
            as total_flights_cancelled,

        coalesce(d.total_departures_diverted, 0)
            + coalesce(a.total_arrivals_diverted, 0)
            as total_flights_diverted,

        coalesce(d.total_departures_occurred, 0)
            + coalesce(a.total_arrivals_occurred, 0)
            as total_flights_occurred,

        -- Optional averages
        (
            coalesce(d.unique_airplanes_departures, 0)
            + coalesce(a.unique_airplanes_arrivals, 0)
        ) / 2.0 as avg_unique_airplanes,

        (
            coalesce(d.unique_airlines_departures, 0)
            + coalesce(a.unique_airlines_arrivals, 0)
        ) / 2.0 as avg_unique_airlines

    from departures d
    full outer join arrivals a
        on d.airport_code = a.airport_code
        and d.flight_date = a.flight_date
)

-- ------------------------------------------------
-- Final mart: ONLY airports with weather
-- ------------------------------------------------
select

    w.airport_code,
    w.date as date,

    s.unique_departure_connections,
    s.unique_arrival_connections,
    s.total_flights_planned,
    s.total_flights_cancelled,
    s.total_flights_diverted,
    s.total_flights_occurred,
    s.avg_unique_airplanes,
    s.avg_unique_airlines,

    -- Airport info (optional)
    ap.name,
    ap.city,
    ap.country,

    -- Weather metrics
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh

from weather w

left join daily_airport_stats s
    on w.airport_code = s.airport_code
    and w.date = s.flight_date

left join airports ap
    on w.airport_code = ap.faa
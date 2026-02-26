with weather_daily as (

    select *
    from {{ ref('prep_weather_daily') }}

)

select

    airport_code,

    date_trunc('week', date) as week_start,

    -- Temperature
    min(min_temp_c) as weekly_min_temp,
    max(max_temp_c) as weekly_max_temp,

    -- Precipitation & Snow
    sum(precipitation_mm) as weekly_total_precipitation,
    sum(max_snow_mm) as weekly_total_snowfall,

    -- Wind
    avg(avg_wind_speed_kmh) as weekly_avg_wind_speed,

    -- If wind direction is numeric degrees
    avg(avg_wind_direction) as weekly_avg_wind_direction,

    -- If wind direction is categorical instead, use:
    -- mode() within group (order by avg_wind_direction) as weekly_mode_wind_direction,

    max(wind_peakgust_kmh) as weekly_peak_gust

from weather_daily
group by
    airport_code,
    date_trunc('week', date)
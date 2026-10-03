-- Dimension: yearly regional climate from Open-Meteo daily archive data.
--
-- DESIGN NOTE (deliberate, not an oversight): grain is one row per NZTA region
-- per year because the confidentialised crash open data carries no date field —
-- only crashYear + region — so a per-crash weather join is impossible. The fact
-- table joins on region + crash_year (= year), or on region_year_key.
-- One representative lat/lon is queried per NZTA region (16 regions), not per
-- road/intersection, which is the coarsest but most honest choice at this grain.
--
-- SOURCE NOTE: the extract fetches only daily=rain_sum,temperature_2m_mean, so
-- there is no wind column to average (avg_wind_speed_kmh omitted intentionally).

with daily as (

    select * from {{ ref('stg_nz_weather') }}

),

yearly as (

    select
        region,
        weather_year as year,
        region || '_' || weather_year::text as region_year_key,
        avg(temp_c)::numeric as avg_temp_c,
        sum(coalesce(rainfall_mm, 0))::numeric as total_rainfall_mm,
        count(*) filter (where rainfall_mm > 1.0)::int as wet_days,
        max(rainfall_mm)::numeric as max_daily_rainfall_mm,
        count(*)::int as days_observed,
        max(source_lat)::numeric as source_lat,
        max(source_lon)::numeric as source_lon,
        max(ingested_at) as fetched_at
    from daily
    group by region, weather_year

)

select * from yearly

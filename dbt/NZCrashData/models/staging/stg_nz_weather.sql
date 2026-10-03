-- Staging for Open-Meteo daily weather.
-- Grain: one row per representative city per day.
-- Maps the raw city label ("Region" column) to the exact NZTA region spelling
-- used by stg_nz_traffic_crash.region so the mart can join on region + year.
-- Coordinates come from load/Static/MainCities.csv (one lat/lon per NZTA region).

with source as (

    select * from {{ source('traffic', 'NZ_Weather') }}

),

cleaned as (

    select
        -- Map representative city -> NZTA region spelling (must match crash table exactly)
        case trim("Region")
            when 'Whangārei' then 'Northland Region'
            when 'Auckland' then 'Auckland Region'
            when 'Hamilton' then 'Waikato Region'
            when 'Tauranga' then 'Bay of Plenty Region'
            when 'Gisborne' then 'Gisborne Region'
            when 'Napier' then 'Hawke''s Bay Region'
            when 'New Plymouth' then 'Taranaki Region'
            when 'Palmerston North' then 'Manawatū-Whanganui Region'
            when 'Wellington' then 'Wellington Region'
            when 'Richmond' then 'Tasman Region'
            when 'Nelson' then 'Nelson Region'
            when 'Blenheim' then 'Marlborough Region'
            when 'Greymouth' then 'West Coast Region'
            when 'Christchurch' then 'Canterbury Region'
            when 'Dunedin' then 'Otago Region'
            when 'Invercargill' then 'Southland Region'
            else null
        end as region,

        trim("Region") as source_city,

        -- Representative coordinate queried for this region (traceability / debugging)
        case trim("Region")
            when 'Whangārei' then -35.73167
            when 'Auckland' then -36.84853
            when 'Hamilton' then -37.78333
            when 'Tauranga' then -37.68611
            when 'Gisborne' then -38.65333
            when 'Napier' then -39.4926
            when 'New Plymouth' then -39.06667
            when 'Palmerston North' then -40.35636
            when 'Wellington' then -41.28664
            when 'Richmond' then -41.33333
            when 'Nelson' then -41.27078
            when 'Blenheim' then -41.51603
            when 'Greymouth' then -42.46667
            when 'Christchurch' then -43.53333
            when 'Dunedin' then -45.87416
            when 'Invercargill' then -46.4
            else null
        end::numeric as source_lat,

        case trim("Region")
            when 'Whangārei' then 174.32391
            when 'Auckland' then 174.76349
            when 'Hamilton' then 175.28333
            when 'Tauranga' then 176.16667
            when 'Gisborne' then 178.00417
            when 'Napier' then 176.91232
            when 'New Plymouth' then 174.08333
            when 'Palmerston North' then 175.61113
            when 'Wellington' then 174.77557
            when 'Richmond' then 173.18333
            when 'Nelson' then 173.28404
            when 'Blenheim' then 173.9528
            when 'Greymouth' then 171.2
            when 'Christchurch' then 172.63333
            when 'Dunedin' then 170.50362
            when 'Invercargill' then 168.35
            else null
        end::numeric as source_lon,

        -- "Date" is stored as text (YYYY-MM-DD) in the raw table
        case
            when "Date" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                then to_date("Date", 'YYYY-MM-DD')
            else null
        end as weather_date,

        rain_sum::numeric as rainfall_mm,
        temperature_2m_mean::numeric as temp_c,

        ingested_at
    from source

)

select
    region,
    source_city,
    source_lat,
    source_lon,
    weather_date,
    extract(year from weather_date)::int as weather_year,
    rainfall_mm,
    temp_c,
    ingested_at
from cleaned
where region is not null
    and weather_date is not null

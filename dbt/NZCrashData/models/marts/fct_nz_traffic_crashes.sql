with staging as (
    select * from {{ ref('stg_nz_traffic_crash') }}
)

select
    crash_id,
    longitude,
    latitude,
    crash_location_1,
    crash_location_2,
    tla_name,
    region,
    road_character,
    road_surface,
    light_condition,
    weather_a,
    weather_b,
    speed_limit,
    crash_severity,
    crash_year,
    crash_financial_year,
    holiday,
    fatal_count,
    serious_injury_count,
    minor_injury_count,

    -- Derived Metrics
    (serious_injury_count + minor_injury_count) as total_injured_count,
    (serious_injury_count + minor_injury_count + fatal_count) as total_casualties,

    -- Boolean Flags
    case when fatal_count > 0 then true else false end as has_fatalities,
    case when (serious_injury_count + minor_injury_count) > 0 then true else false end as has_injuries,
    case when vehicle_count > 1 then true else false end as is_multivehicle_crash,

    -- Vulnerable Road User Flags
    case when bicycle_count > 0 then true else false end as has_bicycle,
    case when (motorcycle_count > 0 or moped_count > 0) then true else false end as has_motorcycle,
    case when pedestrian_hazard > 0 then true else false end as has_pedestrian,

    -- Speed Zone Classification
    case 
        when speed_limit is null then 'Unknown'
        when speed_limit <= 50 then 'Urban (<=50 km/h)'
        when speed_limit <= 80 then 'Suburban/Rural (60-80 km/h)'
        else 'Highway/Open Road (>=90 km/h)'
    end as speed_zone,

    ingested_at

from staging

with cleanedData as 
(select * from {{ref('stg_nz_traffic_crash')}}
),
fact as (
    select 
    crash_id,
    crash_year,
    crash_sh_description,
    region,
    crash_location1,
    crash_location2,
    number_of_lanes,
    CASE 
        WHEN speed_limit = 0 THEN 'Unknown'
        WHEN speed_limit <= 50 THEN 'Urban'
        WHEN speed_limit <= 80 THEN 'Rural'
        ELSE 'Highway' END AS road_type,
    light_description,
    weather_a,
    weather_b,
    CASE WHEN holiday_name IS NULL  THEN 'No Holiday' 
    ELSE holiday_name END AS holiday_name,
    pedestrian_count,
    parked_vehicle_count,
    object_thrown_or_dropped_count,
    kerb_count,
    guard_rail_count,
    house_or_building_count,
    flathill_count,
    fence_count,
    ditch_count,
    bridge_count,
    car_station_wagon_count,
    bus_count,
    minor_injury_count,
    fatal_count,
    serious_injury_count,
    (serious_injury_count + fatal_count + minor_injury_count) as total_casualties ,
    fatal_count > 0  as has_fatality,
    (bicycle_count +  moped_count +  motorcycle_count) > 0 as involves_vulnerable_road_user,
    (serious_injury_count + fatal_count ) > 0 as is_serious_or_fatal,
    {{current_timestamp()}} as ingested_at
    from cleanedData

)

SELECT * FROM fact

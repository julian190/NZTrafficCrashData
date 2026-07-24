WITH source AS (

    SELECT * FROM {{ source('traffic', 'NZ_Traffic_Crash') }}

),

cleaned AS (

    SELECT 
        -- Identifiers 
        CASE WHEN "OBJECTID"::text ~ '^[0-9]+$' THEN "OBJECTID"::int ELSE NULL END AS crash_id,

        -- Area 
        CASE WHEN "X"::text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN "X"::decimal ELSE NULL END AS longitude,
        CASE WHEN "Y"::text ~ '^-?[0-9]+(\.[0-9]+)?$' THEN "Y"::decimal ELSE NULL END AS latitude,
        "crashLocation1"                                                            AS crash_location1,
        "crashLocation2"                                                            AS crash_location2,
        
        -- Conditions 
        CASE WHEN "advisorySpeed"::text ~ '^[0-9]+$' THEN "advisorySpeed"::int ELSE 0 END AS advisory_speed,
        "region"                                                                         AS region,

        -- Crash details 
        "crashFinancialYear"                                                             AS financial_year,
        "crashSeverity"                                                                  AS crash_severity,
        "crashSHDescription"                                                             AS crash_sh_description,
        CASE WHEN "crashYear"::text ~ '^[0-9]+$' THEN "crashYear"::int ELSE NULL END     AS crash_year,
        
        CASE WHEN "minorInjuryCount"::text ~ '^[0-9]+$' THEN "minorInjuryCount"::int ELSE 0 END     AS minor_injury_count,
        CASE WHEN "fatalCount"::text ~ '^[0-9]+$' THEN "fatalCount"::int ELSE 0 END                 AS fatal_count,
        CASE WHEN "seriousInjuryCount"::text ~ '^[0-9]+$' THEN "seriousInjuryCount"::int ELSE 0 END AS serious_injury_count,

        -- Vehicle 
        CASE WHEN "bicycle"::text ~ '^[0-9]+$' THEN "bicycle"::int ELSE 0 END                 AS bicycle_count,
        CASE WHEN "bus"::text ~ '^[0-9]+$' THEN "bus"::int ELSE 0 END                         AS bus_count,
        CASE WHEN "carStationWagon"::text ~ '^[0-9]+$' THEN "carStationWagon"::int ELSE 0 END AS car_station_wagon_count,
        CASE WHEN "moped"::text ~ '^[0-9]+$' THEN "moped"::int ELSE 0 END                     AS moped_count,
        CASE WHEN "motorcycle"::text ~ '^[0-9]+$' THEN "motorcycle"::int ELSE 0 END           AS motorcycle_count,

        -- Environment 
        CASE WHEN "bridge"::text ~ '^[0-9]+$' THEN "bridge"::int ELSE 0 END                           AS bridge_count,
        CASE WHEN "ditch"::text ~ '^[0-9]+$' THEN "ditch"::int ELSE 0 END                             AS ditch_count,
        CASE WHEN "fence"::text ~ '^[0-9]+$' THEN "fence"::int ELSE 0 END                             AS fence_count,
        CASE WHEN "flatHill"::text ~ '^[0-9]+$' THEN "flatHill"::int ELSE 0 END                       AS flathill_count,
        CASE WHEN "houseOrBuilding"::text ~ '^[0-9]+$' THEN "houseOrBuilding"::int ELSE 0 END         AS house_or_building_count,
        CASE WHEN "guardRail"::text ~ '^[0-9]+$' THEN "guardRail"::int ELSE 0 END                     AS guard_rail_count,
        CASE WHEN "kerb"::text ~ '^[0-9]+$' THEN "kerb"::int ELSE 0 END                               AS kerb_count,
        CASE WHEN "NumberOfLanes"::text ~ '^[0-9]+$' THEN "NumberOfLanes"::int ELSE 0 END             AS number_of_lanes,
        CASE WHEN "objectThrownOrDropped"::text ~ '^[0-9]+$' THEN "objectThrownOrDropped"::int ELSE 0 END AS object_thrown_or_dropped_count,
        CASE WHEN "parkedVehicle"::text ~ '^[0-9]+$' THEN "parkedVehicle"::int ELSE 0 END             AS parked_vehicle_count,
        CASE WHEN "pedestrian"::text ~ '^[0-9]+$' THEN "pedestrian"::int ELSE 0 END                   AS pedestrian_count,
        CASE WHEN "roadworks"::text ~ '^[0-9]+$' THEN "roadworks"::int ELSE 0 END                     AS roadworks_count,
        
        "roadSurface"                                                                 AS road_surface,
        
        -- Speed limit clean handling
        CASE WHEN "speedLimit"::text ~ '^[0-9]+$' THEN "speedLimit"::int ELSE 0 END    AS speed_limit,
        
        "holiday"                                                                     AS holiday_name,
        "light"                                                                       AS light_description,
        "weatherA"                                                                    AS weather_a,
        "weatherB"                                                                    AS weather_b,
        {{ current_timestamp() }}                                                     AS ingested_at
    FROM source
)

SELECT * FROM cleaned
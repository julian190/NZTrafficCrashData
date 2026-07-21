with source as (
    select * from {{ source('traffic', 'NZ_Traffic_Crash') }}
),

cleaned as (
    select
        -- Identifiers
        cast("OBJECTID" as bigint) as crash_id,
        cast("tlaId" as integer) as tla_id,
        cast("meshblockId" as bigint) as meshblock_id,
        cast("areaUnitID" as bigint) as area_unit_id,

        -- Location Coordinates
        cast("X" as double precision) as longitude,
        cast("Y" as double precision) as latitude,

        -- Location Names
        "crashLocation1" as crash_location_1,
        "crashLocation2" as crash_location_2,
        "tlaName" as tla_name,
        "region" as region,

        -- Road & Traffic Conditions
        "roadCharacter" as road_character,
        "roadLane" as road_lane,
        "roadSurface" as road_surface,
        "roadworks" as has_roadworks,
        "flatHill" as flat_hill,
        "streetLight" as street_light,
        "light" as light_condition,
        "trafficControl" as traffic_control,
        "urban" as urban_rural,

        -- Weather Conditions
        "weatherA" as weather_a,
        "weatherB" as weather_b,

        -- Speed Limits
        cast("speedLimit" as integer) as speed_limit,
        cast("advisorySpeed" as integer) as advisory_speed,
        cast("temporarySpeedLimit" as integer) as temporary_speed_limit,

        -- Crash Info & Severity
        "crashSeverity" as crash_severity,
        cast("crashYear" as integer) as crash_year,
        "crashFinancialYear" as crash_financial_year,
        "holiday" as holiday,
        "crashDirectionDescription" as crash_direction_description,
        "directionRoleDescription" as direction_role_description,
        "crashSHDescription" as crash_sh_description,
        cast("crashRoadSideRoad" as double precision) as crash_road_side_road,

        -- Casualty Counts
        cast("fatalCount" as integer) as fatal_count,
        cast("seriousInjuryCount" as integer) as serious_injury_count,
        cast("minorInjuryCount" as integer) as minor_injury_count,

        -- Vehicle Involvement (Counts/Presence)
        cast("bicycle" as integer) as bicycle_count,
        cast("bus" as integer) as bus_count,
        cast("carStationWagon" as integer) as car_station_wagon_count,
        cast("moped" as integer) as moped_count,
        cast("motorcycle" as integer) as motorcycle_count,
        cast("schoolBus" as integer) as school_bus_count,
        cast("suv" as integer) as suv_count,
        cast("taxi" as integer) as taxi_count,
        cast("truck" as integer) as truck_count,
        cast("vanOrUtility" as integer) as van_or_utility_count,
        cast("otherVehicleType" as integer) as other_vehicle_type_count,
        cast("unknownVehicleType" as integer) as unknown_vehicle_type_count,
        cast("vehicle" as double precision) as vehicle_count,

        -- Hazard / Object Collision flags (0 or 1 usually)
        cast("bridge" as double precision) as bridge_hazard,
        cast("cliffBank" as double precision) as cliff_bank_hazard,
        cast("debris" as double precision) as debris_hazard,
        cast("ditch" as double precision) as ditch_hazard,
        cast("fence" as double precision) as fence_hazard,
        cast("guardRail" as double precision) as guard_rail_hazard,
        cast("houseOrBuilding" as double precision) as house_or_building_hazard,
        cast("intersection" as double precision) as intersection_hazard,
        cast("kerb" as double precision) as kerb_hazard,
        cast("objectThrownOrDropped" as double precision) as object_thrown_or_dropped_hazard,
        cast("otherObject" as double precision) as other_object_hazard,
        cast("overBank" as double precision) as over_bank_hazard,
        cast("parkedVehicle" as double precision) as parked_vehicle_hazard,
        cast("pedestrian" as double precision) as pedestrian_hazard,
        cast("phoneBoxEtc" as double precision) as phone_box_etc_hazard,
        cast("postOrPole" as double precision) as post_or_pole_hazard,
        cast("slipOrFlood" as double precision) as slip_or_flood_hazard,
        cast("strayAnimal" as double precision) as stray_animal_hazard,
        cast("trafficIsland" as double precision) as traffic_island_hazard,
        cast("trafficSign" as double precision) as traffic_sign_hazard,
        cast("train" as double precision) as train_hazard,
        cast("tree" as double precision) as tree_hazard,
        cast("waterRiver" as double precision) as water_river_hazard,

        -- Infrastructure
        cast("NumberOfLanes" as integer) as number_of_lanes,

        -- Metadata
        ingested_at

    from source
    where "OBJECTID" is not null
)

select * from cleaned

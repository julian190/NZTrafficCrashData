-- Test that no casualty or injury counts are negative.
-- Any rows returned by this query represent data quality issues and will cause the test to fail.
select
    crash_id,
    fatal_count,
    serious_injury_count,
    minor_injury_count
from {{ ref('stg_nz_traffic_crash') }}
where fatal_count < 0 
   or serious_injury_count < 0 
   or minor_injury_count < 0

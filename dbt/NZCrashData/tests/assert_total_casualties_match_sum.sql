-- Test that total_casualties is correctly computed as the sum of fatal, serious injury, and minor injury counts.
-- Any rows returned by this query represent a discrepancy and will cause the test to fail.
select
    crash_id,
    fatal_count,
    serious_injury_count,
    minor_injury_count,
    total_casualties
from {{ ref('fct_nz_traffic_crashes') }}
where total_casualties != (fatal_count + serious_injury_count + minor_injury_count)

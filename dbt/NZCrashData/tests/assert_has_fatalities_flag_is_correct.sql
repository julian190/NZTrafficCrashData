-- Test that has_fatalities boolean flag matches the fatal_count logic.
-- Any rows returned by this query represent a mismatch and will cause the test to fail.
select
    crash_id,
    fatal_count,
    has_fatalities
from {{ ref('fct_nz_traffic_crashes') }}
where (fatal_count > 0 and has_fatalities = false)
   or (fatal_count = 0 and has_fatalities = true)

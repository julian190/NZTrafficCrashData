select 
crash_id,
is_serious_or_fatal,
has_fatality,
serious_injury_count
from {{ref('fct_nz_traffic_crash')}}
where is_serious_or_fatal = true and  (serious_injury_count + fatal_count)  = 0
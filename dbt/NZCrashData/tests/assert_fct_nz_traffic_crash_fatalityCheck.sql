select 
crash_id,
fatal_count,
has_fatality
from {{ref('fct_nz_traffic_crash')}}
where fatal_count > 0 and has_fatality = false
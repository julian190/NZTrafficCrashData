SELECT 
crash_id,
total_casualties
from {{ref('fct_nz_traffic_crash')}}
where total_casualties < 0
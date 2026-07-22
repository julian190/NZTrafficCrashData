select 
crash_id,
crash_year
from {{ref('fct_nz_traffic_crash')}}
where crash_year > extract(year from current_date)
select 
crash_id,
road_type
from {{ref('fct_nz_traffic_crash')}}
where road_type not in ('Urban', 'Rural', 'Highway', 'Unknown')

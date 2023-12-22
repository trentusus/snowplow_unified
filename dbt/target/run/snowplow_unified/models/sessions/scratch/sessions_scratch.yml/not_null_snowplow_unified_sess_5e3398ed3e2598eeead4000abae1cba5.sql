select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select device_session_index
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_sessions_this_run`
where device_session_index is null



      
    ) dbt_internal_test
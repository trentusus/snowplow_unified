select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select absolute_time_in_s
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_sessions_this_run`
where absolute_time_in_s is null



      
    ) dbt_internal_test
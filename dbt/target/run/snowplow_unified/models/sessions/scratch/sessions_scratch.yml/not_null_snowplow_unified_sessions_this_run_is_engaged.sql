select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select is_engaged
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_sessions_this_run`
where is_engaged is null



      
    ) dbt_internal_test
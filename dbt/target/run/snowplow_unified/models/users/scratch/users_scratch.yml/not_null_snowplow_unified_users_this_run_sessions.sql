select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select sessions
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_users_this_run`
where sessions is null



      
    ) dbt_internal_test
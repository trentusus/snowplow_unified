select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select engaged_time_in_s
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_users`
where engaged_time_in_s is null



      
    ) dbt_internal_test
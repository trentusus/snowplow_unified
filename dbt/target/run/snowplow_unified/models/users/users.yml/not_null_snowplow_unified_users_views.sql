select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select views
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_users`
where views is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_identifier
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_users_aggs`
where user_identifier is null



      
    ) dbt_internal_test
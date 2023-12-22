select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select views_in_session
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views_this_run`
where views_in_session is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select derived_tstamp
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views_this_run`
where derived_tstamp is null



      
    ) dbt_internal_test
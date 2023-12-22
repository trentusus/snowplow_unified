select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select collector_tstamp
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views`
where collector_tstamp is null



      
    ) dbt_internal_test
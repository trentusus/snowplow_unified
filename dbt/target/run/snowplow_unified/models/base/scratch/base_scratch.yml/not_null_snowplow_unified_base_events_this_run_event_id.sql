select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select event_id
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_events_this_run`
where event_id is null



      
    ) dbt_internal_test
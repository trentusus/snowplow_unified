select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select view_in_session_index
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views`
where view_in_session_index is null



      
    ) dbt_internal_test
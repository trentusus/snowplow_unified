select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select view_id
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views`
where view_id is null



      
    ) dbt_internal_test
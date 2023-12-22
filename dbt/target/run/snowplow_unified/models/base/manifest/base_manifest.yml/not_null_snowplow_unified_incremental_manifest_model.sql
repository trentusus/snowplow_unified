select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select model
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_incremental_manifest`
where model is null



      
    ) dbt_internal_test
select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select session_identifier
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_lifecycle_manifest`
where session_identifier is null



      
    ) dbt_internal_test
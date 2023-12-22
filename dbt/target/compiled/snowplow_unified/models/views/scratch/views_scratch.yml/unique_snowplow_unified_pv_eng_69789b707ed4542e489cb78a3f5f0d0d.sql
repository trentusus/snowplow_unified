
    
    

with dbt_test__target as (

  select (view_id || '-' || session_identifier) as unique_field
  from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_pv_engaged_time`
  where (view_id || '-' || session_identifier) is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



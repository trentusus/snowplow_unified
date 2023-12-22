
    
    

with dbt_test__target as (

  select event_id as unique_field
  from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run`
  where event_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



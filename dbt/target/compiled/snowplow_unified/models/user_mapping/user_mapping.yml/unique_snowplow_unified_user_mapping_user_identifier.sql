
    
    

with dbt_test__target as (

  select user_identifier as unique_field
  from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_user_mapping`
  where user_identifier is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



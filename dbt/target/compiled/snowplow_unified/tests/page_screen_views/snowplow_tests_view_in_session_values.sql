


with prep as (
  select
    session_identifier,
    count(distinct views_in_session) as dist_pvis_values,
    count(*) - count(distinct view_in_session_index)  as all_minus_dist_pvisi,
    count(*) - count(distinct view_id) as all_minus_dist_pvids

  from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_views`
  group by 1
)

select
  session_identifier

from prep

where dist_pvis_values != 1
or all_minus_dist_pvisi != 0
or all_minus_dist_pvids != 0
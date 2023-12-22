
  
    



    create or replace table `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_user_mapping`
      
    partition by timestamp_trunc(end_tstamp, day)
    

    OPTIONS()
    as (
      




select distinct
  user_identifier,
  last_value(user_id) over(
    partition by user_identifier
    order by collector_tstamp
    rows between unbounded preceding and unbounded following
  ) as user_id,
  max(collector_tstamp) over (partition by user_identifier) as end_tstamp

from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run`

where True --returns false if run doesn't contain new events.
and user_id is not null
and user_identifier is not null
    );
  
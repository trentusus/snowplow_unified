



-- the first page ping fires after the minimum visit length (n seconds), every subsequent page ping fires after every heartbeat length (n seconds)
-- there may be imperfectly timed pings and odd duplicates therefore the safest is to do a special calculation:
-- each pings' epoch timestamp is taken (n seconds) which are then divided by the heartbeat length, floored (to get precise heartbeat length separated intervals)
-- each distinct value means the user spent that * the heartbeat length on the website (minus the first, which needed the minimum visit lenght to fire)





select
  ev.view_id,
  ev.session_identifier,
  max(ev.derived_tstamp) as end_tstamp,
  (30 * (count(distinct(floor(unix_seconds(ev.dvce_created_tstamp)/30))) - 1)) + 30 as engaged_time_in_s

from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run` as ev

where ev.event_name = 'page_ping'
and ev.view_id is not null

group by 1, 2
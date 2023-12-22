

  

    

    merge into `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_incremental_manifest` m
    using ( 
      select
        b.model,
        a.last_success

      from
        (select max(collector_tstamp) as last_success from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_events_this_run`) a,
        ( select 'snowplow_unified_pv_engaged_time' as model union all   select 'snowplow_unified_pv_scroll_depth' as model union all   select 'snowplow_unified_user_mapping' as model ) b

      where a.last_success is not null -- if run contains no data don't add to manifest
     ) s
    on m.model = s.model
    when matched then
        update set last_success = greatest(m.last_success, s.last_success)
    when not matched then
        insert (model, last_success) values(model, last_success);

    

  


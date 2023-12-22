
  
    



    create or replace table `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_this_run`
      
    
    

    OPTIONS()
    as (
      









        select
        s.session_identifier,
        s.user_identifier,
        s.start_tstamp,
        -- end_tstamp used in next step to limit events. When backfilling, set end_tstamp to upper_limit if end_tstamp > upper_limit.
        -- This ensures we don't accidentally process events after upper_limit
        case when s.end_tstamp > 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
     then 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
     else s.end_tstamp end as end_tstamp

        from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_lifecycle_manifest` s

        where
        -- General window of start_tstamps to limit table scans. Logic complicated by backfills.
        -- To be within the run, session start_tstamp must be >= lower_limit - max_session_days as we limit end_tstamp in manifest to start_tstamp + max_session_days
        s.start_tstamp >= 
        cast('2023-12-19 00:00:00+00:00' as timestamp)
    
        and s.start_tstamp <= 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
    
        -- Select sessions within window that either; start or finish between lower & upper limit, start and finish outside of lower and upper limits
        and not (s.start_tstamp > 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
     or s.end_tstamp < 
        cast('2023-12-22 00:00:00+00:00' as timestamp)
    )
    
    );
  
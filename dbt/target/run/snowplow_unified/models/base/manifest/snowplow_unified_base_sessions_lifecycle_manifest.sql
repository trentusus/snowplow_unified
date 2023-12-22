-- back compat for old kwarg name
  
  
        
            
            
        
    

    



    merge into `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_lifecycle_manifest` as DBT_INTERNAL_DEST
        using (







        with new_events_session_ids_init as (
            select
            
                COALESCE(
                    


    coalesce(e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].session_id, e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].session_id)


,e.domain_sessionid,NULL
                ) as session_identifier,
                max(
                    COALESCE(
                        


    coalesce(e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].user_id, e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].user_id)


,e.domain_userid,NULL
                    )
                ) as user_identifier, -- Edge case 1: Arbitary selection to avoid window function like first_value.
            
                min(collector_tstamp) as start_tstamp,
                max(collector_tstamp) as end_tstamp

            from `com-snplow-sales-gcp`.`rt_pipeline_prod1`.`events` e

            where
                dvce_sent_tstamp <= 
    timestamp_add(dvce_created_tstamp, interval 3 day)
 -- don't process data that's too late
                and collector_tstamp >= 
        cast('2023-12-22 00:00:00+00:00' as timestamp)
    
                and collector_tstamp <= 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
    
                and app_id in ('website','console-qa') --filter on app_id if provided
                and cast(True as boolean) --don't reprocess sessions that have already been processed.
                

            group by 1
        ), new_events_session_ids as (
            select *
            from new_events_session_ids_init e
            
                where session_identifier is not null
                and not exists (select 1 from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_quarantined_sessions` as a where a.session_identifier = e.session_identifier) -- don't continue processing v.long sessions

        )
        

        , previous_sessions as (
        select *

        from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_lifecycle_manifest`

        where start_tstamp >= 
        cast('2021-12-22 00:00:00+00:00' as timestamp)
    
        and cast(True as boolean) --don't reprocess sessions that have already been processed.
        )

        , session_lifecycle as (
        select
            ns.session_identifier,
            coalesce(self.user_identifier, ns.user_identifier) as user_identifier, -- Edge case 1: Take previous value to keep domain_userid consistent. Not deterministic but performant
            least(ns.start_tstamp, coalesce(self.start_tstamp, ns.start_tstamp)) as start_tstamp,
            greatest(ns.end_tstamp, coalesce(self.end_tstamp, ns.end_tstamp)) as end_tstamp -- BQ 1 NULL will return null hence coalesce

        from new_events_session_ids ns
        left join previous_sessions as self
            on ns.session_identifier = self.session_identifier

        where
            self.session_identifier is null -- process all new sessions
            or self.end_tstamp < 
    timestamp_add(self.start_tstamp, interval 3 day)
 --stop updating sessions exceeding 3 days
        )

        

        select
        sl.session_identifier,
        sl.user_identifier,
        sl.start_tstamp,
        least(
    timestamp_add(sl.start_tstamp, interval 3 day)
, sl.end_tstamp) as end_tstamp -- limit session length to max_session_days
        

        from session_lifecycle sl
    
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.session_identifier = DBT_INTERNAL_DEST.session_identifier
            )

    
    when matched then update set
        `session_identifier` = DBT_INTERNAL_SOURCE.`session_identifier`,`user_identifier` = DBT_INTERNAL_SOURCE.`user_identifier`,`start_tstamp` = DBT_INTERNAL_SOURCE.`start_tstamp`,`end_tstamp` = DBT_INTERNAL_SOURCE.`end_tstamp`
    

    when not matched then insert
        (`session_identifier`, `user_identifier`, `start_tstamp`, `end_tstamp`)
    values
        (`session_identifier`, `user_identifier`, `start_tstamp`, `end_tstamp`)


    









  

  
  
  

  
  

  



with base_query as (
  
        with identified_events AS (
            select
                COALESCE(
                        


    coalesce(e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].session_id, e.contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].session_id)


,e.domain_sessionid,NULL
                    ) as session_identifier,
                e.*
                

            from `com-snplow-sales-gcp`.`rt_pipeline_prod1`.`events` e

        )

        select
            a.*,
            b.user_identifier -- take user_identifier from manifest. This ensures only 1 domain_userid per session.

        from identified_events as a
        inner join `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_sessions_this_run` as b
        on a.session_identifier = b.session_identifier

        where a.collector_tstamp <= 
    timestamp_add(b.start_tstamp, interval 3 day)

        and a.dvce_sent_tstamp <= 
    timestamp_add(a.dvce_created_tstamp, interval 3 day)

        and a.collector_tstamp >= 
        cast('2023-12-22 00:01:03.250000+00:00' as timestamp)
    
        and a.collector_tstamp <= 
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
    
        and a.collector_tstamp >= b.start_tstamp -- deal with late loading events

        

        and app_id in ('website','console-qa') --filter on app_id if provided

        qualify row_number() over (partition by a.event_id order by a.collector_tstamp, a.dvce_created_tstamp) = 1
    
)

select
  *
  -- extract commonly used contexts / sdes (prefixed)
  

  

  
      ,coalesce(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[safe_offset(0)].id) as page_view__id
  

  

  ,coalesce(contexts_com_iab_snowplow_spiders_and_robots_1_0_0[safe_offset(0)].category) as iab__category,
coalesce(contexts_com_iab_snowplow_spiders_and_robots_1_0_0[safe_offset(0)].primary_impact) as iab__primary_impact,
coalesce(contexts_com_iab_snowplow_spiders_and_robots_1_0_0[safe_offset(0)].reason) as iab__reason,
coalesce(contexts_com_iab_snowplow_spiders_and_robots_1_0_0[safe_offset(0)].spider_or_robot) as iab__spider_or_robot
  

  ,coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].device_family) as ua__device_family,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_family) as ua__os_family,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].useragent_family) as ua__useragent_family,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_major) as ua__os_major,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_minor) as ua__os_minor,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_patch) as ua__os_patch,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_patch_minor) as ua__os_patch_minor,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].os_version) as ua__os_version,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].useragent_major) as ua__useragent_major,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].useragent_minor) as ua__useragent_minor,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].useragent_patch) as ua__useragent_patch,
coalesce(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0[safe_offset(0)].useragent_version) as ua__useragent_version
  

  ,coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].device_class, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].device_class, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].device_class, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].device_class) as yauaa__device_class,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_class, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_class, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_class, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_class) as yauaa__agent_class,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_name, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_name, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_name, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_name) as yauaa__agent_name,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_name_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_name_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_name_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_name_version) as yauaa__agent_name_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_name_version_major, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_name_version_major, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_name_version_major, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_name_version_major) as yauaa__agent_name_version_major,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_version) as yauaa__agent_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].agent_version_major, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].agent_version_major, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].agent_version_major, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].agent_version_major) as yauaa__agent_version_major,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].device_brand, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].device_brand, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].device_brand, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].device_brand) as yauaa__device_brand,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].device_name, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].device_name, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].device_name, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].device_name) as yauaa__device_name,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].device_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].device_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].device_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].device_version) as yauaa__device_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_class, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_class, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_class, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_class) as yauaa__layout_engine_class,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_name, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_name, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_name, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_name) as yauaa__layout_engine_name,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_name_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_name_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_name_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_name_version) as yauaa__layout_engine_name_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_name_version_major, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_name_version_major, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_name_version_major, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_name_version_major) as yauaa__layout_engine_name_version_major,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_version) as yauaa__layout_engine_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].layout_engine_version_major, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].layout_engine_version_major, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].layout_engine_version_major, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].layout_engine_version_major) as yauaa__layout_engine_version_major,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].operating_system_class, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].operating_system_class, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].operating_system_class, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].operating_system_class) as yauaa__operating_system_class,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].operating_system_name, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].operating_system_name, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].operating_system_name, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].operating_system_name) as yauaa__operating_system_name,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].operating_system_name_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].operating_system_name_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].operating_system_name_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].operating_system_name_version) as yauaa__operating_system_name_version,
coalesce(contexts_nl_basjes_yauaa_context_1_0_4[safe_offset(0)].operating_system_version, contexts_nl_basjes_yauaa_context_1_0_3[safe_offset(0)].operating_system_version, contexts_nl_basjes_yauaa_context_1_0_2[safe_offset(0)].operating_system_version, contexts_nl_basjes_yauaa_context_1_0_1[safe_offset(0)].operating_system_version) as yauaa__operating_system_version
  

  

  
    , cast(null as string) as browser__viewport
    , cast(null as string) as browser__document_size
    , cast(null as string) as browser__resolution
    , cast(null as INT64) as browser__color_depth
    , cast(null as FLOAT64) as browser__device_pixel_ratio
    , cast(null as boolean) as browser__cookies_enabled
    , cast(null as boolean) as browser__online
    , cast(null as string) as browser__browser_language
    , cast(null as string) as browser__document_language
    , cast(null as boolean) as browser__webdriver
    , cast(null as INT64) as browser__device_memory
    , cast(null as INT64) as browser__hardware_concurrency
    , cast(null as string) as browser__tab_id
  

  

  

  
    ,coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.id) as screen_view__id,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.name) as screen_view__name,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_id) as screen_view__previous_id,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_name) as screen_view__previous_name,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.previous_type) as screen_view__previous_type,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.transition_type) as screen_view__transition_type,
coalesce(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0.type) as screen_view__type
    

  

  

  
    ,coalesce(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].session_id, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].session_id) as session__session_id,
coalesce(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].session_index, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].session_index) as session__session_index,
coalesce(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].user_id, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].user_id) as session__user_id,
coalesce(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].first_event_id, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].first_event_id) as session__first_event_id,
coalesce(contexts_com_snowplowanalytics_snowplow_client_session_1_0_2[safe_offset(0)].previous_session_id, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1[safe_offset(0)].previous_session_id) as session__previous_session_id
    

  

  

  
    , cast(null as string) as mobile__device_manufacturer
    , cast(null as string) as mobile__device_model
    , cast(null as string) as mobile__os_type
    , cast(null as string) as mobile__os_version
    , cast(null as string) as mobile__android_idfa
    , cast(null as string) as mobile__apple_idfa
    , cast(null as string) as mobile__apple_idfv
    , cast(null as string) as mobile__carrier
    , cast(null as string) as mobile__open_idfa
    , cast(null as string) as mobile__network_technology
    , cast(null as string) as mobile__network_type
    , cast(null as INT64) as mobile__physical_memory
    , cast(null as INT64) as mobile__system_available_memory
    , cast(null as INT64) as mobile__app_available_memory
    , cast(null as INT64) as mobile__battery_level
    , cast(null as string) as mobile__battery_state
    , cast(null as boolean) as mobile__low_power_mode
    , cast(null as INT64) as mobile__available_storage
    , cast(null as INT64) as mobile__total_storage
    , cast(null as boolean) as mobile__is_portrait
    , cast(null as string) as mobile__resolution
    , cast(null as FLOAT64) as mobile__scale
    , cast(null as string) as mobile__language
    , cast(null as string) as mobile__app_set_id
    , cast(null as string) as mobile__app_set_id_scope
  

  

  

  
    , cast(null as FLOAT64) as geo__latitude
    , cast(null as FLOAT64) as geo__longitude
    , cast(null as FLOAT64) as geo__latitude_longitude_accuracy
    , cast(null as FLOAT64) as geo__altitude
    , cast(null as FLOAT64) as geo__altitude_accuracy
    , cast(null as FLOAT64) as geo__bearing
    , cast(null as FLOAT64) as geo__speed
  

  

  

  
    , cast(null as string) as app__build
    , cast(null as string) as app__version
  

  

  

  
      , cast(null as string) as screen__id
      , cast(null as string) as screen__name
      , cast(null as string) as screen__activity
      , cast(null as string) as screen__fragment
      , cast(null as string) as screen__top_view_controller
      , cast(null as string) as screen__type
      , cast(null as string) as screen__view_controller
  

  

  

  
    , cast(null as string) as deep_link__url
    , cast(null as string) as deep_link__referrer
  

  

  

  
    , cast(null as string) as app_error__message
    , cast(null as string) as app_error__programming_language
    , cast(null as string) as app_error__class_name
    , cast(null as string) as app_error__exception_name
    , cast(null as boolean) as app_error__is_fatal
    , cast(null as INT64) as app_error__line_number
    , cast(null as string) as app_error__stack_trace
    , cast(null as INT64) as app_error__thread_id
    , cast(null as string) as app_error__thread_name
  



from base_query

  
    

    create or replace table `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run`
      
    
    

    OPTIONS()
    as (
      





  with base as (

    select
      *,

      coalesce(
        
          ev.page_view__id,
        
        
          ev.screen_view__id,
        
        null, null) as view_id,

        coalesce(
        
          ev.session__session_index,
        
        
          ev.domain_sessionidx,
        
        null, null) as device_session_index,

      coalesce(
        
          ev.page_referrer,
        
        null, null) as referrer,

      coalesce(
        
          ev.page_url,
        
        null, null) as url,

      coalesce(
        
          ev.dvce_screenwidth || 'x' || ev.dvce_screenheight,
        
        null, null) as screen_resolution,

      coalesce(
        
        
          ev.yauaa__operating_system_name,
        
        
          ev.ua__os_family,
        
        null, null) as os_type,

      coalesce(
        
          ev.yauaa__operating_system_version,
        
        
        
          ev.ua__os_version,
        
        null, null) as os_version,

      coalesce(
        
          ev.domain_userid,
        
        
          ev.session__user_id,
        
        null, null) as device_identifier,

      case when platform = 'web' then 'Web' --includes mobile web
          when platform = 'mob' then 'Mobile/Tablet'
          when platform = 'pc' then 'Desktop/Laptop/Netbook'
          when platform = 'srv' then 'Server-Side App'
          when platform = 'app' then 'General App'
          when platform = 'tv' then 'Connected TV'
          when platform = 'cnsl' then 'Games Console'
          when platform = 'iot' then 'Internet of Things' end as platform_name

    from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_events_this_run` as ev

  )

  select
    *,

    
      case when platform = 'web' then yauaa__device_class
          when yauaa__device_class = 'Phone' then 'Mobile'
          when yauaa__device_class = 'Tablet' then 'Tablet'
          else platform_name end as device_category

  from base


    );
  
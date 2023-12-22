



with session_firsts as (
    select
        

    -- event categorization fields
    ev.event_name,
    ev.user_id,
    ev.user_identifier,
    ev.network_userid,

    -- timestamp fields
    ev.dvce_created_tstamp,
    ev.collector_tstamp,
    ev.derived_tstamp,
    ev.derived_tstamp as start_tstamp,

    -- geo fields
    ev.geo_country,
    ev.geo_region,
    ev.geo_region_name,
    ev.geo_city,
    ev.geo_zipcode,
    ev.geo_latitude,
    ev.geo_longitude,
    ev.geo_timezone,
    ev.user_ipaddress,

    -- device fields
    ev.app_id,
    ev.platform,
    ev.device_identifier,
    ev.device_category,
    ev.device_session_index,
    ev.os_version,
    ev.os_type,
    ev.screen_resolution,

    -- marketing fields
    ev.mkt_medium,
    ev.mkt_source,
    ev.mkt_term,
    ev.mkt_content,
    ev.mkt_campaign,
    ev.mkt_clickid,
    ev.mkt_network,
    
case
   when lower(trim(mkt_source)) = '(direct)' and lower(trim(mkt_medium)) in ('(not set)', '(none)') then 'Direct'
   when lower(trim(mkt_medium)) like '%cross-network%' then 'Cross-network'
   when regexp_contains(trim(mkt_medium), r'(?i)^(.*cp.*|ppc|retargeting|paid.*)$') then
      case
         when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
            or regexp_contains(trim(mkt_campaign), r'(?i)^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Paid Shopping'
         when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search'
         when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social'
         when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video'
         else 'Paid Other'
      end
   when lower(trim(mkt_medium)) in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
   when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
      or regexp_contains(trim(mkt_campaign), r'(?i)^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
   when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' or lower(trim(mkt_medium)) in ('social', 'social-network', 'sm', 'social network', 'social media') then 'Organic Social'
   when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
      or regexp_contains(trim(mkt_medium), r'(?i)^(.*video.*)$') then 'Organic Video'
   when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' or lower(trim(mkt_medium)) = 'organic' then 'Organic Search'
   when lower(trim(mkt_medium)) in ('referral', 'app', 'link') then 'Referral'
   when lower(trim(mkt_source)) in ('email', 'e-mail', 'e_mail', 'e mail') or lower(trim(mkt_medium)) in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
   when lower(trim(mkt_medium)) = 'affiliate' then 'Affiliates'
   when lower(trim(mkt_medium)) = 'audio' then 'Audio'
   when lower(trim(mkt_source)) = 'sms' or lower(trim(mkt_medium)) = 'sms' then 'SMS'
   when lower(trim(mkt_medium)) like '%push' or regexp_contains(trim(mkt_medium), r'(?i).*(mobile|notification).*') or lower(trim(mkt_source)) = 'firebase' then 'Mobile Push Notifications'
   else 'Unassigned'
end
 as default_channel_group,

    -- webpage / referer / browser fields
    ev.page_url,
    ev.page_referrer,
    ev.refr_medium,
    ev.refr_source,
    ev.refr_term,
    ev.useragent


        , session_identifier

        
          

      , ev.br_lang
      , ev.br_viewwidth
      , ev.br_viewheight
      , ev.br_renderengine
      , ev.doc_width
      , ev.doc_height
      , ev.page_title
      , ev.page_urlscheme
      , ev.page_urlhost
      , ev.page_urlpath
      , ev.page_urlquery
      , ev.page_urlfragment
      , ev.refr_urlscheme
      , ev.refr_urlhost
      , ev.refr_urlpath
      , ev.refr_urlquery
      , ev.refr_urlfragment
      , ev.os_timezone


        

        
          

  , ev.session__previous_session_id
  , ev.screen_view__name
  , ev.screen_view__previous_id
  , ev.screen_view__previous_name
  , ev.screen_view__previous_type
  , ev.screen_view__transition_type
  , ev.screen_view__type


        

        
          -- updated with mapping as part of post hook on derived sessions table
          , cast(user_identifier as 
    string
) as stitched_user_id
        

        
          

    , ev.iab__category
    , ev.iab__primary_impact
    , ev.iab__reason
    , ev.iab__spider_or_robot


        

        
          

    , ev.yauaa__device_class
    , ev.yauaa__agent_class
    , ev.yauaa__agent_name
    , ev.yauaa__agent_name_version
    , ev.yauaa__agent_name_version_major
    , ev.yauaa__agent_version
    , ev.yauaa__agent_version_major
    , ev.yauaa__device_brand
    , ev.yauaa__device_name
    , ev.yauaa__device_version
    , ev.yauaa__layout_engine_class
    , ev.yauaa__layout_engine_name
    , ev.yauaa__layout_engine_name_version
    , ev.yauaa__layout_engine_name_version_major
    , ev.yauaa__layout_engine_version
    , ev.yauaa__layout_engine_version_major
    , ev.yauaa__operating_system_class
    , ev.yauaa__operating_system_name
    , ev.yauaa__operating_system_name_version
    , ev.yauaa__operating_system_version


        

        
          

      , ev.ua__useragent_family
      , ev.ua__useragent_major
      , ev.ua__useragent_minor
      , ev.ua__useragent_patch
      , ev.ua__useragent_version
      , ev.ua__os_family
      , ev.ua__os_major
      , ev.ua__os_minor
      , ev.ua__os_patch
      , ev.ua__os_patch_minor
      , ev.ua__os_version
      , ev.ua__device_family


        

        

        

        , g.name as geo_country_name
        , g.region as geo_continent
        , l.name as br_lang_name

        

        

        

        , 
  regexp_extract(page_urlquery ,r'utm_source_platform=([^?&#]*)')
 as mkt_source_platform

    from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run` ev
    left join
        `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_ga4_source_categories` c on lower(trim(ev.mkt_source)) = lower(c.source)
    left join
        `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_rfc_5646_language_mapping` l on lower(ev.br_lang) = lower(l.lang_tag)
    left join
        `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_geo_country_mapping` g on lower(ev.geo_country) = lower(g.alpha_2)
    where event_name in ('page_ping', 'page_view', 'screen_view')
    and view_id is not null

    
      
  and not regexp_contains(useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')

    

    
      qualify row_number() over (partition by session_identifier order by derived_tstamp, dvce_created_tstamp, event_id) = 1
    
)

, session_lasts as (
    select

      ev.event_name as last_event_name,
      ev.geo_country as last_geo_country,
      ev.geo_city as last_geo_city,
      ev.geo_region_name as last_geo_region_name,
      g.name as last_geo_country_name,
      g.region as last_geo_continent,
      ev.page_url as last_page_url,

      
        ev.page_title as last_page_title,
        ev.page_urlscheme as last_page_urlscheme,
        ev.page_urlhost as last_page_urlhost,
        ev.page_urlpath as last_page_urlpath,
        ev.page_urlquery as last_page_urlquery,
        ev.page_urlfragment as last_page_urlfragment,
        br_lang as last_br_lang,
        l.name as last_br_lang_name,
      

      
        ev.screen_view__name as last_screen_view__name,
        ev.screen_view__transition_type as last_screen_view__transition_type,
        ev.screen_view__type as last_screen_view__type,
      

      

      session_identifier

    from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run` ev
    left join
        `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_rfc_5646_language_mapping` l on lower(ev.br_lang) = lower(l.lang_tag)
    left join
        `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_geo_country_mapping` g on lower(ev.geo_country) = lower(g.alpha_2)
    where
        event_name in ('page_view', 'screen_view')
        and view_id is not null
        
            
  and not regexp_contains(useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')

        

    
      qualify row_number() over (partition by session_identifier order by derived_tstamp desc, dvce_created_tstamp desc, event_id) = 1
    
)

, session_aggs as (
    select
      session_identifier
      , min(derived_tstamp) as start_tstamp
      , max(derived_tstamp) as end_tstamp
      , count(*) as total_events
      , count(distinct view_id) as views

      

            -- (hb * (#page pings - # distinct page view ids ON page pings)) + (# distinct page view ids ON page pings * min visit length)
        , (30 * (
              -- number of (unqiue in heartbeat increment) pages pings following a page ping (gap of heartbeat)
              count(distinct case when event_name = 'page_ping' and view_id is not null then
                          -- need to get a unique list of floored time PER page view, so create a dummy surrogate key...
                          view_id || cast(floor(unix_seconds(dvce_created_tstamp)/30) as string)
                      else null end) - count(distinct case when event_name = 'page_ping' and view_id is not null then view_id else null end)
                            ))  +
                            -- number of page pings following a page view (or no event) (gap of min visit length)
                            (count(distinct case when event_name = 'page_ping' and view_id is not null then view_id else null end) * 30) as engaged_time_in_s
        , 
    timestamp_diff(max(derived_tstamp), min(derived_tstamp), second)
 as absolute_time_in_s
      

      

      
        , count(distinct screen_view__name) as screen_names_viewed
      

    from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run`
    where 1 = 1

    
        
  and not regexp_contains(useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')

    

    group by session_identifier
)

-- Redshift does not allow listagg and other aggregations in the same CTE

select

  -- event categorization fields
  f.event_name as first_event_name
  , l.last_event_name
  , f.session_identifier
  
    , f.session__previous_session_id
  

  -- user id fields
  , f.user_id
  , f.user_identifier
  , f.stitched_user_id
  , f.network_userid

  -- timestamp fields
  -- when the session starts with a ping we need to add the min visit length to get when the session actually started
  , case when f.event_name = 'page_ping' then 
    timestamp_add(a.start_tstamp, interval -30 second)
 else a.start_tstamp end as start_tstamp
  , a.end_tstamp -- only page views with pings will have a row in table t
  , 
    current_timestamp()
 as model_tstamp

  -- device fields
  , f.app_id
  , f.platform
  , f.device_identifier
  , f.device_category
  , f.device_session_index
  , f.os_version
  , f.os_type

  
    , f.os_timezone
  

  , f.screen_resolution

  
    , f.yauaa__device_class
    , f.yauaa__device_version
    , f.yauaa__operating_system_version
    , f.yauaa__operating_system_class
    , f.yauaa__operating_system_name
    , f.yauaa__operating_system_name_version
  

  

  -- geo fields
  , f.geo_country as first_geo_country
  , f.geo_region_name as first_geo_region_name
  , f.geo_city as first_geo_city
  , f.geo_country_name as first_geo_country_name
  , f.geo_continent as first_geo_continent

  , case when l.last_geo_country is null then coalesce(l.last_geo_country, f.geo_country) else l.last_geo_country end as last_geo_country
  , case when l.last_geo_country is null then coalesce(l.last_geo_region_name, f.geo_region_name) else l.last_geo_region_name end as last_geo_region_name
  , case when l.last_geo_country is null then coalesce(l.last_geo_city, f.geo_city) else l.last_geo_city end as last_geo_city
  , case when l.last_geo_country is null then coalesce(l.last_geo_country_name,f.geo_country_name) else l.last_geo_country_name end as last_geo_country_name
  , case when l.last_geo_country is null then coalesce(l.last_geo_continent, f.geo_continent) else l.last_geo_continent end as last_geo_continent

  , f.geo_zipcode
  , f.geo_latitude
  , f.geo_longitude
  , f.geo_timezone
  , f.user_ipaddress

  -- engagement fields
  , a.views
  , a.total_events
  , 
    views >= 2

    
      or engaged_time_in_s / 30 >= 2
 as is_engaged
  -- when the session starts with a ping we need to add the min visit length to get when the session actually started

  
    , a.engaged_time_in_s
    , a.absolute_time_in_s + case when f.event_name = 'page_ping' then 30 else 0 end as absolute_time_in_s

  
    , 
    timestamp_diff(a.end_tstamp, a.start_tstamp, second)
 as session_duration_s
    , a.screen_names_viewed

  -- marketing fields
  , f.mkt_medium
  , f.mkt_source
  , f.mkt_term
  , f.mkt_content
  , f.mkt_campaign
  , f.mkt_clickid
  , f.mkt_network
  , f.default_channel_group
  , mkt_source_platform

  -- webpage / referrer / browser fields
  , f.page_url as first_page_url
  , case when l.last_page_url is null then coalesce(l.last_page_url, f.page_url) else l.last_page_url end as last_page_url
  , f.page_referrer
  , f.refr_medium
  , f.refr_source
  , f.refr_term

  
    , f.page_title as first_page_title
    , f.page_urlscheme as first_page_urlscheme
    , f.page_urlhost as first_page_urlhost
    , f.page_urlpath as first_page_urlpath
    , f.page_urlquery as first_page_urlquery
    , f.page_urlfragment as first_page_urlfragment
    -- only take the first value when the last is genuinely missing (base on url as has to always be populated)
    , case when l.last_page_url is null then coalesce(l.last_page_title, f.page_title) else l.last_page_title end as last_page_title
    , case when l.last_page_url is null then coalesce(l.last_page_urlscheme, f.page_urlscheme) else l.last_page_urlscheme end as last_page_urlscheme
    , case when l.last_page_url is null then coalesce(l.last_page_urlhost, f.page_urlhost) else l.last_page_urlhost end as last_page_urlhost
    , case when l.last_page_url is null then coalesce(l.last_page_urlpath, f.page_urlpath) else l.last_page_urlpath end as last_page_urlpath
    , case when l.last_page_url is null then coalesce(l.last_page_urlquery, f.page_urlquery) else l.last_page_urlquery end as last_page_urlquery
    , case when l.last_page_url is null then coalesce(l.last_page_urlfragment, f.page_urlfragment) else l.last_page_urlfragment end as last_page_urlfragment
    , f.refr_urlscheme
    , f.refr_urlhost
    , f.refr_urlpath
    , f.refr_urlquery
    , f.refr_urlfragment
    , f.br_renderengine
    , f.br_lang as first_br_lang
    , f.br_lang_name as first_br_lang_name
    , case when l.last_br_lang is null then coalesce(l.last_br_lang, f.br_lang) else l.last_br_lang end as last_br_lang
    , case when l.last_br_lang is null then coalesce(l.last_br_lang_name, f.br_lang_name) else l.last_br_lang_name end as last_br_lang_name
  

  -- iab enrichment fields
  
    , f.iab__category
    , f.iab__primary_impact
    , f.iab__reason
    , f.iab__spider_or_robot
  

  -- yauaa enrichment fields
  
    , f.yauaa__device_name
    , f.yauaa__agent_class
    , f.yauaa__agent_name
    , f.yauaa__agent_name_version
    , f.yauaa__agent_name_version_major
    , f.yauaa__agent_version
    , f.yauaa__agent_version_major
    , f.yauaa__layout_engine_class
    , f.yauaa__layout_engine_name
    , f.yauaa__layout_engine_name_version
    , f.yauaa__layout_engine_name_version_major
    , f.yauaa__layout_engine_version
    , f.yauaa__layout_engine_version_major
  

  -- ua parser enrichment fields
  
    , f.ua__device_family
    , f.ua__os_version
    , f.ua__os_major
    , f.ua__os_minor
    , f.ua__os_patch
    , f.ua__os_patch_minor
    , f.ua__useragent_family
    , f.ua__useragent_major
    , f.ua__useragent_minor
    , f.ua__useragent_patch
    , f.ua__useragent_version
  

  -- mobile only
  
    , f.screen_view__name as first_screen_view__name
    , f.screen_view__type as first_screen_view__type
    , case when l.last_screen_view__name is null then coalesce(l.last_screen_view__name, f.screen_view__name) else l.last_screen_view__name end as last_screen_view__name
    , case when l.last_screen_view__transition_type is null then coalesce(l.last_screen_view__transition_type, f.screen_view__transition_type) else l.last_screen_view__transition_type end as last_screen_view__transition_type
    , case when l.last_screen_view__type is null then coalesce(l.last_screen_view__type, f.screen_view__type) else l.last_screen_view__type end as last_screen_view__type
    , f.screen_view__previous_id
    , f.screen_view__previous_name
    , f.screen_view__previous_type

  

  

  

  

  

  , f.useragent

  -- conversion fields

  -- passthrough fields

from session_firsts f

left join session_lasts l
on f.session_identifier = l.session_identifier



left join session_aggs a
on f.session_identifier = a.session_identifier


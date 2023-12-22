



with prep as (
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


    , view_id
    , session_identifier
    , event_id


    
      

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


      , 
  case when ev.page_url like '%/product%' then 'PDP'
      when ev.page_url like '%/list%' then 'PLP'
      when ev.page_url like '%/checkout%' then 'checkout'
      when ev.page_url like '%/home%' then 'homepage'
      else 'other'
  end

 as content_group
      , coalesce(
      
        ev.br_colordepth,
      
      null) as br_colordepth
    

    
     

  , ev.session__previous_session_id
  , ev.screen_view__name
  , ev.screen_view__previous_id
  , ev.screen_view__previous_name
  , ev.screen_view__previous_type
  , ev.screen_view__transition_type
  , ev.screen_view__type


    

    
      , cast(null as 
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


    

    

    

    

    

    

    from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_events_this_run` as ev

    left join `com-snplow-sales-gcp`.`scratch_snowplow_manifest`.`snowplow_unified_dim_ga4_source_categories` c on lower(trim(ev.mkt_source)) = lower(c.source)

    where ev.event_name in ('page_view', 'screen_view')
    and ev.view_id is not null

    
      
  and not regexp_contains(ev.useragent, '(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)')

    

    
      qualify row_number() over (partition by ev.view_id order by ev.derived_tstamp, ev.dvce_created_tstamp) = 1
    
)

, view_events as (
  select

    p.*

    , row_number() over (partition by p.session_identifier order by p.derived_tstamp, p.dvce_created_tstamp, p.event_id) AS view_in_session_index

    , coalesce(t.end_tstamp, p.derived_tstamp) as end_tstamp -- only page views with pings will have a row in table t

    
      , coalesce(t.engaged_time_in_s, 0) as engaged_time_in_s -- where there are no pings, engaged time is 0.
      , 

    datetime_diff(
        cast(coalesce(t.end_tstamp, p.derived_tstamp) as datetime),
        cast(p.derived_tstamp as datetime),
        second
    )

   as absolute_time_in_s
      , sd.hmax as horizontal_pixels_scrolled
      , sd.vmax as vertical_pixels_scrolled
      , sd.relative_hmax as horizontal_percentage_scrolled
      , sd.relative_vmax as vertical_percentage_scrolled
    

    , 
    current_timestamp()
 as model_tstamp

  from prep p

  left join `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_pv_engaged_time` t
  on p.view_id = t.view_id and p.session_identifier = t.session_identifier

  left join `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_pv_scroll_depth` sd
  on p.view_id = sd.view_id and p.session_identifier = sd.session_identifier

  

)

select

    -- event categorization fields
    pve.view_id
    , pve.event_name
    , pve.event_id
    , pve.session_identifier
    , pve.view_in_session_index
    , max(pve.view_in_session_index) over (partition by pve.session_identifier) as views_in_session
    
      , pve.session__previous_session_id
    

    -- user id fields
    , pve.user_id
    , pve.user_identifier
    , pve.stitched_user_id
    , pve.network_userid

    -- timestamp fields
    , pve.dvce_created_tstamp
    , pve.collector_tstamp
    , pve.derived_tstamp
    , pve.derived_tstamp as start_tstamp
    , pve.end_tstamp -- only page views with pings will have a row in table t
    , pve.model_tstamp

    -- device fields
    , pve.app_id
    , pve.platform
    , pve.device_identifier
    , pve.device_category
    , pve.device_session_index
    , pve.os_version
    , pve.os_type
    
    
      , pve.os_timezone
    
    , pve.screen_resolution
    
      , pve.yauaa__device_class
      , pve.yauaa__device_version
      , pve.yauaa__operating_system_version
      , pve.yauaa__operating_system_class
      , pve.yauaa__operating_system_name
      , pve.yauaa__operating_system_name_version
    

    -- geo fields
    , pve.geo_country
    , pve.geo_region
    , pve.geo_region_name
    , pve.geo_city
    , pve.geo_zipcode
    , pve.geo_latitude
    , pve.geo_longitude
    , pve.geo_timezone
    , pve.user_ipaddress

    -- engagement fields
    
      , pve.engaged_time_in_s -- where there are no pings, engaged time is 0.
      , pve.absolute_time_in_s
      , pve.horizontal_pixels_scrolled
      , pve.vertical_pixels_scrolled
      , pve.horizontal_percentage_scrolled
      , pve.vertical_percentage_scrolled
    

    -- marketing fields
    , pve.mkt_medium
    , pve.mkt_source
    , pve.mkt_term
    , pve.mkt_content
    , pve.mkt_campaign
    , pve.mkt_clickid
    , pve.mkt_network
    , pve.default_channel_group

    -- webpage / referer / browser fields
    , pve.page_url
    , pve.page_referrer
    , pve.refr_medium
    , pve.refr_source
    , pve.refr_term

    

      , pve.page_title
      , pve.content_group

      , pve.page_urlscheme
      , pve.page_urlhost
      , pve.page_urlpath
      , pve.page_urlquery
      , pve.page_urlfragment

      , pve.refr_urlscheme
      , pve.refr_urlhost
      , pve.refr_urlpath
      , pve.refr_urlquery
      , pve.refr_urlfragment


      , pve.br_lang
      , pve.br_viewwidth
      , pve.br_viewheight
      , pve.br_colordepth
      , pve.br_renderengine

      , pve.doc_width
      , pve.doc_height

    

    -- iab enrichment fields
    
      , pve.iab__category
      , pve.iab__primary_impact
      , pve.iab__reason
      , pve.iab__spider_or_robot
    

    -- yauaa enrichment fields
    
      , pve.yauaa__device_name
      , pve.yauaa__agent_class
      , pve.yauaa__agent_name
      , pve.yauaa__agent_name_version
      , pve.yauaa__agent_name_version_major
      , pve.yauaa__agent_version
      , pve.yauaa__agent_version_major
      , pve.yauaa__layout_engine_class
      , pve.yauaa__layout_engine_name
      , pve.yauaa__layout_engine_name_version
      , pve.yauaa__layout_engine_name_version_major
      , pve.yauaa__layout_engine_version
      , pve.yauaa__layout_engine_version_major
    

    -- ua parser enrichment fields
    
      , pve.ua__device_family
      , pve.ua__os_version
      , pve.ua__os_major
      , pve.ua__os_minor
      , pve.ua__os_patch
      , pve.ua__os_patch_minor
      , pve.ua__useragent_family
      , pve.ua__useragent_major
      , pve.ua__useragent_minor
      , pve.ua__useragent_patch
      , pve.ua__useragent_version
    

    -- mobile only
    
      , pve.screen_view__name
      , pve.screen_view__previous_id
      , pve.screen_view__previous_name
      , pve.screen_view__previous_type
      , pve.screen_view__transition_type
      , pve.screen_view__type
    

    

    

    

    , pve.useragent

from view_events pve
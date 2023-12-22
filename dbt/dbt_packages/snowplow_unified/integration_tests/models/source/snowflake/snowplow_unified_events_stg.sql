{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# CWV tests run on a different source dataset, this is an easy way to hack them together. #}
{% if not var("snowplow__enable_cwv", false) %}

  -- page view context is given as json string in csv. Parse json
  with prep as (
    select
      *,
      parse_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as contexts_com_snowplowanalytics_snowplow_web_page_1,
      parse_json(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
      parse_json(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
      parse_json(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as contexts_com_iab_snowplow_spiders_and_robots_1,
      parse_json(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
      parse_json(contexts_nl_basjes_yauaa_context_1_0_0) as contexts_nl_basjes_yauaa_context_1,
      parse_json(unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0) as unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
      parse_json(contexts_com_snowplowanalytics_snowplow_client_session_1_0_0) as contexts_com_snowplowanalytics_snowplow_client_session_1,
      parse_json(contexts_com_snowplowanalytics_snowplow_geolocation_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_geolocation_context_1,
      parse_json(contexts_com_snowplowanalytics_mobile_application_1_0_0) as contexts_com_snowplowanalytics_mobile_application_1,
      parse_json(contexts_com_snowplowanalytics_mobile_deep_link_1_0_0) as contexts_com_snowplowanalytics_mobile_deep_link_1,
      parse_json(contexts_com_snowplowanalytics_snowplow_browser_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_browser_context_1,
      parse_json(contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_mobile_context_1,
      parse_json(unstruct_event_com_snowplowanalytics_snowplow_application_error_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_application_error_1,
      parse_json(contexts_com_snowplowanalytics_mobile_screen_1_0_0) as contexts_com_snowplowanalytics_mobile_screen_1

    from {{ ref('snowplow_unified_events') }}
    )

  , flatten as (
    select
      *,
      contexts_nl_basjes_yauaa_context_1[0].agentClass as agent_class,
      contexts_nl_basjes_yauaa_context_1[0].agentInformationEmail as agent_information_email,
      contexts_nl_basjes_yauaa_context_1[0].agentName as agent_name,
      contexts_nl_basjes_yauaa_context_1[0].agentNameVersion as agent_name_version,
      contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor as agent_name_version_major,
      contexts_nl_basjes_yauaa_context_1[0].agentVersion as agent_version,
      contexts_nl_basjes_yauaa_context_1[0].agentVersionMajor as agent_version_major,
      contexts_nl_basjes_yauaa_context_1[0].deviceBrand as device_brand,
      contexts_nl_basjes_yauaa_context_1[0].deviceClass as device_class,
      contexts_nl_basjes_yauaa_context_1[0].deviceCpu as device_cpu,
      contexts_nl_basjes_yauaa_context_1[0].deviceCpuBits as device_cpu_bits,
      contexts_nl_basjes_yauaa_context_1[0].deviceName as device_name,
      contexts_nl_basjes_yauaa_context_1[0].deviceVersion as device_version,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineClass as layout_engine_class,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineName as layout_engine_name,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineNameVersion as layout_engine_name_version,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineNameVersionMajor as layout_engine_name_version_major,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineVersion as layout_engine_version,
      contexts_nl_basjes_yauaa_context_1[0].layoutEngineVersionMajor as layout_engine_version_major,
      contexts_nl_basjes_yauaa_context_1[0].networkType as network_type,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemClass as operating_system_class,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemName as operating_system_name,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemNameVersion as operating_system_name_version,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemNameVersionMajor as operating_system_name_version_major,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemVersion as operating_system_version,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemVersionBuild as operating_system_version_build,
      contexts_nl_basjes_yauaa_context_1[0].operatingSystemVersionMajor as operating_system_version_major,
      contexts_nl_basjes_yauaa_context_1[0].webviewAppName as webview_app_name,
      contexts_nl_basjes_yauaa_context_1[0].webviewAppNameVersionMajor as webview_app_name_version_major,
      contexts_nl_basjes_yauaa_context_1[0].webviewAppVersion as webview_app_version,
      contexts_nl_basjes_yauaa_context_1[0].webviewAppVersionMajor as webview_app_version_major,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].category as category,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].primaryImpact as primaryImpact,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].reason as reason,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].spiderOrRobot as spiderOrRobot,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].basis_for_processing as basisForProcessing,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].consent_scopes as consentScopes,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].consent_url as consentUrl,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].consent_version as consentVersion,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].domains_applied as domainsApplied,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].event_type as eventType,
      unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1[0].gdpr_applies as gdprApplies,
      unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1[0]:elapsed_time as elapsedTime,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:id::varchar AS id,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:name::varchar AS name,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:previousId::varchar AS previousId,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:previousName::varchar AS previousName,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:previousType::varchar AS previousType,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:transitionType::varchar AS transitionType,
      unstruct_event_com_snowplowanalytics_mobile_screen_view_1[0]:type::varchar AS type,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:firstEventId::varchar AS firstEventId,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:previousSessionId::varchar AS previousSessionId,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionId::varchar AS sessionId,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:sessionIndex::int AS sessionIndex,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:userId::varchar AS userId,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:eventIndex::int AS eventIndex,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:storageMechanism::varchar AS storageMechanism,
      contexts_com_snowplowanalytics_snowplow_client_session_1[0]:firstEventTimestamp::timestamp AS firstEventTimestamp,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:latitude::float AS latitude,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:longitude::float AS longitude,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:latitudeLongitudeAccuracy::float AS latitudeLongitudeAccuracy,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:altitude::float AS altitude,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:altitudeAccuracy::float AS altitudeAccuracy,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:bearing::float AS bearing,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:speed::float AS speed,
      contexts_com_snowplowanalytics_snowplow_geolocation_context_1[0]:timestamp::int AS timestamp,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:viewport::varchar AS viewport,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:documentSize::varchar AS documentSize,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:resolution::varchar AS resolution,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:colorDepth::int AS colorDepth,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:devicePixelRatio::float AS devicePixelRatio,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:cookiesEnabled::boolean AS cookiesEnabled,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:online::boolean AS online,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:browserLanguage::varchar AS browserLanguage,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:documentLanguage::varchar AS documentLanguage,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:webdriver::boolean AS webdriver,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:deviceMemory::int AS deviceMemory,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:hardwareConcurrency::int AS hardwareConcurrency,
      contexts_com_snowplowanalytics_snowplow_browser_context_1[0]:tabId::varchar AS tabId,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:deviceManufacturer::varchar AS deviceManufacturer,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:deviceModel::varchar AS deviceModel,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:osType::varchar AS osType,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:osVersion::varchar AS osVersion,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:androidIdfa::varchar AS androidIdfa,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:appleIdfa::varchar AS appleIdfa,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:appleIdfv::varchar AS appleIdfv,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:carrier::varchar AS carrier,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:openIdfa::varchar AS openIdfa,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:networkTechnology::varchar AS networkTechnology,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]:networkType::varchar(255) AS networkType,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].physicalMemory::int AS physicalMemory,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].systemAvailableMemory::int AS systemAvailableMemory,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].appAvailableMemory::int AS appAvailableMemory,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].batteryLevel::int AS batteryLevel,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].batteryState::string AS batteryState,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].lowPowerMode::string AS lowPowerMode,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].availableStorage::int AS availableStorage,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].totalStorage::int AS totalStorage,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].isPortrait::boolean AS isPortrait,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].resolution::string AS resolution2,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].scale::string AS scale,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].language::string AS language,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].appSetId::string AS appSetId,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].appSetIdScope::string AS appSetIdScope,
      contexts_com_snowplowanalytics_mobile_screen_1[0].id::varchar AS id2,
      contexts_com_snowplowanalytics_mobile_screen_1[0].name::varchar AS name2,
      contexts_com_snowplowanalytics_mobile_screen_1[0].activity::varchar AS activity,
      contexts_com_snowplowanalytics_mobile_screen_1[0].fragment::varchar AS fragment,
      contexts_com_snowplowanalytics_mobile_screen_1[0].topViewController::varchar AS topViewController,
      contexts_com_snowplowanalytics_mobile_screen_1[0].type::varchar AS type2,
      contexts_com_snowplowanalytics_mobile_screen_1[0].viewController::varchar(255) AS viewController,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].message::varchar AS message,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].programmingLanguage::varchar AS programmingLanguage,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].className::varchar AS className,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].exceptionName::varchar AS exceptionName,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].isFatal::boolean AS isFatal,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].lineNumber::float AS lineNumber,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].stackTrace::varchar AS stackTrace,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].threadId::int AS threadId,
      unstruct_event_com_snowplowanalytics_snowplow_application_error_1[0].threadName::varchar AS threadName

    from prep

  )

  select
    app_id,
    platform,
    etl_tstamp,
    collector_tstamp,
    dvce_created_tstamp,
    event,
    event_id,
    txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    se_category,
    se_action,
    se_label,
    se_property,
    se_value,
    tr_orderid,
    tr_affiliation,
    tr_total,
    tr_tax,
    tr_shipping,
    tr_city,
    tr_state,
    tr_country,
    ti_orderid,
    ti_sku,
    ti_name,
    ti_category,
    ti_price,
    ti_quantity,
    pp_xoffset_min,
    pp_xoffset_max,
    pp_yoffset_min,
    pp_yoffset_max,
    useragent,
    br_name,
    br_family,
    br_version,
    br_type,
    br_renderengine,
    br_lang,
    br_features_pdf,
    br_features_flash,
    br_features_java,
    br_features_director,
    br_features_quicktime,
    br_features_realplayer,
    br_features_windowsmedia,
    br_features_gears,
    br_features_silverlight,
    br_cookies,
    br_colordepth,
    br_viewwidth,
    br_viewheight,
    os_name,
    os_family,
    os_manufacturer,
    os_timezone,
    dvce_type,
    dvce_ismobile,
    dvce_screenwidth,
    dvce_screenheight,
    doc_charset,
    doc_width,
    doc_height,
    tr_currency,
    tr_total_base,
    tr_tax_base,
    tr_shipping_base,
    ti_currency,
    ti_price_base,
    base_currency,
    geo_timezone,
    mkt_clickid,
    mkt_network,
    etl_tags,
    dvce_sent_tstamp,
    refr_domain_userid,
    refr_dvce_tstamp,
    domain_sessionid,
    derived_tstamp,
    event_vendor,
    event_name,
    event_format,
    event_version,
    event_fingerprint,
    true_tstamp,
    load_tstamp,
    contexts_com_snowplowanalytics_snowplow_web_page_1,
    object_construct('basisForProcessing', basisForProcessing,'consentScopes', consentScopes, 'consentUrl', consentUrl, 'consentVersion', consentVersion, 'domainsApplied', domainsApplied, 'eventType', eventType, 'gdprApplies', gdprApplies) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
    object_construct_keep_null('elapsedTime', elapsedTime) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
    contexts_com_iab_snowplow_spiders_and_robots_1,
    contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
    contexts_nl_basjes_yauaa_context_1,
    object_construct('id',id,'name',name,'previousId',previousId,'previousName',previousName,'previousType',previousType,'transitionType',transitionType,'type',type) as unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
    parse_json('[{"sessionId":"'||sessionId||'","userId":"'||userId||'", "sessionIndex":"'||sessionIndex||'", "firstEventId":"'||firstEventId||'", "previousSessionId":"'||previousSessionId||'", "eventIndex":"'||eventIndex||'", "storageMechanism":"'||storageMechanism||'", "firstEventTimestamp":"'||firstEventTimestamp||'"}]' ) as contexts_com_snowplowanalytics_snowplow_client_session_1,
    to_variant([OBJECT_CONSTRUCT_KEEP_NULL('latitude', latitude, 'longitude', longitude, 'latitudeLongitudeAccuracy', latitudeLongitudeAccuracy, 'altitude', altitude, 'altitudeAccuracy', altitudeAccuracy, 'bearing', bearing,'speed', speed,'timestamp', timestamp)]) as contexts_com_snowplowanalytics_snowplow_geolocation_context_1,
    contexts_com_snowplowanalytics_mobile_application_1,
    contexts_com_snowplowanalytics_mobile_deep_link_1,
    parse_json('[{"viewport":"'||viewport||'", "documentSize":"'||documentSize||'", "resolution":"'||resolution||'", "colorDepth":"'||colorDepth||'", "devicePixelRatio":"'||devicePixelRatio||'", "cookiesEnabled":"'||cookiesEnabled||'", "online":"'||online||'", "browserLanguage":"'||browserLanguage||'","documentLanguage":"'||documentLanguage||'", "webdriver":"'||webdriver||'", "deviceMemory":"'||deviceMemory||'", "hardwareConcurrency":"'||hardwareConcurrency||'", "tabId":"'||tabId||'"}]' ) as contexts_com_snowplowanalytics_snowplow_browser_context_1,
    parse_json('[{"deviceManufacturer":"'||deviceManufacturer||'", "deviceModel":"'||deviceModel||'", "osType":"'||osType||'", "osVersion":"'||osVersion||'", "androidIdfa":"'||androidIdfa||'", "appleIdfa":"'||appleIdfa||'", "appleIdfv":"'||appleIdfv||'", "carrier":"'||carrier||'", "openIdfa":"'||openIdfa||'", "networkTechnology":"'||networkTechnology||'", "networkType":"'||networkType||'", "physicalMemory":"'||physicalMemory||'", "systemAvailableMemory":"'||systemAvailableMemory||'", "appAvailableMemory":"'||appAvailableMemory||'", "batteryLevel":"'||batteryLevel||'", "batteryState":"'||batteryState||'", "lowPowerMode":"'||lowPowerMode||'", "availableStorage":"'||availableStorage||'", "isPortrait":"'||isPortrait||'", "totalStorage":"'||totalStorage||'", "resolution":"'||resolution2||'", "scale":"'||scale||'", "language":"'||language||'", "appSetId":"'||appSetId||'", "appSetIdScope":"'||appSetIdScope||'"}]' ) as contexts_com_snowplowanalytics_snowplow_mobile_context_1,
    parse_json('[{"id":"'||id2||'", "name":"'||name2||'", "activity":"'||activity||'", "fragment":"'||fragment||'", "topViewController":"'||topViewController||'", "type":"'||type2||'", "viewController":"'||viewController||'"}]' ) as contexts_com_snowplowanalytics_mobile_screen_1,
    object_construct('message', message,'programmingLanguage', programmingLanguage, 'className', className, 'exceptionName', exceptionName, 'isFatal', isFatal, 'lineNumber', lineNumber, 'stackTrace', stackTrace, 'threadId', threadId, 'threadName', threadName) as unstruct_event_com_snowplowanalytics_snowplow_application_error_1

  from flatten

{% else %}

  -- page view context is given as json string in csv. Parse json

    with prep as (
      select
      *,
      parse_json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as contexts_com_snowplowanalytics_snowplow_web_page_1,
      parse_json(unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1,
      parse_json(contexts_nl_basjes_yauaa_context_1_0_0) as contexts_nl_basjes_yauaa_context_1,
      parse_json(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as contexts_com_iab_snowplow_spiders_and_robots_1
    from {{ ref('snowplow_unified_web_vital_events') }}

    )

    , flatten as (
    select
      *,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].lcp as lcp,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].fcp as fcp,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].fid as fid,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].cls as cls,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].inp as inp,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].ttfb as ttfb,
      unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1[0].navigation_type as navigationType,
      contexts_nl_basjes_yauaa_context_1[0].device_class as deviceClass,
      contexts_nl_basjes_yauaa_context_1[0].agent_class as agentClass,
      contexts_nl_basjes_yauaa_context_1[0].agent_name as agentName,
      contexts_nl_basjes_yauaa_context_1[0].agent_name_version as agentNameVersion,
      contexts_nl_basjes_yauaa_context_1[0].agent_name_version_major as agentNameVersionMajor,
      contexts_nl_basjes_yauaa_context_1[0].agent_version as agentVersion,
      contexts_nl_basjes_yauaa_context_1[0].agent_version_major as agentVersionMajor,
      contexts_nl_basjes_yauaa_context_1[0].device_brand as deviceBrand,
      contexts_nl_basjes_yauaa_context_1[0].device_name as deviceName,
      contexts_nl_basjes_yauaa_context_1[0].device_version as deviceVersion,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_class as layoutEngineClass,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_name as layoutEngineName,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_name_version as layoutEngineNameVersion,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_name_version_major as layoutEngineNameVersionMajor,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_version as layoutEngineVersion,
      contexts_nl_basjes_yauaa_context_1[0].layout_engine_version_major as layoutEngineVersionMajor,
      contexts_nl_basjes_yauaa_context_1[0].operating_system_class as operatingSystemClass,
      contexts_nl_basjes_yauaa_context_1[0].operating_system_name as operatingSystemName,
      contexts_nl_basjes_yauaa_context_1[0].operating_system_name_version as operatingSystemNameVersion,
      contexts_nl_basjes_yauaa_context_1[0].operating_system_version as operatingSystemVersion,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].category as category,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].primary_impact as primaryImpact,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].reason as reason,
      contexts_com_iab_snowplow_spiders_and_robots_1[0].spider_or_robot as spiderOrRobot

    from prep

  )

  select
    app_id,
    platform,
    etl_tstamp,
    collector_tstamp,
    dvce_created_tstamp,
    event,
    event_id,
    txn_id,
    name_tracker,
    v_tracker,
    v_collector,
    v_etl,
    user_id,
    user_ipaddress,
    user_fingerprint,
    domain_userid,
    domain_sessionidx,
    network_userid,
    geo_country,
    geo_region,
    geo_city,
    geo_zipcode,
    geo_latitude,
    geo_longitude,
    geo_region_name,
    ip_isp,
    ip_organization,
    ip_domain,
    ip_netspeed,
    page_url,
    page_title,
    page_referrer,
    page_urlscheme,
    page_urlhost,
    page_urlport,
    page_urlpath,
    page_urlquery,
    page_urlfragment,
    refr_urlscheme,
    refr_urlhost,
    refr_urlport,
    refr_urlpath,
    refr_urlquery,
    refr_urlfragment,
    refr_medium,
    refr_source,
    refr_term,
    mkt_medium,
    mkt_source,
    mkt_term,
    mkt_content,
    mkt_campaign,
    se_category,
    se_action,
    se_label,
    se_property,
    se_value,
    tr_orderid,
    tr_affiliation,
    tr_total,
    tr_tax,
    tr_shipping,
    tr_city,
    tr_state,
    tr_country,
    ti_orderid,
    ti_sku,
    ti_name,
    ti_category,
    ti_price,
    ti_quantity,
    pp_xoffset_min,
    pp_xoffset_max,
    pp_yoffset_min,
    pp_yoffset_max,
    useragent,
    br_name,
    br_family,
    br_version,
    br_type,
    br_renderengine,
    br_lang,
    br_features_pdf,
    br_features_flash,
    br_features_java,
    br_features_director,
    br_features_quicktime,
    br_features_realplayer,
    br_features_windowsmedia,
    br_features_gears,
    br_features_silverlight,
    br_cookies,
    br_colordepth,
    br_viewwidth,
    br_viewheight,
    os_name,
    os_family,
    os_manufacturer,
    os_timezone,
    dvce_type,
    dvce_ismobile,
    dvce_screenwidth,
    dvce_screenheight,
    doc_charset,
    doc_width,
    doc_height,
    tr_currency,
    tr_total_base,
    tr_tax_base,
    tr_shipping_base,
    ti_currency,
    ti_price_base,
    base_currency,
    geo_timezone,
    mkt_clickid,
    mkt_network,
    etl_tags,
    dvce_sent_tstamp,
    refr_domain_userid,
    refr_dvce_tstamp,
    domain_sessionid,
    derived_tstamp,
    event_vendor,
    event_name,
    event_format,
    event_version,
    event_fingerprint,
    true_tstamp,
    load_tstamp,
    contexts_com_snowplowanalytics_snowplow_web_page_1,
    object_construct('cls', cls, 'fcp', fcp, 'fid', fid, 'inp', inp, 'lcp', lcp, 'navigationType', navigationType, 'ttfb', ttfb) as unstruct_event_com_snowplowanalytics_snowplow_web_vitals_1,
    parse_json('[{"deviceClass":"'||deviceClass||'", "agentClass":"'||agentClass||'", "agentName":"'||agentName||'", "agentNameVersion":"'||agentNameVersion||'", "agentNameVersionMajor":"'||agentNameVersionMajor||'", "agentVersion":"'||agentVersion||'", "agentVersionMajor":"'||agentVersionMajor||'", "deviceBrand":"'||deviceBrand||'", "deviceName":"'||deviceName||'", "deviceVersion":"'||deviceVersion||'", "layoutEngineClass":"'||layoutEngineClass||'", "layoutEngineName":"'||layoutEngineName||'", "layoutEngineNameVersion":"'||layoutEngineNameVersion||'", "layoutEngineNameVersionMajor":"'||layoutEngineNameVersionMajor||'", "layoutEngineVersion":"'||layoutEngineVersion||'", "layoutEngineVersionMajor":"'||layoutEngineVersionMajor||'", "operatingSystemClass":"'||operatingSystemClass||'", "operatingSystemName":"'||operatingSystemName||'", "operatingSystemNameVersion":"'||operatingSystemNameVersion||'", "operatingSystemVersion":"'||operatingSystemVersion||'"}]') as contexts_nl_basjes_yauaa_context_1,
    parse_json('[{"category":"'||category||'", "primaryImpact":"'||primaryImpact||'", "reason":"'||reason||'", "spiderOrRobot":"'||spiderOrRobot||'"}]') as contexts_com_iab_snowplow_spiders_and_robots_1

from flatten

{% endif %}
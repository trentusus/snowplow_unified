
  
    



    create or replace table `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_new_event_limits`
      
    
    

    OPTIONS()
    as (
      


      select 
        cast('2023-12-22' as timestamp)
     as lower_limit,
              least(
        cast('2023-12-22 12:55:06.094000+00:00' as timestamp)
    ,
              
    timestamp_add(
        cast('2023-12-22' as timestamp)
    , interval 90 day)
) as upper_limit
    
    );
  
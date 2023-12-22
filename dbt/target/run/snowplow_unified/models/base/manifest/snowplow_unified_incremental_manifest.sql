-- back compat for old kwarg name
  
  
        
    

    



    merge into `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_incremental_manifest` as DBT_INTERNAL_DEST
        using (






        with prep as (
        select
            cast(null as 
    string
) model,
            cast('1970-01-01' as timestamp) as last_success
        )

        select *

        from prep
        where false
    
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`model`, `last_success`)
    values
        (`model`, `last_success`)


    
-- back compat for old kwarg name
  
  
        
    

    



    merge into `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_base_quarantined_sessions` as DBT_INTERNAL_DEST
        using (






        with prep as (
        select
            
                cast(null as 
    string
) session_identifier
            
        )

        select *

        from prep
        where false

    
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`session_identifier`)
    values
        (`session_identifier`)


    

    
    



select session_identifier
from `com-snplow-sales-gcp`.`scratch`.`snowplow_unified_sessions_this_run`
where session_identifier is null



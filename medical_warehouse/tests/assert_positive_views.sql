-- Custom test: Ensure view counts are non-negative
-- This test should return 0 rows to pass

select 
    message_id,
    channel_name,
    views
from {{ ref('stg_telegram_messages') }}
where views < 0

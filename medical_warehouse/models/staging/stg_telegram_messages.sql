{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'telegram_messages') }}
),

cleaned as (
    select
        message_id,
        channel_name,
        -- Cast message_date to timestamp
        case 
            when message_date is not null then message_date::timestamp
            else null
        end as message_date,
        -- Clean message text
        trim(coalesce(message_text, '')) as message_text,
        -- Calculate message length
        length(trim(coalesce(message_text, ''))) as message_length,
        -- Standardize boolean
        coalesce(has_media, false) as has_media,
        -- Clean image path
        case 
            when image_path is not null and trim(image_path) != '' then trim(image_path)
            else null
        end as image_path,
        -- Ensure views and forwards are non-negative
        greatest(coalesce(views, 0), 0) as views,
        greatest(coalesce(forwards, 0), 0) as forwards,
        loaded_at
    from source
    -- Filter out invalid records
    where message_id is not null
        and channel_name is not null
        and trim(channel_name) != ''
)

select * from cleaned

{{
    config(
        materialized='table'
    )
}}

with staging as (
    select * from {{ ref('stg_telegram_messages') }}
),

dim_channels as (
    select * from {{ ref('dim_channels') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
)

select
    s.message_id,
    dc.channel_key,
    dd.date_key,
    s.message_text,
    s.message_length,
    s.views as view_count,
    s.forwards as forward_count,
    s.has_media as has_image
from staging s
inner join dim_channels dc on s.channel_name = dc.channel_name
inner join dim_dates dd on date(s.message_date) = dd.full_date
where s.message_date is not null

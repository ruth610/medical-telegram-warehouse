{{
    config(
        materialized='table'
    )
}}

with staging as (
    select * from {{ ref('stg_telegram_messages') }}
),

channel_stats as (
    select
        channel_name,
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(views) as avg_views
    from staging
    group by channel_name
),

channel_types as (
    select
        channel_name,
        case
            when lower(channel_name) like '%pharma%' or lower(channel_name) like '%drug%' then 'Pharmaceutical'
            when lower(channel_name) like '%cosmetic%' or lower(channel_name) like '%beauty%' then 'Cosmetics'
            else 'Medical'
        end as channel_type
    from channel_stats
)

select
    md5(cs.channel_name)::uuid as channel_key,
    cs.channel_name,
    ct.channel_type,
    cs.first_post_date,
    cs.last_post_date,
    cs.total_posts,
    round(cs.avg_views::numeric, 2) as avg_views
from channel_stats cs
left join channel_types ct on cs.channel_name = ct.channel_name

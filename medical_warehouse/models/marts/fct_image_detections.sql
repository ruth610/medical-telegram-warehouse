{{
    config(
        materialized='table'
    )
}}

with yolo_results as (
    select * from {{ source('raw', 'yolo_detections') }}
),

fct_messages as (
    select * from {{ ref('fct_messages') }}
),

dim_channels as (
    select * from {{ ref('dim_channels') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

enriched as (
    select
        yolo.detection_id,
        yolo.message_id,
        fm.channel_key,
        fm.date_key,
        yolo.detected_class,
        yolo.confidence_score,
        yolo.image_category
    from yolo_results yolo
    inner join fct_messages fm on yolo.message_id = fm.message_id
)

select * from enriched

{{
    config(
        materialized='table'
    )
}}

with date_series as (
    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as full_date
),

date_dimension as (
    select
        full_date,
        extract(dow from full_date) + 1 as day_of_week,
        to_char(full_date, 'Day') as day_name,
        extract(week from full_date) as week_of_year,
        extract(month from full_date) as month,
        to_char(full_date, 'Month') as month_name,
        extract(quarter from full_date) as quarter,
        extract(year from full_date) as year,
        case 
            when extract(dow from full_date) in (0, 6) then true
            else false
        end as is_weekend
    from date_series
)

select
    md5(full_date::text)::uuid as date_key,
    full_date,
    day_of_week::integer,
    trim(day_name) as day_name,
    week_of_year::integer,
    month::integer,
    trim(month_name) as month_name,
    quarter::integer,
    year::integer,
    is_weekend
from date_dimension

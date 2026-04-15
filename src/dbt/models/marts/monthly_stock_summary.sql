{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(bank_id, year, month)'
    )
}}

select
    fp.bank_id,
    dd.year,
    dd.month,
    avg(fp.open_price)  as avg_open_price,
    avg(fp.close_price) as avg_close_price,
    avg(fp.volume)      as avg_volume
from {{ ref('fact_price') }} fp
inner join {{ ref('dim_date') }} dd on fp.date_id = dd.date_id
where dd.year in (2024, 2025)
group by fp.bank_id, dd.year, dd.month

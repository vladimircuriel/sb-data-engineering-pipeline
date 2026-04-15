{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='bank_id'
    )
}}

select
    ticker          as bank_id,
    industry,
    sector,
    employees       as employee_count,
    city,
    phone,
    state,
    country,
    website,
    address
from {{ ref('stg_company') }}

{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(bank_id, date_id, holder_id)'
    )
}}

select
    ticker                                    as bank_id,
    toInt32(formatDateTime(date, '%Y%m%d'))   as date_id,
    holder                                     as holder_id,
    shares,
    value
from {{ ref('stg_holders') }}

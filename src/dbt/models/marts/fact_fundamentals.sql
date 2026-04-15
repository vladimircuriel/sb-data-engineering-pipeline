{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(bank_id, date_id)',
        partition_by='toYear(toDate(toString(date_id)))'
    )
}}

select
    ticker                                    as bank_id,
    toInt32(formatDateTime(date, '%Y%m%d'))   as date_id,
    assets,
    debt,
    assets - debt                             as invested_capital,
    shares                                    as share_issued
from {{ ref('stg_fundamentals') }}

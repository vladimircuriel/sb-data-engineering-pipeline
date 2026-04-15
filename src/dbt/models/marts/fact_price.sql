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
    open                                       as open_price,
    high                                       as high_price,
    low                                        as low_price,
    close                                      as close_price,
    volume
from {{ ref('stg_prices') }}

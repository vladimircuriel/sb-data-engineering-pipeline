{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(bank_id, date_id, agency_id)'
    )
}}

select
    ticker                                    as bank_id,
    toInt32(formatDateTime(date, '%Y%m%d'))   as date_id,
    firm                                       as agency_id,
    to_grade,
    from_grade,
    action
from {{ ref('stg_recommendations') }}

{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='holder_id'
    )
}}

select
    holder        as holder_id,
    holder        as name,
    ''            as holder_type
from {{ ref('stg_holders') }}
group by holder

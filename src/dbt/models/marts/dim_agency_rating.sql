{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='agency_id'
    )
}}

select
    firm  as agency_id,
    firm  as agency_name
from {{ ref('stg_recommendations') }}
group by firm

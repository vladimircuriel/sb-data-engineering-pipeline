select
    ticker,
    toDate(date)    as date,
    firm,
    to_grade,
    from_grade,
    action
from {{ source('landing', 'yfinance_recommendations') }}
where _airbyte_meta not like '%"errors"%'

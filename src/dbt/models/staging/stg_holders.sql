select
    ticker,
    holder,
    toDate(date)                                    as date,
    shares,
    CAST(value AS Nullable(Decimal(18, 2)))         as value
from {{ source('landing', 'yfinance_holders') }}
where _airbyte_meta not like '%"errors"%'

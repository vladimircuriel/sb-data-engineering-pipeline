select
    ticker,
    toDate(date)                                    as date,
    CAST(assets AS Nullable(Decimal(18, 2)))        as assets,
    CAST(debt   AS Nullable(Decimal(18, 2)))        as debt,
    shares
from {{ source('landing', 'yfinance_fundamentals') }}
where _airbyte_meta not like '%"errors"%'

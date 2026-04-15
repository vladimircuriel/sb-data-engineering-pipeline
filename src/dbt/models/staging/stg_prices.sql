select
    ticker,
    toDate(date)                                    as date,
    CAST(open  AS Nullable(Decimal(18, 4)))         as open,
    CAST(high  AS Nullable(Decimal(18, 4)))         as high,
    CAST(low   AS Nullable(Decimal(18, 4)))         as low,
    CAST(close AS Nullable(Decimal(18, 4)))         as close,
    volume
from {{ source('landing', 'yfinance_prices') }}
where _airbyte_meta not like '%"errors"%'

select
    ticker,
    industry,
    sector,
    employees,
    city,
    phone,
    state,
    country,
    website,
    address
from {{ source('landing', 'yfinance_company') }}
where _airbyte_meta not like '%"errors"%'

{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='date_id'
    )
}}

select
    toInt32(formatDateTime(date, '%Y%m%d'))  as date_id,
    date                                      as full_date,
    toDayOfMonth(date)                        as day,
    toDayOfYear(date)                         as day_year,
    toDayOfWeek(date)                         as day_week,
    toISOWeek(date)                           as week_number,
    toMonth(date)                             as month,
    toYear(date)                              as year,
    toQuarter(date)                           as quarter,
    toUInt8(0)                                as is_holiday
from (
    select toDate('1990-01-01') + number as date
    from numbers(toUInt64(dateDiff('day', toDate('1990-01-01'), toDate('2050-12-31'))))
)

{#
  The window comes from a dbt var, never the wall clock. Every recency metric in
  gold joins to is_window_end.
#}
with days as (
    select generate_series(
        '{{ var("data_window_start") }}'::date,
        '{{ var("data_window_end") }}'::date,
        interval '1 day'
    )::date as date_day
)

select
    date_day,
    extract(year   from date_day)::integer  as calendar_year,
    extract(month  from date_day)::integer  as calendar_month,
    extract(day    from date_day)::integer  as calendar_day,
    extract(isodow from date_day)::integer  as iso_day_of_week,
    to_char(date_day, 'YYYY-MM')            as year_month,
    to_char(date_day, 'Day')                as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend,
    date_day = '{{ var("data_window_end") }}'::date as is_window_end,
    ('{{ var("data_window_end") }}'::date - date_day)::integer as days_before_window_end
from days

with source as (

    select * from {{ source('taxi_dw', 'dim_date') }}

)

select
    date_key,
    full_date,
    year_num,
    quarter_num,
    month_num,
    day_of_month,
    day_of_week,
    day_name,
    week_of_year,
    dw_loaded_at

from source

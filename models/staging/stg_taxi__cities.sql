with source as (

    select * from {{ source('taxi_dw', 'dim_city') }}

)

select
    city_key,
    city_name,
    state_code,
    dw_loaded_at

from source

with source as (

    select * from {{ source('taxi_dw', 'dim_zipcode') }}

)

select
    zipcode_key,
    zipcode,
    city_key,
    dw_loaded_at

from source

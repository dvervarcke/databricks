with source as (

    select * from {{ source('taxi_dw', 'fact_taxi_rides') }}

),

renamed as (

    select
        ride_key,
        pickup_ts,
        dropoff_ts,
        pickup_date_key,
        dropoff_date_key,
        pickup_zipcode_key,
        dropoff_zipcode_key,
        fare_amount,
        dw_loaded_at

    from source

)

select * from renamed

with rides as (

    select * from {{ ref('stg_taxi__rides') }}

),

zipcodes as (

    select * from {{ ref('stg_taxi__zipcodes') }}

),

cities as (

    select * from {{ ref('stg_taxi__cities') }}

),

dates as (

    select * from {{ ref('stg_taxi__dates') }}

),

final as (

    select
        rides.ride_key,
        rides.pickup_ts,
        rides.dropoff_ts,

        -- pickup location
        pickup_zip.zipcode      as pickup_zipcode,
        pickup_city.city_name   as pickup_city,
        pickup_city.state_code  as pickup_state,

        -- dropoff location
        dropoff_zip.zipcode     as dropoff_zipcode,
        dropoff_city.city_name  as dropoff_city,
        dropoff_city.state_code as dropoff_state,

        -- pickup date attributes
        pickup_date.full_date   as pickup_date,
        pickup_date.year_num    as pickup_year,
        pickup_date.month_num   as pickup_month,
        pickup_date.day_name    as pickup_day_name,

        -- measures
        rides.fare_amount,
        unix_timestamp(rides.dropoff_ts) - unix_timestamp(rides.pickup_ts)
            as trip_duration_seconds

    from rides

    left join zipcodes as pickup_zip
        on rides.pickup_zipcode_key = pickup_zip.zipcode_key
    left join cities as pickup_city
        on pickup_zip.city_key = pickup_city.city_key

    left join zipcodes as dropoff_zip
        on rides.dropoff_zipcode_key = dropoff_zip.zipcode_key
    left join cities as dropoff_city
        on dropoff_zip.city_key = dropoff_city.city_key

    left join dates as pickup_date
        on rides.pickup_date_key = pickup_date.date_key

)

select * from final

with fct as (

    select * from {{ ref('fct_rides') }}

),

final as (

    select
        pickup_date,
        pickup_city,
        pickup_state,

        count(*)                     as ride_count,
        sum(fare_amount)             as total_fare,
        round(avg(fare_amount), 2)   as avg_fare,
        round(avg(trip_duration_seconds), 0) as avg_trip_seconds

    from fct
    group by
        pickup_date,
        pickup_city,
        pickup_state

)

select * from final

WITH stg_flight_booking AS (
    SELECT
        departure_date,
        COUNT(trip_id) as total_booking,
        SUM(price) as total_price
    FROM {{ ref("stg_pactravel-dwh_flight_bookings") }}
    GROUP BY departure_date
    ORDER BY departure_date
),

dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

final_fct_total_flight_booking AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["sfb.departure_date"] ) }} as fct_total_flight_booking_id,
        dd.date_id as departure_date,
        sfb.total_booking,
        sfb.total_price,
        {{ dbt_date.now() }} as created_at
    FROM stg_flight_booking sfb
    JOIN dim_date dd
    ON dd.date_actual = sfb.departure_date
)

SELECT * FROM final_fct_total_flight_booking
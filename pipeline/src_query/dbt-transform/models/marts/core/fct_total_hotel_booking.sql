WITH stg_hotel_booking AS (
    SELECT
        check_in_date,
        COUNT(trip_id) as total_booking,
        SUM(price) as total_price
    FROM {{ ref("stg_pactravel-dwh_hotel_bookings") }}
    GROUP BY check_in_date
    ORDER BY check_in_date
),

dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

final_fct_total_hotel_booking AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key( ["shb.check_in_date"] ) }} as fct_total_hotel_booking_id,
        dd.date_id as check_in_date,
        shb.total_booking,
        shb.total_price,
        {{ dbt_date.now() }} as created_at
    FROM stg_hotel_booking shb
    JOIN dim_date dd
    ON dd.date_actual = shb.check_in_date
)

SELECT * FROM final_fct_total_hotel_booking
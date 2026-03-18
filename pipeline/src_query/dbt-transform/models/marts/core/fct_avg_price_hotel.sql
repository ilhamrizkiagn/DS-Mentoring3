WITH stg_hotel_booking AS (
    SELECT
        *
    FROM {{ ref("stg_pactravel-dwh_hotel_bookings") }}
),

price_overtime AS (
    SELECT
        check_in_date,
        AVG(price) AS avg_daily
    FROM {{ ref("stg_pactravel-dwh_hotel_bookings") }}
    GROUP BY check_in_date
),

price_running AS (
    SELECT
        *,
        ROUND(
            AVG(avg_daily) OVER(
                ORDER BY check_in_date
            ),2
        ) AS running_avg_price
    FROM price_overtime
),

dim_customer AS (
    SELECT *
    FROM {{ ref("dim_customer") }}
),

dim_hotel AS (
    SELECT *
    FROM {{ ref("dim_hotel") }}
),

dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

final_fct_avg_price_hotel AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["shb.trip_id"] ) }} as fct_avg_price_hotel_id,
        shb.trip_id,
        dc.customer_id,
        dh.hotel_id,
        dd.date_id AS check_in_date,
        dd2.date_id AS check_out_date,
        shb.price,
        pr.running_avg_price,
        {{ dbt_date.now() }} as created_at
    FROM stg_hotel_booking shb
    JOIN dim_customer dc
        ON dc.customer_nk = shb.customer_id
    JOIN dim_hotel dh
        ON dh.hotel_nk = shb.hotel_id
    JOIN dim_date dd
        ON dd.date_actual = shb.check_in_date
    JOIN dim_date dd2
        ON dd2.date_actual = shb.check_out_date
    JOIN price_running pr
        ON pr.check_in_date = shb.check_in_date
    ORDER BY 5
)

SELECT * FROM final_fct_avg_price_hotel
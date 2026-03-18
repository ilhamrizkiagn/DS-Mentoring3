WITH stg_flight_booking AS (
    SELECT
        *
    FROM {{ ref("stg_pactravel-dwh_flight_bookings") }}
),

price_overtime AS (
    SELECT
        departure_date,
		departure_time,
        AVG(price) AS avg_price_overtime
    FROM {{ ref("stg_pactravel-dwh_flight_bookings") }}
    GROUP BY departure_date, departure_time
),

price_running AS (
    SELECT
        *,
        ROUND(
            AVG(avg_price_overtime) OVER(
                ORDER BY departure_date, departure_time
            ),2
        ) AS running_avg_price
    FROM price_overtime
),

dim_customer AS (
    SELECT *
    FROM {{ ref("dim_customer") }}
),

dim_airline AS (
    SELECT *
    FROM {{ ref("dim_airline") }}
),

dim_airport AS (
    SELECT *
    FROM {{ ref("dim_airport") }}
),

dim_aircraft AS (
    SELECT *
    FROM {{ ref("dim_aircraft") }}
),

dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

dim_time AS (
    SELECT *
    FROM {{ ref("dim_time") }}
),
 
final_fct_avg_price_flight AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["sfb.trip_id", "sfb.flight_number", "sfb.seat_number"] ) }} as fct_avg_price_flight_id,
        sfb.trip_id,
        dc.customer_id,
        sfb.flight_number,
        dal.airline_id,
        dar.aircraft_id,
        dair.airport_id AS airport_src,
        dair2.airport_id AS airport_dst,
        dt.time_id AS departure_time,
        dd.date_id AS departure_date,
        sfb.flight_duration,
        sfb.travel_class,
        sfb.seat_number,
        sfb.price,
        pr.running_avg_price,
        {{ dbt_date.now() }} as created_at
    FROM stg_flight_booking sfb
    JOIN dim_customer dc
        ON dc.customer_nk = sfb.customer_id
    JOIN dim_aircraft dar
        ON dar.aircraft_nk = sfb.aircraft_id
    JOIN dim_airline dal
        ON dal.airline_nk = sfb.airline_id
    JOIN dim_airport dair
        ON dair.airport_nk = sfb.airport_src
    JOIN dim_airport dair2
        ON dair2.airport_nk = sfb.airport_dst
    JOIN dim_date dd
        ON dd.date_actual = sfb.departure_date
    JOIN dim_time dt
        ON dt.time_actual::time = sfb.departure_time::time
    JOIN price_running pr
        ON pr.departure_date = sfb.departure_date
        AND pr.departure_time = sfb.departure_time
    ORDER BY 10,9
)

SELECT * FROM final_fct_avg_price_flight
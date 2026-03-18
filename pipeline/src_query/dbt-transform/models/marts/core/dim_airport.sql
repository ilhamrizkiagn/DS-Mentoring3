WITH stg_airports AS (
    SELECT
        airport_id as airport_nk,
        airport_name,
        city,
        latitude,
        longitude
    FROM {{ ref("stg_pactravel-dwh_airports") }}
),

final_dim_airport AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["airport_nk"] ) }} as airport_id,
        *,
        {{ dbt_date.now() }} as created_at
    FROM stg_airports
)

SELECT * FROM final_dim_airport
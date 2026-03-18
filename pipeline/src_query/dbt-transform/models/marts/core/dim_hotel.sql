WITH stg_hotel AS (
    SELECT
        hotel_id as hotel_nk,
        hotel_name,
        hotel_address,
        city,
        country,
        hotel_score
    FROM {{ ref("stg_pactravel-dwh_hotel") }}
),

final_dim_hotel AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["hotel_nk"] ) }} as hotel_id,
        *,
        {{ dbt_date.now() }} as created_at
    FROM stg_hotel
)

SELECT * FROM final_dim_hotel
WITH stg_airlines AS (
    SELECT
        airline_id as airline_nk,
        airline_name,
        country,
        airline_iata,
        airline_icao,
        alias
    FROM {{ ref("stg_pactravel-dwh_airlines") }}
),

final_dim_airline AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["airline_nk"] ) }} as airline_id,
        *,
        {{ dbt_date.now() }} as created_at
    FROM stg_airlines
)

SELECT * FROM final_dim_airline
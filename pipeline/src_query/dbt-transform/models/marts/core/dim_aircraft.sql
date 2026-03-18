WITH stg_aircrafts AS (
    SELECT
        aircraft_id as aircraft_nk,
        aircraft_name,
        aircraft_iata,
        aircraft_icao
    FROM {{ ref("stg_pactravel-dwh_aircrafts") }}
),

final_dim_aircraft AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["aircraft_nk"] ) }} as aircraft_id,
        *,
        {{ dbt_date.now() }} as created_at
    FROM stg_aircrafts
)

SELECT * FROM final_dim_aircraft
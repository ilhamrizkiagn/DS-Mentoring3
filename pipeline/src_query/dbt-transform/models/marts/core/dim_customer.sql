WITH stg_customers AS (
    SELECT
        customer_id as customer_nk,
        customer_first_name,
        customer_family_name,
        customer_gender,
        customer_birth_date,
        customer_country,
        customer_phone_number
    FROM {{ ref("stg_pactravel-dwh_customers") }}
),

final_dim_customer AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key( ["customer_nk"] ) }} as customer_id,
        *,
        {{ dbt_date.now() }} as created_at
    FROM stg_customers
)

SELECT * FROM final_dim_customer
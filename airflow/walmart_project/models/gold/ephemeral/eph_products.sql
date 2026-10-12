SELECT
    DISTINCT
    product_id,
    product_name,
    product_created_timestamp,
    product_updated_timestamp,
    product_processed_at,
    category,
    price,
    brand,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
    FROM
    {{ ref('obt_b') }}

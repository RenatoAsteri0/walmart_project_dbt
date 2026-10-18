SELECT
    DISTINCT
    product_id,
    product_name,
    created_timestamp AS product_created_timestamp,
    updated_timestamp AS product_updated_timestamp,
    processed_at AS product_processed_at,
    category,
    price,
    brand,
    CURRENT_TIMESTAMP() AS product_gold_processed_at
FROM {{ ref('products_t') }}

SELECT 
    customer as fk_customer, 
    sku as sku_config, 
    updated_at as wishlist_updated_at,
    "wishlist" as product_mapping_type
FROM `namshi-analytics.DatalabsSelfServe.product_wishlist` 
WHERE updated_at >= "2023-04-01"
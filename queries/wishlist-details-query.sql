// Fetch data only required for relationships
SELECT 
    pw.customer as fk_customer,
    pw.sku as sku_config,
    pw.updated_at as wishlist_updated_at,
    "wishlist" as product_mapping_type,
    d.brand_clean as brand, 
    d.name,
    d.activated_at, 
    d.season, 
    d.color, 
    d.department, 
    d.category_clean as category, 
    d.subcategory_clean as subcategory,
    d.gender_clean as product_gender, 
    d.age_group, 
    d.price, 
    d.price_level, 
    d.merch_type, 
    d.occasion, 
    d.short_description, 
    d.namshi_description, 
    d.special_type, 
    d.world_tag,
    c.country_name,
    c.gender,
    c.generation,
    c.RPC_gross_per_month,
    c.RPC_net_per_month,
    c.TPC_per_month,
    
FROM 
    `namshi-analytics.DatalabsSelfServe.product_wishlist` pw

LEFT JOIN
    `namshi-analytics.DatalabsSelfServe.customer_d` c
    ON pw.customer = c.id_customer

LEFT JOIN `namshi-analytics.DatalabsSelfServe.item_d` d
    ON pw.sku = d.sku_config
    
WHERE 
    pw.updated_at >= '2023-03-01'
ORDER BY pw.customer,pw.sku, pw.updated_at desc
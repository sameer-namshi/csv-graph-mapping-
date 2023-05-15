// Fetch data required only for relationships
SELECT 
    i.sku_config,
    i.fk_customer, 
    i.order_date_key,
    i.order_nr,
    i.discount_value as sale_price,
    "purchase_history" as product_mapping_type,
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
    c.id_customer,
    c.country_name,
    c.gender,
    c.generation,
    c.RPC_gross_per_month,
    c.RPC_net_per_month,
    c.TPC_per_month,
FROM 
    `namshi-analytics.DatalabsSelfServe.item_sales` i

LEFT JOIN
    `namshi-analytics.DatalabsSelfServe.customer_d` c
    ON i.fk_customer = c.id_customer

LEFT JOIN `namshi-analytics.DatalabsSelfServe.item_d` d
    ON i.sku = d.sku

WHERE 
    i.order_date_key >= '2023-04-01'
    AND i.city = "Dubai"
    AND i.country = "AE"
ORDER BY i.fk_customer,i.sku_config, i.order_date_key desc
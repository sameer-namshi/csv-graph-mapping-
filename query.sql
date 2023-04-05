SELECT 
    i.sku_config,
    i.fk_customer, 
    i.order_date_key,
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
    c.gender as customer_gender

FROM 
    `namshi-analytics.DatalabsSelfServe.item_sales` i
JOIN 
    `namshi-analytics.DatalabsSelfServe.item_d` d 
ON 
    i.sku_config = d.sku_config
JOIN 
    `namshi-analytics.DatalabsSelfServe.customer_d` c
ON 
    i.fk_customer = c.id_customer
WHERE 
    i.order_date_key >= '2023-01-01' 
    AND i.order_date_key < '2023-03-01'
    AND i.country = 'AE' 
    AND i.city = 'Dubai' 
    AND i.status_name = 'delivered'
 

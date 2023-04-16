SELECT 
    i.sku_config,
    i.fk_customer, 
    i.order_date_key,
    i.order_nr,
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
    c.gender as customer_gender,
    c.generation as customer_generation,
    c.country_name as customer_country_name,
    c.RPC_gross_per_month,
    c.RPC_net_per_month,
    c.TPC_per_month,
    pw.customer as wishlist_customer,
    pw.sku as wishlist_sku,
    pw.updated_at as wishlist_updated_at,
    wid.brand_clean as wl_p_brand, 
    wid.name as wl_p_name,
    wid.activated_at as wl_p_activated_at, 
    wid.season as wl_p_season, 
    wid.color as wl_p_color, 
    wid.department as wl_p_department, 
    wid.category_clean as wl_p_category, 
    wid.subcategory_clean as wl_p_subcategory,
    wid.gender_clean as wl_p_product_gender, 
    wid.age_group as wl_p_age_group, 
    wid.price as wl_p_price , 
    wid.price_level as wl_p_price_level , 
    wid.merch_type as wl_p_merch_type, 
    wid.occasion as wl_p_occasion, 
    wid.short_description as wl_p_short_description, 
    wid.namshi_description as wl_p_namshi_description, 
    wid.special_type as wl_p_special_type

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
JOIN
  `namshi-analytics.DatalabsSelfServe.product_wishlist` pw
ON 
  i.fk_customer = pw.customer
  AND pw.updated_at >= '2023-04-01'
JOIN 
    `namshi-analytics.DatalabsSelfServe.item_d` wid
ON 
    pw.sku = wid.sku_config
WHERE 
    i.order_date_key >= '2023-04-01'
    AND i.country = 'AE' 
    AND i.city = 'Dubai' 
    AND i.status_name = 'delivered'
ORDER BY i.fk_customer, i.order_date_key, i.order_nr desc
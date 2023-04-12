SELECT 
    i.sku_config,
    i.fk_customer, 
    i.order_date_key,
    i.order_nr,
    pw.customer as wishlist_customer,
    pw.sku as wishlist_sku,
    pw.updated_at as wishlist_updated_at,
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
    cust.gender as wishlist_customer_gender,
    cust.generation as wishlist_customer_generation,
    cust.country_name as wishlist_customer_country,
    cust.RPC_gross_per_month as wishlist_rpc_gross,
    cust.RPC_net_per_month as wishlist_rpc_net,
    cust.TPC_per_month as wishlist_tpc

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
  i.sku_config = pw.sku
  AND pw.updated_at >= '2023-04-01'
JOIN 
    `namshi-analytics.DatalabsSelfServe.customer_d` cust
ON 
    pw.customer = cust.id_customer
WHERE 
    i.order_date_key >= '2023-04-01'
    AND i.country = 'AE' 
    AND i.city = 'Dubai' 
    AND i.status_name = 'delivered'
ORDER BY i.order_date_key desc
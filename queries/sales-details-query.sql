// Fetch data required only for relationships
SELECT 
    i.sku,
    i.sku_config,
    i.id_sales_order_item,
    i.fk_customer, 
    i.order_date_key,
    i.order_nr,
    i.unit_price as sale_price,
    "purchase_history" as product_mapping_type,
    d.brand_clean as brand, 
    d.name,
    d.activated_at, 
    d.size, 
    d.units_all_ksa as ksa_stocks, 
    d.units_all_uae as uae_stocks, 
    d.is_visible, 
    d.country_visibility_ae as ae_visibility, 
    d.country_visibility_bh as bh_visibility, 
    d.country_visibility_kw as kw_visibility, 
    d.country_visibility_om as om_visibility, 
    d.country_visibility_qa as qa_visibility, 
    d.country_visibility_sa as sa_visibility, 
    d.country_visibility_us as us_visibility,
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
    c.m0_flag,						
    c.m0006_flag,						
    c.m0612_flag,						
    c.m1218_flag,
    c.last_purchased,
    c.last_session_date
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
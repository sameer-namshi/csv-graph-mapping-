SELECT
    d.sku, 
    d.sku_config,
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
    d.world_tag
FROM 
    `namshi-analytics.DatalabsSelfServe.item_d` d
WHERE d.sku is not null 
AND d.status != "deleted" 
AND if_activated = 1
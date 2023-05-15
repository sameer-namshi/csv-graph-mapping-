SELECT 
    order_date_key, 
    hour, 
    order_nr, 
    country, 
    city, 
    fk_customer, 
    sku, 
    sku_config, 
    id_sales_order_item, 
    unit_price, 
    items_gross,
    "purchase_history" as product_mapping_type
FROM `namshi-analytics.DatalabsSelfServe.item_sales`
WHERE order_date_key >= "2023-04-01"
ORDER BY order_date_key DESC, hour DESC

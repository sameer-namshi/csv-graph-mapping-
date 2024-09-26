// Find the top 3 most frequently purchased price levels by the customer with ID '5654988'
MATCH (customer:Customer{id:$customerId})-[:BOUGHT_PRICE_LEVEL]->(priceLevel:PriceLevel)
WITH priceLevel, count(*) AS priceLevelCount
ORDER BY priceLevelCount DESC
LIMIT 3
WITH collect(priceLevel) as topPriceLevels

// Find the top 3 most frequently purchased brands by the customer with ID $customerId, in combination with the top price levels
MATCH (customer:Customer{id:$customerId})-[:LIKES]->(brand:Brand)
WITH brand, topPriceLevels, count(*) AS brandCount
ORDER BY brandCount DESC
LIMIT 3
WITH collect(brand) as topBrands, topPriceLevels

// Find the top 3 most frequently purchased subcategories by the customer with ID $customerId, in combination with the top price levels and brands
MATCH (customer:Customer{id:$customerId})-[:INTERESTED_IN]->(subcategory:Subcategory)
WITH topBrands, topPriceLevels, subcategory, count(*) AS subcategoryCount
ORDER BY subcategoryCount DESC
LIMIT 3
WITH collect(subcategory) as topSubcategories, topBrands, topPriceLevels

// For each combination of top price level, brand, and subcategory, find all products that have not been bought by the customer with ID $customerId
UNWIND topPriceLevels AS currentPriceLevel
UNWIND topBrands AS currentBrand
UNWIND topSubcategories AS currentSubcategory

MATCH (product:Product)-[:IN_BRAND]->(currentBrand)
MATCH (product)-[:IN_PRICE_LEVEL]->(currentPriceLevel)
MATCH (product)-[:IN_SUBCATEGORY]->(currentSubcategory)
MATCH (product)-[:HAS_COLOR]->(color:Color)
MATCH (product)-[:FOR_OCCASION]->(occasion:Occasion)

WHERE NOT (:Customer{id:$customerId})-[:BOUGHT]->(product)

// Return the SKU, name, gender, subcategory, color, brand, occasion, and price level of each product that meets the above conditions
RETURN DISTINCT product.sku_config as sku , product.name AS name, product.product_gender as product_gender, currentSubcategory.name AS subcategory, color.name AS color, currentBrand.name AS brand, occasion.name AS occasion, currentPriceLevel.name AS price_level
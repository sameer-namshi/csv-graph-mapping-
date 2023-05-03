// Basic Version of Recommended For You based on purchase history and wishlist products
// This is a modulation to the existing similar purchase history cypher which returns product recommendations based on purchase history
// We have now combined the result with the wishlist products and provide better recommendations
// We analyse the purchase history and the wishlist of the customer and get the top 3 favourite brands, subcategory and price level of customers and recommend products based on the same
============================================================================

// Find the top 3 most frequently purchased price levels by the customer with ID '5654988'
//147944
//417232 , 58788
CALL {
    MATCH (customer:Customer{id:"147944"})<-[:IN_WISHLIST]-(:Product)-[:IN_PRICE_LEVEL]->(priceLevel:PriceLevel)
    RETURN priceLevel, count(*) AS priceLevelCount
    UNION
    MATCH (customer:Customer{id:"147944"})<-[:BOUGHT]-(:Product)-[:IN_PRICE_LEVEL]->(priceLevel:PriceLevel)
    RETURN priceLevel, count(*) AS priceLevelCount
}
WITH priceLevel, priceLevelCount 
ORDER BY priceLevelCount desc
LIMIT 5
WITH collect(priceLevel) as topPriceLevels, priceLevelCount
CALL {
    MATCH (customer:Customer{id:"147944"})<-[:IN_WISHLIST]-(product:Product)-[:IN_BRAND]->(brand:Brand)
    RETURN brand, count(*) AS brandCount
    UNION
    MATCH (customer:Customer{id:"147944"})-[:BOUGHT]->(purchasedProduct:Product)-[:IN_BRAND]->(brand:Brand)
    RETURN brand, count(*) AS brandCount
}
WITH brand, brandCount,topPriceLevels
ORDER BY brandCount desc
LIMIT 5
WITH collect(brand) as topBrands, topPriceLevels

// Find the top 3 most frequently purchased subcategories by the customer with ID $customerId, in combination with the top price levels and brands
CALL {
    MATCH (customer:Customer{id:"147944"})<-[:IN_WISHLIST]-(product:Product)-[:IN_SUBCATEGORY]->(subcategory:Subcategory)
    RETURN subcategory, count(*) AS subcategoryCount
    UNION
    MATCH (customer:Customer{id:"147944"})-[:BOUGHT]->(purchasedProduct:Product)-[:IN_SUBCATEGORY]->(subcategory:Subcategory)
    RETURN subcategory, count(*) AS subcategoryCount
 
}
WITH subcategory, count(*) AS subcategoryCount, topBrands, topPriceLevels
ORDER BY subcategoryCount DESC
LIMIT 5
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

WHERE NOT (:Customer{id:"147944"})-[:BOUGHT]->(product)

// Return the SKU, name, gender, subcategory, color, brand, occasion, and price level of each product that meets the above conditions
RETURN DISTINCT product.sku_config as sku , product.name AS name, product.product_gender as product_gender, currentSubcategory.name AS subcategory, color.name AS color, currentBrand.name AS brand, occasion.name AS occasion, currentPriceLevel.name AS price_level
Wishlist Recommendation
Provides recommendations to customer based on the products added in their wishlist
We get products from wishlist of various customers which match the category, subcategory and price level of the customer and get the most popular one of each subcategory
============================================================================

//Get products in wishlist
MATCH (p:Product)-[:IN_WISHLIST]->(c:Customer {id:"29899"})

//Get category, subcategory and price level the products in wishlist belongs to
MATCH (p)-[:IN_CATEGORY]->(category:Category)
MATCH (p)-[:IN_SUBCATEGORY]->(subcategory:Subcategory)
MATCH (p)-[:IN_PRICE_LEVEL]->(pl:PriceLevel)

// Get products from same category, subcategory, and price level that are added in the wishlist of other customers.
WITH p,category,c,subcategory,pl
MATCH (rp:Product)-[:IN_CATEGORY]->(category)
MATCH (rp:Product)-[:IN_SUBCATEGORY]->(subcategory)
MATCH (rp:Product)-[:IN_PRICE_LEVEL]->(pl)
WHERE (rp)-[:IN_WISHLIST]->()
AND NOT (rp)-[:IN_WISHLIST]->(c)

//Sort by popularity / Count of customers who have added the specific products in their wishlist
WITH collect(rp)[..1] AS rps , subcategory.name as subCatName, count(*) AS recommendedScore

UNWIND rps as recommendedProducts 
RETURN recommendedProducts.sku_config, recommendedProducts.name
ORDER BY recommendedScore desc
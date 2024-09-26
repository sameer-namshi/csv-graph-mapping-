MATCH (customer:Customer{id:$customerId})-[:BOUGHT_PRICE_LEVEL]->(priceLevel:PriceLevel)
WITH priceLevel, count(*) AS priceLevelCount
ORDER BY priceLevelCount DESC
LIMIT 3
RETURN priceLevel.name
MATCH (customer:Customer{id:$customerId})-[:BOUGHT]->(:Product)-[:BY_BRAND]->(brand:Brand)
WITH brand, count(*) AS brandCount
ORDER BY brandCount DESC
LIMIT 3
RETURN brand.name
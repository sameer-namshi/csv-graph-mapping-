MATCH (c:Customer)-[:BOUGHT]->(:Product)
WITH c, COUNT(*) AS totalBought
ORDER BY totalBought DESC
LIMIT 1
RETURN c, totalBought
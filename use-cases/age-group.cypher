MATCH (customer:Customer{id:$customerId})-[:BOUGHT]->(:Product)-[:FOR_AGE_GROUP]->(ag:AgeGroup)
WITH ag, count(*) AS agCount
ORDER BY agCount DESC
LIMIT 2
RETURN ag.name
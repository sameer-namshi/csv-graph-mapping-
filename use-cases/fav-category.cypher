MATCH (customer:Customer{id:$customerId})-[:BOUGHT]->(:Product)-[:IN_SUBCATEGORY]->(subcategory:Subcategory)
WITH subcategory, count(*) AS subcategoryCount
ORDER BY subcategoryCount DESC
LIMIT 3
RETURN subcategory.name
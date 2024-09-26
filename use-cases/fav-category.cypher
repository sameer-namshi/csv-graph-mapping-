MATCH (customer:Customer{id:$customerId})-[:INTERESTED_IN]->(subcategory:Subcategory)
WITH subcategory, count(*) AS subcategoryCount
ORDER BY subcategoryCount DESC
LIMIT 3
RETURN subcategory.name